// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef AMR_H
#define AMR_H

#include "a-memory-library/aml_alloc.h"
#include "a-memory-library/aml_pool.h"
#include "a-memory-library/aml_buffer.h"
#include "the-io-library/io.h"
#include "the-io-library/io_out.h"
#include "the-io-library/io_in.h"
#include "the-macro-library/macro_map.h"

#include "a-map-reduce-library/amr_common_datatypes.h"

#include <stdio.h>
#include <time.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * A-MAP-REDUCE (AMR) - CORE ARCHITECTURE & INTUITION
 * ========================================================================
 * AMR is a single-node, multi-core DAG (Directed Acyclic Graph) execution
 * engine for out-of-core data processing.
 *
 * 1. The Scheduler (amr_t): The global orchestrator. You allocate it, define
 * the total RAM/CPUs/Partitions, define your Tasks, and call amr_run.
 *
 * 2. The Task (amr_task_t): A logical step in the DAG (e.g., "count_words").
 * Tasks are defined in the MAIN thread during setup. A Task declares what
 * inputs it needs, what outputs it produces, and what function to run.
 *
 * 3. The Worker (amr_worker_t): The actual thread executing a slice of a Task.
 * If a Task has 16 partitions, AMR spawns 16 Workers. Workers open the
 * physical files, read records, and write outputs.
 *
 * ========================================================================
 * THE STATEFUL SETUP API (IMPORTANT!)
 * ========================================================================
 * When defining a task in its setup callback, the API is stateful.
 * Calling amr_task_output() or amr_task_input_...() sets the "current"
 * active port. Any subsequent configuration calls (like setting the format,
 * reducer, or sort parameters) apply ONLY to that most recently declared port.
 *
 * Example:
 * amr_task_output(t, "my_out.bin", 1.0);
 * amr_task_output_format(t, io_fixed(16));        // Applies to "my_out.bin"
 * amr_task_output_shuffle_by(t, "Hash_A", NULL);  // Applies to "my_out.bin"
 * ======================================================================== */

/* ========================================================================
 * UNDERSTANDING THE CORE I/O TYPES & CALLBACKS
 * ========================================================================
 * AMR relies on the underlying I/O library to process and compare records.
 * When using the raw `_with` modifiers (e.g., `amr_task_output_sort_with`),
 * you must provide callbacks matching these signatures.
 *
 * io_record_t: The standard wrapper for a parsed record.
 * - char *record: Pointer to the raw record bytes.
 * - uint32_t length: Length of the record in bytes.
 * - int32_t tag: Optional metadata tag.
 *
 * io_compare_cb: Used for sorting. Must return < 0, 0, or > 0.
 * typedef int (*io_compare_cb)(const io_record_t *a, const io_record_t *b, void *tag);
 *
 * io_partition_cb: Computes bucket indices. Must return 0 to (num_part - 1).
 * typedef size_t (*io_partition_cb)(const io_record_t *r, size_t num_part, void *tag);
 *
 * io_reducer_cb: Combines `num_r` identical-key records into `res`.
 * typedef bool (*io_reducer_cb)(io_record_t *res, const io_record_t *r,
 * size_t num_r, aml_buffer_t *bh, void *tag);
 * ======================================================================== */

/* ========================================================================
 * A-MAP-REDUCE (AMR) - COMMON TOPOLOGY PATTERNS
 * ========================================================================
 * The way you configure outputs and wire inputs dictates your data flow.
 * Here are the 5 most common distributed patterns:
 *
 * 1. THE FUNNEL / GLOBAL GATHER (M Partitions -> 1 Partition)
 * Use Case: Reduce distributed logs/metrics into a single global file.
 * - Producer (M parts) : amr_task_output(t, "data.bin", 1.0);
 * - Consumer (1 part)  : amr_task_input_from_task_all_to_all(t, "prod", "data.bin", 1.0);
 * Result: The single consumer automatically concatenates and reads the
 * files from all M upstream workers as one continuous stream.
 *
 * 2. 1-TO-1 PIPELINING (M Partitions -> M Partitions)
 * Use Case: Sequential processing (parse -> enrich -> filter) where data
 * doesn't need to cross worker boundaries.
 * - Producer (M parts) : amr_task_output(t, "data.bin", 1.0);
 * - Consumer (M parts) : amr_task_input_from_task_partition(t, "prod", "data.bin", 1.0);
 * Result: Consumer Worker X reads exclusively from Producer Worker X.
 * Perfect parallel isolation; zero network/shuffle overhead.
 *
 * 3. THE GLOBAL SHUFFLE (M Partitions -> N Partitions)
 * Use Case: Grouping data by a specific key (e.g., User ID) so all records
 * for the same user land on the exact same worker.
 * - Producer (M parts) : amr_task_output(t, "data.bin", 1.0);
 * amr_task_output_shuffle_by(t, "Hash_User", NULL);
 * - Consumer (N parts) : amr_task_input_from_task_shuffle(t, "prod", "data.bin", 1.0);
 * Result: The producer scatters its output into N hash buckets. Consumer X
 * gathers bucket X from all M producers.
 *
 * 4. MAP-SIDE JOIN / READ FIRST (M Partitions -> N Partitions)
 * Use Case: All upstream partitions output the exact same global state
 * (like a shared dictionary), or the upstream task only has 1 partition.
 * - Producer (M parts) : amr_task_output(t, "dict.bin", 1.0);
 * - Consumer (N parts) : amr_task_input_from_task_first(t, "prod", "dict.bin", 1.0);
 * Result: Every parallel consumer thread reads ONLY the file produced
 * by producer Partition 0, safely avoiding duplicate reads.
 *
 * 5. FULL CROSS JOIN / DISTRIBUTED BROADCAST (M Partitions -> N Partitions)
 * Use Case: Every worker needs to read the entirety of a partitioned dataset
 * (e.g., comparing every user against every other user, or matrix math).
 * - Producer (M parts) : amr_task_output(t, "data.bin", 1.0);
 * - Consumer (N parts) : amr_task_input_from_task_all_to_all(t, "prod", "data.bin", 1.0);
 * Result: Producer writes M individual files. Consumer spawns N workers.
 * EVERY consumer worker reads ALL M producer files.
 * ======================================================================== */

/* ========================================================================
 * TRANSFORM CHAINING & OUTPUT ROUTING
 * ========================================================================
 * A Task can execute a single transform or a chain of multiple transforms.
 * How you configure this dictates how the framework handles your outputs
 * (outs[0], outs[1], ... outs[N]).
 *
 * The outs[] ordering is defined by the order of names in the transform's
 * outp string (e.g., "tmp_pipe|error_log" => outs[0]=tmp_pipe, outs[1]=error_log).
 *
 * SCENARIO A: SINGLE TRANSFORM (The Default)
 * If your task only declares one transform, all outputs are treated equally.
 * outs[0], outs[1], etc., are all finalized, written to disk, and closed
 * when the transform completes.
 *
 * SCENARIO B: CHAINED TRANSFORMS (Pipelining)
 * If you declare multiple transforms sequentially in the same task, the
 * framework automatically routes data between them.
 * Terminology:
 * - 'intermediate output' refers to a task output artifact that is eligible
 * for GC after all consumers finish
 * - 'temporary files/chunks' refers to internal scratch used for
 * sort/merge/compression.
 *
 * 1. The Pipe (outs[0]): ONLY the first output (outs[0]) of the current
 * transform is piped forward as the input (ins[0]) to the next transform.
 * By default, the piped artifact is treated as an intermediate and is
 * garbage-collected after the downstream transform consumes it. Use
 * amr_keep_intermediate_files(h) to retain intermediates globally (debug),
 * or amr_task_output_keep(task) to retain a specific materialized output.
 *
 * 2. The Branches (outs[1..N]): All subsequent outputs are NOT piped.
 * They are treated as terminal outputs for that transform: they are fully
 * finalized (including merge/rename/ack as configured) when the transform
 * completes. This is ideal for branching off error logs or side-effects.
 *
 * THE "HIJACKED MERGE" OPTIMIZATION (Sorted Pipes):
 * If you apply `amr_task_output_sort_by` to outs[0] during a chained
 * transform, the framework performs a massive optimization. Instead of
 * writing a fully merged, sorted file to disk only to immediately read it
 * again, AMR merges the temporary sorted chunks *directly* into the RAM
 * of the downstream transform.
 * ======================================================================== */

/*
========================================================================
HELLO WORLD: MINIMUM WORKING DAG
========================================================================
A tiny example of a complete AMR application. It reads an input,
applies an identity transform (copies data), and writes to an output.

bool hello_setup(amr_task_t *t) {
    amr_task_input_files(t, "input.txt", 1.0, NULL);
    amr_task_output(t, "output.txt", 1.0);
    amr_task_default_runner(t);
    // Passing NULL for the runner uses the default identity copy
    amr_task_transform(t, "input.txt", "output.txt", NULL);
    return true;
}

int main(int argc, char **argv) {
    // 16 partitions, 4 CPUs, 1024 MB RAM
    amr_t *sched = amr_init(argc, argv, 16, 4, 1024);
    amr_task(sched, "hello_task", true, hello_setup);

    // amr_worker_complete provides built-in telemetry out of the box.
    // Check the return value to catch execution failures!
    if (!amr_run(sched, amr_worker_complete)) {
        fprintf(stderr, "DAG Execution Failed!\n");
        amr_destroy(sched);
        return 1;
    }

    amr_destroy(sched);
    return 0;
}
========================================================================
*/

/* ========================================================================
 * A-MAP-REDUCE (AMR) - USAGE & EXECUTION LIFECYCLE (GOTCHAS)
 * ========================================================================
 * 1. Setup Phase vs. CLI Parsing:
 * Task setup callbacks run BEFORE CLI arguments are parsed. Do NOT use
 * conditional logic in setup callbacks based on CLI flags. The DAG
 * topology must be defined statically.
 *
 * 2. Default Transforms:
 * If no transforms are explicitly declared for a task using the default
 * runner, AMR creates a default identity transform that copies all
 * inputs to all outputs.
 *
 * 3. Runner Constraints:
 * The standard amr_runner_cb (record-at-a-time) only supports a SINGLE
 * input. For multi-input tasks, you MUST use amr_task_io_transform()
 * or amr_task_group_transform().
 *
 * 4. Compression Semantics & Opaque Ports:
 * Compression is inferred automatically from file extensions (.gz, .lz4).
 * For Opaque Artifacts, standard I/O handles are NULL. Use
 * amr_worker_opaque_inputs() helpers.
 *
 * 5. Input Options Inheritance:
 * Inputs automatically inherit the format/sort/reducer options from their
 * producer outputs during graph wiring.
 *
 * 6. Thread-Safety & Concurrency:
 * Task `setup` callbacks are executed sequentially on the MAIN thread.
 * Worker `runner` callbacks execute CONCURRENTLY across multiple threads.
 * Treat `amr_task_t` metadata as strictly read-only inside workers.
 *
 * 7. Error Handling Philosophy (Fail-Fast vs. Recoverable):
 * Graph topology and configuration errors during setup are FATAL and trigger
 * `abort()`. Execution errors in custom `amr_worker_cb` tasks should be
 * reported by returning `false` to safely halt the DAG. Transform callbacks
 * (e.g., `amr_runner_cb`, `amr_io_runner_cb`) return `void` and must signal
 * failure via fail-fast (`abort()`) or custom state tracking.
 *
 * 8. Output Retention & Garbage Collection:
 * Intermediate task outputs are automatically GC'd (deleted) based on
 * downstream reference counting. Terminal outputs always survive.
 * Temporary sort-merge scratch files are strictly managed by the I/O layer.
 * ======================================================================== */

/* ========================================================================
 * A-MAP-REDUCE (AMR) - BUILT-IN CLI & CACHING MODEL
 * ========================================================================
 * AMR parses CLI arguments during amr_run(). To prevent configuration drift
 * between cached runs, custom args are persisted to `tasks/custom_args`.
 *
 * --- DAG Execution & Selection ---
 * -t, --task <tasks>      Run ONLY specific tasks/partitions (e.g. -t taskA:0,2).
 * -f, --force             Force selected tasks to run, ignoring the cache.
 * -o                      Only run selected tasks (ignore out-of-date upstream).
 * --run <N>               Isolate execution to tasks/run_<N>/. (Pre-scanned
 * so main() can access it early via amr_current_run()).
 * --new-args              Overwrite the persisted `custom_args` file.
 *
 * --- Inspection & Debugging (Note: These force cpus = 1) ---
 * -l, --list              Print DAG execution plan and exit.
 * -s, --show-files        Print plan AND resolved physical file paths.
 * -d, --dump <files>      Print human-readable contents of output files.
 * --sample <recs>:<parts> Randomly sample N records from M active partitions.
 * --debug <t:p> <dir>     Run a specific task partition in strict isolation.
 * --keep-files            Globally disable intermediate file garbage collection.
 *
 * --- Resource Overrides ---
 * -c, --cpus <N>          Override the number of CPU threads at runtime.
 * -r, --ram <MB>          Override the total RAM limit at runtime.
 *
 * --- Incremental Caching ("Make" Behavior) ---
 * AMR compares input file timestamps against internal `.ack` files. If inputs
 * are older than the ack file, the worker partition is silently skipped.
 * ======================================================================== */

/* --- Forward Declarations --- */
struct amr_s;
typedef struct amr_s amr_t;

struct amr_task_s;
typedef struct amr_task_s amr_task_t;

/* Opaque internals (Defined in amr_internal.h) */
struct amr_worker_input_s;
typedef struct amr_worker_input_s amr_worker_input_t;

struct amr_worker_output_s;
typedef struct amr_worker_output_s amr_worker_output_t;

struct amr_thread_s;
typedef struct amr_thread_s amr_thread_t;

struct amr_task_state_link_s;
typedef struct amr_task_state_link_s amr_task_state_link_t;

struct amr_worker_s;
typedef struct amr_worker_s amr_worker_t;


/* ========================================================================
 * WORKER RUNTIME CONTEXT & ACCESSORS
 * ========================================================================
 * The amr_worker_t is the opaque execution context passed to your runner
 * callbacks. It represents a single thread processing a single partition.
 * ======================================================================== */

/* --- Memory Management (Allocated by Framework) --- */

/* Context: [Runtime | Worker Thread]
   Survives the ENTIRE execution of this worker partition. Use it for
   allocating persistent state, lookup tables, or file paths needed across
   the whole stream. NEVER call aml_pool_clear() on this pool! */
aml_pool_t *amr_worker_pool(amr_worker_t *w);

/* Context: [Runtime | Worker Thread]
   Your scratch memory for inner loops. In standard amr_runner_cb, the
   framework clears this automatically at record boundaries, and at group
   boundaries in group runners. If writing custom amr_io_runner_cb callbacks,
   you MUST call aml_pool_clear() repeatedly to prevent OOM errors. */
aml_pool_t *amr_worker_scratch_pool(amr_worker_t *w);

/* Context: [Runtime | Worker Thread]
   A reusable byte buffer for constructing strings or binary records.
   Call aml_buffer_clear() before building and writing a new record. */
aml_buffer_t *amr_worker_buffer(amr_worker_t *w);

/* --- Execution State (Read-Only) --- */

/* Context: [Runtime | Worker Thread]
   Retrieves the custom arguments passed to the scheduler during init. */
void *amr_worker_custom_arg(const amr_worker_t *w);

/* Context: [Runtime | Worker Thread (Read-Only)]
   Retrieves the root scheduler object. Mutation during runtime is unsupported. */
amr_t *amr_worker_schedule(const amr_worker_t *w);

/* Context: [Runtime | Worker Thread]
   Retrieves the custom state initialized by amr_task_transform_data. */
void *amr_worker_transform_data(const amr_worker_t *w);

/* Context: [Runtime | Worker Thread]
   Retrieves the internal framework data pointer for this worker. */
void *amr_worker_data(const amr_worker_t *w);

/* Context: [Runtime | Worker Thread]
   The specific data slice ID this worker is handling (0 to num-1). */
size_t amr_worker_partition(const amr_worker_t *w);

/* Context: [Runtime | Worker Thread]
   Total number of partitions configured for the parent task. */
size_t amr_worker_num_partitions(const amr_worker_t *w);

/* Context: [Runtime | Worker Thread]
   The physical CPU thread ID currently running this worker. */
size_t amr_worker_thread_id(const amr_worker_t *w);

/* Context: [Runtime | Worker Thread]
   Returns true if the `--debug` isolated execution flag is active for this task. */
bool amr_worker_debug(const amr_worker_t *w);

/* --- Callback Types --- */

/* Context: [Setup | Main Thread]
   Called once per task to construct the DAG edges and configuration. */
typedef bool (*amr_task_cb)(amr_task_t *task);

/* Context: [Runtime | Worker Thread]
   The raw execution logic called by threaded workers. */
typedef bool (*amr_worker_cb)(amr_worker_t *w);

/* Context: [Runtime | Worker Thread]
   Data lifecycle hooks for custom structs needed by workers. */
typedef void *(*amr_worker_data_cb)(amr_worker_t *w);
typedef void (*amr_destroy_worker_data_cb)(amr_worker_t *w, void *d);

/* Context: [Runtime | Worker Thread]
   Dumper callback for `--dump` CLI inspection. */
typedef void (*amr_task_dump_cb)(amr_worker_t *w, io_record_t *r,
                                 aml_buffer_t *bh, void *arg);

/* A built-in dumper that assumes the record is a null-terminated string */
void amr_task_dump_text(amr_worker_t *w, io_record_t *r, aml_buffer_t *bh,
                        void *arg);

/* Context: [Runtime | Worker Thread]
   Standard stream runner: Takes one record in, writes to multiple out ports. */
typedef void (*amr_runner_cb)(amr_worker_t *w, io_record_t *r, io_out_t **out);

/* Context: [Runtime | Worker Thread]
   Grouped stream runner: Takes an array of records that share the same key. */
typedef void (*amr_group_runner_cb)(amr_worker_t *w, io_record_t *r,
                                    size_t num_r, io_out_t **out);

/* Context: [Runtime | Worker Thread]
   Low-level IO runner: Gives you raw access to the input/output handles. */
typedef void (*amr_io_runner_cb)(amr_worker_t *w, io_in_t **ins, size_t num_ins,
                                 io_out_t **outs, size_t num_outs);

/* Context: [Runtime | Worker Thread]
   Resolves external input files into the pipeline. */
typedef io_file_info_t *(*amr_worker_file_info_cb)(amr_worker_t *w,
                                                   size_t *num_files,
                                                   amr_worker_input_t *inp);

/* CLI parsers */
typedef int (*amr_parse_args_cb)(int argc, char **argv, void *arg);
typedef bool (*amr_finish_args_cb)(int argc, char **argv, void *arg);


/* ========================================================================
 * PHASE 1: SCHEDULER INITIALIZATION & CLI
 * ========================================================================
 * Called in main(). Prepares the framework before task definitions.
 * ======================================================================== */

/* Context: [Init | Main Thread]
 * Initialize the global scheduler.
 * num_partitions is your maximum concurrency level (e.g., 16).
 * cpus is the number of active threads processing partitions simultaneously.
 * ram is total system RAM in Megabytes available for the framework.
 */
amr_t *amr_init(int argc, char **args, size_t num_partitions,
                                size_t cpus, size_t ram);

/* Context: [Init | Main Thread]
 * Enables iterative DAG execution via the `--run <N>` CLI flag.
 * Forces outputs to be stored in tasks/run_<N>/.
 */
void amr_use_runs(amr_t *h);

/* Context: [Any Phase | Main Thread]
 * Returns the current run number (defaults to 0 if --run isn't specified). */
size_t amr_current_run(amr_t *h);

/* Context: [Any Phase | Main Thread]
 * Generate the exact file path for a file produced by a specific task/partition
 * in a specific run. Resolves internal suffix appending automatically.
 * LIFETIME: Returns an allocated string from the scheduler's pool. Do not free. */
char *amr_run_file_path(amr_t *h, size_t run, const char *task_name,
                        size_t partition, const char *file_base);

/* Context: [Any Phase | Main Thread]
 * Globally prevents AMR from deleting upstream task outputs across the
 * entire DAG. By default, AMR deletes a task's output once all downstream
 * consumers have finished reading it. */
void amr_keep_intermediate_files(amr_t *h);

/* Context: [Init | Main Thread]
 * Directory overrides */
void amr_ack_dir(amr_t *h, const char *ack_dir);
void amr_task_dir(amr_t *h, const char *task_dir);

/* Context: [Init | Main Thread]
 * CLI custom argument registration */
void amr_custom_args(amr_t *h, void (*custom_usage)(),
                             amr_parse_args_cb parse_args, amr_finish_args_cb finish_args,
                             void *arg);

/* Context: [Init | Main Thread]
 * Registers a new Task in the DAG. */
amr_task_t *amr_task(amr_t *h, const char *task_name,
                            bool partitioned, amr_task_cb setup);

/* Context: [Provided by AMR | Pass to amr_run]
 * Built-in hook to monitor task completion. Prints memory and timing metrics. */
bool amr_worker_complete(amr_worker_t *w);

/* Context: [Init | Main Thread]
 * Fires the DAG. Resolves dependencies, allocates RAM, spawns threads.
 * Returns false if any worker encounters a runtime failure. */
bool amr_run(amr_t *h, amr_worker_cb on_complete);

/* Context: [Shutdown | Main Thread] */
void amr_destroy(amr_t *h);


/* ========================================================================
 * PHASE 2: TASK SETUP & DAG WIRING
 * ========================================================================
 * Called inside your setup callback to define what a task does.
 * ======================================================================== */

/* Context: [Setup | Main Thread]
 * Retrieve custom args inside your setup function */
void *amr_task_custom_arg(amr_task_t *task);

/* Context: [Setup | Main Thread]
 * Assign a custom worker execution function to this task. */
void amr_task_runner(amr_task_t *task, amr_worker_cb runner);
void amr_task_default_runner(amr_task_t *task);

/* Context: [Setup | Main Thread]
 * Setup transforms: Connect named inputs to named outputs with a specific runner */
void amr_task_transform(amr_task_t *task, const char *inp, const char *outp,
                        amr_runner_cb runner);

/* Context: [Setup | Main Thread] */
void amr_task_io_transform(amr_task_t *task, const char *inp, const char *outp,
                          amr_io_runner_cb runner);

/* Context: [Setup | Main Thread]
 * Group runners feed chunks of records sharing the same key to your callback. */
void amr_task_group_transform(amr_task_t *task, const char *inp, const char *outp,
                              amr_group_runner_cb runner, io_compare_cb compare);

/* Context: [Setup | Main Thread]
 * Manage custom memory/state needed by your transform runners. The create
   callback fires before the worker starts, populating w->transform_data. */
void amr_task_group_compare_arg(amr_task_t *task, amr_worker_data_cb create,
                               amr_destroy_worker_data_cb destroy);
void amr_task_transform_data(amr_task_t *task, amr_worker_data_cb create,
                             amr_destroy_worker_data_cb destroy);

/* Context: [Setup | Main Thread]
 * Control Task Execution logic */
void amr_task_do_nothing(amr_task_t *task);      /* Acts as a sync barrier */
void amr_task_run_everytime(amr_task_t *task);   /* Ignores checkpoint caching */

/* ------------------------------------------------------------------------
 * TASK DEPENDENCY BARRIERS
 * Context: [Setup | Main Thread]
 * Force execution ordering. You can specify multiple dependencies by
 * separating them with a vertical bar (e.g., "task_a|task_b").
 * ------------------------------------------------------------------------ */

/* FULL BARRIER: This task cannot start ANY partition until the target
   dependency has finished ALL of its partitions. Use this when you need a
   global view of the data (e.g., waiting for a global sort or dictionary). */
bool amr_task_dependency(amr_task_t *task, const char *dependency);

/* PIPELINE BARRIER: This task's Partition X can begin execution the exact
   moment the dependency's Partition X finishes. Use this to keep workers
   busy and pipeline data efficiently without waiting for the whole cluster. */
bool amr_task_partial_dependency(amr_task_t *task, const char *dependency);


/* ========================================================================
 * PHASE 3: OUTPUT DECLARATIONS & MODIFIERS (STATEFUL BUILDER API)
 * Context: [Setup | Main Thread]
 * ========================================================================
 * First declare an output, then use modifiers to change its behavior.
 * ======================================================================== */

/* Declares a standard sequential output port (1 file per worker partition).
   This makes it the "current" output for subsequent modifier calls. */
void amr_task_output(amr_task_t *task, const char *name, double out_ram_pct);

/* Prevents the framework's GC from deleting this specific output artifact
   once materialized on disk, keeping it permanently. */
void amr_task_output_keep(amr_task_t *task);

/* Bypasses standard I/O writers. Tells the framework that you are manually
   writing a custom artifact (e.g. SQLite database, custom binary blob) to
   the generated file path. */
void amr_task_output_opaque(amr_task_t *task);

/* ------------------------------------------------------------------------
 * OUTPUT ROUTING (SHUFFLE).  The default is to not shuffle which is a
 * common use case.
 * ------------------------------------------------------------------------ */

/* Scatters data evenly across downstream partitions (Round-Robin/Load Balance).
   No key or hashing strategy required. */
void amr_task_output_shuffle(amr_task_t *task);

/* Groups data by routing records to buckets based on a registered hashing
   strategy (e.g., "Hash_A").
   * @param arg An optional pointer passed directly to the underlying
   * partitioner callback as its 'tag' or state. Pass NULL if unused. */
void amr_task_output_shuffle_by(amr_task_t *task, const char *partitioner_name, void *arg);

/* Groups data using a raw, custom C partition callback.
   * @param arg Passed directly to the callback as its 'tag'. Pass NULL if unused. */
void amr_task_output_shuffle_with(amr_task_t *task, io_partition_cb part, void *arg);

/* ------------------------------------------------------------------------
 * OUTPUT SORTING & REDUCING
 * ------------------------------------------------------------------------ */

/* Sorts the output file using a registered comparison strategy (e.g., "Sort_A").
   Behind the scenes, the framework writes temporary sorted chunks to disk
   and runs a multi-way merge when the task finishes.
   * @param arg An optional pointer passed directly to the underlying
   * comparator callback as its 'tag' or state. Pass NULL if unused. */
void amr_task_output_sort_by(amr_task_t *task, const char *comparator_name, void *arg);

/* Sorts the output file using a raw, custom C callback.
   * @param arg Passed directly to the callback as its 'tag'. Pass NULL if unused. */
void amr_task_output_sort_with(amr_task_t *task, io_compare_cb compare, void *arg);

/* Aggregates/combines records that share the same sort key during the final
   multi-way merge phase using a registered reduction strategy.
   * @param arg An optional pointer passed directly to the underlying
   * reducer callback as its 'tag' or state. Pass NULL if unused. */
void amr_task_output_reduce_by(amr_task_t *task, const char *reducer_name, void *arg);

/* Aggregates/combines records using a raw C callback.
   * @param arg Passed directly to the callback as its 'tag'. Pass NULL if unused. */
void amr_task_output_reduce_with(amr_task_t *task, io_reducer_cb reducer, void *arg);

/* Shorthand reducer. Automatically drops duplicate records (records where
   compare() == 0), keeping only the first instance. */
void amr_task_output_reduce_by_keeping_first(amr_task_t *task);

/* Allows you to use a different comparison function for the temporary
   intermediate chunks written to disk *before* the final merge.
   * @param arg Passed directly to the underlying callback. Pass NULL if unused. */
void amr_task_output_intermediate_sort_by(amr_task_t *task, const char *comparator_name, void *arg);
void amr_task_output_intermediate_sort_with(amr_task_t *task, io_compare_cb compare, void *arg);

/* Allows you to use a different reduction function for the temporary chunks.
   * @param arg Passed directly to the underlying callback. Pass NULL if unused. */
void amr_task_output_intermediate_reduce_by(amr_task_t *task, const char *reducer_name, void *arg);
void amr_task_output_intermediate_reduce_with(amr_task_t *task, io_reducer_cb reducer, void *arg);


/* ========================================================================
 * ADVANCED: IN-MEMORY SORTING & MAP-SIDE COMBINERS
 * ========================================================================
 * If you configure an output to be both SHUFFLED and SORTED, you must
 * decide when the sorting happens relative to the physical fan-out.
 *
 * DEFAULT (sort_before_partitioning):
 * The worker uses 100% of its allocated RAM as a single, massive sorting buffer.
 * As data streams in, it is sorted and reduced (squashing duplicates) *in-memory*.
 * Only at the end of the task is this highly compressed, global stream written
 * out to the N partition buckets.
 * BEST FOR: High reduction rates (e.g., counting frequencies) or large partition counts.
 *
 * sort_while_partitioning:
 * The worker splits its RAM into N tiny buffers (one per partition bucket).
 * Data is hashed into a bucket's buffer, sorted, and spilled to disk as needed.
 * DANGER: If you have 100 partitions and 100MB of RAM, each buffer is only 1MB.
 * This causes rapid disk spilling, thrashing I/O, and crippling performance
 * unless N is very small (e.g., 4-16).
 *
 * sort_after_partitioning:
 * Records are hashed and immediately flushed to raw, unsorted temp files on disk
 * with zero in-memory reduction. When the worker finishes, a massive post-processing
 * phase reads, sorts, reduces, and merges those temp files.
 * BEST FOR: Raw sorts with no reducer, where you just want data in buckets quickly.
 * ======================================================================== */

void amr_task_output_sort_after_partitioning(amr_task_t *task);
void amr_task_output_sort_while_partitioning(amr_task_t *task);

void amr_task_output_num_sort_threads(amr_task_t *task, size_t num_sort_threads);

/* ------------------------------------------------------------------------
 * OUTPUT FORMATTING & I/O TWEAKS
 * ------------------------------------------------------------------------ */

/* Defines the exact physical format of the records (e.g., io_prefix(),
   io_delimiter('\n')). If you don't set this, io_prefix is used by default. */
void amr_task_output_format(amr_task_t *task, io_format_t format);

/* Writes to a ".safe" temporary file and renames it to the final name only
   upon success. Prevents corrupted half-written files on crash. */
void amr_task_output_safe_mode(amr_task_t *task);

/* Writes an empty .ack file alongside the data file when complete. Useful
   if an external non-AMR system is polling for this data. */
void amr_task_output_write_ack_file(amr_task_t *task);

/* Forces the output to be written with GZIP or LZ4 compression. */
void amr_task_output_gz(amr_task_t *task, int level);
void amr_task_output_lz4(amr_task_t *task, int level, lz4_block_size_t size, bool block_checksum, bool content_checksum);

/* Registers a custom string-builder for this output so it can be viewed
   in human-readable format when using the `--dump` CLI flag. */
void amr_task_output_dump(amr_task_t *task, amr_task_dump_cb dump, void *arg);

/* When sorting, AMR merges temporary files. If num_per_group is set, AMR will
   perform intermediate merges when it hits this many temp files, preventing
   fd exhaustion on huge datasets. */
void amr_task_output_group_size(amr_task_t *task, size_t num_per_group, size_t start);

/* Spawns a background pthread dedicated to compressing and writing sorted chunks. */
void amr_task_output_use_extra_thread(amr_task_t *task);

/* Disables LZ4 compression for temporary sort files. Trades disk IO for CPU. */
void amr_task_output_dont_compress_tmp(amr_task_t *task);


/* ========================================================================
 * PHASE 4: INPUT DECLARATIONS & MODIFIERS
 * Context: [Setup | Main Thread]
 * ========================================================================
 *
 * MAGIC INHERITANCE RULE:
 * If you link an input to a previous task, the input AUTOMATICALLY inherits
 * the format, sort, and reducer configurations from the producer's output.
 *
 * You only need to use the input configuration modifiers if:
 * 1. You are reading raw external files (`amr_task_input_files`).
 * 2. You want to apply a read-time reducer/sort that differs from the write-time.
 * ======================================================================== */

/* Syntax Sugar for Input Edges */
/* 1-to-All Broadcast: Consumer partitions read ONLY Producer partition 0. */
void amr_task_input_from_task_first(amr_task_t *c, const char *prod, const char *out, double pct);

/* 1-to-1 Pipeline: Consumer partition X reads ONLY from Producer partition X. */
void amr_task_input_from_task_partition(amr_task_t *c, const char *prod, const char *out, double pct);

/* M-to-N Hash/Group: The classic MapReduce shuffle. */
void amr_task_input_from_task_shuffle(amr_task_t *c, const char *prod, const char *out, double pct);

/* M-to-N Cartesian Product: EVERY Consumer partition reads ALL Producer partitions. (Default) */
void amr_task_input_from_task_all_to_all(amr_task_t *c, const char *prod, const char *out, double pct);

/* Link an input directly to files on disk (Requires manual amr_task_input_format!). */
void amr_task_input_files(amr_task_t *task, const char *name, double ram_pct,
                          amr_worker_file_info_cb file_info);

/* Read an output from the exact same task topology executed in the PREVIOUS run.
   * Unlike normal edges, NO DAG dependency is registered for previous-run edges. */
void amr_task_input_from_previous_run(amr_task_t *consumer,
                                      const char *prev_task_name,
                                      const char *output_name,
                                      double in_ram_pct,
                                      size_t edge_flags);

/* Bypasses standard I/O readers. Tells the framework that you are manually
   reading an opaque custom artifact. */
void amr_task_input_opaque(amr_task_t *task);

/* ------------------------------------------------------------------------
 * INPUT MODIFIERS
 * ------------------------------------------------------------------------ */

/* Defines how the input stream is parsed (e.g., io_prefix(), io_delimiter('\n')). */
void amr_task_input_format(amr_task_t *task, io_format_t format);

/* Merge-sort multiple input streams using a registered strategy.
   * @param arg An optional pointer passed directly to the underlying
   * comparator callback as its 'tag' or state. Pass NULL if unused. */
void amr_task_input_sort_by(amr_task_t *task, const char *comparator_name, void *arg);

/* Merge-sort multiple input streams using a raw C callback.
   * @param arg Passed directly to the callback as its 'tag'. Pass NULL if unused. */
void amr_task_input_sort_with(amr_task_t *task, io_compare_cb compare, void *arg);

/* Aggregate/combine records during a merge-sort read using a registered strategy.
   * @param arg An optional pointer passed directly to the underlying
   * reducer callback as its 'tag' or state. Pass NULL if unused. */
void amr_task_input_reduce_by(amr_task_t *task, const char *reducer_name, void *arg);

/* Aggregate/combine records during a merge-sort read using a raw C callback.
   * @param arg Passed directly to the callback as its 'tag'. Pass NULL if unused. */
void amr_task_input_reduce_with(amr_task_t *task, io_reducer_cb reducer, void *arg);

/* Shorthand to automatically drop duplicate records with the same sort key during a read. */
void amr_task_input_reduce_by_keeping_first(amr_task_t *task);

/* Registers a custom string-builder for this input so it can be viewed
   in human-readable format when using the `--dump` CLI flag. */
void amr_task_input_dump(amr_task_t *task, amr_task_dump_cb dump, void *arg);

/* Overrides the internal buffer size used for decompressing LZ4/GZ files. */
void amr_task_input_compressed_buffer_size(amr_task_t *task, size_t buffer_size);

/* Hard limit on the number of records to read from this input. Excellent for fast testing. */
void amr_task_input_limit(amr_task_t *task, size_t limit);


/* ========================================================================
 * PHASE 5: WORKER RUNTIME API (Inside your threaded callbacks)
 * Context: [Runtime | Worker Thread]
 * ========================================================================
 * GLOBAL OWNERSHIP NOTE: Unless otherwise stated, strings and path arrays
 * returned by amr_worker_* helpers are allocated in the worker's persistent
 * pool (amr_worker_pool(w)) and remain valid for the lifetime of the worker.
 * Do NOT call free() on them.
 * ======================================================================== */

/* Instantiates and returns the physical output writer stream for the Nth
   declared output.
   LIFETIME: If you call this manually inside a raw custom amr_worker_cb,
   YOU own the handle and must call io_out_destroy(). If you are inside a
   standard transform (amr_io_runner_cb/amr_runner_cb), the framework manages
   and destroys these automatically. */
io_out_t *amr_worker_out(amr_worker_t *w, size_t n);

/* Context: [Runtime | Worker Thread]
   Bypasses the io_out_t buffering layer entirely and returns a raw POSIX file
   descriptor (fd) opened for writing (O_WRONLY | O_CREAT | O_TRUNC).
   LIFETIME: YOU own the file descriptor and must call close(fd). */
int amr_worker_out_as_file(amr_worker_t *w, size_t n);

/* Instantiates and returns the physical input reader stream for the Nth
   declared input.
   LIFETIME: If you call this manually inside a raw custom amr_worker_cb,
   YOU own the handle and must call io_in_destroy(). If you are inside a
   standard transform (amr_io_runner_cb/amr_runner_cb), the framework manages
   and destroys these automatically. */
io_in_t *amr_worker_in(amr_worker_t *w, size_t n);

/* Returns the abstract definition/metadata for the Nth output.
   Does not open any files. Rarely needed in standard runners. */
amr_worker_output_t *amr_worker_output(amr_worker_t *w, size_t n);

/* Returns the abstract definition/metadata for the Nth input. */
amr_worker_input_t *amr_worker_input(amr_worker_t *w, size_t n);

/* Slurps an entire input file completely into RAM and returns a pointer to it.
   Only works for num_files == 1.
   LIFETIME: Memory is owned by AMR and remains valid for the lifetime of the
   worker. Do NOT free() it.
   NOTE: Compression support depends entirely on the underlying io_read_file()
   implementation; usually raw bytes only. If parsing string data, the returned
   buffer is NOT guaranteed to be NUL-terminated unless formatted that way on disk. */
char *amr_worker_read(amr_worker_t *w, size_t *num_records, char **endp, size_t n);

/* Context: [Any Phase | Any Thread]
   Returns the human-readable string name of the current task. */
const char *amr_task_name(const amr_task_t *task);

/* Context: [Any Phase | Any Thread (Read-Only)]
   Retrieves the root scheduler object.
   NOTE: Mutating the scheduler state during runtime is unsupported. */
amr_t *amr_task_schedule(const amr_task_t *task);

/* Calculates exactly how many bytes of RAM this worker is allowed to use,
   based on the global RAM limit, the task's requested percentage (pct),
   and the number of active threads. */
size_t amr_worker_ram(amr_worker_t *w, double pct);

/* Resolves the physical base path being written to for a given output. */
char *amr_worker_output_base(const amr_worker_t *w, const amr_worker_output_t *outp);

/* Resolves the physical base path, injecting a specific string suffix. */
char *amr_worker_output_base2(const amr_worker_t *w, const amr_worker_output_t *outp, const char *suffix);

/* Resolves the exact physical file path that a specific producer partition
   wrote to, so this consumer can read it. */
char *amr_worker_input_name(const amr_worker_t *w, const amr_worker_input_t *inp, size_t partition);

/* Helper function to print a specific record using the dump callback
   registered for the Nth input. */
void amr_worker_dump_input(amr_worker_t *w, io_record_t *r, size_t n);


/* =========================================================================
 * OPAQUE ARTIFACT UTILITIES
 * Context: [Runtime | Worker Thread]
 * =========================================================================
 * Use these when you are building databases, models, or binary caches that
 * shouldn't be parsed by the standard io_in_t stream reader.
 * ========================================================================= */

/* Returns arrays of physical file paths to the custom resources. */
char **amr_worker_opaque_inputs(amr_worker_t *w, size_t n, size_t *num_paths);

/* Returns arrays of physical file paths to the custom resources.
   NOTE: If the output is a SHUFFLE, the generated paths follow a strict
   "%base_%zu" pattern without standard compression suffix logic. */
char **amr_worker_opaque_outputs(amr_worker_t *w, size_t n, size_t *num_paths);

/* Automatically opens standard FILE handles using the opaque paths.
   LIFETIME: Array allocated in w->worker_pool. FILE handles must be
   closed manually or via amr_worker_close_opaque_files(). */
FILE **amr_worker_open_opaque_inputs(amr_worker_t *w, size_t n, const char *mode, size_t *num_files);

/* Automatically opens standard FILE handles using the opaque paths.
   LIFETIME: Array allocated in w->worker_pool. FILE handles must be
   closed manually or via amr_worker_close_opaque_files(). */
FILE **amr_worker_open_opaque_outputs(amr_worker_t *w, size_t n, const char *mode, size_t *num_files);

/* Safely closes an array of FILE handles */
void amr_worker_close_opaque_files(FILE **files, size_t num_files);

/* ========================================================================
 * PRE-LOADED MEMORY / DISTRIBUTED CACHE
 * ======================================================================== */
typedef struct {
    void *buffer;
    size_t length;
    size_t partition;
} amr_loaded_data_t;

/* Context: [Setup | Main Thread]
   Flags an input port to bypass standard I/O streams. The framework will
   read all associated files entirely into RAM. */
void amr_task_input_load_into_memory(amr_task_t *task);

/* Context: [Runtime | Worker Thread]
   Retrieves the array of loaded buffers for the Nth input port.
   Memory is owned by the framework and survives the worker's lifecycle. */
amr_loaded_data_t *amr_worker_loaded_data(amr_worker_t *w, size_t n, size_t *num_data);

/* Context: [Runtime | Worker Thread]
   Retrieves the specific loaded buffer produced by a specific upstream partition.
   Returns NULL if the data was not found or the input is not loaded in memory. */
amr_loaded_data_t *amr_worker_loaded_data_for_partition(amr_worker_t *w, size_t n, size_t partition);


/* =========================================================================
 * ADVANCED & INTERNAL UTILITIES
 * ========================================================================= */

/* Returns a space-separated string of parameters/paths for the Nth output. */
char *amr_worker_output_params(amr_worker_t *w, size_t n);

/* Returns a space-separated string of the physical filenames for the Nth input. */
char *amr_worker_input_params(amr_worker_t *w, size_t n);

/* =========================================================================
 * CALLBACK ACCESSORS
 * Context: [Runtime | Worker Thread]
 * Retrieves the configured runtime callbacks and their arguments for specific ports.
 * This allows you to write highly generic runners that defer to the type registry.
 * ========================================================================= */

/*
The Golden Rule for AMR Joins:
- Homogeneous Joins (Same Types): You can use amr_worker_input_compare(w, 0). The runner can be 100% generic.
- Heterogeneous Joins (Different Types): You must write a custom local comparator because only you know
the exact struct layouts of both the left and right sides of the join.
*/

io_compare_cb amr_worker_input_compare(amr_worker_t *w, size_t n);
void *amr_worker_input_compare_arg(amr_worker_t *w, size_t n);

io_reducer_cb amr_worker_input_reducer(amr_worker_t *w, size_t n);
void *amr_worker_input_reducer_arg(amr_worker_t *w, size_t n);

io_compare_cb amr_worker_output_compare(amr_worker_t *w, size_t n);
void *amr_worker_output_compare_arg(amr_worker_t *w, size_t n);

io_partition_cb amr_worker_output_partition(amr_worker_t *w, size_t n);
void *amr_worker_output_partition_arg(amr_worker_t *w, size_t n);

io_reducer_cb amr_worker_output_reducer(amr_worker_t *w, size_t n);
void *amr_worker_output_reducer_arg(amr_worker_t *w, size_t n);

/* Debugging - print the internal scheduler state to stdout */
void amr_print(amr_t *h);

/* =========================================================================
 * TYPE REGISTRY & SCHEMAS
 * ========================================================================= */

/* Typedefs for the Type Lifecycle & Debugging Callbacks */
typedef void (*amr_serialize_fn)(const void *obj, aml_buffer_t *out_buffer);
typedef void* (*amr_deserialize_fn)(aml_pool_t *pool, const void *buffer, size_t len);
typedef void (*amr_to_string_fn)(const void *obj, aml_buffer_t *out_buffer);

/* Register a new data type with the framework */
void amr_register_datatype(amr_t *amr,
                           const char *datatype_name,
                           const char *desc,
                           amr_serialize_fn serialize,
                           amr_deserialize_fn deserialize,
                           amr_to_string_fn to_string);

/* Add named behaviors to a registered type by referencing its string name */
void amr_datatype_add_compare(amr_t *amr, const char *datatype_name, const char *name, io_compare_cb cmp);
void amr_datatype_add_partition(amr_t *amr, const char *datatype_name, const char *name, io_partition_cb part);
void amr_datatype_add_reducer(amr_t *amr, const char *datatype_name, const char *name, io_reducer_cb reducer);

/* ----- Output Configuration ----- */
/* Sets the expected data type for the currently configured output */
void amr_task_output_type(amr_task_t *t, const char *type_name);

/* ----- Input Configuration ----- */
/* Validates that the upstream output matches this expected type */
void amr_task_input_expect_type(amr_task_t *t, const char *type_name);

/* Deserializes a raw record from the Nth input into a typed C struct.
   LIFETIME: Memory is allocated in w->pool and lives until the next record/group. */
void* amr_worker_deserialize(amr_worker_t *w, size_t input_idx, const io_record_t *r);

/* Serializes a typed C struct and writes it immediately to the Nth output port. */
void amr_worker_serialize(amr_worker_t *w, size_t output_idx, io_out_t *out, const void *obj);


/* ========================================================================
 * PIPELINE / SUBGRAPH API
 * ======================================================================== */
struct amr_pipeline_s;
typedef struct amr_pipeline_s amr_pipeline_t;

/* Define the setup function signature for a pipeline */
typedef bool (*amr_pipeline_cb)(amr_pipeline_t *p);

/* Context: [Init | Main Thread]
   Instantiates a top-level pipeline. */
amr_pipeline_t *amr_pipeline_create(amr_t *sched, const char *ns, amr_pipeline_cb setup, void *config);

/* Context: [Setup | Main Thread]
   Instantiates a child pipeline INSIDE a parent pipeline. Its namespace will automatically
   be prefixed by the parent's namespace to avoid collisions. */
amr_pipeline_t *amr_pipeline_create_nested(amr_pipeline_t *parent, const char *ns, amr_pipeline_cb setup, void *config);

/* Context: [Init | Main Thread]
   Maps an external task's physical output to a logical pipeline input port. */
void amr_pipeline_bind_input(amr_pipeline_t *p, const char *port_name, const char *ext_task, const char *ext_out);

/* Context: [Setup | Main Thread]
   Exposes a child pipeline's logical input port to the outside world via the parent. */
void amr_pipeline_expose_input(amr_pipeline_t *parent, const char *parent_port,
                               amr_pipeline_t *child, const char *child_port);

/* Context: [Setup | Main Thread] (Called INSIDE pipeline setup)
   Creates a task whose name is automatically prefixed by the pipeline namespace. */
amr_task_t *amr_pipeline_task(amr_pipeline_t *p, const char *task_base_name, bool partitioned, amr_task_cb setup);

/* Context: [Setup | Main Thread] (Called INSIDE task setup)
   Wires a task to consume from the pipeline's logical input port. */
void amr_task_input_from_pipeline_port_first(amr_task_t *t, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_port_partition(amr_task_t *t, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_port_shuffle(amr_task_t *t, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_port_all_to_all(amr_task_t *t, const char *port_name, double in_ram_pct);

/* Context: [Setup | Main Thread] (Called INSIDE task setup)
   Wires a task to a sibling task within the same pipeline (auto-resolves namespace). */
void amr_task_input_from_sibling_first(amr_task_t *t, const char *sibling_base_name, const char *out_name, double in_ram_pct);
void amr_task_input_from_sibling_partition(amr_task_t *t, const char *sibling_base_name, const char *out_name, double in_ram_pct);
void amr_task_input_from_sibling_shuffle(amr_task_t *t, const char *sibling_base_name, const char *out_name, double in_ram_pct);
void amr_task_input_from_sibling_all_to_all(amr_task_t *t, const char *sibling_base_name, const char *out_name, double in_ram_pct);

/* Context: [Setup | Main Thread] (Called INSIDE pipeline setup)
   Exposes an internal task's physical output as a logical port on the pipeline. */
void amr_pipeline_bind_output(amr_pipeline_t *p, const char *port_name,
                              const char *internal_task_base, const char *internal_out);

/* Context: [Setup | Main Thread]
   Exposes a child pipeline's logical output port to the outside world via the parent. */
void amr_pipeline_expose_output(amr_pipeline_t *parent, const char *parent_port,
                                amr_pipeline_t *child, const char *child_port);

/* Context: [Setup | Main Thread] (Called INSIDE a downstream task's setup)
   Wires a task to consume from a pipeline's exposed logical output port. */
void amr_task_input_from_pipeline_first(amr_task_t *t, amr_pipeline_t *p, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_partition(amr_task_t *t, amr_pipeline_t *p, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_shuffle(amr_task_t *t, amr_pipeline_t *p, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_all_to_all(amr_task_t *t, amr_pipeline_t *p, const char *port_name, double in_ram_pct);

/* Context: [Setup | Main Thread]
   Wires two pipelines together */
void amr_pipeline_bind_link(amr_pipeline_t *dest_pipe, const char *dest_in_port,
                            amr_pipeline_t *src_pipe,  const char *src_out_port);

/* Context: [Runtime | Worker Thread]
   Retrieves the pipeline context this worker is executing within. */
amr_pipeline_t *amr_worker_pipeline(amr_worker_t *w);

/* Retrieves the root scheduler attached to this pipeline. */
amr_t *amr_pipeline_scheduler(amr_pipeline_t *p);

/* Accessors */
void *amr_pipeline_config(amr_pipeline_t *p);
amr_pipeline_t *amr_task_pipeline(amr_task_t *t);

#ifdef __cplusplus
}
#endif

#endif  /* AMR_H */
