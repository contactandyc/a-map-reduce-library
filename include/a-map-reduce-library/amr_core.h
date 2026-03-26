// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef AMR_CORE_H
#define AMR_CORE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Pull in foundational I/O and Memory types so callbacks can use them */
#include "a-memory-library/aml_buffer.h"
#include "the-io-library/io.h"
#include "the-io-library/io_in.h"
#include "the-io-library/io_out.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * A-MAP-REDUCE (AMR) - CORE ARCHITECTURE & INTUITION
 * ========================================================================
 * AMR is a single-node, partitioned DAG (Directed Acyclic Graph) execution
 * engine for out-of-core data processing in C.
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
 * physical files, read records, and write outputs concurrently.
 * ======================================================================== */

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
 * Graph topology and configuration errors during setup are fatal and trigger
 * `abort()`. Execution errors in custom `amr_worker_cb` tasks should be
 * reported by returning `false` to safely halt the DAG.
 *
 * 8. Conditional Branching & DAG Pruning:
 * You can dynamically alter execution flow by calling `amr_worker_skip_output`
 * inside a runner. If a producer skips an output, the DAG explicitly prunes
 * downstream paths. Consumers relying solely on that skipped artifact will be
 * "starved" and skipped automatically to save CPU.
 *
 * 9. Output Retention & Garbage Collection:
 * Intermediate task outputs are automatically unlinked (deleted) based on
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
 * --run <N>               Isolate execution to tasks/run_<N>/.
 * --new-args              Overwrite the persisted `custom_args` file.
 *
 * --- Inspection & Debugging (Note: These force cpus = 1) ---
 * -l, --list              Print logical DAG execution plan and exit.
 * -s, --show-files        Print plan AND resolved physical file paths.
 * -d, --dump <files>      Print human-readable contents of output files.
 * --sample <recs>:<parts> Randomly sample N records from M active partitions.
 * --match <str>           Lazily filter --sample/--dump to matching records.
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

/* ========================================================================
 * FORWARD DECLARATIONS
 * ======================================================================== */
struct amr_s;
typedef struct amr_s amr_t;

struct amr_task_s;
typedef struct amr_task_s amr_task_t;

struct amr_worker_s;
typedef struct amr_worker_s amr_worker_t;

struct amr_pipeline_s;
typedef struct amr_pipeline_s amr_pipeline_t;

struct amr_worker_input_s;
typedef struct amr_worker_input_s amr_worker_input_t;

struct amr_worker_output_s;
typedef struct amr_worker_output_s amr_worker_output_t;

struct amr_thread_s;
typedef struct amr_thread_s amr_thread_t;

struct amr_task_state_link_s;
typedef struct amr_task_state_link_s amr_task_state_link_t;

/* ========================================================================
 * UNDERSTANDING THE CORE I/O TYPES & CALLBACKS
 * ========================================================================
 * AMR relies on the underlying I/O library to process and compare records.
 * When using raw `_with` modifiers (e.g., `amr_task_output_sort_with`),
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
 * CORE CALLBACK TYPES
 * ======================================================================== */

/* Context: [Setup | Main Thread] */
typedef bool (*amr_task_cb)(amr_task_t *task);

/* Context: [Runtime | Worker Thread] */
typedef bool (*amr_worker_cb)(amr_worker_t *w);

/* Context: [Runtime | Worker Thread]
 * Data lifecycle hooks for custom structs needed by workers. */
typedef void *(*amr_worker_data_cb)(amr_worker_t *w);
typedef void (*amr_destroy_worker_data_cb)(amr_worker_t *w, void *d);

/* Context: [Runtime | Worker Thread]
 * Dumper callback for `--dump` CLI inspection. */
typedef void (*amr_task_dump_cb)(amr_worker_t *w, io_record_t *r, aml_buffer_t *bh, void *arg);

/* Context: [Runtime | Worker Thread]
 * Standard stream runner: Takes one record in, writes to multiple out ports. */
typedef void (*amr_runner_cb)(amr_worker_t *w, io_record_t *r, io_out_t **out);

/* Context: [Runtime | Worker Thread]
 * Grouped stream runner: Takes an array of records that share the same key. */
typedef void (*amr_group_runner_cb)(amr_worker_t *w, io_record_t *r, size_t num_r, io_out_t **out);

/* Context: [Runtime | Worker Thread]
 * Low-level IO runner: Gives raw access to the input/output handles. */
typedef void (*amr_io_runner_cb)(amr_worker_t *w, io_in_t **ins, size_t num_ins, io_out_t **outs, size_t num_outs);

/* Context: [Runtime | Worker Thread]
 * Resolves external input files into the pipeline. */
typedef io_file_info_t *(*amr_worker_file_info_cb)(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp);

/* CLI parsers */
typedef int (*amr_parse_args_cb)(int argc, char **argv, void *arg);
typedef bool (*amr_finish_args_cb)(int argc, char **argv, void *arg);

/* ========================================================================
 * SCHEDULER LIFECYCLE & EXECUTION
 * ======================================================================== */

/* Context: [Init | Main Thread]
 * Initialize the global scheduler.
 * @param num_partitions The maximum concurrency level (e.g., 16).
 * @param cpus The number of active threads processing partitions simultaneously.
 * @param ram Total system RAM in Megabytes available for the framework.
 */
amr_t *amr_init(int argc, char **argv, size_t num_partitions, size_t cpus, size_t ram);

/*
 * Overrides the default "tasks" workspace directory.
 * Useful for pointing I/O at dedicated scratch disks or RAM disks.
 * MUST be called before amr_run().
 */
void amr_set_workspace_dir(amr_t *sched, const char *path);

/* Context: [Init | Main Thread]
 * Fires the DAG. Resolves dependencies, allocates RAM, spawns threads.
 * Returns false if any worker encounters a runtime failure. */
bool amr_run(amr_t *sched, amr_worker_cb on_complete);

/* Context: [Shutdown | Main Thread]
 * Safely tears down the scheduler and frees all global resources. */
void amr_destroy(amr_t *sched);

/* Context: [Provided by AMR | Pass to amr_run]
 * Built-in hook to monitor task completion. Prints memory and timing metrics. */
bool amr_worker_complete(amr_worker_t *w);

/* ========================================================================
 * GLOBAL CONFIGURATION & CLI BINDINGS
 * ======================================================================== */

/* Context: [Any Phase | Main Thread]
 * Globally prevents AMR from deleting upstream task outputs across the
 * entire DAG. By default, AMR deletes an intermediate task's output once
 * all downstream consumers have finished reading it. */
void amr_keep_intermediate_files(amr_t *sched);

/* Context: [Init | Main Thread]
 * Directory overrides for persistent storage. */
void amr_ack_dir(amr_t *sched, const char *ack_dir);
void amr_task_dir(amr_t *sched, const char *task_dir);

/* Context: [Init | Main Thread]
 * CLI custom argument registration. Hooks into the framework's parser. */
void amr_custom_args(amr_t *sched, void (*custom_usage)(void),
                     amr_parse_args_cb parse_args, amr_finish_args_cb finish_args,
                     void *arg);

/* Context: [Init | Main Thread]
 * Enables iterative DAG execution via the `--run <N>` CLI flag.
 * Forces outputs to be stored in tasks/run_<N>/. */
void amr_use_runs(amr_t *sched);

/* Context: [Any Phase | Main Thread]
 * Returns the current run number (defaults to 0 if --run isn't specified). */
size_t amr_current_run(amr_t *sched);

/* Context: [Any Phase | Main Thread]
 * Generate the exact file path for a file produced by a specific task/partition
 * in a specific run. Resolves internal suffix appending automatically.
 * LIFETIME: Returns an allocated string from the scheduler's pool. Do not free. */
char *amr_run_file_path(amr_t *sched, size_t run, const char *task_name,
                        size_t partition, const char *file_base);

/* Context: [Debug | Any Thread]
 * Prints the internal scheduler state to stdout for diagnostics. */
void amr_print(amr_t *sched);

#ifdef __cplusplus
}
#endif

#endif /* AMR_CORE_H */
