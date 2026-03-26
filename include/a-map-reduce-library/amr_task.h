// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef AMR_TASK_H
#define AMR_TASK_H

#include "a-map-reduce-library/amr_core.h"
#include "a-map-reduce-library/amr_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * THE STATEFUL SETUP API (IMPORTANT!)
 * ========================================================================
 * When defining a task in its setup callback, the API is stateful.
 * Calling amr_task_output() or amr_task_input_...() sets the "current"
 * active port. Any subsequent configuration calls (like setting the format,
 * reducer, or sort parameters) apply ONLY to that most recently declared port.
 *
 * Example:
 * amr_task_output(task, "my_out.bin", 1.0);
 * amr_task_output_format(task, io_fixed(16));       // Applies to "my_out.bin"
 * amr_task_output_shuffle_by(task, "Hash_A", NULL); // Applies to "my_out.bin"
 * ======================================================================== */

/* ========================================================================
 * A-MAP-REDUCE (AMR) - COMMON TOPOLOGY PATTERNS
 * ========================================================================
 * The way you configure outputs and wire inputs dictates your data flow.
 * Here are the 5 most common partitioned topologies:
 *
 * 1. MANY-TO-ONE / GLOBAL GATHER (M Partitions -> 1 Partition)
 * Use Case: Reduce distributed logs/metrics into a single global file.
 * - Producer (M parts) : amr_task_output(task, "data.bin", 1.0);
 * - Consumer (1 part)  : amr_task_input_from_task_all_to_all(task, "prod", "data.bin", 1.0);
 * Result: The single consumer automatically concatenates and reads the
 * files from all M upstream workers as one continuous stream.
 *
 * 2. 1-TO-1 PIPELINING (M Partitions -> M Partitions)
 * Use Case: Sequential processing (parse -> enrich -> filter) where data
 * doesn't need to cross worker boundaries.
 * - Producer (M parts) : amr_task_output(task, "data.bin", 1.0);
 * - Consumer (M parts) : amr_task_input_from_task_partition(task, "prod", "data.bin", 1.0);
 * Result: Consumer Worker X reads exclusively from Producer Worker X.
 * Perfect parallel isolation; zero cross-partition I/O overhead.
 *
 * 3. THE GLOBAL SHUFFLE (M Partitions -> N Partitions)
 * Use Case: Grouping data by a specific key (e.g., User ID) so all records
 * for the same user land on the exact same worker.
 * - Producer (M parts) : amr_task_output(task, "data.bin", 1.0);
 * amr_task_output_shuffle_by(task, "Hash_User", NULL);
 * - Consumer (N parts) : amr_task_input_from_task_shuffle(task, "prod", "data.bin", 1.0);
 * Result: The producer scatters its output into N hash buckets. Consumer X
 * gathers bucket X from all M producers.
 *
 * 4. MAP-SIDE JOIN / READ FIRST (M Partitions -> N Partitions)
 * Use Case: All upstream partitions output the exact same global state
 * (like a shared dictionary), or the upstream task only has 1 partition.
 * - Producer (M parts) : amr_task_output(task, "dict.bin", 1.0);
 * - Consumer (N parts) : amr_task_input_from_task_first(task, "prod", "dict.bin", 1.0);
 * Result: Every parallel consumer thread reads ONLY the file produced
 * by producer Partition 0, safely avoiding duplicate reads.
 *
 * 5. MANY-TO-MANY / ALL-TO-ALL (M Partitions -> N Partitions)
 * Use Case: Every worker needs to read the entirety of a partitioned dataset
 * (e.g., comparing every user against every other user, or matrix math).
 * - Producer (M parts) : amr_task_output(task, "data.bin", 1.0);
 * - Consumer (N parts) : amr_task_input_from_task_all_to_all(task, "prod", "data.bin", 1.0);
 * Result: Producer writes M individual files. Consumer spawns N workers.
 * EVERY consumer worker reads ALL M producer files.
 * ======================================================================== */

/* ========================================================================
 * TRANSFORM CHAINING & EXPLICIT ROUTING (INTERNAL DAGs)
 * ========================================================================
 * A Task can execute a single transform or a chain of multiple transforms.
 * AMR uses Explicit Name-Based Routing to move data between them.
 *
 * The arrays passed to your runner (e.g. `ins[]` and `outs[]`) map
 * DIRECTLY to the local order of names provided in the transform string
 * ("inA|inB" -> ins[0]=inA, ins[1]=inB).
 *
 * EXPOSED VS INTERNAL OUTPUTS:
 * By default, any output declared via `amr_task_output()` is EXPOSED.
 * It is permanently written to disk and available for downstream DAG tasks.
 *
 * If you mark an output as INTERNAL via `amr_task_output_internal()`, it
 * becomes a temporary pipe:
 * 1. You can explicitly route it to a downstream transform in the same task
 * by using its name: `amr_task_transform(t, "my_internal_tmp.bin", ...)`
 * 2. The framework will intelligently hand off the read handles in memory.
 * 3. When the task finishes, the framework automatically garbage-collects
 * (unlinks) the internal file, preventing disk leaks.
 *
 * THE SORT-MERGE OPTIMIZATION:
 * If you apply `amr_task_output_sort_by` to an output, and immediately pipe
 * it to a downstream transform, AMR performs a Map-Side Combine. It merges
 * the temporary sorted chunks directly into the RAM of the downstream
 * transform without ever writing a final merged file to disk.
 * ======================================================================== */

/* Context: [Init | Main Thread]
 * Registers a new Task in the DAG. */
amr_task_t *amr_task(amr_t *sched, const char *task_name, bool partitioned, amr_task_cb setup);

/* Context: [Any Phase | Any Thread (Read-Only)]
 * Retrieves the root scheduler object associated with a task. */
amr_t *amr_task_scheduler(const amr_task_t *task);

/* Context: [Setup | Main Thread]
 * Retrieve custom args inside your setup function */
void *amr_task_custom_arg(amr_task_t *task);

/* ------------------------------------------------------------------------
 * TASK DEPENDENCY BARRIERS
 * Context: [Setup | Main Thread]
 * Force execution ordering. You can specify multiple dependencies by
 * separating them with a vertical bar (e.g., "task_a|task_b").
 * ------------------------------------------------------------------------ */

/* FULL BARRIER: This task cannot start ANY partition until the target
 * dependency has finished ALL of its partitions. Use this when you need a
 * global view of the data (e.g., waiting for a global sort or dictionary). */
bool amr_task_dependency(amr_task_t *task, const char *dependency);

/* PIPELINE BARRIER: This task's Partition X can begin execution the exact
 * moment the dependency's Partition X finishes. Use this to keep workers
 * busy and pipeline data efficiently without waiting for the whole cluster. */
bool amr_task_partial_dependency(amr_task_t *task, const char *dependency);

/* ========================================================================
 * OUTPUT DECLARATIONS & MODIFIERS
 * Context: [Setup | Main Thread]
 * ======================================================================== */

/* Declares a standard sequential output port (1 file per worker partition).
 * This makes it the "current" output for subsequent modifier calls. */
void amr_task_output(amr_task_t *task, const char *name, double out_ram_pct);

/* Marks an output as an internal pipe. It will be automatically deleted from
 * disk as soon as the worker finishes executing its transform chain. */
void amr_task_output_internal(amr_task_t *task);

/* Prevents the framework's GC from deleting this specific output artifact
 * once materialized on disk, keeping it permanently. */
void amr_task_output_keep(amr_task_t *task);

/* Bypasses standard I/O writers. Tells the framework that you are manually
 * writing a custom artifact (e.g. SQLite database, custom binary blob) to
 * the generated file path. */
void amr_task_output_opaque(amr_task_t *task);

/* ----- Output Type Configuration ----- */
/* Sets the expected data type for the currently configured output */
void amr_task_output_type(amr_task_t *task, const char *type_name);

/* ------------------------------------------------------------------------
 * OUTPUT ROUTING (SHUFFLE)
 * ------------------------------------------------------------------------ */

/* Scatters data evenly across downstream partitions (Round-Robin/Load Balance).
 * No key or hashing strategy required. */
void amr_task_output_shuffle(amr_task_t *task);

/* Groups data by routing records to buckets based on a registered hashing
 * strategy (e.g., "Hash_A").
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
 * Behind the scenes, the framework writes temporary sorted chunks to disk
 * and runs a multi-way merge when the task finishes.
 * @param arg An optional pointer passed directly to the underlying
 * comparator callback as its 'tag' or state. Pass NULL if unused. */
void amr_task_output_sort_by(amr_task_t *task, const char *comparator_name, void *arg);

/* Sorts the output file using a raw, custom C callback.
 * @param arg Passed directly to the callback as its 'tag'. Pass NULL if unused. */
void amr_task_output_sort_with(amr_task_t *task, io_compare_cb compare, void *arg);

/* Aggregates/combines records that share the same sort key during the final
 * multi-way merge phase using a registered reduction strategy.
 * @param arg An optional pointer passed directly to the underlying
 * reducer callback as its 'tag' or state. Pass NULL if unused. */
void amr_task_output_reduce_by(amr_task_t *task, const char *reducer_name, void *arg);

/* Aggregates/combines records using a raw C callback.
 * @param arg Passed directly to the callback as its 'tag'. Pass NULL if unused. */
void amr_task_output_reduce_with(amr_task_t *task, io_reducer_cb reducer, void *arg);

/* Shorthand reducer. Automatically drops duplicate records (records where
 * compare() == 0), keeping only the first instance. */
void amr_task_output_reduce_by_keeping_first(amr_task_t *task);

/* Allows you to use a different comparison function for the temporary
 * intermediate chunks written to disk *before* the final merge.
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
 * io_delimiter('\n')). If you don't set this, io_prefix is used by default. */
void amr_task_output_format(amr_task_t *task, io_format_t format);

/* Writes to a ".safe" temporary file and renames it to the final name only
 * upon success. Prevents corrupted half-written files on crash. */
void amr_task_output_safe_mode(amr_task_t *task);

/* Writes an empty .ack file alongside the data file when complete. Useful
 * if an external non-AMR system is polling for this data. */
void amr_task_output_write_ack_file(amr_task_t *task);

/* Forces the output to be written with GZIP or LZ4 compression. */
void amr_task_output_gz(amr_task_t *task, int level);
void amr_task_output_lz4(amr_task_t *task, int level, lz4_block_size_t size, bool block_checksum, bool content_checksum);

/* Registers a custom string-builder for this output so it can be viewed
 * in human-readable format when using the `--dump` CLI flag. */
void amr_task_output_dump(amr_task_t *task, amr_task_dump_cb dump, void *arg);

/* When sorting, AMR merges temporary files. If num_per_group is set, AMR will
 * perform intermediate merges when it hits this many temp files, preventing
 * fd exhaustion on huge datasets. */
void amr_task_output_group_size(amr_task_t *task, size_t num_per_group, size_t start);

/* Spawns a background pthread dedicated to compressing and writing sorted chunks. */
void amr_task_output_use_extra_thread(amr_task_t *task);

/* Disables LZ4 compression for temporary sort files. Trades disk IO for CPU. */
void amr_task_output_dont_compress_tmp(amr_task_t *task);

/* ========================================================================
 * INPUT DECLARATIONS & MODIFIERS
 * Context: [Setup | Main Thread]
 *
 * ⚠️ IMPLICIT ATTRIBUTE INHERITANCE ⚠️
 * If you link an input to a previous task (e.g., amr_task_input_from_task_*),
 * the input AUTOMATICALLY inherits the format, sort, and reducer configurations
 * from the producer's output.
 *
 * You ONLY need to use the input configuration modifiers below if:
 * 1. You are reading raw external files via `amr_task_input_files()`.
 * 2. You intentionally want to apply a read-time reducer/sort that differs
 * from the upstream write-time configuration.
 * ======================================================================== */

/* Syntax Sugar for Input Edges */
void amr_task_input_from_task_first(amr_task_t *task, const char *prod, const char *out, double pct);
void amr_task_input_from_task_partition(amr_task_t *task, const char *prod, const char *out, double pct);
void amr_task_input_from_task_shuffle(amr_task_t *task, const char *prod, const char *out, double pct);
void amr_task_input_from_task_all_to_all(amr_task_t *task, const char *prod, const char *out, double pct);

/* Link an input directly to files on disk (Requires manual amr_task_input_format!). */
void amr_task_input_files(amr_task_t *task, const char *name, double ram_pct,
                          amr_worker_file_info_cb file_info);

/* Read an output from the exact same task topology executed in the PREVIOUS run.
 * Unlike normal edges, NO DAG dependency is registered for previous-run edges. */
void amr_task_input_from_previous_run(amr_task_t *task,
                                      const char *prev_task_name,
                                      const char *output_name,
                                      double in_ram_pct,
                                      size_t edge_flags);

/* Bypasses standard I/O readers. Tells the framework that you are manually
 * reading an opaque custom artifact. */
void amr_task_input_opaque(amr_task_t *task);

/* ----- Input Type Configuration ----- */
/* Validates that the upstream output matches this expected type */
void amr_task_input_expect_type(amr_task_t *task, const char *type_name);

/* ------------------------------------------------------------------------
 * INPUT MODIFIERS
 * ------------------------------------------------------------------------ */

/* Defines how the input stream is parsed (e.g., io_prefix(), io_delimiter('\n')).
 * NOTE: Automatically inherited if wired to an upstream task. Only required
 * for raw external files (amr_task_input_files). */
void amr_task_input_format(amr_task_t *task, io_format_t format);

/* Merge-sort multiple input streams using a registered strategy.
 *
 * IMPORTANT INHERITANCE: If this input is wired to a previous task (e.g.,
 * amr_task_input_from_task_*), it AUTOMATICALLY inherits the upstream output's
 * sort configuration. You ONLY need to call this if:
 * 1. You are reading raw external files (amr_task_input_files).
 * 2. You explicitly want to override the inherited write-time sort behavior.
 *
 * @param arg An optional pointer passed directly to the underlying
 * comparator callback as its 'tag' or state. Pass NULL if unused. */
void amr_task_input_sort_by(amr_task_t *task, const char *comparator_name, void *arg);

/* Merge-sort multiple input streams using a raw C callback.
 * IMPORTANT INHERITANCE: Automatically inherited from upstream tasks. See
 * amr_task_input_sort_by for details on when to use this.
 * @param arg Passed directly to the callback as its 'tag'. Pass NULL if unused. */
void amr_task_input_sort_with(amr_task_t *task, io_compare_cb compare, void *arg);

/* Aggregate/combine records during a merge-sort read using a registered strategy.
 *
 * IMPORTANT INHERITANCE: If this input is wired to a previous task, it
 * AUTOMATICALLY inherits the upstream output's reducer. You ONLY need to call
 * this to apply a read-time reducer to raw files, or to override the upstream reducer.
 *
 * @param arg An optional pointer passed directly to the underlying
 * reducer callback as its 'tag' or state. Pass NULL if unused. */
void amr_task_input_reduce_by(amr_task_t *task, const char *reducer_name, void *arg);

/* Aggregate/combine records during a merge-sort read using a raw C callback.
 * IMPORTANT INHERITANCE: Automatically inherited from upstream tasks. See
 * amr_task_input_reduce_by for details on when to use this.
 * @param arg Passed directly to the callback as its 'tag'. Pass NULL if unused. */
void amr_task_input_reduce_with(amr_task_t *task, io_reducer_cb reducer, void *arg);

/* Shorthand to automatically drop duplicate records with the same sort key during a read.
 * NOTE: Automatically inherited from upstream tasks if they used this reducer. */
void amr_task_input_reduce_by_keeping_first(amr_task_t *task);

/* Registers a custom string-builder for this input so it can be viewed
 * in human-readable format when using the `--dump` CLI flag. */
void amr_task_input_dump(amr_task_t *task, amr_task_dump_cb dump, void *arg);

/* Overrides the internal buffer size used for decompressing LZ4/GZ files. */
void amr_task_input_compressed_buffer_size(amr_task_t *task, size_t buffer_size);

/* Hard limit on the number of records to read from this input. Excellent for fast testing. */
void amr_task_input_limit(amr_task_t *task, size_t limit);

/* ========================================================================
 * PRE-LOADED MEMORY / DISTRIBUTED CACHE
 * ======================================================================== */

/* Context: [Setup | Main Thread]
 * Flags an input port to bypass standard I/O streams. The framework will
 * read all associated files entirely into RAM before executing the worker. */
void amr_task_input_load_into_memory(amr_task_t *task);

/* ========================================================================
 * TRANSFORM BINDINGS
 * Context: [Setup | Main Thread]
 * ======================================================================== */

/* Assign a custom worker execution function to this task. */
void amr_task_runner(amr_task_t *task, amr_worker_cb runner);
void amr_task_default_runner(amr_task_t *task);

/* Setup transforms: Connect named inputs to named outputs with a specific runner.
 * Order of string names determines local indices (0, 1...) passed to runner callbacks. */
void amr_task_transform(amr_task_t *task, const char *inp, const char *outp,
                        amr_runner_cb runner);

/* Binds a raw I/O runner, granting direct control over io_in_t and io_out_t handles. */
void amr_task_io_transform(amr_task_t *task, const char *inp, const char *outp,
                           amr_io_runner_cb runner);

/* Group runners feed chunks of records sharing the same key to your callback. */
void amr_task_group_transform(amr_task_t *task, const char *inp, const char *outp,
                              amr_group_runner_cb runner, io_compare_cb compare);

/* Manage custom memory/state needed by your transform runners. The create
 * callback fires before the worker starts, populating w->transform_data. */
void amr_task_group_compare_arg(amr_task_t *task, amr_worker_data_cb create,
                                amr_destroy_worker_data_cb destroy);
void amr_task_transform_data(amr_task_t *task, amr_worker_data_cb create,
                             amr_destroy_worker_data_cb destroy);

/* Control Task Execution logic */
void amr_task_do_nothing(amr_task_t *task);      /* Acts as a sync barrier */
void amr_task_run_everytime(amr_task_t *task);   /* Ignores checkpoint caching */

#ifdef __cplusplus
}
#endif

#endif /* AMR_TASK_H */
