// SPDX-FileCopyrightText: 2025-2026 Andy worker_skip_output@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef AMR_WORKER_H
#define AMR_WORKER_H

#include "a-map-reduce-library/amr_core.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * WORKER RUNTIME CONTEXT & ACCESSORS
 * ========================================================================
 * The amr_worker_t is the opaque execution context passed to your runner
 * callbacks. It represents a single thread processing a single partition.
 *
 * GLOBAL OWNERSHIP NOTE: Unless otherwise stated, strings and path arrays
 * returned by amr_worker_* helpers are allocated in the worker's persistent
 * pool and remain valid for the lifetime of the worker. Do NOT call free()
 * on them.
 * ======================================================================== */

/* --- Memory Management (Allocated by Framework) --- */
aml_pool_t *amr_worker_pool(amr_worker_t *w);
aml_pool_t *amr_worker_scratch_pool(amr_worker_t *w);
aml_buffer_t *amr_worker_buffer(amr_worker_t *w);

/* --- Execution State (Read-Only) --- */
void *amr_worker_custom_arg(const amr_worker_t *w);
amr_t *amr_worker_scheduler(const amr_worker_t *w);
void *amr_worker_transform_data(const amr_worker_t *w);
void *amr_worker_data(const amr_worker_t *w);
size_t amr_worker_partition(const amr_worker_t *w);
size_t amr_worker_num_partitions(const amr_worker_t *w);
size_t amr_worker_thread_id(const amr_worker_t *w);
bool amr_worker_debug(const amr_worker_t *w);
size_t amr_worker_ram(amr_worker_t *w, double pct);

/* --- Physical I/O Accessors --- */
io_out_t *amr_worker_out(amr_worker_t *w, size_t output_idx);
int amr_worker_out_as_file(amr_worker_t *w, size_t output_idx);
io_in_t *amr_worker_in(amr_worker_t *w, size_t input_idx);
amr_worker_output_t *amr_worker_output(amr_worker_t *w, size_t n);
amr_worker_input_t *amr_worker_input(amr_worker_t *w, size_t n);
char *amr_worker_read(amr_worker_t *w, size_t *num_records, char **endp, size_t input_idx);

/* --- Pre-Loaded Memory Cache --- */
typedef struct {
    void *buffer;
    size_t length;
    size_t partition;
} amr_loaded_data_t;

amr_loaded_data_t *amr_worker_loaded_data(amr_worker_t *w, size_t input_idx, size_t *num_data);
amr_loaded_data_t *amr_worker_loaded_data_for_partition(amr_worker_t *w, size_t input_idx, size_t partition);

/* --- Opaque Artifact Utilities --- */
char **amr_worker_opaque_inputs(amr_worker_t *w, size_t input_idx, size_t *num_paths);
char **amr_worker_opaque_outputs(amr_worker_t *w, size_t output_idx, size_t *num_paths);
FILE **amr_worker_open_opaque_inputs(amr_worker_t *w, size_t input_idx, const char *mode, size_t *num_files);
FILE **amr_worker_open_opaque_outputs(amr_worker_t *w, size_t output_idx, const char *mode, size_t *num_files);
void amr_worker_close_opaque_files(FILE **files, size_t num_files);

/* --- Path & Param Utilities --- */
char *amr_worker_output_params(amr_worker_t *w, size_t n);
char *amr_worker_input_params(amr_worker_t *w, size_t n);
char *amr_worker_output_base(const amr_worker_t *w, const amr_worker_output_t *outp);
char *amr_worker_output_base2(const amr_worker_t *w, const amr_worker_output_t *outp, const char *suffix);
char *amr_worker_input_name(const amr_worker_t *w, const amr_worker_input_t *inp, size_t partition);

/* --- Callbacks & Debug --- */
void amr_task_dump_text(amr_worker_t *w, io_record_t *r, aml_buffer_t *bh, void *arg);
void amr_worker_dump_input(amr_worker_t *w, io_record_t *r, size_t n);
const char *amr_task_name(const amr_task_t *task);

/* =========================================================================
 * CALLBACK ACCESSORS
 * Context: [Runtime | Worker Thread]
 * Retrieves the configured runtime callbacks and their arguments for specific ports.
 * This allows you to write highly generic runners that defer to the type registry.
 * ========================================================================= */

/*
COMPARATOR REQUIREMENTS FOR JOINS:
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


/* =========================================================================
 * CONDITIONAL BRANCHING & DAG PRUNING
 * ========================================================================= */

/*
 * Dynamically skips the specified local output port for this partition.
 * * If an output is skipped, NO files are written to disk for it. More importantly,
 * any downstream DAG tasks (or pipeline branches) that depend exclusively on
 * this specific partition's output will be deliberately starved and skipped
 * as well (Cascading Skips).
 * * This is a control-flow toggle, not just a data toggle.
 * * @param local_output_idx The local array index (0, 1, ...) corresponding to the
 * output's position in the string passed to `amr_task_transform()`.
 */
void amr_worker_skip_output(amr_worker_t *w, size_t local_output_idx);

#ifdef __cplusplus
}
#endif

#endif /* AMR_WORKER_H */
