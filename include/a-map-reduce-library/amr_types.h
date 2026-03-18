// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef AMR_TYPES_H
#define AMR_TYPES_H

#include "a-map-reduce-library/amr_core.h"
#include "a-memory-library/aml_buffer.h"
#include "a-memory-library/aml_pool.h"
#include "the-io-library/io.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * DATATYPE REGISTRY & TYPED I/O
 * ========================================================================
 * AMR provides a centralized type registry to map C structs to physical
 * I/O operations, sorting callbacks, and partitioning logic.
 *
 * SYSTEM CONSTRAINTS & BEHAVIORS:
 * 1. Optional Typing: Types are strictly optional. If an output or input
 * does not explicitly declare an expected type, AMR defaults to raw byte
 * processing via the underlying io_record_t structures.
 * 2. Mixed Graphs: Raw I/O and Typed I/O can be safely mixed within the
 * same DAG, provided the topology edges don't enforce a mismatch.
 * 3. Fail-Fast Validation: If an input expects Type A, but the upstream
 * producer outputs Type B (or lacks a type entirely), the DAG resolution
 * phase will instantly trigger an abort() before any workers are spawned.
 * ======================================================================== */

/* Context: [Type Registry]
 * Serializes a typed C struct into a physical byte buffer.
 * @param obj The populated C struct.
 * @param out_buffer The destination buffer. Ensure you call aml_buffer_append(). */
typedef void (*amr_serialize_fn)(const void *obj, aml_buffer_t *out_buffer);

/* Context: [Type Registry]
 * Deserializes a physical byte buffer into a typed C struct.
 * LIFETIME & OWNERSHIP: You must allocate the returning struct using the
 * provided pool. This pool is the worker's scratch pool, meaning the returned
 * struct is automatically garbage collected at the end of the current record
 * or group iteration. Do NOT call free(). */
typedef void* (*amr_deserialize_fn)(aml_pool_t *pool, const void *buffer, size_t len);

/* Context: [Type Registry]
 * Translates a typed C struct into a human-readable string representation.
 * Used exclusively by the `--dump` and `--sample` CLI diagnostics. */
typedef void (*amr_to_string_fn)(const void *obj, aml_buffer_t *out_buffer);

/* Context: [Init | Main Thread]
 * Registers a new data type with the framework. Must be called before `amr_run()`. */
void amr_register_datatype(amr_t *sched,
                           const char *datatype_name,
                           const char *desc,
                           amr_serialize_fn serialize,
                           amr_deserialize_fn deserialize,
                           amr_to_string_fn to_string);

/* Context: [Init | Main Thread]
 * Binds named behavioral callbacks to a registered type.
 * These string names (e.g., "Hash_ID", "Sort_Desc") can then be referenced
 * directly in task setup callbacks to trigger framework-level sorting and routing. */
void amr_datatype_add_compare(amr_t *sched, const char *datatype_name, const char *name, io_compare_cb cmp);
void amr_datatype_add_partition(amr_t *sched, const char *datatype_name, const char *name, io_partition_cb part);
void amr_datatype_add_reducer(amr_t *sched, const char *datatype_name, const char *name, io_reducer_cb reducer);

/* ========================================================================
 * WORKER-LEVEL SERIALIZATION ACCESSORS
 * Context: [Runtime | Worker Thread]
 * ======================================================================== */

/* Deserializes a raw record into a typed C struct using LOCAL indices.
 * * @param local_input_idx The local array index (0, 1, ...) corresponding to the
 * input's position in the string passed to `amr_task_transform()`.
 * (e.g., "inA|inB" -> inB is index 1).
 * * LIFETIME: Memory is allocated in the worker's scratch pool and lives
 * until the next record/group. */
void* amr_worker_deserialize(amr_worker_t *w, size_t local_input_idx, const io_record_t *r);

/* Serializes a typed C struct and writes it to the output port using LOCAL indices.
 * * @param local_output_idx The local array index (0, 1, ...) corresponding to the
 * output's position in the string passed to `amr_task_transform()`.
 * (e.g., "outA|outB" -> outB is index 1). */
void amr_worker_serialize(amr_worker_t *w, size_t local_output_idx, io_out_t *out, const void *obj);

#ifdef __cplusplus
}
#endif

#endif /* AMR_TYPES_H */
