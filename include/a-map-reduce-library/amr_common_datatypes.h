// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef AMR_COMMON_DATATYPES_H
#define AMR_COMMON_DATATYPES_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct amr_s;

/* ========================================================================
 * AMR STANDARD LIBRARY DATA TYPES
 * ========================================================================
 * ⚠️ MEMORY OWNERSHIP AND LIFETIME RULES ⚠️
 *
 * When these structs are deserialized during worker execution (via
 * amr_worker_deserialize), memory is allocated using the worker's
 * scratch pool.
 *
 * 1. DO NOT call `free()` on any pointers (like `char *a` or `char *b`)
 * inside these structs.
 * 2. LIFETIME: The strings and structs are only guaranteed to be valid
 * for the duration of the current record/group loop iteration. The
 * framework automatically clears the scratch pool to prevent OOM.
 * 3. PERSISTENCE: If you need to save a string across loop iterations,
 * you MUST duplicate it into the worker's persistent pool using:
 * `aml_pool_strdup(amr_worker_pool(w), my_struct->a);`
 * ======================================================================== */

typedef struct {
    char *a;
    char *b;
} amr_string_pair_t;

typedef struct {
    char *str;
} amr_string_singleton_t;

typedef struct {
    double w;
    char *str;
} amr_string_weight_t;

typedef struct {
    double w;
    char *a;
    char *b;
} amr_string_pair_weight_t;

typedef struct {
    double w;
    double aw;
    double bw;
    char *a;
    char *b;
} amr_string_pair_weights_t;

typedef struct {
    uint32_t val;
} amr_uint32_singleton_t;

typedef struct {
    uint32_t a;
    uint32_t b;
} amr_uint32_pair_t;

typedef struct {
    uint32_t id;
    char *str;
} amr_id_string_pair_t;

typedef struct {
    uint32_t a;
    uint32_t b;
    double w;
} amr_id_pair_weight_t;

typedef struct {
    uint32_t a;
    uint32_t b;
    double w;
    double aw;
    double bw;
} amr_id_pair_weights_t;

/* Context: [Init | Main Thread]
 * Registers all of the common datatypes above into the scheduler's
 * type registry, enabling automatic serialization, partitioning, and sorting. */
void amr_register_common_datatypes(struct amr_s *sched);

#ifdef __cplusplus
}
#endif

#endif /* AMR_COMMON_DATATYPES_H */
