// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef AMR_COMMON_DATATYPES_H
#define AMR_COMMON_DATATYPES_H

#ifdef __cplusplus
extern "C" {
#endif

struct amr_s;

/* ========================================================================
 * AMR STANDARD LIBRARY DATA TYPES
 * ======================================================================== */

typedef struct {
    char *a;
    char *b;
} amr_string_pair_t;

typedef struct {
    char *str;
} amr_string_singleton_t;

typedef struct {
    double weight;
    char *str;
} amr_string_weight_t;

typedef struct {
    double weight;
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

void amr_register_common_datatypes(struct amr_s *sched);

#ifdef __cplusplus
}
#endif

#endif /* AMR_COMMON_DATATYPES_H */
