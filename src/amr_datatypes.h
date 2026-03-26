// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef INTERNAL_AMR_DATATYPES_H
#define INTERNAL_AMR_DATATYPES_H

#include "amr_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ================================================================
 * Internal Registry API (Used by AMR DAG Scheduler)
 * ================================================================ */

/* The actual datatype struct. Exposed internally so amr_task.c and amr_worker.c
   can invoke the function pointers (serialize, deserialize, to_string) directly. */
struct amr_datatype_s {
    macro_map_t node;

    char *name;
    char *description;

    amr_serialize_fn serialize;
    amr_deserialize_fn deserialize;
    amr_to_string_fn to_string;

    macro_map_t *comparators;
    macro_map_t *partitioners;
    macro_map_t *reducers;

    aml_pool_t *pool;
};

amr_datatype_registry_t* amr_datatype_registry_init(void);
void amr_datatype_registry_destroy(amr_datatype_registry_t *reg);

amr_datatype_t* amr_datatype_find(amr_datatype_registry_t *reg, const char *name);
io_compare_cb amr_datatype_get_compare(amr_datatype_t *dt, const char *name);
io_partition_cb amr_datatype_get_partition(amr_datatype_t *dt, const char *name);
io_reducer_cb amr_datatype_get_reducer(amr_datatype_t *dt, const char *name);

#ifdef __cplusplus
}
#endif

#endif // AMR_DATATYPES_H
