// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "amr_datatypes.h"
#include <string.h>
#include <stdio.h>

/* ================================================================
 * Internal Structures
 * ================================================================ */
struct amr_datatype_registry_s {
    aml_pool_t *pool;
    macro_map_t *datatypes;
};

/* ================================================================
 * Intrusive Maps (Datatypes, Comparators, Partitioners)
 * ================================================================ */

/* 1. Datatypes Map */
static int compare_dt_node(const char *key, const amr_datatype_t *n) { return strcmp(key, n->name); }
static int compare_dt_insert(const amr_datatype_t *a, const amr_datatype_t *b) { return strcmp(a->name, b->name); }
macro_map_find_kv(dt_find, char, amr_datatype_t, compare_dt_node)
macro_map_insert(dt_insert, amr_datatype_t, compare_dt_insert)

/* 2. Comparators Map */
typedef struct { macro_map_t node; char *name; io_compare_cb func; } amr_cmp_node_t;
static int compare_cmp_node(const char *key, const amr_cmp_node_t *n) { return strcmp(key, n->name); }
static int compare_cmp_insert(const amr_cmp_node_t *a, const amr_cmp_node_t *b) { return strcmp(a->name, b->name); }
macro_map_find_kv(cmp_find, char, amr_cmp_node_t, compare_cmp_node)
macro_map_insert(cmp_insert, amr_cmp_node_t, compare_cmp_insert)

/* 3. Partitioners Map */
typedef struct { macro_map_t node; char *name; io_partition_cb func; } amr_part_node_t;
static int compare_part_node(const char *key, const amr_part_node_t *n) { return strcmp(key, n->name); }
static int compare_part_insert(const amr_part_node_t *a, const amr_part_node_t *b) { return strcmp(a->name, b->name); }
macro_map_find_kv(part_find, char, amr_part_node_t, compare_part_node)
macro_map_insert(part_insert, amr_part_node_t, compare_part_insert)


/* ================================================================
 * Registry Lifecycle
 * ================================================================ */
amr_datatype_registry_t* amr_datatype_registry_init(void) {
    aml_pool_t *p = aml_pool_init(4096);
    if (!p) return NULL;

    amr_datatype_registry_t *reg = aml_pool_zalloc(p, sizeof(amr_datatype_registry_t));
    reg->pool = p;
    return reg;
}

void amr_datatype_registry_destroy(amr_datatype_registry_t *reg) {
    if (!reg) return;
    aml_pool_destroy(reg->pool);
}


/* ================================================================
 * Public Type Registration API
 * ================================================================ */
void amr_register_datatype(amr_t *amr,
                           const char *datatype_name, const char *description,
                           amr_serialize_fn serialize,
                           amr_deserialize_fn deserialize,
                           amr_to_string_fn to_string) {
    if (!amr || !amr->types || !datatype_name) return;

    amr_datatype_registry_t *reg = amr->types;

    if (dt_find(reg->datatypes, (char*)datatype_name)) {
        fprintf(stderr, "[WARN] Datatype '%s' is already registered.\n", datatype_name);
        return;
    }

    amr_datatype_t *dt = aml_pool_zalloc(reg->pool, sizeof(amr_datatype_t));
    dt->pool = reg->pool;
    dt->name = aml_pool_strdup(reg->pool, datatype_name);
    dt->description = description ? aml_pool_strdup(reg->pool, description) : NULL;

    dt->serialize = serialize;
    dt->deserialize = deserialize;
    dt->to_string = to_string;

    dt_insert(&reg->datatypes, dt);
}

void amr_datatype_add_compare(amr_t *amr, const char *datatype_name, const char *name, io_compare_cb cmp_fn) {
    if (!amr || !amr->types || !datatype_name || !name || !cmp_fn) return;

    amr_datatype_t *dt = dt_find(amr->types->datatypes, (char*)datatype_name);
    if (!dt) {
        fprintf(stderr, "[ERROR] Tried to add comparator '%s' to unknown datatype '%s'\n", name, datatype_name);
        return;
    }

    amr_cmp_node_t *n = aml_pool_zalloc(dt->pool, sizeof(amr_cmp_node_t));
    n->name = aml_pool_strdup(dt->pool, name);
    n->func = cmp_fn;
    cmp_insert(&dt->comparators, n);
}

void amr_datatype_add_partition(amr_t *amr, const char *datatype_name, const char *name, io_partition_cb part_fn) {
    if (!amr || !amr->types || !datatype_name || !name || !part_fn) return;

    amr_datatype_t *dt = dt_find(amr->types->datatypes, (char*)datatype_name);
    if (!dt) {
        fprintf(stderr, "[ERROR] Tried to add partitioner '%s' to unknown datatype '%s'\n", name, datatype_name);
        return;
    }

    amr_part_node_t *n = aml_pool_zalloc(dt->pool, sizeof(amr_part_node_t));
    n->name = aml_pool_strdup(dt->pool, name);
    n->func = part_fn;
    part_insert(&dt->partitioners, n);
}


/* ================================================================
 * Internal Lookup Functions (For DAG Edge Setup)
 * ================================================================ */
amr_datatype_t* amr_datatype_find(amr_datatype_registry_t *reg, const char *name) {
    if (!reg || !name) return NULL;
    return dt_find(reg->datatypes, (char*)name);
}

io_compare_cb amr_datatype_get_compare(amr_datatype_t *dt, const char *name) {
    if (!dt || !name) return NULL;
    amr_cmp_node_t *n = cmp_find(dt->comparators, (char*)name);
    return n ? n->func : NULL;
}

io_partition_cb amr_datatype_get_partition(amr_datatype_t *dt, const char *name) {
    if (!dt || !name) return NULL;
    amr_part_node_t *n = part_find(dt->partitioners, (char*)name);
    return n ? n->func : NULL;
}

/* ================================================================
 * 4. Reducers Map (NEW)
 * ================================================================ */
typedef struct {
    macro_map_t node;
    char *name;
    io_reducer_cb func;
} amr_red_node_t;

static int compare_red_node(const char *key, const amr_red_node_t *n) {
    return strcmp(key, n->name);
}
static int compare_red_insert(const amr_red_node_t *a, const amr_red_node_t *b) {
    return strcmp(a->name, b->name);
}

macro_map_find_kv(red_find, char, amr_red_node_t, compare_red_node)
macro_map_insert(red_insert, amr_red_node_t, compare_red_insert)

/* ================================================================
 * Public Registration API Additions
 * ================================================================ */

void amr_datatype_add_reducer(amr_t *amr, const char *datatype_name, const char *name, io_reducer_cb red_fn) {
    if (!amr || !amr->types || !datatype_name || !name || !red_fn) return;

    amr_datatype_t *dt = dt_find(amr->types->datatypes, (char*)datatype_name);
    if (!dt) {
        fprintf(stderr, "[ERROR] Tried to add reducer '%s' to unknown datatype '%s'\n", name, datatype_name);
        return;
    }

    amr_red_node_t *n = aml_pool_zalloc(dt->pool, sizeof(amr_red_node_t));
    n->name = aml_pool_strdup(dt->pool, name);
    n->func = red_fn;
    red_insert(&dt->reducers, n);
}

/* ================================================================
 * Internal Lookup Functions Addition
 * ================================================================ */

io_reducer_cb amr_datatype_get_reducer(amr_datatype_t *dt, const char *name) {
    if (!dt || !name) return NULL;
    amr_red_node_t *n = red_find(dt->reducers, (char*)name);
    return n ? n->func : NULL;
}
