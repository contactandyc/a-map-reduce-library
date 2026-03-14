// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "pipeline_inv_freq.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include <string.h>

// 1. IDList (Adjacency List for the Indices)
typedef struct { uint32_t key_id; uint32_t count; uint32_t elements[]; } id_list_t;
static void id_list_ser(const void *o, aml_buffer_t *bh) {
    const id_list_t *list = (const id_list_t *)o;
    aml_buffer_append(bh, list, sizeof(id_list_t) + (list->count * sizeof(uint32_t)));
}
static void* id_list_des(aml_pool_t *p __attribute__((unused)), const void *b, size_t l __attribute__((unused))) {
    return (void *)b;
}
static void id_list_str(const void *o, aml_buffer_t *bh) {
    const id_list_t *list = (const id_list_t *)o;
    aml_buffer_appendf(bh, "Key(%u) -> [%u elements]", list->key_id, list->count);
}
static size_t id_list_part(const io_record_t *r, size_t np, void *arg __attribute__((unused))) {
    return ((const id_list_t *)r->record)->key_id % np;
}

// 2. Custom Comparators & Hashers
static int idsp_cmp_str(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const char *s1 = (const char *)rA->record + sizeof(uint32_t);
    const char *s2 = (const char *)rB->record + sizeof(uint32_t);
    return strcmp(s1, s2);
}
static int sp_cmp_a(const io_record_t *a, const io_record_t *b, void *arg __attribute__((unused))) {
    return strcmp((const char *)a->record, (const char *)b->record);
}
static int u32p_cmp_b(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const amr_uint32_pair_t *a = (const amr_uint32_pair_t *)rA->record, *b = (const amr_uint32_pair_t *)rB->record;
    return a->b > b->b ? 1 : (a->b < b->b ? -1 : 0);
}
static int u32p_cmp_a(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const amr_uint32_pair_t *a = (const amr_uint32_pair_t *)rA->record, *b = (const amr_uint32_pair_t *)rB->record;
    return a->a > b->a ? 1 : (a->a < b->a ? -1 : 0);
}

// Extensions for the Billion-Scale configuration
static size_t idpw_part_b(const io_record_t *r, size_t np, void *arg __attribute__((unused))) {
    return ((const amr_id_pair_weight_t *)r->record)->b % np;
}
static int idpw_cmp_b_a(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const amr_id_pair_weight_t *a = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *b = (const amr_id_pair_weight_t *)rB->record;
    if (a->b > b->b) return 1;
    if (a->b < b->b) return -1;
    return a->a > b->a ? 1 : (a->a < b->a ? -1 : 0);
}

static void register_types(amr_t *sched) {
    static bool registered = false;
    if (registered) return;
    amr_register_datatype(sched, "IDList", "", id_list_ser, id_list_des, id_list_str);
    amr_datatype_add_partition(sched, "IDList", "Hash_ID", id_list_part);

    // Register extensions for IDPairWeight used in config switching
    amr_datatype_add_partition(sched, "IDPairWeight", "Hash_B", idpw_part_b);
    amr_datatype_add_compare(sched, "IDPairWeight", "Sort_B_A", idpw_cmp_b_a);
    registered = true;
}

/* ========================================================================
 * STRING -> INTEGER ENCODING
 * ======================================================================== */
static void assign_users_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    uint32_t p = amr_worker_partition(w), P = amr_worker_num_partitions(w), local_idx = 0;
    size_t num_r = 0; bool more = false; io_record_t *g;

    while ((g = io_in_advance_group(ins[0], &num_r, &more, sp_cmp_a, NULL)) != NULL) {
        aml_pool_clear(amr_worker_scratch_pool(w));
        uint32_t user_id = (local_idx * P) + p; local_idx++;
        for (size_t i = 0; i < num_r; i++) {
            amr_string_pair_t *pair = (amr_string_pair_t *)amr_worker_deserialize(w, 0, &g[i]);
            amr_id_string_pair_t out = { user_id, pair->b };
            amr_worker_serialize(w, 0, outs[0], &out);
        }
    }
}
static bool assign_users_setup(amr_task_t *t) {
    amr_task_input_from_pipeline_port_shuffle(t, "in_sessions", 1.0);
    amr_task_input_expect_type(t, "StringPair");

    amr_task_output(t, "user_id_item_str.bin", 0.5);
    amr_task_output_type(t, "IdStringPair");
    amr_task_output_shuffle_by(t, "Hash_Str", NULL);
    amr_task_output_sort_by(t, "Sort_Str", NULL);

    amr_task_io_transform(t, "in_sessions", "user_id_item_str.bin", assign_users_runner);
    return true;
}

static void assign_items_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    uint32_t p = amr_worker_partition(w), P = amr_worker_num_partitions(w), local_idx = 0;
    size_t num_r = 0; bool more = false; io_record_t *g;

    while ((g = io_in_advance_group(ins[0], &num_r, &more, idsp_cmp_str, NULL)) != NULL) {
        aml_pool_clear(amr_worker_scratch_pool(w));
        uint32_t item_id = (local_idx * P) + p; local_idx++;

        amr_id_string_pair_t *first = (amr_id_string_pair_t *)amr_worker_deserialize(w, 0, &g[0]);
        amr_id_string_pair_t dict_out = { item_id, first->str };
        amr_worker_serialize(w, 0, outs[0], &dict_out);

        amr_uint32_pair_t count_out = { item_id, (uint32_t)num_r };
        amr_worker_serialize(w, 2, outs[2], &count_out);

        for (size_t i = 0; i < num_r; i++) {
            amr_id_string_pair_t *pair = (amr_id_string_pair_t *)amr_worker_deserialize(w, 0, &g[i]);
            amr_uint32_pair_t sess_out = { pair->id, item_id };
            amr_worker_serialize(w, 1, outs[1], &sess_out);
        }
    }
}
static bool assign_items_setup(amr_task_t *t) {
    amr_task_input_from_sibling_shuffle(t, "assign_users", "user_id_item_str.bin", 0.5);
    amr_task_input_expect_type(t, "IdStringPair");

    amr_task_output(t, "item_dict.bin", 0.2);
    amr_task_output_type(t, "IdStringPair");

    amr_task_output(t, "binary_sessions.bin", 0.4);
    amr_task_output_type(t, "UInt32Pair");
    amr_task_output_shuffle_by(t, "Hash_B", NULL);
    amr_task_output_sort_by(t, "Sort_B_A", NULL);

    amr_task_output(t, "item_counts.bin", 0.05);
    amr_task_output_type(t, "UInt32Pair");

    amr_task_io_transform(t, "user_id_item_str.bin", "item_dict.bin|binary_sessions.bin|item_counts.bin", assign_items_runner);
    return true;
}

/* ========================================================================
 * INVERTED INDEX ALGORITHM
 * ======================================================================== */
static void extract_graphs_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    uint32_t P = amr_worker_num_partitions(w);
    aml_pool_t *scratch = amr_worker_scratch_pool(w);

    io_out_options_t opts; io_out_options_init(&opts);
    io_out_options_format(&opts, io_fixed(sizeof(amr_uint32_pair_t)));
    io_out_options_buffer_size(&opts, amr_worker_ram(w, 0.1));
    io_out_ext_options_t ext_opts; io_out_ext_options_init(&ext_opts);
    io_out_ext_options_compare(&ext_opts, u32p_cmp_a, NULL);

    char *tmp_path = amr_worker_output_base2(w, amr_worker_output(w, 1), ".tmp_u");
    io_out_t *tmp_out = io_out_ext_init(tmp_path, &opts, &ext_opts);

    size_t num_r = 0; bool more = false; io_record_t *g;

    while ((g = io_in_advance_group(ins[0], &num_r, &more, u32p_cmp_b, NULL)) != NULL) {
        aml_pool_clear(scratch);
        uint32_t item_id = ((amr_uint32_pair_t *)g[0].record)->b;

        uint32_t item_id_local = item_id / P;

        id_list_t *i_list = (id_list_t *)aml_pool_alloc(scratch, sizeof(id_list_t) + (num_r * sizeof(uint32_t)));
        i_list->key_id = item_id; i_list->count = num_r;

        for (size_t i = 0; i < num_r; i++) {
            amr_uint32_pair_t *p = (amr_uint32_pair_t *)g[i].record;
            i_list->elements[i] = p->a;

            amr_uint32_pair_t local_pair = { p->a, item_id_local };
            io_out_write_record(tmp_out, &local_pair, sizeof(amr_uint32_pair_t));
        }
        amr_worker_serialize(w, 0, outs[0], i_list);
    }

    io_in_t *tmp_in = io_out_in(tmp_out);
    while ((g = io_in_advance_group(tmp_in, &num_r, &more, u32p_cmp_a, NULL)) != NULL) {
        aml_pool_clear(scratch);
        uint32_t user_id = ((amr_uint32_pair_t *)g[0].record)->a;
        id_list_t *u_list = (id_list_t *)aml_pool_alloc(scratch, sizeof(id_list_t) + (num_r * sizeof(uint32_t)));
        u_list->key_id = user_id; u_list->count = num_r;
        for (size_t i = 0; i < num_r; i++) u_list->elements[i] = ((amr_uint32_pair_t *)g[i].record)->b;
        amr_worker_serialize(w, 1, outs[1], u_list);
    }
    io_in_destroy(tmp_in);
}

static bool extract_graphs_setup(amr_task_t *t) {
    amr_task_input_from_sibling_shuffle(t, "assign_items", "binary_sessions.bin", 0.5);
    amr_task_input_expect_type(t, "UInt32Pair");

    amr_task_output(t, "item_to_users.bin", 0.3);
    amr_task_output_type(t, "IDList");
    amr_task_output(t, "user_to_partial_items.bin", 0.3);
    amr_task_output_type(t, "IDList");

    amr_task_io_transform(t, "binary_sessions.bin", "item_to_users.bin|user_to_partial_items.bin", extract_graphs_runner);
    return true;
}

static void intersect_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    int min_overlap = 1;
    amr_pipeline_t *pipe = amr_worker_pipeline(w);
    if (pipe && amr_pipeline_config(pipe)) {
        min_overlap = ((inv_freq_config_t *)amr_pipeline_config(pipe))->min_overlap;
    }

    aml_pool_t *persist = amr_worker_pool(w);
    uint32_t P = amr_worker_num_partitions(w);
    uint32_t p = amr_worker_partition(w);

    size_t num_f; amr_loaded_data_t *d_users = amr_worker_loaded_data(w, 0, &num_f);

    uint32_t max_u = 0, max_i_local = 0;
    for (size_t f = 0; f < num_f; f++) {
        char *ptr = (char *)d_users[f].buffer, *end = ptr + d_users[f].length;
        while (ptr < end) {
            uint32_t r_len = *(uint32_t *)ptr; ptr += sizeof(uint32_t);
            id_list_t *l = (id_list_t *)ptr;
            if (l->key_id > max_u) max_u = l->key_id;
            for(size_t i=0; i<l->count; i++) { if(l->elements[i] > max_i_local) max_i_local = l->elements[i]; }
            ptr += r_len;
        }
    }

    id_list_t **u_arr = (id_list_t **)aml_pool_zalloc(persist, (max_u + 1) * sizeof(id_list_t*));
    double *acc = (double *)aml_pool_zalloc(persist, (max_i_local + 1) * sizeof(double));
    uint32_t *pop = (uint32_t *)aml_pool_alloc(persist, (max_i_local + 1) * sizeof(uint32_t));
    uint32_t pop_c = 0;

    for (size_t f = 0; f < num_f; f++) {
        char *ptr = (char *)d_users[f].buffer, *end = ptr + d_users[f].length;
        while (ptr < end) {
            uint32_t r_len = *(uint32_t *)ptr; ptr += sizeof(uint32_t);
            id_list_t *l = (id_list_t *)ptr;
            u_arr[l->key_id] = l;
            ptr += r_len;
        }
    }

    io_record_t *r;
    while ((r = io_in_advance(ins[1]))) {
        id_list_t *item_x = (id_list_t *)r->record;
        uint32_t X = item_x->key_id;

        for (size_t i = 0; i < item_x->count; i++) {
            uint32_t U = item_x->elements[i];
            if (U <= max_u && u_arr[U]) {
                for (size_t j = 0; j < u_arr[U]->count; j++) {
                    uint32_t A_local = u_arr[U]->elements[j];
                    if (acc[A_local] == 0.0) pop[pop_c++] = A_local;
                    acc[A_local] += 1.0;
                }
            }
        }

        for (size_t i = 0; i < pop_c; i++) {
            uint32_t A_local = pop[i];
            uint32_t A_global = (A_local * P) + p;

            // ZERO-SHUFFLE OPTIMIZATION
            if (X != A_global) {
                double fAB = acc[A_local];
                if (fAB >= min_overlap) {
                    amr_id_pair_weight_t out = { A_global, X, fAB };
                    amr_worker_serialize(w, 0, outs[0], &out);
                }
            }
            acc[A_local] = 0.0;
        }
        pop_c = 0;
    }
}

static bool intersect_setup(amr_task_t *t) {
    amr_task_input_from_sibling_partition(t, "extract_graphs", "user_to_partial_items.bin", 0.5);
    amr_task_input_expect_type(t, "IDList");
    amr_task_input_load_into_memory(t);

    amr_task_input_from_sibling_all_to_all(t, "extract_graphs", "item_to_users.bin", 0.3);
    amr_task_input_expect_type(t, "IDList");

    amr_task_output(t, "co_freqs_int.bin", 0.5);
    amr_task_output_type(t, "IDPairWeight");

    amr_pipeline_t *pipe = amr_task_pipeline(t);
    inv_freq_config_t *cfg = pipe ? (inv_freq_config_t *)amr_pipeline_config(pipe) : NULL;

    // THE DYNAMIC CONFIGURATION
    if (cfg && cfg->out_mode == INV_FREQ_OUT_B_A) {
        // Prepare data directly for downstream merge-joins!
        amr_task_output_shuffle_by(t, "Hash_B", NULL);
        amr_task_output_sort_by(t, "Sort_B_A", NULL);
    } else {
        // Standard zero-shuffle, perfectly structured for O(1) formatting
        amr_task_output_sort_by(t, "Sort_A_WDesc", NULL);
    }

    amr_task_io_transform(t, "user_to_partial_items.bin|item_to_users.bin", "co_freqs_int.bin", intersect_runner);
    return true;
}

bool pipeline_inv_freq_setup(amr_pipeline_t *p) {
    register_types(amr_pipeline_scheduler(p));

    inv_freq_config_t *cfg = (inv_freq_config_t *)amr_pipeline_config(p);
    inv_freq_out_mode_t mode = cfg ? cfg->out_mode : INV_FREQ_OUT_A_WDESC;

    amr_pipeline_task(p, "assign_users",   true, assign_users_setup);
    amr_pipeline_task(p, "assign_items",   true, assign_items_setup);
    amr_pipeline_task(p, "extract_graphs", true, extract_graphs_setup);

    amr_pipeline_bind_output(p, "out_item_dict", "assign_items", "item_dict.bin");
    amr_pipeline_bind_output(p, "out_item_counts", "assign_items", "item_counts.bin");

    // Expose raw bipartite graphs for advanced external math!
    amr_pipeline_bind_output(p, "out_item_to_users", "extract_graphs", "item_to_users.bin");
    amr_pipeline_bind_output(p, "out_user_to_partial_items", "extract_graphs", "user_to_partial_items.bin");

    // Conditionally execute the intersection
    if (mode != INV_FREQ_INDEX_ONLY) {
        amr_pipeline_task(p, "intersect", true, intersect_setup);
        amr_pipeline_bind_output(p, "out_freqs", "intersect", "co_freqs_int.bin");
    }

    return true;
}
