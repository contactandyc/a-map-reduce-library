// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "pipeline_substitutes.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>

/* ========================================================================
 * INTERNAL DATATYPE
 * ======================================================================== */
typedef struct { uint32_t id; float w; } amr_id_weight_t;
typedef struct { uint32_t key_id; uint32_t count; amr_id_weight_t elements[]; } amr_id_weight_list_t;

static void idwl_ser(const void *o, aml_buffer_t *bh) {
    const amr_id_weight_list_t *list = (const amr_id_weight_list_t *)o;
    aml_buffer_append(bh, list, sizeof(amr_id_weight_list_t) + (list->count * sizeof(amr_id_weight_t)));
}
static void* idwl_des(aml_pool_t *p __attribute__((unused)), const void *b, size_t l __attribute__((unused))) {
    return (void *)b;
}
static void idwl_str(const void *o, aml_buffer_t *bh) {
    const amr_id_weight_list_t *list = (const amr_id_weight_list_t *)o;
    aml_buffer_appendf(bh, "Key(%u) -> [%u features]", list->key_id, list->count);
}
static size_t idwl_part(const io_record_t *r, size_t np, void *arg __attribute__((unused))) {
    return ((const amr_id_weight_list_t *)r->record)->key_id % np;
}

static void register_internal_types(amr_t *sched) {
    static bool registered = false;
    if (registered) return;
    amr_register_datatype(sched, "IDWeightList", "", idwl_ser, idwl_des, idwl_str);
    amr_datatype_add_partition(sched, "IDWeightList", "Hash_ID", idwl_part);
    registered = true;
}

/* ========================================================================
 * 1. TF-IDF & L2 NORMALIZATION
 * ======================================================================== */
typedef struct { uint32_t *counts; uint32_t max_id; } idf_context_t;

static void* load_idf_counts(amr_worker_t *w) {
    size_t num_data;
    amr_loaded_data_t *d = amr_worker_loaded_data(w, 1, &num_data);
    uint32_t max_id = 0;

    for(size_t i=0; i<num_data; i++) {
        char *p = (char *)d[i].buffer, *end = p + d[i].length;
        while(p < end) {
            uint32_t rlen = *(uint32_t*)p; p += 4;
            amr_uint32_pair_t *pair = (amr_uint32_pair_t *)p;
            if (pair->a > max_id) max_id = pair->a;
            p += rlen;
        }
    }

    idf_context_t *ctx = (idf_context_t *)aml_pool_alloc(amr_worker_pool(w), sizeof(idf_context_t));
    ctx->counts = (uint32_t *)aml_pool_zalloc(amr_worker_pool(w), (max_id + 1) * sizeof(uint32_t));
    ctx->max_id = max_id;

    for(size_t i=0; i<num_data; i++) {
        char *p = (char *)d[i].buffer, *end = p + d[i].length;
        while(p < end) {
            uint32_t rlen = *(uint32_t*)p; p += 4;
            amr_uint32_pair_t *pair = (amr_uint32_pair_t *)p;
            ctx->counts[pair->a] = pair->b;
            p += rlen;
        }
    }
    return ctx;
}

static int qsort_idpw_wdesc(const void *a, const void *b) {
    double wa = ((const amr_id_pair_weight_t *)((const io_record_t *)a)->record)->w;
    double wb = ((const amr_id_pair_weight_t *)((const io_record_t *)b)->record)->w;
    return (wa > wb) ? -1 : (wa < wb ? 1 : 0);
}

static void norm_runner(amr_worker_t *w, io_record_t *first, size_t num_r, io_out_t **outs) {
    // FIX: Require a minimum vector density to participate in Second-Order similarity!
    if (num_r < 3) return;

    aml_pool_t *scratch = amr_worker_scratch_pool(w);
    idf_context_t *ctx = (idf_context_t *)amr_worker_transform_data(w);
    double N = (double)ctx->max_id;

    amr_id_pair_weight_t *penalized = (amr_id_pair_weight_t *)aml_pool_alloc(scratch, num_r * sizeof(amr_id_pair_weight_t));
    io_record_t *sorted = (io_record_t *)aml_pool_alloc(scratch, num_r * sizeof(io_record_t));

    for (size_t i = 0; i < num_r; i++) {
        penalized[i] = *(amr_id_pair_weight_t *)first[i].record;
        uint32_t neighbor_id = penalized[i].b;

        double doc_freq = (neighbor_id <= ctx->max_id) ? (double)ctx->counts[neighbor_id] : 1.0;
        if (doc_freq < 1.0) doc_freq = 1.0;

        // TF-IDF Math: Logarithmic discount for popular hubs
        double idf = log((N + 1.0) / doc_freq) + 1.0;
        penalized[i].w *= idf;

        sorted[i].record = (char *)&penalized[i];
        sorted[i].length = sizeof(amr_id_pair_weight_t);
    }

    qsort(sorted, num_r, sizeof(io_record_t), qsort_idpw_wdesc);

    size_t limit = num_r > 50 ? 50 : num_r; // Keep top 50 strongest features
    double sum_sq = 0.0;
    for (size_t i = 0; i < limit; i++) {
        double val = ((amr_id_pair_weight_t *)sorted[i].record)->w;
        sum_sq += (val * val);
    }
    double l2 = sqrt(sum_sq);
    if (l2 == 0.0) return;

    for (size_t i = 0; i < limit; i++) {
        amr_id_pair_weight_t out = *(amr_id_pair_weight_t *)sorted[i].record;
        out.w = out.w / l2; // L2 Normalization
        io_out_write_record(outs[0], &out, sizeof(amr_id_pair_weight_t));
    }
}

static int local_idpw_cmp_a(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const amr_id_pair_weight_t *a = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *b = (const amr_id_pair_weight_t *)rB->record;
    return (a->a > b->a) ? 1 : (a->a < b->a ? -1 : 0);
}

static int local_idpw_cmp_b(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const amr_id_pair_weight_t *a = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *b = (const amr_id_pair_weight_t *)rB->record;
    return (a->b > b->b) ? 1 : (a->b < b->b ? -1 : 0);
}

static bool sim_norm_setup(amr_task_t *t) {
    amr_task_input_from_pipeline_port_partition(t, "in_freqs", 0.5);
    amr_task_input_expect_type(t, "IDPairWeight");

    amr_task_input_from_pipeline_port_all_to_all(t, "in_counts", 0.1);
    amr_task_input_expect_type(t, "UInt32Pair");
    amr_task_input_load_into_memory(t);

    amr_task_output(t, "norm_pairs.bin", 0.5);
    amr_task_output_type(t, "IDPairWeight");

    amr_task_group_transform(t, "in_freqs|in_counts", "norm_pairs.bin", norm_runner, local_idpw_cmp_a);
    amr_task_transform_data(t, load_idf_counts, NULL);
    return true;
}

/* ========================================================================
 * 2. GRAPH EXTRACTION & INTERSECTION
 * ======================================================================== */
static void extract_features_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    uint32_t P = amr_worker_num_partitions(w);
    aml_pool_t *scratch = amr_worker_scratch_pool(w);

    io_out_options_t opts; io_out_options_init(&opts);
    io_out_options_format(&opts, io_fixed(sizeof(amr_id_pair_weight_t)));
    io_out_options_buffer_size(&opts, amr_worker_ram(w, 0.1));
    io_out_ext_options_t ext_opts; io_out_ext_options_init(&ext_opts);
    io_out_ext_options_compare(&ext_opts, local_idpw_cmp_b, NULL);

    char *tmp_path = amr_worker_output_base2(w, amr_worker_output(w, 1), ".tmp_f");
    io_out_t *tmp_out = io_out_ext_init(tmp_path, &opts, &ext_opts);

    size_t num_r = 0; bool more = false; io_record_t *g;

    while ((g = io_in_advance_group(ins[0], &num_r, &more, local_idpw_cmp_a, NULL)) != NULL) {
        aml_pool_clear(scratch);
        uint32_t item_A = ((amr_id_pair_weight_t *)g[0].record)->a;

        amr_id_weight_list_t *i_list = (amr_id_weight_list_t *)aml_pool_alloc(scratch, sizeof(amr_id_weight_list_t) + (num_r * sizeof(amr_id_weight_t)));
        i_list->key_id = item_A; i_list->count = num_r;

        for (size_t i = 0; i < num_r; i++) {
            amr_id_pair_weight_t *p = (amr_id_pair_weight_t *)g[i].record;
            i_list->elements[i].id = p->b;
            i_list->elements[i].w = (float)p->w;
            io_out_write_record(tmp_out, p, sizeof(amr_id_pair_weight_t));
        }
        amr_worker_serialize(w, 0, outs[0], i_list);
    }

    io_in_t *tmp_in = io_out_in(tmp_out);

    while ((g = io_in_advance_group(tmp_in, &num_r, &more, local_idpw_cmp_b, NULL)) != NULL) {
        aml_pool_clear(scratch);
        uint32_t feat_F = ((amr_id_pair_weight_t *)g[0].record)->b;
        amr_id_weight_list_t *f_list = (amr_id_weight_list_t *)aml_pool_alloc(scratch, sizeof(amr_id_weight_list_t) + (num_r * sizeof(amr_id_weight_t)));
        f_list->key_id = feat_F; f_list->count = num_r;

        for (size_t i = 0; i < num_r; i++) {
            amr_id_pair_weight_t *p = (amr_id_pair_weight_t *)g[i].record;
            f_list->elements[i].id = p->a / P; // Save RAM via Local ID!
            f_list->elements[i].w = (float)p->w;
        }
        amr_worker_serialize(w, 1, outs[1], f_list);
    }
    io_in_destroy(tmp_in);
}

static bool sim_extract_setup(amr_task_t *t) {
    amr_task_input_from_sibling_partition(t, "norm", "norm_pairs.bin", 0.5);
    amr_task_input_expect_type(t, "IDPairWeight");

    amr_task_output(t, "item_to_feat.bin", 0.3);
    amr_task_output_type(t, "IDWeightList");
    amr_task_output(t, "partial_feat_to_item.bin", 0.3);
    amr_task_output_type(t, "IDWeightList");

    amr_task_io_transform(t, "norm_pairs.bin", "item_to_feat.bin|partial_feat_to_item.bin", extract_features_runner);
    return true;
}

static void intersect_sim_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    aml_pool_t *persist = amr_worker_pool(w);
    uint32_t P = amr_worker_num_partitions(w);
    uint32_t p = amr_worker_partition(w);

    size_t num_f; amr_loaded_data_t *d_graph = amr_worker_loaded_data(w, 0, &num_f);
    uint32_t max_f = 0, max_local_item = 0;

    for (size_t i = 0; i < num_f; i++) {
        char *ptr = (char *)d_graph[i].buffer, *end = ptr + d_graph[i].length;
        while (ptr < end) {
            uint32_t r_len = *(uint32_t *)ptr;
            amr_id_weight_list_t *l = (amr_id_weight_list_t *)(ptr + 4);
            if (l->key_id > max_f) max_f = l->key_id;
            for(size_t j=0; j<l->count; j++) { if(l->elements[j].id > max_local_item) max_local_item = l->elements[j].id; }
            ptr += r_len + 4;
        }
    }

    amr_id_weight_list_t **f_arr = (amr_id_weight_list_t **)aml_pool_zalloc(persist, (max_f + 1) * sizeof(void*));
    double *acc = (double *)aml_pool_zalloc(persist, (max_local_item + 1) * sizeof(double));
    uint32_t *pop = (uint32_t *)aml_pool_alloc(persist, (max_local_item + 1) * sizeof(uint32_t));
    uint32_t pop_c = 0;

    for (size_t i = 0; i < num_f; i++) {
        char *ptr = (char *)d_graph[i].buffer, *end = ptr + d_graph[i].length;
        while (ptr < end) {
            uint32_t r_len = *(uint32_t *)ptr;
            amr_id_weight_list_t *l = (amr_id_weight_list_t *)(ptr + 4);
            f_arr[l->key_id] = l;
            ptr += r_len + 4;
        }
    }

    io_record_t *r;
    while ((r = io_in_advance(ins[1]))) {
        amr_id_weight_list_t *list_X = (amr_id_weight_list_t *)r->record;
        uint32_t item_X = list_X->key_id;

        for (size_t i = 0; i < list_X->count; i++) {
            uint32_t feat_F = list_X->elements[i].id;
            float w_XF = list_X->elements[i].w;

            if (feat_F <= max_f && f_arr[feat_F]) {
                for (size_t j = 0; j < f_arr[feat_F]->count; j++) {
                    uint32_t A_local = f_arr[feat_F]->elements[j].id;
                    float w_AF = f_arr[feat_F]->elements[j].w;
                    if (acc[A_local] == 0.0) pop[pop_c++] = A_local;
                    acc[A_local] += (w_XF * w_AF);
                }
            }
        }

        for (size_t i = 0; i < pop_c; i++) {
            uint32_t A_local = pop[i];
            uint32_t A_global = (A_local * P) + p;
            double cosine = acc[A_local];

            if (item_X != A_global && cosine >= 0.05) {
                amr_id_pair_weight_t out = { item_X, A_global, cosine };
                amr_worker_serialize(w, 0, outs[0], &out);
            }
            acc[A_local] = 0.0;
        }
        pop_c = 0;
    }
}

static bool sim_intersect_setup(amr_task_t *t) {
    amr_task_input_from_sibling_partition(t, "extract", "partial_feat_to_item.bin", 0.3);
    amr_task_input_expect_type(t, "IDWeightList");
    amr_task_input_load_into_memory(t);

    amr_task_input_from_sibling_all_to_all(t, "extract", "item_to_feat.bin", 0.6);
    amr_task_input_expect_type(t, "IDWeightList");

    amr_task_output(t, "substitutes_scores.bin", 0.5);
    amr_task_output_type(t, "IDPairWeight");
    amr_task_output_shuffle_by(t, "Hash_A", NULL);
    amr_task_output_sort_by(t, "Sort_A_WDesc", NULL);

    amr_task_io_transform(t, "partial_feat_to_item.bin|item_to_feat.bin", "substitutes_scores.bin", intersect_sim_runner);
    return true;
}

bool pipeline_substitutes_setup(amr_pipeline_t *p) {
    register_internal_types(amr_pipeline_scheduler(p));
    amr_pipeline_task(p, "norm", true, sim_norm_setup);
    amr_pipeline_task(p, "extract", true, sim_extract_setup);
    amr_pipeline_task(p, "intersect", true, sim_intersect_setup);

    amr_pipeline_bind_output(p, "out_scores", "intersect", "substitutes_scores.bin");
    return true;
}
