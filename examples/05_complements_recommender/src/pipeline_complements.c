// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "pipeline_complements.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include <string.h>
#include <math.h>

// 1. We must define the struct so we can deserialize the upstream data
typedef struct { uint32_t key_id; uint32_t count; uint32_t elements[]; } id_list_t;

/* ========================================================================
 * PHASE 3: OPTIMIZED INTERSECTION + COSINE SCORING
 * ======================================================================== */
static void* load_global_counts(amr_worker_t *w) {
    size_t num_data; amr_loaded_data_t *d = amr_worker_loaded_data(w, 2, &num_data);
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
    uint32_t *counts = (uint32_t *)aml_pool_zalloc(amr_worker_pool(w), (max_id + 1) * sizeof(uint32_t));
    for(size_t i=0; i<num_data; i++) {
        char *p = (char *)d[i].buffer, *end = p + d[i].length;
        while(p < end) {
            uint32_t rlen = *(uint32_t*)p; p += 4;
            amr_uint32_pair_t *pair = (amr_uint32_pair_t *)p;
            counts[pair->a] = pair->b;
            p += rlen;
        }
    }
    return counts;
}

static void intersect_and_score_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    uint32_t *global_counts = (uint32_t *)amr_worker_transform_data(w);

    // Configurable Thresholds
    int min_overlap = 3;
    double min_cosine = 0.01;

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
        double fX = global_counts[X];

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
            // We trade CPU for I/O and emit directly to local partitions!
            if (X != A_global) {
                double fAB = acc[A_local];

                if (fAB >= min_overlap) {
                    double fA = global_counts[A_global];

                    if (fA > 0 && fX > 0) {
                        double cosine = fAB / (sqrt(fA) * sqrt(fX));

                        if (cosine >= min_cosine) {
                            // Only emit the edge owned by THIS partition
                            amr_id_pair_weight_t out = { A_global, X, cosine };
                            amr_worker_serialize(w, 0, outs[0], &out);
                        }
                    }
                }
            }
            acc[A_local] = 0.0;
        }
        pop_c = 0;
    }
}

static bool intersect_and_score_setup(amr_task_t *t) {
    amr_task_input_from_pipeline_port_partition(t, "in_user_to_partial_items", 0.4);
    amr_task_input_expect_type(t, "IDList");
    amr_task_input_load_into_memory(t);

    amr_task_input_from_pipeline_port_all_to_all(t, "in_item_to_users", 0.4);
    amr_task_input_expect_type(t, "IDList");

    amr_task_input_from_pipeline_port_all_to_all(t, "in_item_counts", 0.1);
    amr_task_input_expect_type(t, "UInt32Pair");
    amr_task_input_load_into_memory(t);

    amr_task_output(t, "complements_scores.bin", 0.5);
    amr_task_output_type(t, "IDPairWeight");

    // Output is perfectly partitioned! No Hash_A shuffle required!
    amr_task_output_sort_by(t, "Sort_A_WDesc", NULL);

    amr_task_io_transform(t, "in_user_to_partial_items|in_item_to_users|in_item_counts", "complements_scores.bin", intersect_and_score_runner);
    amr_task_transform_data(t, load_global_counts, NULL);
    return true;
}

bool pipeline_complements_setup(amr_pipeline_t *p) {
    amr_pipeline_task(p, "intersect_and_score", true, intersect_and_score_setup);
    amr_pipeline_bind_output(p, "out_scores", "intersect_and_score", "complements_scores.bin");
    return true;
}
