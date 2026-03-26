// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "pipeline_reciprocal_nearest_neighbors.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include <stdlib.h>
#include <string.h>

static void* load_counts(amr_worker_t *w) {
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

static int local_idpw_cmp_a(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const amr_id_pair_weight_t *a = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *b = (const amr_id_pair_weight_t *)rB->record;
    return a->a > b->a ? 1 : (a->a < b->a ? -1 : 0);
}

/* ------------------------------------------------------------------------
 * TASK 1: Extract Top-1 & Enforce A < B
 * ------------------------------------------------------------------------ */
static void top1_runner(amr_worker_t *w, io_record_t *first, size_t num_r __attribute__((unused)), io_out_t **outs) {
    uint32_t *counts = (uint32_t *)amr_worker_transform_data(w);
    rnn_config_t *cfg = (rnn_config_t *)amr_pipeline_config(amr_worker_pipeline(w));

    uint32_t min_users = cfg ? cfg->min_users : 50;
    double min_sim = cfg ? cfg->min_sim : 0.1;

    // The stream is pre-sorted by weight descending.
    // We strictly evaluate the #1 choice.
    amr_id_pair_weight_t *top1 = (amr_id_pair_weight_t *)amr_worker_deserialize(w, 0, &first[0]);

    uint32_t cA = counts[top1->a];
    uint32_t cB = counts[top1->b];

    if (cA >= min_users && cB >= min_users && top1->w >= min_sim) {
        amr_id_pair_weights_t out;

        // Enforce deterministic ordering
        if (top1->a < top1->b) {
            out.a = top1->a; out.b = top1->b;
            out.aw = (double)cA; out.bw = (double)cB;
        } else {
            out.a = top1->b; out.b = top1->a;
            out.aw = (double)cB; out.bw = (double)cA;
        }
        out.w = top1->w;
        amr_worker_serialize(w, 0, outs[0], &out);
    }
}

static bool top1_setup(amr_task_t *t) {
    // Read the local partitioned data from the Substitutes pipeline
    amr_task_input_from_pipeline_port_shuffle(t, "in_scores", 0.5);
    amr_task_input_expect_type(t, "IDPairWeight");

    amr_task_input_from_pipeline_port_all_to_all(t, "in_counts", 0.1);
    amr_task_input_expect_type(t, "UInt32Pair");
    amr_task_input_load_into_memory(t);

    amr_task_output(t, "top1_pairs.bin", 0.5);
    amr_task_output_type(t, "IDPairWeights");
    amr_task_output_shuffle_by(t, "Hash_A", NULL);
    amr_task_output_sort_by(t, "Sort_A_B", NULL);

    amr_task_group_transform(t, "in_scores", "top1_pairs.bin", top1_runner, local_idpw_cmp_a);
    amr_task_transform_data(t, load_counts, NULL);
    return true;
}

/* ------------------------------------------------------------------------
 * TASK 2: Validate Reciprocity
 * ------------------------------------------------------------------------ */
static int local_idpws_cmp_ab(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const amr_id_pair_weights_t *a = (const amr_id_pair_weights_t *)rA->record;
    const amr_id_pair_weights_t *b = (const amr_id_pair_weights_t *)rB->record;
    if (a->a > b->a) return 1;
    if (a->a < b->a) return -1;
    return a->b > b->b ? 1 : (a->b < b->b ? -1 : 0);
}

static int local_idpws_cmp_wdesc(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const amr_id_pair_weights_t *a = (const amr_id_pair_weights_t *)rA->record;
    const amr_id_pair_weights_t *b = (const amr_id_pair_weights_t *)rB->record;
    return a->w > b->w ? -1 : (a->w < b->w ? 1 : 0);
}

static void reciprocal_runner(amr_worker_t *w, io_record_t *first, size_t num_r, io_out_t **outs) {
    // A group size of exactly 2 confirms A chose B, and B chose A.
    if (num_r == 2) {
        amr_id_pair_weights_t *p1 = (amr_id_pair_weights_t *)amr_worker_deserialize(w, 0, &first[0]);
        amr_id_pair_weights_t *p2 = (amr_id_pair_weights_t *)amr_worker_deserialize(w, 0, &first[1]);

        amr_id_pair_weights_t out = *p1;
        out.w = (p1->w + p2->w) / 2.0; // Average the similarity scores

        amr_worker_serialize(w, 0, outs[0], &out);
    }
}

static bool reciprocal_setup(amr_task_t *t) {
    amr_task_input_from_sibling_shuffle(t, "top1", "top1_pairs.bin", 0.5);
    amr_task_input_expect_type(t, "IDPairWeights");

    amr_task_output(t, "rnn_pairs.bin", 0.5);
    amr_task_output_type(t, "IDPairWeights");

    // Sort output globally by highest weight so JSON formatter emits best pairs first
    amr_datatype_add_compare(amr_pipeline_scheduler(amr_task_pipeline(t)), "IDPairWeights", "Sort_WDesc", local_idpws_cmp_wdesc);
    amr_task_output_sort_by(t, "Sort_WDesc", NULL);

    amr_task_group_transform(t, "top1_pairs.bin", "rnn_pairs.bin", reciprocal_runner, local_idpws_cmp_ab);
    return true;
}

bool pipeline_rnn_setup(amr_pipeline_t *p) {
    amr_pipeline_task(p, "top1", true, top1_setup);
    amr_pipeline_task(p, "reciprocal", true, reciprocal_setup);
    amr_pipeline_bind_output(p, "out_rnn", "reciprocal", "rnn_pairs.bin");
    return true;
}
