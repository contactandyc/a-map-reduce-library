// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "pipeline_co_freq.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include <string.h>

static int sp_cmp_a(const io_record_t *a, const io_record_t *b, void *arg __attribute__((unused))) {
    return strcmp((const char*)a->record, (const char*)b->record);
}

static void generate_pairs_runner(amr_worker_t *w, io_record_t *first, size_t num_r, io_out_t **outs) {
    size_t limit = num_r; //  > 50 ? 50 : num_r;
    aml_pool_t *scratch = amr_worker_scratch_pool(w);
    const char **items = aml_pool_alloc(scratch, limit * sizeof(char*));

    for (size_t i = 0; i < limit; i++) {
        amr_string_pair_t *sp = amr_worker_deserialize(w, 0, &first[i]);
        items[i] = sp->b;
    }

    for (size_t i = 0; i < limit; i++) {
        for (size_t j = 0; j < limit; j++) {
            if (i == j) continue;
            amr_string_pair_weight_t out = { 1.0, (char*)items[i], (char*)items[j] };
            amr_worker_serialize(w, 0, outs[0], &out);
        }
    }
}

static bool generate_pairs_setup(amr_task_t *t) {
    amr_task_input_from_pipeline_port_shuffle(t, "in_sessions", 1.0);
    amr_task_input_expect_type(t, "StringPair");

    amr_task_output(t, "raw_pairs.bin", 0.5);
    amr_task_output_type(t, "StringPairWeight");
    amr_task_output_shuffle_by(t, "Hash_A", NULL);

    amr_task_group_transform(t, "in_sessions", "raw_pairs.bin", generate_pairs_runner, sp_cmp_a);
    return true;
}

static bool reduce_pairs_setup(amr_task_t *t) {
    amr_task_input_from_sibling_shuffle(t, "generate_pairs", "raw_pairs.bin", 1.0);
    amr_task_input_expect_type(t, "StringPairWeight");

    amr_task_output(t, "reduced_pairs.bin", 0.5);
    amr_task_output_type(t, "StringPairWeight");
    amr_task_output_sort_by(t, "Sort_A_B", NULL);
    amr_task_output_reduce_by(t, "Sum_W", NULL);

    amr_task_default_runner(t);
    amr_task_transform(t, "raw_pairs.bin", "reduced_pairs.bin", NULL);
    return true;
}

bool pipeline_co_freq_setup(amr_pipeline_t *p) {
    amr_pipeline_task(p, "generate_pairs", true, generate_pairs_setup);
    amr_pipeline_task(p, "reduce_pairs",   true, reduce_pairs_setup);
    amr_pipeline_bind_output(p, "out_pairs", "reduce_pairs", "reduced_pairs.bin");
    return true;
}
