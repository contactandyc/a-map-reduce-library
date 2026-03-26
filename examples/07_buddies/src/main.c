// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-map-reduce-library/amr.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include "the-io-library/io.h"
#include "a-json-library/ajson.h"
#include "a-memory-library/aml_pool.h"

#include "pipeline_amazon.h"
#include "pipeline_inv_freq.h"
#include "pipeline_substitutes.h"
#include "pipeline_reciprocal_nearest_neighbors.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static const char *ITEMS_FILE = "../data/amazon_2023/meta.jsonl";
static const char *EVENTS_FILE = "../data/amazon_2023/reviews.jsonl";

static amr_pipeline_t *amazon_pipe = NULL;
static amr_pipeline_t *inv_freq = NULL;
static amr_pipeline_t *pipe_subs = NULL;
static amr_pipeline_t *pipe_rnn = NULL;

/* ================================================================
 * APP DATATYPES (Dense Dictionary)
 * ================================================================ */
typedef struct { uint32_t id; char *asin; char *title; } item_meta_t;

static void meta_ser(const void *o, aml_buffer_t *bh) {
    const item_meta_t *m = (const item_meta_t *)o;
    aml_buffer_append(bh, &m->id, sizeof(uint32_t));
    aml_buffer_appends(bh, m->asin); aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, m->title); aml_buffer_appendc(bh, '\0');
}

static void* meta_des(aml_pool_t *p, const void *b, size_t l __attribute__((unused))) {
    item_meta_t *m = (item_meta_t *)aml_pool_zalloc(p, sizeof(*m));
    const char *ptr = (const char *)b;
    memcpy(&m->id, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
    m->asin = aml_pool_strdup(p, ptr); ptr += strlen(ptr)+1;
    m->title = aml_pool_strdup(p, ptr);
    return m;
}

static void meta_str(const void *o, aml_buffer_t *bh) {
    const item_meta_t *m = (const item_meta_t *)o;
    aml_buffer_appendf(bh, "ID(%u) -> %s (%s)", m->id, m->asin, m->title);
}

/* ================================================================
 * 1. LOCAL DICTIONARY JOIN
 * ================================================================ */
static void build_dense_dict_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    io_record_t *r_id = io_in_advance(ins[0]);
    io_record_t *r_str = io_in_advance(ins[1]);

    while(r_id) {
        aml_pool_clear(amr_worker_scratch_pool(w));
        amr_id_string_pair_t *id_pair = (amr_id_string_pair_t *)amr_worker_deserialize(w, 0, r_id);

        while(r_str) {
            amr_string_pair_t *str_pair = (amr_string_pair_t *)amr_worker_deserialize(w, 1, r_str);
            int c = strcmp(str_pair->a, id_pair->str);
            if (c < 0) r_str = io_in_advance(ins[1]);
            else break;
        }

        char *title = id_pair->str;
        if (r_str) {
            amr_string_pair_t *str_pair = (amr_string_pair_t *)amr_worker_deserialize(w, 1, r_str);
            if (strcmp(str_pair->a, id_pair->str) == 0) title = str_pair->b;
        }

        item_meta_t m = { id_pair->id, id_pair->str, title };
        amr_worker_serialize(w, 0, outs[0], &m);
        r_id = io_in_advance(ins[0]);
    }
}

static bool build_dense_dict_setup(amr_task_t *t) {
    amr_task_input_from_pipeline_partition(t, inv_freq, "out_item_dict", 0.5);
    amr_task_input_expect_type(t, "IdStringPair");
    amr_task_input_from_pipeline_shuffle(t, amazon_pipe, "out_dict", 0.5);
    amr_task_input_expect_type(t, "StringPair");

    amr_task_output(t, "dense_dict.bin", 0.5);
    amr_task_output_type(t, "ItemMeta");
    amr_task_io_transform(t, "out_item_dict|out_dict", "dense_dict.bin", build_dense_dict_runner);
    return true;
}

/* ================================================================
 * FLAT JSON FORMATTING (RNN)
 * ================================================================ */
static void* load_dict(amr_worker_t *w) {
    aml_pool_t *pool = amr_worker_pool(w);
    size_t num_dict;
    amr_loaded_data_t *d_dict = amr_worker_loaded_data(w, 0, &num_dict);
    uint32_t max_id = 0;

    for(size_t i=0; i<num_dict; i++) {
        char *p = (char *)d_dict[i].buffer, *end = p + d_dict[i].length;
        while(p < end) {
            uint32_t rlen = *(uint32_t*)p; p+=4;
            uint32_t id; memcpy(&id, p, 4);
            if (id > max_id) max_id = id;
            p += rlen;
        }
    }

    item_meta_t **dict = (item_meta_t **)aml_pool_zalloc(pool, (max_id + 1) * sizeof(item_meta_t*));
    for(size_t i=0; i<num_dict; i++) {
        char *p = (char *)d_dict[i].buffer, *end = p + d_dict[i].length;
        while(p < end) {
            uint32_t rlen = *(uint32_t*)p; p+=4;
            item_meta_t *m = (item_meta_t *)meta_des(pool, p, rlen);
            dict[m->id] = m;
            p += rlen;
        }
    }
    return dict;
}

static void format_rnn_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    aml_pool_t *pool = amr_worker_scratch_pool(w);
    aml_buffer_t *bh = amr_worker_buffer(w);
    item_meta_t **dict = (item_meta_t **)amr_worker_transform_data(w);

    amr_id_pair_weights_t *pw = (amr_id_pair_weights_t *)amr_worker_deserialize(w, 1, r);
    item_meta_t *meta_a = dict[pw->a];
    item_meta_t *meta_b = dict[pw->b];

    ajson_t *root = ajsono(pool);

    ajsono_append(root, "item_a", ajson_str_nocopy(pool, meta_a->asin), false);
    ajsono_append(root, "title_a", ajson_encode_str_nocopy(pool, meta_a->title), false);
    ajsono_append(root, "reviews_a", ajson_decimal_stringf(pool, "%.0f", pw->aw), false);

    ajsono_append(root, "item_b", ajson_str_nocopy(pool, meta_b->asin), false);
    ajsono_append(root, "title_b", ajson_encode_str_nocopy(pool, meta_b->title), false);
    ajsono_append(root, "reviews_b", ajson_decimal_stringf(pool, "%.0f", pw->bw), false);

    ajsono_append(root, "rnn_score", ajson_decimal_stringf(pool, "%.4f", pw->w), false);

    aml_buffer_clear(bh);
    ajson_dump_to_buffer(bh, root);
    aml_buffer_appendc(bh, '\n');
    io_out_write_record(outs[0], aml_buffer_data(bh), aml_buffer_length(bh));
}

static bool format_rnn_setup(amr_task_t *t) {
    amr_task_input_from_task_all_to_all(t, "build_dense_dict", "dense_dict.bin", 0.5);
    amr_task_input_expect_type(t, "ItemMeta");
    amr_task_input_load_into_memory(t);

    amr_task_input_from_pipeline_partition(t, pipe_rnn, "out_rnn", 0.5);
    amr_task_input_expect_type(t, "IDPairWeights");

    amr_task_output(t, "reciprocal_neighbors.jsonl", 1.0);
    amr_task_output_format(t, io_delimiter('\n'));

    // Streams individually without requiring an A/B grouping comparison!
    amr_task_default_runner(t);
    amr_task_transform(t, "out_rnn", "reciprocal_neighbors.jsonl", format_rnn_runner);
    amr_task_transform_data(t, load_dict, NULL);
    return true;
}

/* ================================================================
 * MAIN EXECUTION
 * ================================================================ */
int main(int argc, char **argv) {
    if (access(ITEMS_FILE, F_OK) != 0 || access(EVENTS_FILE, F_OK) != 0) {
        fprintf(stderr, "\n[ERROR] Data files not found!\n");
        return 1;
    }

    size_t parts = 4;
    int shift = 1;
    if (argc > 1 && argv[1][0] != '-') {
        parts = (size_t)atoi(argv[1]);
        if (parts == 0) parts = 4;
        shift = 2;
    }

    printf("[INFO] Starting Example 07: Reciprocal Nearest Neighbors. Partitions: %zu\n", parts);

    amr_t *sched = amr_init(argc - shift, argv + shift, parts, 12, 4096);
    amr_keep_intermediate_files(sched);
    amr_register_common_datatypes(sched);
    amr_register_datatype(sched, "ItemMeta", "Dense Dict", meta_ser, meta_des, meta_str);

    // 1. Amazon SAX Ingestion
    amazon_config_t amazon_cfg = { ITEMS_FILE, EVENTS_FILE };
    amazon_pipe = amr_pipeline_create(sched, "amazon", pipeline_amazon_setup, &amazon_cfg);

    // 2. Universal Indexing Engine
    inv_freq_config_t cfg = { .min_overlap = 2, .out_mode = INV_FREQ_OUT_A_WDESC };
    inv_freq = amr_pipeline_create(sched, "inv_freq", pipeline_inv_freq_setup, &cfg);
    amr_pipeline_bind_link(inv_freq, "in_sessions", amazon_pipe, "out_sessions");

    amr_task(sched, "build_dense_dict", true, build_dense_dict_setup);

    // 3. Substitutes Math Engine
    pipe_subs = amr_pipeline_create(sched, "substitutes", pipeline_substitutes_setup, NULL);
    amr_pipeline_bind_link(pipe_subs, "in_freqs", inv_freq, "out_freqs");
    amr_pipeline_bind_link(pipe_subs, "in_counts", inv_freq, "out_item_counts");

    // 4. Reciprocal Nearest Neighbors Extraction
    rnn_config_t cfg_rnn = { .min_users = 100, .min_sim = 0.80 };
    pipe_rnn = amr_pipeline_create(sched, "rnn", pipeline_rnn_setup, &cfg_rnn);
    amr_pipeline_bind_link(pipe_rnn, "in_scores", pipe_subs, "out_scores");
    amr_pipeline_bind_link(pipe_rnn, "in_counts", inv_freq, "out_item_counts");

    amr_task(sched, "format_rnn", true, format_rnn_setup);

    bool success = amr_run(sched, amr_worker_complete);
    amr_destroy(sched);

    return success ? 0 : 1;
}
