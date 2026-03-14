// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "a-map-reduce-library/amr.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include "the-io-library/io.h"
#include "the-io-library/io_in.h"
#include "the-io-library/io_out.h"
#include "a-json-library/ajson.h"
#include "a-memory-library/aml_pool.h"

// Include all three pipelines!
#include "pipeline_amazon.h"
#include "pipeline_inv_freq.h"
#include "pipeline_substitutes.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static const char *ITEMS_FILE = "../data/amazon_2023/meta.jsonl";
static const char *EVENTS_FILE = "../data/amazon_2023/reviews.jsonl";

static amr_pipeline_t *amazon_pipe = NULL;
static amr_pipeline_t *inv_freq = NULL;
static amr_pipeline_t *pipe_subs = NULL;

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

    // Pull the raw ASIN->Title pairs straight from our modular Amazon pipeline
    amr_task_input_from_pipeline_shuffle(t, amazon_pipe, "out_dict", 0.5);
    amr_task_input_expect_type(t, "StringPair");

    amr_task_output(t, "dense_dict.bin", 0.5);
    amr_task_output_type(t, "ItemMeta");

    amr_task_io_transform(t, "out_item_dict|out_dict", "dense_dict.bin", build_dense_dict_runner);
    return true;
}

/* ================================================================
 * 2. FORMATTING (SUBSTITUTES)
 * ================================================================ */
typedef struct { item_meta_t **dict; uint32_t *counts; } format_ctx_t;

static void* load_format_context(amr_worker_t *w) {
    aml_pool_t *pool = amr_worker_pool(w);
    format_ctx_t *ctx = (format_ctx_t *)aml_pool_alloc(pool, sizeof(format_ctx_t));

    // Load Dictionary
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

    ctx->dict = (item_meta_t **)aml_pool_zalloc(pool, (max_id + 1) * sizeof(item_meta_t*));
    for(size_t i=0; i<num_dict; i++) {
        char *p = (char *)d_dict[i].buffer, *end = p + d_dict[i].length;
        while(p < end) {
            uint32_t rlen = *(uint32_t*)p; p+=4;
            item_meta_t *m = (item_meta_t *)meta_des(pool, p, rlen);
            ctx->dict[m->id] = m;
            p += rlen;
        }
    }

    // Load Counts
    size_t num_counts;
    amr_loaded_data_t *d_counts = amr_worker_loaded_data(w, 1, &num_counts);
    ctx->counts = (uint32_t *)aml_pool_zalloc(pool, (max_id + 1) * sizeof(uint32_t));
    for(size_t i=0; i<num_counts; i++) {
        char *p = (char *)d_counts[i].buffer, *end = p + d_counts[i].length;
        while(p < end) {
            uint32_t rlen = *(uint32_t*)p; p += 4;
            amr_uint32_pair_t *pair = (amr_uint32_pair_t *)p;
            if (pair->a <= max_id) ctx->counts[pair->a] = pair->b;
            p += rlen;
        }
    }
    return ctx;
}

static int idpw_cmp_a(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const amr_id_pair_weight_t *a = (const amr_id_pair_weight_t *)rA->record;
    const amr_id_pair_weight_t *b = (const amr_id_pair_weight_t *)rB->record;
    return a->a > b->a ? 1 : (a->a < b->a ? -1 : 0);
}

static int qsort_w_desc(const void *a, const void *b) {
    const amr_id_pair_weight_t *pa = (const amr_id_pair_weight_t *)((const io_record_t *)a)->record;
    const amr_id_pair_weight_t *pb = (const amr_id_pair_weight_t *)((const io_record_t *)b)->record;
    if (pa->w > pb->w) return -1;
    if (pa->w < pb->w) return 1;
    return pa->b > pb->b ? 1 : (pa->b < pb->b ? -1 : 0);
}

static void format_json_runner(amr_worker_t *w, io_record_t *first, size_t num_r, io_out_t **outs) {
    aml_pool_t *pool = amr_worker_scratch_pool(w);
    aml_buffer_t *bh = amr_worker_buffer(w);
    format_ctx_t *ctx = (format_ctx_t *)amr_worker_transform_data(w);

    amr_id_pair_weight_t *first_pw = (amr_id_pair_weight_t *)amr_worker_deserialize(w, 2, &first[0]);
    item_meta_t *meta_a = ctx->dict[first_pw->a];
    uint32_t count_a = ctx->counts[first_pw->a];

    ajson_t *root = ajsono(pool);
    ajsono_append(root, "asin", ajson_str_nocopy(pool, meta_a->asin), false);
    ajsono_append(root, "title", ajson_encode_str_nocopy(pool, meta_a->title), false);
    ajsono_append(root, "total_reviews", ajson_decimal_stringf(pool, "%u", count_a), false);

    io_record_t *sorted = (io_record_t *)aml_pool_alloc(pool, num_r * sizeof(io_record_t));
    memcpy(sorted, first, num_r * sizeof(io_record_t));
    qsort(sorted, num_r, sizeof(io_record_t), qsort_w_desc);

    ajson_t *recs = ajsona(pool);
    size_t printed = 0;

    for (size_t i = 0; i < num_r; i++) {
        amr_id_pair_weight_t *pw = (amr_id_pair_weight_t *)amr_worker_deserialize(w, 2, &sorted[i]);
        item_meta_t *meta_b = ctx->dict[pw->b];
        uint32_t count_b = ctx->counts[pw->b];

        ajson_t *rec_obj = ajsono(pool);
        ajsono_append(rec_obj, "asin", ajson_str_nocopy(pool, meta_b->asin), false);
        ajsono_append(rec_obj, "title", ajson_encode_str_nocopy(pool, meta_b->title), false);
        ajsono_append(rec_obj, "total_reviews", ajson_decimal_stringf(pool, "%u", count_b), false);

        // Hardcode the metric string for Example 06
        ajsono_append(rec_obj, "substitutes_score", ajson_decimal_stringf(pool, "%.4f", pw->w), false);
        ajsona_append(recs, rec_obj);

        printed++;
        if (printed >= 20) break;
    }

    if (printed > 0) {
        ajsono_append(root, "recommendations", recs, false);
        aml_buffer_clear(bh);
        ajson_dump_to_buffer(bh, root);
        io_out_write_record(outs[0], aml_buffer_data(bh), aml_buffer_length(bh));
    }
}

static bool format_subs_setup(amr_task_t *t) {
    amr_task_input_from_task_all_to_all(t, "build_dense_dict", "dense_dict.bin", 0.5);
    amr_task_input_expect_type(t, "ItemMeta");
    amr_task_input_load_into_memory(t);

    amr_task_input_from_pipeline_all_to_all(t, inv_freq, "out_item_counts", 0.1);
    amr_task_input_expect_type(t, "UInt32Pair");
    amr_task_input_load_into_memory(t);

    amr_task_input_from_pipeline_shuffle(t, pipe_subs, "out_scores", 0.5);
    amr_task_input_expect_type(t, "IDPairWeight");

    amr_task_output(t, "substitutes.jsonl", 1.0);
    amr_task_output_format(t, io_delimiter('\n'));

    amr_task_group_transform(t, "out_scores", "substitutes.jsonl", format_json_runner, idpw_cmp_a);
    amr_task_transform_data(t, load_format_context, NULL);
    return true;
}

/* ================================================================
 * MAIN EXECUTION (EXAMPLE 06: SUBSTITUTES)
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

    printf("[INFO] Starting Example 06: Substitutes Engine. Partitions: %zu\n", parts);

    amr_t *sched = amr_init(argc - shift, argv + shift, parts, 12, 4096);
    amr_keep_intermediate_files(sched);
    amr_register_common_datatypes(sched);
    amr_register_datatype(sched, "ItemMeta", "Dense Dict", meta_ser, meta_des, meta_str);

    // 1. Instantiate the modular Amazon SAX Ingestion Pipeline
    amazon_config_t amazon_cfg = { ITEMS_FILE, EVENTS_FILE };
    amazon_pipe = amr_pipeline_create(sched, "amazon", pipeline_amazon_setup, &amazon_cfg);

    // 2. Fire up the Universal Indexing Engine
    inv_freq_config_t cfg = { .min_overlap = 2, .out_mode = INV_FREQ_OUT_A_WDESC };
    inv_freq = amr_pipeline_create(sched, "inv_freq", pipeline_inv_freq_setup, &cfg);

    // Bind the Amazon output to the Indexing engine
    amr_pipeline_bind_link(inv_freq, "in_sessions", amazon_pipe, "out_sessions");

    amr_task(sched, "build_dense_dict", true, build_dense_dict_setup);

    // 3. Plug in the Substitutes Module!
    pipe_subs = amr_pipeline_create(sched, "substitutes", pipeline_substitutes_setup, NULL);
    amr_pipeline_bind_link(pipe_subs, "in_freqs", inv_freq, "out_freqs");
    amr_pipeline_bind_link(pipe_subs, "in_counts", inv_freq, "out_item_counts");
    amr_task(sched, "format_subs", true, format_subs_setup);

    bool success = amr_run(sched, amr_worker_complete);
    amr_destroy(sched);

    if (!success) return 1;
    return 0;
}
