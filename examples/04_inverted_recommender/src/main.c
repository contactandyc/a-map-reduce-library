// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-map-reduce-library/amr.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include "the-io-library/io.h"
#include "the-io-library/io_in.h"
#include "the-io-library/io_out.h"
#include "a-json-library/ajson.h"
#include "a-memory-library/aml_pool.h"

#include "pipeline_amazon.h"
#include "pipeline_inv_freq.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static const char *ITEMS_FILE = "../data/amazon_2023/meta.jsonl";
static const char *EVENTS_FILE = "../data/amazon_2023/reviews.jsonl";

static amr_pipeline_t *amazon_pipe = NULL;
static amr_pipeline_t *inv_freq = NULL;

/* ================================================================
 * 1. DATATYPES FOR DISTRIBUTED INTEGER JOINS
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
static size_t meta_part_id(const io_record_t *r, size_t np, void *arg __attribute__((unused))) {
    return ((const item_meta_t *)r->record)->id % np;
}
static int meta_cmp_id(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    uint32_t a = ((const item_meta_t *)rA->record)->id;
    uint32_t b = ((const item_meta_t *)rB->record)->id;
    return a > b ? 1 : (a < b ? -1 : 0);
}

// --- HalfEnrichedInt (Target ID, Neighbor ID & Title) ---
typedef struct { double w; uint32_t a; uint32_t b; char *asin_b; char *title_b; } he_int_t;

static void he_ser(const void *o, aml_buffer_t *bh) {
    const he_int_t *v = (const he_int_t *)o;
    aml_buffer_append(bh, &v->w, sizeof(double));
    aml_buffer_append(bh, &v->a, sizeof(uint32_t));
    aml_buffer_append(bh, &v->b, sizeof(uint32_t));
    aml_buffer_appends(bh, v->asin_b ? v->asin_b : ""); aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, v->title_b ? v->title_b : ""); aml_buffer_appendc(bh, '\0');
}
static void* he_des(aml_pool_t *p, const void *b, size_t l __attribute__((unused))) {
    he_int_t *v = (he_int_t *)aml_pool_zalloc(p, sizeof(*v));
    const char *ptr = (const char *)b;
    memcpy(&v->w, ptr, sizeof(double)); ptr += sizeof(double);
    memcpy(&v->a, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
    memcpy(&v->b, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
    v->asin_b = aml_pool_strdup(p, ptr); ptr += strlen(ptr) + 1;
    v->title_b = aml_pool_strdup(p, ptr);
    return v;
}
static void he_str(const void *o, aml_buffer_t *bh) {
    const he_int_t *v = (const he_int_t *)o;
    aml_buffer_appendf(bh, "W: %.2f | ID(%u) -> ID(%u) [%s]", v->w, v->a, v->b, v->title_b);
}
static size_t he_part_a(const io_record_t *r, size_t np, void *arg __attribute__((unused))) {
    uint32_t a; memcpy(&a, r->record + sizeof(double), sizeof(uint32_t));
    return a % np;
}

static int he_cmp_a_wdesc(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    uint32_t a1; memcpy(&a1, rA->record + sizeof(double), sizeof(uint32_t));
    uint32_t a2; memcpy(&a2, rB->record + sizeof(double), sizeof(uint32_t));

    // 1. Sort by Target Item A
    if (a1 > a2) return 1;
    if (a1 < a2) return -1;

    // 2. Sort by Weight Descending
    double w1; memcpy(&w1, rA->record, sizeof(double));
    double w2; memcpy(&w2, rB->record, sizeof(double));
    if (w1 > w2) return -1;
    if (w1 < w2) return 1;

    // 3. EXPLICIT TIE-BREAKER: Sort alphabetically by ASIN B!
    // Skip past the double (8 bytes) and two uint32_t IDs (8 bytes) to reach the asin_b string
    const char *asin_b1 = rA->record + sizeof(double) + (sizeof(uint32_t) * 2);
    const char *asin_b2 = rB->record + sizeof(double) + (sizeof(uint32_t) * 2);

    return strcmp(asin_b1, asin_b2);
}

// --- FullEnrichedInt (Both IDs & Both Titles) ---
typedef struct { double w; uint32_t a; char *asin_a; char *title_a; uint32_t b; char *asin_b; char *title_b; } fe_int_t;

static void fe_ser(const void *o, aml_buffer_t *bh) {
    const fe_int_t *v = (const fe_int_t *)o;
    aml_buffer_append(bh, &v->w, sizeof(double));
    aml_buffer_append(bh, &v->a, sizeof(uint32_t));
    aml_buffer_appends(bh, v->asin_a ? v->asin_a : ""); aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, v->title_a ? v->title_a : ""); aml_buffer_appendc(bh, '\0');
    aml_buffer_append(bh, &v->b, sizeof(uint32_t));
    aml_buffer_appends(bh, v->asin_b ? v->asin_b : ""); aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, v->title_b ? v->title_b : ""); aml_buffer_appendc(bh, '\0');
}
static void* fe_des(aml_pool_t *p, const void *b, size_t l __attribute__((unused))) {
    fe_int_t *v = (fe_int_t *)aml_pool_zalloc(p, sizeof(*v));
    const char *ptr = (const char *)b;
    memcpy(&v->w, ptr, sizeof(double)); ptr += sizeof(double);
    memcpy(&v->a, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
    v->asin_a = aml_pool_strdup(p, ptr); ptr += strlen(ptr) + 1;
    v->title_a = aml_pool_strdup(p, ptr); ptr += strlen(ptr) + 1;
    memcpy(&v->b, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
    v->asin_b = aml_pool_strdup(p, ptr); ptr += strlen(ptr) + 1;
    v->title_b = aml_pool_strdup(p, ptr);
    return v;
}
static void fe_str(const void *o, aml_buffer_t *bh) {
    const fe_int_t *v = (const fe_int_t *)o;
    aml_buffer_appendf(bh, "W: %.2f | [%s] -> [%s]", v->w, v->title_a, v->title_b);
}
static int fe_cmp_a(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    uint32_t a1; memcpy(&a1, rA->record + sizeof(double), sizeof(uint32_t));
    uint32_t a2; memcpy(&a2, rB->record + sizeof(double), sizeof(uint32_t));
    return a1 > a2 ? 1 : (a1 < a2 ? -1 : 0);
}

static void register_app_types(amr_t *sched) {
    amr_register_datatype(sched, "ItemMeta", "", meta_ser, meta_des, meta_str);
    amr_datatype_add_partition(sched, "ItemMeta", "Hash_ID", meta_part_id);
    amr_datatype_add_compare(sched, "ItemMeta", "Sort_ID", meta_cmp_id);

    amr_register_datatype(sched, "HalfEnrichedInt", "", he_ser, he_des, he_str);
    amr_datatype_add_partition(sched, "HalfEnrichedInt", "Hash_A", he_part_a);
    amr_datatype_add_compare(sched, "HalfEnrichedInt", "Sort_A_WDesc", he_cmp_a_wdesc);

    amr_register_datatype(sched, "FullEnrichedInt", "", fe_ser, fe_des, fe_str);
    amr_datatype_add_compare(sched, "FullEnrichedInt", "Sort_A", fe_cmp_a);
}

/* ================================================================
 * 2. PREPARING THE DISTRIBUTED DICTIONARY
 * ================================================================ */
static void build_master_dict_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
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

static bool build_master_dict_setup(amr_task_t *t) {
    amr_task_input_from_pipeline_partition(t, inv_freq, "out_item_dict", 0.5);
    amr_task_input_expect_type(t, "IdStringPair");

    // Wires directly into the output of the Amazon ingest pipeline!
    amr_task_input_from_pipeline_shuffle(t, amazon_pipe, "out_dict", 0.5);
    amr_task_input_expect_type(t, "StringPair");

    amr_task_output(t, "master_dict.bin", 0.5);
    amr_task_output_type(t, "ItemMeta");

    // Partition and sort master dictionary by Integer ID for our upcoming Merge Joins!
    amr_task_output_shuffle_by(t, "Hash_ID", NULL);
    amr_task_output_sort_by(t, "Sort_ID", NULL);

    amr_task_io_transform(t, "out_item_dict|out_dict", "master_dict.bin", build_master_dict_runner);
    return true;
}

/* ================================================================
 * 3. DISTRIBUTED MERGE JOINS (INTEGER EDITION)
 * ================================================================ */

static void join_b_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    io_record_t *r_pw = io_in_advance(ins[0]);
    io_record_t *r_dict = io_in_advance(ins[1]);

    while (r_pw) {
        aml_pool_clear(amr_worker_scratch_pool(w));
        amr_id_pair_weight_t *pw = (amr_id_pair_weight_t *)amr_worker_deserialize(w, 0, r_pw);

        while (r_dict) {
            item_meta_t *meta = (item_meta_t *)amr_worker_deserialize(w, 1, r_dict);
            if (meta->id < pw->b) r_dict = io_in_advance(ins[1]);
            else break;
        }

        char *asin_b = "UNKNOWN"; char *title_b = "UNKNOWN";
        if (r_dict) {
            item_meta_t *meta = (item_meta_t *)amr_worker_deserialize(w, 1, r_dict);
            if (meta->id == pw->b) { asin_b = meta->asin; title_b = meta->title; }
        }

        he_int_t out = { pw->w, pw->a, pw->b, asin_b, title_b };
        amr_worker_serialize(w, 0, outs[0], &out);
        r_pw = io_in_advance(ins[0]);
    }
}

static bool join_b_setup(amr_task_t *t) {
    // 1. Pull the perfectly-shaped integers from the pipeline
    amr_task_input_from_pipeline_shuffle(t, inv_freq, "out_freqs", 0.5);
    amr_task_input_expect_type(t, "IDPairWeight");

    amr_task_input_from_task_shuffle(t, "build_master_dict", "master_dict.bin", 0.5);
    amr_task_input_expect_type(t, "ItemMeta");

    amr_task_output(t, "half_enriched.bin", 0.5);
    amr_task_output_type(t, "HalfEnrichedInt");

    amr_task_output_shuffle_by(t, "Hash_A", NULL);
    amr_task_output_sort_by(t, "Sort_A_WDesc", NULL);

    amr_task_io_transform(t, "out_freqs|master_dict.bin", "half_enriched.bin", join_b_runner);
    return true;
}


static void join_a_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    io_record_t *r_half = io_in_advance(ins[0]);
    io_record_t *r_dict = io_in_advance(ins[1]);

    while (r_half) {
        aml_pool_clear(amr_worker_scratch_pool(w));
        he_int_t *half = (he_int_t *)amr_worker_deserialize(w, 0, r_half);

        while (r_dict) {
            item_meta_t *meta = (item_meta_t *)amr_worker_deserialize(w, 1, r_dict);
            if (meta->id < half->a) r_dict = io_in_advance(ins[1]);
            else break;
        }

        char *asin_a = "UNKNOWN"; char *title_a = "UNKNOWN";
        if (r_dict) {
            item_meta_t *meta = (item_meta_t *)amr_worker_deserialize(w, 1, r_dict);
            if (meta->id == half->a) { asin_a = meta->asin; title_a = meta->title; }
        }

        fe_int_t out = { half->w, half->a, asin_a, title_a, half->b, half->asin_b, half->title_b };
        amr_worker_serialize(w, 0, outs[0], &out);
        r_half = io_in_advance(ins[0]);
    }
}

static bool join_a_setup(amr_task_t *t) {
    // The half_enriched data WAS shuffled by join_b, so we must use _shuffle to read it!
    amr_task_input_from_task_shuffle(t, "join_b", "half_enriched.bin", 0.5);
    amr_task_input_expect_type(t, "HalfEnrichedInt");

    // Use _shuffle to gather the dictionary buckets!
    amr_task_input_from_task_shuffle(t, "build_master_dict", "master_dict.bin", 0.5);
    amr_task_input_expect_type(t, "ItemMeta");

    amr_task_output(t, "full_enriched.bin", 0.5);
    amr_task_output_type(t, "FullEnrichedInt");

    amr_task_io_transform(t, "half_enriched.bin|master_dict.bin", "full_enriched.bin", join_a_runner);
    return true;
}
/* ================================================================
 * 4. FINAL JSON FORMATTING
 * ================================================================ */
static void format_json_runner(amr_worker_t *w, io_record_t *first, size_t num_r, io_out_t **outs) {
    aml_pool_t *pool = amr_worker_scratch_pool(w);
    aml_buffer_t *bh = amr_worker_buffer(w);

    fe_int_t *first_fe = (fe_int_t *)amr_worker_deserialize(w, 0, &first[0]);

    ajson_t *root = ajsono(pool);
    ajsono_append(root, "asin", ajson_str_nocopy(pool, first_fe->asin_a), false);
    ajsono_append(root, "title", ajson_encode_str_nocopy(pool, first_fe->title_a), false);

    ajson_t *recs = ajsona(pool);
    size_t limit = num_r > 20 ? 20 : num_r;

    // The stream arrives perfectly sorted.
    for (size_t i = 0; i < limit; i++) {
        fe_int_t *fe = (fe_int_t *)amr_worker_deserialize(w, 0, &first[i]);

        ajson_t *rec_obj = ajsono(pool);
        ajsono_append(rec_obj, "asin", ajson_str_nocopy(pool, fe->asin_b), false);
        ajsono_append(rec_obj, "title", ajson_encode_str_nocopy(pool, fe->title_b), false);
        ajsono_append(rec_obj, "co_reviews", ajson_decimal_stringf(pool, "%.0f", fe->w), false);
        ajsona_append(recs, rec_obj);
    }

    ajsono_append(root, "recommendations", recs, false);
    aml_buffer_clear(bh);
    ajson_dump_to_buffer(bh, root);
    io_out_write_record(outs[0], aml_buffer_data(bh), aml_buffer_length(bh));
}

static bool app_format_json_setup(amr_task_t *t) {
    amr_task_input_from_task_partition(t, "join_a", "full_enriched.bin", 0.5);
    amr_task_input_expect_type(t, "FullEnrichedInt");

    amr_task_output(t, "recommendations.jsonl", 1.0);
    amr_task_output_format(t, io_delimiter('\n'));

    amr_task_group_transform(t, "full_enriched.bin", "recommendations.jsonl", format_json_runner, fe_cmp_a);
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

    printf("[INFO] Starting Billion-Scale Configurable DAG. Partitions: %zu\n", parts);

    amr_t *sched = amr_init(argc - shift, argv + shift, parts, 12, 4096);
    amr_register_common_datatypes(sched);
    register_app_types(sched);

    // 1. Initialize the Amazon SAX Ingestion Pipeline
    amazon_config_t amazon_cfg = { ITEMS_FILE, EVENTS_FILE };
    amazon_pipe = amr_pipeline_create(sched, "amazon", pipeline_amazon_setup, &amazon_cfg);

    // 2. Configure the pipeline to output exactly what our merge-joins need!
    inv_freq_config_t cfg = { .min_overlap = 2, .out_mode = INV_FREQ_OUT_B_A };
    inv_freq = amr_pipeline_create(sched, "inv_freq", pipeline_inv_freq_setup, &cfg);

    // Wire the sessions output from amazon_pipe straight into inv_freq
    amr_pipeline_bind_link(inv_freq, "in_sessions", amazon_pipe, "out_sessions");

    amr_task(sched, "build_master_dict",   true, build_master_dict_setup);
    amr_task(sched, "join_b",              true, join_b_setup);
    amr_task(sched, "join_a",              true, join_a_setup);
    amr_task(sched, "app_format_json",     true, app_format_json_setup);

    bool success = amr_run(sched, amr_worker_complete);
    amr_destroy(sched);

    if (!success) {
        fprintf(stderr, "[ERROR] DAG Execution Failed!\n");
        return 1;
    }
    return 0;
}
