// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "a-map-reduce-library/amr.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include "the-io-library/io.h"
#include "the-io-library/io_in.h"
#include "the-io-library/io_out.h"
#include "a-json-library/ajson.h"
#include "a-memory-library/aml_pool.h"
#include "the-macro-library/macro_map.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

/* ================================================================
 * HARDCODED DATA PATHS
 * ================================================================ */
static const char *ITEMS_FILE = "../data/amazon_2023/meta.jsonl";
static const char *EVENTS_FILE = "../data/amazon_2023/reviews.jsonl";


/* ================================================================
 * 1. CUSTOM DATATYPES FOR DISTRIBUTED JOINS
 * ================================================================ */

// --- Half Enriched: W, A, B, TitleB ---
typedef struct { double w; char *a; char *b; char *title_b; } half_enriched_t;

static void he_ser(const void *o, aml_buffer_t *bh) {
    const half_enriched_t *v = o;
    aml_buffer_append(bh, &v->w, sizeof(double));
    aml_buffer_appends(bh, v->a ? v->a : ""); aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, v->b ? v->b : ""); aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, v->title_b ? v->title_b : ""); aml_buffer_appendc(bh, '\0');
}
static void* he_des(aml_pool_t *p, const void *b, size_t l __attribute__((unused))) {
    half_enriched_t *v = aml_pool_zalloc(p, sizeof(*v));
    const char *ptr = b;
    memcpy(&v->w, ptr, sizeof(double)); ptr += sizeof(double);
    v->a = aml_pool_strdup(p, ptr); ptr += strlen(ptr) + 1;
    v->b = aml_pool_strdup(p, ptr); ptr += strlen(ptr) + 1;
    v->title_b = aml_pool_strdup(p, ptr);
    return v;
}
static void he_str(const void *o, aml_buffer_t *bh) {
    const half_enriched_t *v = o;
    aml_buffer_appendf(bh, "W: %.2f | %s -> %s (%s)", v->w, v->a, v->b, v->title_b);
}
// Hash on A so it goes to the correct worker for the final A Join
static size_t he_part_a(const io_record_t *r, size_t np, void *arg __attribute__((unused))) {
    const char *a = r->record + sizeof(double);
    size_t h = 0; while (*a) h = h * 131 + (unsigned char)*a++;
    return h % np;
}
// Sort by A ascending, then Weight Descending
static int he_cmp_a_wdesc(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const char *a1 = rA->record + sizeof(double);
    const char *a2 = rB->record + sizeof(double);
    int c = strcmp(a1, a2);
    if (c != 0) return c;

    double w1; memcpy(&w1, rA->record, sizeof(double));
    double w2; memcpy(&w2, rB->record, sizeof(double));
    if (w1 > w2) return -1;
    if (w1 < w2) return 1;
    return 0;
}

// --- Full Enriched: W, A, TitleA, B, TitleB ---
typedef struct { double w; char *a; char *title_a; char *b; char *title_b; } full_enriched_t;

static void fe_ser(const void *o, aml_buffer_t *bh) {
    const full_enriched_t *v = o;
    aml_buffer_append(bh, &v->w, sizeof(double));
    aml_buffer_appends(bh, v->a ? v->a : ""); aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, v->title_a ? v->title_a : ""); aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, v->b ? v->b : ""); aml_buffer_appendc(bh, '\0');
    aml_buffer_appends(bh, v->title_b ? v->title_b : ""); aml_buffer_appendc(bh, '\0');
}
static void* fe_des(aml_pool_t *p, const void *b, size_t l __attribute__((unused))) {
    full_enriched_t *v = aml_pool_zalloc(p, sizeof(*v));
    const char *ptr = b;
    memcpy(&v->w, ptr, sizeof(double)); ptr += sizeof(double);
    v->a = aml_pool_strdup(p, ptr); ptr += strlen(ptr) + 1;
    v->title_a = aml_pool_strdup(p, ptr); ptr += strlen(ptr) + 1;
    v->b = aml_pool_strdup(p, ptr); ptr += strlen(ptr) + 1;
    v->title_b = aml_pool_strdup(p, ptr);
    return v;
}
static void fe_str(const void *o, aml_buffer_t *bh) {
    const full_enriched_t *v = o;
    aml_buffer_appendf(bh, "W: %.2f | %s (%s) -> %s (%s)", v->w, v->a, v->title_a, v->b, v->title_b);
}
static int fe_cmp_a(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const char *a1 = rA->record + sizeof(double);
    const char *a2 = rB->record + sizeof(double);
    return strcmp(a1, a2);
}

// Custom Type Registration
static void register_custom_types(amr_t *sched) {
    amr_register_datatype(sched, "HalfEnriched", "", he_ser, he_des, he_str);
    amr_datatype_add_partition(sched, "HalfEnriched", "Hash_A", he_part_a);
    amr_datatype_add_compare(sched, "HalfEnriched", "Sort_A_WDesc", he_cmp_a_wdesc);

    amr_register_datatype(sched, "FullEnriched", "", fe_ser, fe_des, fe_str);
    amr_datatype_add_compare(sched, "FullEnriched", "Sort_A", fe_cmp_a);
}


/* ================================================================
 * 2. SHARED REDUCERS & UTILITIES
 * ================================================================ */

static bool sum_spw_reducer(io_record_t *res, const io_record_t *r, size_t num_r, aml_buffer_t *bh, void *tag __attribute__((unused))) {
    double total = 0.0;
    for (size_t i = 0; i < num_r; i++) {
        double w; memcpy(&w, r[i].record, sizeof(double));
        total += w;
    }
    aml_buffer_clear(bh);
    aml_buffer_append(bh, &total, sizeof(double));
    size_t str_len = r[0].length - sizeof(double);
    aml_buffer_append(bh, r[0].record + sizeof(double), str_len);
    res->record = aml_buffer_data(bh);
    res->length = aml_buffer_length(bh);
    return true;
}

static int sp_cmp_a(const io_record_t *a, const io_record_t *b, void *arg __attribute__((unused))) {
    return strcmp((const char*)a->record, (const char*)b->record);
}


/* ================================================================
 * 3. DATA INGESTION
 * ================================================================ */

static void items_json_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    aml_pool_t *pool = aml_pool_init(4096);
    io_record_t *r;
    while ((r = io_in_advance(ins[0])) != NULL) {
        aml_pool_clear(pool);
        aml_buffer_clear(amr_worker_buffer(w));
        aml_buffer_append(amr_worker_buffer(w), r->record, r->length);
        aml_buffer_appendc(amr_worker_buffer(w), '\0');

        ajson_t *root = ajson_parse_string(pool, aml_buffer_data(amr_worker_buffer(w)));
        if (!ajson_is_error(root)) {
            char *asin = ajsono_get_strd(pool, root, "parent_asin", NULL);
            char *title = ajsono_get_strd(pool, root, "title", NULL);
            if (asin && title) {
                amr_string_pair_t sp = { asin, title };
                amr_worker_serialize(w, 0, outs[0], &sp);
            }
        }
    }
    aml_pool_destroy(pool);
}

static bool app_ingest_items_setup(amr_task_t *t) {
    amr_task_input_files(t, ITEMS_FILE, 1.0, NULL);
    amr_task_input_format(t, io_delimiter('\n'));

    // Output: Partitioned and sorted by ASIN. This is perfect for streaming joins!
    amr_task_output(t, "items.dict", 1.0);
    amr_task_output_type(t, "StringPair");
    amr_task_output_shuffle_by(t, "Hash_A", NULL);
    amr_task_output_sort_by(t, "Sort_A", NULL);
    amr_task_output_reduce_by_keeping_first(t);

    amr_task_io_transform(t, ITEMS_FILE, "items.dict", items_json_runner);
    return true;
}

static void events_json_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    aml_pool_t *pool = aml_pool_init(4096);
    io_record_t *r;
    while ((r = io_in_advance(ins[0])) != NULL) {
        aml_pool_clear(pool);
        aml_buffer_clear(amr_worker_buffer(w));
        aml_buffer_append(amr_worker_buffer(w), r->record, r->length);
        aml_buffer_appendc(amr_worker_buffer(w), '\0');

        ajson_t *root = ajson_parse_string(pool, aml_buffer_data(amr_worker_buffer(w)));
        if (!ajson_is_error(root)) {
            char *uid  = ajsono_get_strd(pool, root, "user_id", NULL);
            char *asin = ajsono_get_strd(pool, root, "parent_asin", NULL);
            if (uid && asin) {
                amr_string_pair_t sp = { uid, asin };
                amr_worker_serialize(w, 0, outs[0], &sp);
            }
        }
    }
    aml_pool_destroy(pool);
}

static bool app_ingest_events_setup(amr_task_t *t) {
    amr_task_input_files(t, EVENTS_FILE, 1.0, NULL);
    amr_task_input_format(t, io_delimiter('\n'));

    amr_task_output(t, "sessions.bin", 1.0);
    amr_task_output_type(t, "StringPair");
    amr_task_output_shuffle_by(t, "Hash_A", NULL);
    amr_task_output_sort_by(t, "Sort_A_B", NULL);
    amr_task_output_reduce_by_keeping_first(t);

    amr_task_io_transform(t, EVENTS_FILE, "sessions.bin", events_json_runner);
    return true;
}


/* ================================================================
 * 4. CORE RECOMMENDER ALGORITHM (Map & Reduce)
 * ================================================================ */

static void generate_pairs_runner(amr_worker_t *w, io_record_t *first, size_t num_r, io_out_t **outs) {
    size_t limit = num_r; // > 50 ? 50 : num_r;

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
    amr_task_input_from_task_shuffle(t, "app_ingest_events", "sessions.bin", 1.0);
    amr_task_input_expect_type(t, "StringPair");

    amr_task_output(t, "raw_pairs.bin", 0.5);
    amr_task_output_type(t, "StringPairWeight");
    amr_task_output_shuffle_by(t, "Hash_A", NULL);

    amr_task_group_transform(t, "sessions.bin", "raw_pairs.bin", generate_pairs_runner, sp_cmp_a);
    return true;
}

static bool reduce_pairs_setup(amr_task_t *t) {
    amr_task_input_from_task_shuffle(t, "generate_pairs", "raw_pairs.bin", 1.0);
    amr_task_input_expect_type(t, "StringPairWeight");

    amr_task_output(t, "reduced_pairs.bin", 0.5);
    amr_task_output_type(t, "StringPairWeight");
    amr_task_output_sort_by(t, "Sort_A_B", NULL);
    amr_task_output_reduce_by(t, "SumSPW", NULL);

    amr_task_default_runner(t);
    amr_task_transform(t, "raw_pairs.bin", "reduced_pairs.bin", NULL);
    return true;
}

/* ================================================================
 * 5. THE DISTRIBUTED SORT-MERGE JOINS
 * ================================================================ */

// Filter weight >= 2.0 AND route by 'B' to align with items.dict for the first join
static void route_b_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    amr_string_pair_weight_t *spw = amr_worker_deserialize(w, 0, r);
    if (spw->weight >= 2.0) {
        amr_worker_serialize(w, 0, outs[0], spw);
    }
}

static bool filter_route_b_setup(amr_task_t *t) {
    amr_task_input_from_task_partition(t, "reduce_pairs", "reduced_pairs.bin", 0.5);
    amr_task_input_expect_type(t, "StringPairWeight");

    amr_task_output(t, "b_routed.bin", 0.5);
    amr_task_output_type(t, "StringPairWeight");

    // We shuffle to B, and sort by B! This perfectly aligns with items.dict
    amr_task_output_shuffle_by(t, "Hash_B", NULL);
    amr_task_output_sort_by(t, "Sort_B", NULL);

    amr_task_default_runner(t);
    amr_task_transform(t, "reduced_pairs.bin", "b_routed.bin", route_b_runner);
    return true;
}

// Join B
static void join_b_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    io_record_t *r_spw = io_in_advance(ins[0]);   // b_routed.bin (sorted by B)
    io_record_t *r_item = io_in_advance(ins[1]);  // items.dict (sorted by A (ASIN))

    while (r_spw) {
        aml_pool_clear(amr_worker_scratch_pool(w));
        amr_string_pair_weight_t *spw = amr_worker_deserialize(w, 0, r_spw);

        // Fast forward items until Item >= spw->B
        while (r_item) {
            amr_string_pair_t *item = amr_worker_deserialize(w, 1, r_item);
            if (strcmp(item->a, spw->b) < 0) {
                r_item = io_in_advance(ins[1]);
            } else {
                break;
            }
        }

        char *title_b = spw->b; // Default to ASIN if title missing
        if (r_item) {
            amr_string_pair_t *item = amr_worker_deserialize(w, 1, r_item);
            if (strcmp(item->a, spw->b) == 0) title_b = item->b;
        }

        half_enriched_t out = { spw->weight, spw->a, spw->b, title_b };
        amr_worker_serialize(w, 0, outs[0], &out);

        r_spw = io_in_advance(ins[0]);
    }
}

static bool join_b_setup(amr_task_t *t) {
    // Both inputs are accessed as a 1-to-1 PARTITION edge because they are
    // mathematically guaranteed to live on the same worker (both hashed by ASIN)!
    amr_task_input_from_task_shuffle(t, "filter_route_b", "b_routed.bin", 0.5);
    amr_task_input_expect_type(t, "StringPairWeight");

    amr_task_input_from_task_shuffle(t, "app_ingest_items", "items.dict", 0.5);
    amr_task_input_expect_type(t, "StringPair");

    amr_task_output(t, "half_enriched.bin", 0.5);
    amr_task_output_type(t, "HalfEnriched");

    // For the final join, shuffle it back to A, and sort by A -> Weight Descending!
    amr_task_output_shuffle_by(t, "Hash_A", NULL);
    amr_task_output_sort_by(t, "Sort_A_WDesc", NULL);

    amr_task_io_transform(t, "b_routed.bin|items.dict", "half_enriched.bin", join_b_runner);
    return true;
}

// Join A
static void join_a_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    io_record_t *r_half = io_in_advance(ins[0]);  // half_enriched.bin (sorted by A)
    io_record_t *r_item = io_in_advance(ins[1]);  // items.dict (sorted by A)

    while (r_half) {
        aml_pool_clear(amr_worker_scratch_pool(w));
        half_enriched_t *half = amr_worker_deserialize(w, 0, r_half);

        while (r_item) {
            amr_string_pair_t *item = amr_worker_deserialize(w, 1, r_item);
            if (strcmp(item->a, half->a) < 0) {
                r_item = io_in_advance(ins[1]);
            } else {
                break;
            }
        }

        char *title_a = half->a;
        if (r_item) {
            amr_string_pair_t *item = amr_worker_deserialize(w, 1, r_item);
            if (strcmp(item->a, half->a) == 0) title_a = item->b;
        }

        full_enriched_t out = { half->w, half->a, title_a, half->b, half->title_b };
        amr_worker_serialize(w, 0, outs[0], &out);

        r_half = io_in_advance(ins[0]);
    }
}

static bool join_a_setup(amr_task_t *t) {
    // Both already aligned by Hash_A!
    amr_task_input_from_task_shuffle(t, "join_b", "half_enriched.bin", 0.5);
    amr_task_input_expect_type(t, "HalfEnriched");

    amr_task_input_from_task_shuffle(t, "app_ingest_items", "items.dict", 0.5);
    amr_task_input_expect_type(t, "StringPair");

    amr_task_output(t, "full_enriched.bin", 0.5);
    amr_task_output_type(t, "FullEnriched");
    // No sort required! Sequential merge-join perfectly preserves the A_WDesc sort!

    amr_task_io_transform(t, "half_enriched.bin|items.dict", "full_enriched.bin", join_a_runner);
    return true;
}


/* ================================================================
 * 6. FINAL JSON FORMATTING
 * ================================================================ */
static void format_json_runner(amr_worker_t *w, io_record_t *first, size_t num_r, io_out_t **outs) {
    aml_pool_t *pool = amr_worker_scratch_pool(w);
    aml_buffer_t *bh = amr_worker_buffer(w);

    // The streams arrive perfectly sorted. Top 20 limits memory usage!
    full_enriched_t *first_fe = amr_worker_deserialize(w, 0, &first[0]);

    ajson_t *root = ajsono(pool);
    ajsono_append(root, "asin", ajson_str_nocopy(pool, first_fe->a), false);
    ajsono_append(root, "title", ajson_encode_str_nocopy(pool, first_fe->title_a), false);

    ajson_t *recs = ajsona(pool);
    size_t limit = num_r > 20 ? 20 : num_r;

    for (size_t i = 0; i < limit; i++) {
        full_enriched_t *fe = amr_worker_deserialize(w, 0, &first[i]);

        ajson_t *rec_obj = ajsono(pool);
        ajsono_append(rec_obj, "asin", ajson_str_nocopy(pool, fe->b), false);
        ajsono_append(rec_obj, "title", ajson_encode_str_nocopy(pool, fe->title_b), false);
        ajsono_append(rec_obj, "co_reviews", ajson_decimal_stringf(pool, "%.0f", fe->w), false);
        ajsona_append(recs, rec_obj);
    }

    ajsono_append(root, "recommendations", recs, false);

    aml_buffer_clear(bh);
    ajson_dump_to_buffer(bh, root);
    aml_buffer_appendc(bh, '\n');
    io_out_write_record(outs[0], aml_buffer_data(bh), aml_buffer_length(bh));
}

static bool app_format_json_setup(amr_task_t *t) {
    amr_task_input_from_task_partition(t, "join_a", "full_enriched.bin", 0.5);
    amr_task_input_expect_type(t, "FullEnriched");

    amr_task_output(t, "recommendations.jsonl", 1.0);
    amr_task_output_format(t, io_delimiter('\n'));

    amr_task_group_transform(t, "full_enriched.bin", "recommendations.jsonl", format_json_runner, fe_cmp_a);
    return true;
}


/* ================================================================
 * 7. MAIN EXECUTION
 * ================================================================ */
int main(int argc, char **argv) {
    if (access(ITEMS_FILE, F_OK) != 0 || access(EVENTS_FILE, F_OK) != 0) {
        fprintf(stderr, "\n[ERROR] Data files not found!\n");
        fprintf(stderr, "Please run the downloader script first:\n");
        fprintf(stderr, "  cd ../data && ./download_data.sh\n\n");
        return 1;
    }

    size_t parts = 4;
    int shift = 1;

    if (argc > 1 && argv[1][0] != '-') {
        parts = (size_t)atoi(argv[1]);
        if (parts == 0) parts = 4;
        shift = 2;
    }

    printf("[INFO] Starting Streaming Recommender DAG. Partitions: %zu\n", parts);

    amr_t *sched = amr_init(argc - shift, argv + shift, parts, 12, 4096);

    // Uncomment if you wish to preserve intermediate files
    // amr_keep_intermediate_files(sched);


    amr_register_common_datatypes(sched);
    amr_datatype_add_reducer(sched, "StringPairWeight", "SumSPW", sum_spw_reducer);
    register_custom_types(sched);

    // Wire the DAG
    amr_task(sched, "app_ingest_items",    true, app_ingest_items_setup);
    amr_task(sched, "app_ingest_events",   true, app_ingest_events_setup);

    amr_task(sched, "generate_pairs",      true, generate_pairs_setup);
    amr_task(sched, "reduce_pairs",        true, reduce_pairs_setup);
    amr_task(sched, "filter_route_b",      true, filter_route_b_setup);

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
