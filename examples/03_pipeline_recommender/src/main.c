// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "a-map-reduce-library/amr.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include "the-io-library/io.h"
#include "the-io-library/io_in.h"
#include "the-io-library/io_out.h"
#include "a-json-library/ajson.h"

// Include our custom pipelines!
#include "pipeline_co_freq.h"
#include "pipeline_enrich.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static const char *ITEMS_FILE = "../data/amazon_2023/meta.jsonl";
static const char *EVENTS_FILE = "../data/amazon_2023/reviews.jsonl";

// Store globally so the formatting task can pull from the pipeline output
static amr_pipeline_t *enrich_pipeline = NULL;

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

/* ================================================================
 * 1. DATA INGESTION
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
 * 2. FINAL JSON FORMATTER
 * ================================================================ */
static int fe_cmp_a(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const char *a1 = rA->record + sizeof(double);
    const char *a2 = rB->record + sizeof(double);
    return strcmp(a1, a2);
}

static void format_json_runner(amr_worker_t *w, io_record_t *first, size_t num_r, io_out_t **outs) {
    aml_pool_t *pool = amr_worker_scratch_pool(w);
    aml_buffer_t *bh = amr_worker_buffer(w);

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
    io_out_write_record(outs[0], aml_buffer_data(bh), aml_buffer_length(bh));
}

static bool app_format_json_setup(amr_task_t *t) {
    // Pull from the pipeline output port. The framework will now internally map
    // the physical file but expose it to us under this exact alias!
    amr_task_input_from_pipeline_partition(t, enrich_pipeline, "out_enriched", 0.5);
    amr_task_input_expect_type(t, "FullEnriched");

    amr_task_output(t, "recommendations.jsonl", 1.0);
    amr_task_output_format(t, io_delimiter('\n'));

    amr_task_group_transform(t, "out_enriched", "recommendations.jsonl", format_json_runner, fe_cmp_a);
    return true;
}

/* ================================================================
 * 3. MAIN EXECUTION
 * ================================================================ */
int main(int argc, char **argv) {
    if (access(ITEMS_FILE, F_OK) != 0 || access(EVENTS_FILE, F_OK) != 0) {
        fprintf(stderr, "\n[ERROR] Data files not found!\n");
        return 1;
    }

    size_t parts = (argc > 1 && argv[1][0] != '-') ? (size_t)atoi(argv[1]) : 4;
    if (parts == 0) parts = 4;
    int shift = (argc > 1 && argv[1][0] != '-') ? 2 : 1;

    printf("[INFO] Starting Pipelined Recommender DAG. Partitions: %zu\n", parts);

    amr_t *sched = amr_init(argc - shift, argv + shift, parts, 12, 4096);

    // Uncomment if you wish to preserve intermediate files
    // amr_keep_intermediate_files(sched);

    // Register basic dependencies for the pipelines
    amr_register_common_datatypes(sched);
    amr_datatype_add_reducer(sched, "StringPairWeight", "SumSPW", sum_spw_reducer);

    // 1. Setup App Data Extractors
    amr_task(sched, "app_ingest_items",  true, app_ingest_items_setup);
    amr_task(sched, "app_ingest_events", true, app_ingest_events_setup);

    // 2. Map the Pipelines
    amr_pipeline_t *co_freq = amr_pipeline_create(sched, "co_freq", pipeline_co_freq_setup, NULL);
    amr_pipeline_bind_input(co_freq, "in_sessions", "app_ingest_events", "sessions.bin");

    enrich_pipeline = amr_pipeline_create(sched, "enrich", pipeline_enrich_setup, NULL);
    amr_pipeline_bind_link(enrich_pipeline, "in_pairs", co_freq, "out_pairs");
    amr_pipeline_bind_input(enrich_pipeline, "in_dict", "app_ingest_items", "items.dict");

    // 3. Final Formatter
    amr_task(sched, "app_format_json", true, app_format_json_setup);

    bool success = amr_run(sched, amr_worker_complete);
    amr_destroy(sched);

    if (!success) {
        fprintf(stderr, "[ERROR] DAG Execution Failed!\n");
        return 1;
    }
    return 0;
}
