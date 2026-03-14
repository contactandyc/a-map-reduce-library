// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "a-map-reduce-library/amr.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include "a-json-library/ajson.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

// Include our modular pipelines
#include "pipeline_amazon.h"
#include "pipeline_co_freq.h"
#include "pipeline_enrich.h"

static const char *ITEMS_FILE = "../data/amazon_2023/meta.jsonl";
static const char *EVENTS_FILE = "../data/amazon_2023/reviews.jsonl";

static amr_pipeline_t *enrich_pipeline = NULL;

/* ================================================================
 * FINAL JSON FORMATTER TASK
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
    amr_task_input_from_pipeline_partition(t, enrich_pipeline, "out_enriched", 0.5);
    amr_task_input_expect_type(t, "FullEnriched");

    amr_task_output(t, "recommendations.jsonl", 1.0);
    amr_task_output_format(t, io_delimiter('\n'));

    amr_task_group_transform(t, "out_enriched", "recommendations.jsonl", format_json_runner, fe_cmp_a);
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

    size_t parts = (argc > 1 && argv[1][0] != '-') ? (size_t)atoi(argv[1]) : 4;
    if (parts == 0) parts = 4;
    int shift = (argc > 1 && argv[1][0] != '-') ? 2 : 1;

    printf("[INFO] Starting Pipelined Recommender DAG. Partitions: %zu\n", parts);

    amr_t *sched = amr_init(argc - shift, argv + shift, parts, 12, 4096);
    amr_register_common_datatypes(sched);

    // 1. Initialize the Amazon Ingestion Pipeline with config
    amazon_config_t amazon_cfg = { ITEMS_FILE, EVENTS_FILE };
    amr_pipeline_t *amazon_pipe = amr_pipeline_create(sched, "amazon", pipeline_amazon_setup, &amazon_cfg);

    // 2. Initialize the Math Pipeline
    amr_pipeline_t *co_freq = amr_pipeline_create(sched, "co_freq", pipeline_co_freq_setup, NULL);
    amr_pipeline_bind_link(co_freq, "in_sessions", amazon_pipe, "out_sessions");

    // 3. Initialize the Enrichment Pipeline
    enrich_pipeline = amr_pipeline_create(sched, "enrich", pipeline_enrich_setup, NULL);
    amr_pipeline_bind_link(enrich_pipeline, "in_pairs", co_freq, "out_pairs");
    amr_pipeline_bind_link(enrich_pipeline, "in_dict",  amazon_pipe, "out_dict");

    // 4. Final Formatter Task
    amr_task(sched, "app_format_json", true, app_format_json_setup);

    // Execute!
    bool success = amr_run(sched, amr_worker_complete);
    amr_destroy(sched);

    if (!success) {
        fprintf(stderr, "[ERROR] DAG Execution Failed!\n");
        return 1;
    }
    return 0;
}
