// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "pipeline_amazon.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include "a-json-library/ajson.h"
#include <string.h>

/* ================================================================
 * ITEMS (META) SCATTER & INGESTION
 * ================================================================ */

static bool scatter_items_setup(amr_task_t *t) {
    amr_pipeline_t *pipe = amr_task_pipeline(t);
    amazon_config_t *cfg = (amazon_config_t *)amr_pipeline_config(pipe);

    amr_task_input_files(t, cfg->items_file, 1.0, NULL);
    amr_task_input_format(t, io_delimiter('\n'));

    amr_task_output(t, "scattered_items.jsonl", 1.0);
    amr_task_output_format(t, io_delimiter('\n'));

    // Actively deal the lines out across the partition buckets!
    amr_task_output_shuffle(t);

    amr_task_default_runner(t);
    amr_task_io_transform(t, cfg->items_file, "scattered_items.jsonl", NULL);
    return true;
}

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

static bool ingest_items_setup(amr_task_t *t) {
    // Read the perfectly scattered chunks from the sibling task!
    amr_task_input_from_sibling_shuffle(t, "scatter_items", "scattered_items.jsonl", 1.0);
    amr_task_input_format(t, io_delimiter('\n'));

    amr_task_output(t, "items.dict", 1.0);
    amr_task_output_type(t, "StringPair");
    amr_task_output_shuffle_by(t, "Hash_A", NULL);
    amr_task_output_sort_by(t, "Sort_A", NULL);
    amr_task_output_reduce_by_keeping_first(t);

    amr_task_io_transform(t, "scattered_items.jsonl", "items.dict", items_json_runner);
    return true;
}

/* ================================================================
 * EVENTS (REVIEWS) SCATTER & INGESTION
 * ================================================================ */

static bool scatter_events_setup(amr_task_t *t) {
    amr_pipeline_t *pipe = amr_task_pipeline(t);
    amazon_config_t *cfg = (amazon_config_t *)amr_pipeline_config(pipe);

    amr_task_input_files(t, cfg->events_file, 1.0, NULL);
    amr_task_input_format(t, io_delimiter('\n'));

    amr_task_output(t, "scattered_events.jsonl", 1.0);
    amr_task_output_format(t, io_delimiter('\n'));

    // Actively deal the lines out across the partition buckets!
    amr_task_output_shuffle(t);

    amr_task_default_runner(t);
    amr_task_io_transform(t, cfg->events_file, "scattered_events.jsonl", NULL);
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

static bool ingest_events_setup(amr_task_t *t) {
    // Read the perfectly scattered chunks from the sibling task!
    amr_task_input_from_sibling_shuffle(t, "scatter_events", "scattered_events.jsonl", 1.0);
    amr_task_input_format(t, io_delimiter('\n'));

    amr_task_output(t, "sessions.bin", 1.0);
    amr_task_output_type(t, "StringPair");
    amr_task_output_shuffle_by(t, "Hash_A", NULL);
    amr_task_output_sort_by(t, "Sort_A_B", NULL);
    amr_task_output_reduce_by_keeping_first(t);

    amr_task_io_transform(t, "scattered_events.jsonl", "sessions.bin", events_json_runner);
    return true;
}

/* ================================================================
 * PIPELINE DEFINITION
 * ================================================================ */
bool pipeline_amazon_setup(amr_pipeline_t *p) {
    // 1. The Scatter Phase: Singletons reading the raw files and chunking them.
    amr_pipeline_task(p, "scatter_items", false, scatter_items_setup);
    amr_pipeline_task(p, "scatter_events", false, scatter_events_setup);

    // 2. The Ingestion Phase: Parallel workers grabbing the chunks and DOM parsing them.
    amr_pipeline_task(p, "ingest_items", true, ingest_items_setup);
    amr_pipeline_task(p, "ingest_events", true, ingest_events_setup);

    // 3. Expose the parallel artifacts!
    amr_pipeline_bind_output(p, "out_dict", "ingest_items", "items.dict");
    amr_pipeline_bind_output(p, "out_sessions", "ingest_events", "sessions.bin");

    return true;
}
