// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "pipeline_amazon.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include "a-json-sax-library/ajson_sax.h"
#include "a-json-sax-library/ajson_string_utils.h"

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

    amr_task_output_shuffle(t);

    // The Magic: Default runner just passes the raw lines straight through!
    amr_task_default_runner(t);
    amr_task_io_transform(t, cfg->items_file, "scattered_items.jsonl", NULL);
    return true;
}

typedef enum { ITEM_KEY_NONE, ITEM_KEY_ASIN, ITEM_KEY_TITLE } item_key_state_t;
typedef struct { char *asin; char *title; item_key_state_t active_key; } item_sax_ctx_t;

static int item_on_key(void *ctx, ajson_sax_t *sax, const char *key, size_t len __attribute__((unused))) {
    item_sax_ctx_t *c = (item_sax_ctx_t *)ctx;
    c->active_key = ITEM_KEY_NONE;
    if (sax->current_depth == 1) {
        if (strcmp(key, "parent_asin") == 0) c->active_key = ITEM_KEY_ASIN;
        else if (strcmp(key, "title") == 0) c->active_key = ITEM_KEY_TITLE;
    }
    return 0;
}

static int item_on_string(void *ctx, ajson_sax_t *sax, const char *val, size_t len) {
    item_sax_ctx_t *c = (item_sax_ctx_t *)ctx;
    if (c->active_key == ITEM_KEY_ASIN) {
        c->asin = (char *)val;
    } else if (c->active_key == ITEM_KEY_TITLE) {
        c->title = ajson_decode(sax->pool, (char *)val, len);
    }
    c->active_key = ITEM_KEY_NONE;
    return 0;
}

static const ajson_sax_cb_t item_sax_cb = { .on_key = item_on_key, .on_string = item_on_string };

static void items_json_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    aml_pool_t *pool = amr_worker_scratch_pool(w);
    aml_buffer_t *bh = amr_worker_buffer(w);
    io_record_t *r;
    char *err = NULL;

    while ((r = io_in_advance(ins[0])) != NULL) {
        aml_pool_clear(pool);
        aml_buffer_clear(bh);
        aml_buffer_append(bh, r->record, r->length);
        aml_buffer_appendc(bh, '\0');

        item_sax_ctx_t ctx = {0};
        char *p = aml_buffer_data(bh);
        char *ep = p + r->length;

        ajson_sax_parse_destructive(p, ep, &item_sax_cb, pool, &ctx, &err);

        if (ctx.asin && ctx.title) {
            amr_string_pair_t sp = { ctx.asin, ctx.title };
            amr_worker_serialize(w, 0, outs[0], &sp);
        }
    }
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

    amr_task_output_shuffle(t);

    amr_task_default_runner(t);
    amr_task_io_transform(t, cfg->events_file, "scattered_events.jsonl", NULL);
    return true;
}

typedef enum { EVENT_KEY_NONE, EVENT_KEY_UID, EVENT_KEY_ASIN } event_key_state_t;
typedef struct { char *uid; char *asin; event_key_state_t active_key; } event_sax_ctx_t;

static int event_on_key(void *ctx, ajson_sax_t *sax, const char *key, size_t len __attribute__((unused))) {
    event_sax_ctx_t *c = (event_sax_ctx_t *)ctx;
    c->active_key = EVENT_KEY_NONE;
    if (sax->current_depth == 1) {
        if (strcmp(key, "user_id") == 0) c->active_key = EVENT_KEY_UID;
        else if (strcmp(key, "parent_asin") == 0) c->active_key = EVENT_KEY_ASIN;
    }
    return 0;
}

static int event_on_string(void *ctx, ajson_sax_t *sax __attribute__((unused)), const char *val, size_t len __attribute__((unused))) {
    event_sax_ctx_t *c = (event_sax_ctx_t *)ctx;
    if (c->active_key == EVENT_KEY_UID) c->uid = (char *)val;
    else if (c->active_key == EVENT_KEY_ASIN) c->asin = (char *)val;
    c->active_key = EVENT_KEY_NONE;
    return 0;
}

static const ajson_sax_cb_t event_sax_cb = { .on_key = event_on_key, .on_string = event_on_string };

static void events_json_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    aml_pool_t *pool = amr_worker_scratch_pool(w);
    aml_buffer_t *bh = amr_worker_buffer(w);
    io_record_t *r;
    char *err = NULL;

    while ((r = io_in_advance(ins[0])) != NULL) {
        aml_pool_clear(pool);
        aml_buffer_clear(bh);
        aml_buffer_append(bh, r->record, r->length);
        aml_buffer_appendc(bh, '\0');

        event_sax_ctx_t ctx = {0};
        char *p = aml_buffer_data(bh);
        char *ep = p + r->length;

        ajson_sax_parse_destructive(p, ep, &event_sax_cb, pool, &ctx, &err);

        if (ctx.uid && ctx.asin) {
            amr_string_pair_t sp = { ctx.uid, ctx.asin };
            amr_worker_serialize(w, 0, outs[0], &sp);
        }
    }
}

static bool ingest_events_setup(amr_task_t *t) {
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

    // 2. The Ingestion Phase: Parallel workers grabbing the chunks and SAX parsing them.
    amr_pipeline_task(p, "ingest_items", true, ingest_items_setup);
    amr_pipeline_task(p, "ingest_events", true, ingest_events_setup);

    // 3. Expose the parallel artifacts!
    amr_pipeline_bind_output(p, "out_dict", "ingest_items", "items.dict");
    amr_pipeline_bind_output(p, "out_sessions", "ingest_events", "sessions.bin");

    return true;
}
