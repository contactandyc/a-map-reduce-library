// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "pipeline_enrich.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include <string.h>

// --- Internal Data Types ---
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
static size_t he_part_a(const io_record_t *r, size_t np, void *arg __attribute__((unused))) {
    const char *a = r->record + sizeof(double);
    size_t h = 0; while (*a) h = h * 131 + (unsigned char)*a++;
    return h % np;
}
static int he_cmp_a_wdesc(const io_record_t *rA, const io_record_t *rB, void *arg __attribute__((unused))) {
    const char *a1 = rA->record + sizeof(double);
    const char *a2 = rB->record + sizeof(double);
    int c = strcmp(a1, a2);
    if (c != 0) return c;

    double w1; memcpy(&w1, rA->record, sizeof(double));
    double w2; memcpy(&w2, rB->record, sizeof(double));
    if (w1 > w2) return -1;
    if (w1 < w2) return 1;

    // EXPLICIT TIE-BREAKER: Sort alphabetically by ASIN B
    // Jump past the double and the null-terminated A string to get to B
    const char *b1 = rA->record + sizeof(double) + strlen(a1) + 1;
    const char *b2 = rB->record + sizeof(double) + strlen(a2) + 1;
    return strcmp(b1, b2);
}

// --- Full Enriched Type Reg ---
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

static void register_types(amr_t *sched) {
    static bool registered = false;
    if (registered) return;
    amr_register_datatype(sched, "HalfEnriched", "", he_ser, he_des, he_str);
    amr_datatype_add_partition(sched, "HalfEnriched", "Hash_A", he_part_a);
    amr_datatype_add_compare(sched, "HalfEnriched", "Sort_A_WDesc", he_cmp_a_wdesc);

    amr_register_datatype(sched, "FullEnriched", "", fe_ser, fe_des, fe_str);
    amr_datatype_add_compare(sched, "FullEnriched", "Sort_A", fe_cmp_a);
    registered = true;
}

// --- Tasks ---
static void route_b_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    amr_string_pair_weight_t *spw = amr_worker_deserialize(w, 0, r);
    if (spw->weight >= 2.0) {
        amr_worker_serialize(w, 0, outs[0], spw);
    }
}
static bool filter_route_b_setup(amr_task_t *t) {
    amr_task_input_from_pipeline_port_partition(t, "in_pairs", 0.5);
    amr_task_input_expect_type(t, "StringPairWeight");

    amr_task_output(t, "b_routed.bin", 0.5);
    amr_task_output_type(t, "StringPairWeight");
    amr_task_output_shuffle_by(t, "Hash_B", NULL);
    amr_task_output_sort_by(t, "Sort_B", NULL);

    amr_task_default_runner(t);
    amr_task_transform(t, "in_pairs", "b_routed.bin", route_b_runner);
    return true;
}

static void join_b_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    io_record_t *r_spw = io_in_advance(ins[0]);
    io_record_t *r_item = io_in_advance(ins[1]);
    while (r_spw) {
        aml_pool_clear(amr_worker_scratch_pool(w));
        amr_string_pair_weight_t *spw = amr_worker_deserialize(w, 0, r_spw);
        while (r_item) {
            amr_string_pair_t *item = amr_worker_deserialize(w, 1, r_item);
            if (strcmp(item->a, spw->b) < 0) r_item = io_in_advance(ins[1]);
            else break;
        }
        char *title_b = spw->b;
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
    amr_task_input_from_sibling_shuffle(t, "filter_route_b", "b_routed.bin", 0.5);
    amr_task_input_expect_type(t, "StringPairWeight");
    // Inherits "Sort_B" automatically!

    amr_task_input_from_pipeline_port_shuffle(t, "in_dict", 0.5);
    amr_task_input_expect_type(t, "StringPair");
    // Inherits "Sort_A" automatically!

    amr_task_output(t, "half_enriched.bin", 0.5);
    amr_task_output_type(t, "HalfEnriched");
    amr_task_output_shuffle_by(t, "Hash_A", NULL);
    amr_task_output_sort_by(t, "Sort_A_WDesc", NULL);

    amr_task_io_transform(t, "b_routed.bin|in_dict", "half_enriched.bin", join_b_runner);
    return true;
}

static void join_a_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins __attribute__((unused)), io_out_t **outs, size_t num_outs __attribute__((unused))) {
    io_record_t *r_half = io_in_advance(ins[0]);
    io_record_t *r_item = io_in_advance(ins[1]);
    while (r_half) {
        aml_pool_clear(amr_worker_scratch_pool(w));
        half_enriched_t *half = amr_worker_deserialize(w, 0, r_half);
        while (r_item) {
            amr_string_pair_t *item = amr_worker_deserialize(w, 1, r_item);
            if (strcmp(item->a, half->a) < 0) r_item = io_in_advance(ins[1]);
            else break;
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
    amr_task_input_from_sibling_shuffle(t, "join_b", "half_enriched.bin", 0.5);
    amr_task_input_expect_type(t, "HalfEnriched");

    amr_task_input_from_pipeline_port_shuffle(t, "in_dict", 0.5);
    amr_task_input_expect_type(t, "StringPair");

    amr_task_output(t, "full_enriched.bin", 0.5);
    amr_task_output_type(t, "FullEnriched");

    amr_task_io_transform(t, "half_enriched.bin|in_dict", "full_enriched.bin", join_a_runner);
    return true;
}

bool pipeline_enrich_setup(amr_pipeline_t *p) {
    register_types(amr_pipeline_scheduler(p));
    amr_pipeline_task(p, "filter_route_b", true, filter_route_b_setup);
    amr_pipeline_task(p, "join_b", true, join_b_setup);
    amr_pipeline_task(p, "join_a", true, join_a_setup);
    amr_pipeline_bind_output(p, "out_enriched", "join_a", "full_enriched.bin");
    return true;
}
