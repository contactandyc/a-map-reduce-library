// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-map-reduce-library/amr.h"
#include <stdio.h>
#include <string.h>

/* ================================================================
 * Stage 1: The Raw Event Generator (Simulating a firehose)
 * ================================================================ */
static void generator_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins, io_out_t **outs, size_t num_outs) {
    (void)ins; (void)num_ins; (void)num_outs;

    // Simulate a highly repetitive clickstream log
    const char *categories[] = { "Search", "Search", "AI", "AI", "AI", "Search" };
    const char *urls[]       = { "google", "bing", "openai", "anthropic", "openai", "google" };

    for (size_t i = 0; i < 6; i++) {
        // We emit 10 identical records for each of the 6 events above to simulate volume
        for (size_t j = 0; j < 10; j++) {
            amr_string_pair_weight_t rec = { 1.0, (char *)categories[i], (char *)urls[i] };
            amr_worker_serialize(w, 0, outs[0], &rec);
        }
    }
}

static bool generator_setup(amr_task_t *t) {
    amr_task_output(t, "raw_clicks.bin", 1.0);
    amr_task_output_type(t, "StringPairWeight");
    amr_task_io_transform(t, "", "raw_clicks.bin", generator_runner);
    return true;
}

/* ================================================================
 * Stage 2: The Chained Grouping Task
 * ================================================================ */

// Boundary Logic: Tells the group_runner when a new Category begins.
static int group_by_category(const io_record_t *a, const io_record_t *b, void *arg) {
    (void)arg;
    // Memory layout of StringPairWeight: [double w][null-term a][null-term b]
    // We offset by sizeof(double) to compare just the 'a' string (Category)
    const char *cat_a = (const char *)a->record + sizeof(double);
    const char *cat_b = (const char *)b->record + sizeof(double);
    return strcmp(cat_a, cat_b);
}

// The Group Runner: Fires ONCE per unique Category.
static void summarize_category_runner(amr_worker_t *w, io_record_t *r, size_t num_r, io_out_t **outs) {
    aml_buffer_t *bh = amr_worker_buffer(w);

    // Extract the category from r[0] since every record in this array shares it
    amr_string_pair_weight_t *first = amr_worker_deserialize(w, 0, &r[0]);
    aml_buffer_setf(bh, "CATEGORY: [%s]\n", first->a);

    double total_category_clicks = 0;

    // Loop through the pre-compressed URLs in this category
    for (size_t i = 0; i < num_r; i++) {
        amr_string_pair_weight_t *item = amr_worker_deserialize(w, 0, &r[i]);
        total_category_clicks += item->w;
        aml_buffer_appendf(bh, "  -> %s (%.0f clicks)\n", item->b, item->w);
    }
    aml_buffer_appendf(bh, "  TOTAL: %.0f\n\n", total_category_clicks);

    io_out_write_record(outs[0], aml_buffer_data(bh), aml_buffer_length(bh));
}

static bool analyzer_setup(amr_task_t *t) {
    amr_task_input_from_task_partition(t, "generator", "raw_clicks.bin", 1.0);
    amr_task_input_expect_type(t, "StringPairWeight");

    /* ---------------------------------------------------------
     * TRANSFORM 1: The Map-Side Combiner (Compress the Data)
     * --------------------------------------------------------- */
    amr_task_output(t, "compressed_tmp.bin", 0.5);
    amr_task_output_internal(t);
    amr_task_output_type(t, "StringPairWeight");

    // Sort by BOTH Category and URL ("Sort_A_B"), then squash duplicates
    // summing their weights ("Sum_W").
    // 60 raw records instantly become 4 highly compressed records!
    amr_task_output_sort_by(t, "Sort_A_B", NULL);
    amr_task_output_reduce_by(t, "Sum_W", NULL);

    // We pass NULL for the runner. AMR defaults to an identity copy,
    // letting the I/O layer's sort/reduce handle all the work.
    amr_task_transform(t, "raw_clicks.bin", "compressed_tmp.bin", NULL);

    /* ---------------------------------------------------------
     * TRANSFORM 2: The Group Runner (Complex Processing)
     * --------------------------------------------------------- */
    amr_task_output(t, "final_report.txt", 0.5);
    amr_task_output_format(t, io_delimiter('\n'));

    // Feed the compressed data into the group runner, chunking by Category.
    // Because Transform 1 already sorted the data by Category+URL,
    // the contiguous grouping requirement is perfectly satisfied.
    amr_task_group_transform(t, "compressed_tmp.bin", "final_report.txt",
                             summarize_category_runner, group_by_category);

    return true;
}

/* ================================================================
 * Main
 * ================================================================ */
int main(int argc, char **argv) {
    amr_t *sched = amr_init(argc > 1 ? argc - 1 : 0,
                            argc > 1 ? argv + 1 : NULL,
                            1, 4, 1024);

    amr_register_common_datatypes(sched);

    amr_task(sched, "generator", true, generator_setup);
    amr_task(sched, "analyzer", true, analyzer_setup);

    bool success = amr_run(sched, amr_worker_complete);
    amr_destroy(sched);

    if (success) {
        printf("\nSuccess! Run the dump tool to view the grouped report:\n");
        printf("  cat tasks/analyzer_0/final_report.txt_0\n");
    }
    return success ? 0 : 1;
}
