// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

/*
 * Example: Distributed Word Count DAG
 * -----------------------------------
 * This program demonstrates a multi-stage MapReduce pipeline that counts word
 * frequencies in a text file and globally sorts the results.
 * * Pipeline Stages:
 * 1. Ingest      -> Tokenize text and shuffle words to specific partitions.
 * 2. Reduce      -> Sort words alphabetically and sum their counts locally.
 * 3. Global Sort -> Gather all partitions and sort descending by frequency.
 * 4. Format Text -> Convert the binary data back to human-readable text.
 */

#include "a-map-reduce-library/amr.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include "the-io-library/io.h"
#include "the-io-library/io_in.h"
#include "the-io-library/io_out.h"

#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ================================================================
 * Custom Reducer for StringWeight
 * ================================================================ */
/*
 * The framework calls this function when it finds consecutive identical records
 * (which happens after sorting). We take an array of 'StringWeight' records
 * (e.g., fifty records of {"apple", 1.0}) and squash them into a single
 * aggregate record ({"apple", 50.0}).
 */
static bool sum_weights_reducer(io_record_t *res, const io_record_t *r, size_t num_r, aml_buffer_t *bh, void *tag __attribute__((unused))) {
    double total = 0.0;

    // Sum the floating-point weights from all duplicate records
    for (size_t i = 0; i < num_r; i++) {
        double w;
        memcpy(&w, r[i].record, sizeof(double));
        total += w;
    }

    // Write the new total weight, clearing the buffer first
    aml_buffer_set(bh, &total, sizeof(double));

    // Append the string itself (taken from the very first record)
    size_t string_len = r[0].length - sizeof(double);
    aml_buffer_append(bh, r[0].record + sizeof(double), string_len);

    res->record = aml_buffer_data(bh);
    res->length = aml_buffer_length(bh);
    return true;
}

/* ================================================================
 * Stage 1: Ingest (Read & Shuffle)
 * ================================================================ */
/*
 * The runner processes the raw input data. It reads a line of text, extracts
 * alphanumeric words, and emits each word with a starting weight of 1.0.
 */
static void ingest_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    const char *line = (const char *)r->record;
    size_t len = r->length;

    char word_buf[256];
    size_t w_len = 0;

    for (size_t i = 0; i <= len; i++) {
        if (i < len && isalnum(line[i])) {
            if (w_len < 255) word_buf[w_len++] = tolower(line[i]);
        } else if (w_len > 0) {
            word_buf[w_len] = '\0';
            amr_string_weight_t out_rec = { 1.0, word_buf };
            amr_worker_serialize(w, 0, outs[0], &out_rec);
            w_len = 0;
        }
    }
}

static bool ingest_setup(amr_task_t *t) {
    const char *filename = (const char *)amr_task_custom_arg(t);
    amr_task_input_files(t, filename, 0.5, NULL);
    amr_task_input_format(t, io_delimiter('\n'));

    // CRITICAL TRANSFORMATION: Shuffle
    // Shuffling guarantees that every instance of a specific word (like "apple")
    // is sent across the network to the exact same worker partition. If we didn't
    // shuffle, two different workers might count "apple" independently, resulting
    // in fragmented, incomplete totals.
    amr_task_output(t, "mapped_words.bin", 0.5);
    amr_task_output_type(t, "StringWeight");
    amr_task_output_shuffle_by(t, "Hash_Str", NULL);

    amr_task_default_runner(t);
    amr_task_transform(t, filename, "mapped_words.bin", ingest_runner);
    return true;
}

/* ================================================================
 * Stage 2: Local Reduce (Sort & Sum)
 * ================================================================ */
/*
 * This stage takes the shuffled data and aggregates it into actual counts.
 * It uses a "null runner" because all the heavy lifting is done purely by
 * the framework's internal topological sorting and reducing engines.
 */
static bool reduce_setup(amr_task_t *t) {
    amr_task_input_from_task_shuffle(t, "ingest", "mapped_words.bin", 1.0);
    amr_task_input_expect_type(t, "StringWeight");

    amr_task_output(t, "local_counts.bin", 0.5);
    amr_task_output_type(t, "StringWeight");

    // CRITICAL TRANSFORMATIONS: Sort & Reduce
    // 1. Sort_Str forces the framework to alphabetize the records in RAM.
    //    This physically groups identical words right next to each other.
    // 2. SumWeights kicks in automatically when the framework sees those adjacent
    //    duplicates, cleanly collapsing millions of 1.0s into a single total count.
    amr_task_output_sort_by(t, "Sort_Str", NULL);
    amr_task_output_reduce_by(t, "SumWeights", NULL);

    amr_task_default_runner(t);
    amr_task_transform(t, "mapped_words.bin", "local_counts.bin", NULL);
    return true;
}

/* ================================================================
 * Stage 3: Global Gather & Final Sort by Frequency
 * ================================================================ */
static bool global_sort_setup(amr_task_t *t) {
    // CRITICAL TRANSFORMATION: All-to-All Gathering
    // We have N separate worker files holding local alphabetical counts.
    // A single worker pulls them all into memory simultaneously to merge them.
    amr_task_input_from_task_all_to_all(t, "reduce", "local_counts.bin", 1.0);
    amr_task_input_expect_type(t, "StringWeight");

    amr_task_output(t, "global_sorted.bin", 0.5);
    amr_task_output_type(t, "StringWeight");

    // We break the alphabetical order and apply a descending numeric sort
    // so the most frequent words float to the very top of the final matrix.
    amr_task_output_sort_by(t, "Sort_W_Desc", NULL);

    amr_task_default_runner(t);
    amr_task_transform(t, "local_counts.bin", "global_sorted.bin", NULL);
    return true;
}

/* ================================================================
 * Stage 4: Text Formatting
 * ================================================================ */
/*
 * Finally, we read the globally sorted binary stream and convert it into
 * human-readable text for final output.
 */
static void format_text_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    amr_string_weight_t *sw = amr_worker_deserialize(w, 0, r);
    aml_buffer_t *bh = amr_worker_buffer(w);

    // Use setf to instantly clear the buffer and write the new text payload.
    // We do not add a '\n' here because the IO framework handles delimiters natively.
    aml_buffer_setf(bh, "%s\t%.0f", sw->str, sw->weight);

    io_out_write_record(outs[0], aml_buffer_data(bh), aml_buffer_length(bh));
}

static bool format_text_setup(amr_task_t *t) {
    // Read directly from the previous single-worker partition
    amr_task_input_from_task_partition(t, "global_sort", "global_sorted.bin", 1.0);
    amr_task_input_expect_type(t, "StringWeight");

    // Inform the IO layer to separate our printed records with newlines
    amr_task_output(t, "word_counts.txt", 1.0);
    amr_task_output_format(t, io_delimiter('\n'));

    amr_task_default_runner(t);
    amr_task_transform(t, "global_sorted.bin", "word_counts.txt", format_text_runner);
    return true;
}

/* ================================================================
 * Main: Executing the DAG
 * ================================================================ */
int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <input.txt> [partitions] [amr_flags...]\n", argv[0]);
        return 1;
    }

    const char *filename = argv[1];
    size_t parts = (argc > 2 && argv[2][0] != '-') ? (size_t)atoi(argv[2]) : 4;
    int shift = (argc > 2 && argv[2][0] != '-') ? 3 : 2;

    // Initialize the scheduler with the requested number of worker partitions
    amr_t *sched = amr_init(argc - shift, argv + shift, parts, 4, 1024);

    // Register our common data types and our custom squash logic
    amr_register_common_datatypes(sched);
    amr_datatype_add_reducer(sched, "StringWeight", "SumWeights", sum_weights_reducer);

    // Pass the input file down into the DAG tasks
    amr_custom_args(sched, NULL, NULL, NULL, (void *)filename);

    // Wire up the Directed Acyclic Graph (DAG) in chronological order
    amr_task(sched, "ingest",      false, ingest_setup);
    amr_task(sched, "reduce",      true,  reduce_setup);
    amr_task(sched, "global_sort", false, global_sort_setup);
    amr_task(sched, "format_text", false, format_text_setup);

    // Execute the distributed pipeline
    bool success = amr_run(sched, amr_worker_complete);
    amr_destroy(sched);

    if (!success) {
        fprintf(stderr, "[ERROR] DAG Execution Failed!\n");
        return 1;
    }
    return 0;
}
