// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

/*
 * Example: Conditional Branching & Union Inputs (The Cascading Skip)
 * ------------------------------------------------------------------
 * This program demonstrates dynamic DAG routing. We use 4 partitions to
 * test every possible state of a conditional branch:
 * * Partition 0: Routes to EVEN, skips ODD.
 * Partition 1: Routes to ODD, skips EVEN.
 * Partition 2: Routes to BOTH branches (skips neither).
 * Partition 3: Skips BOTH branches (Starves the converge task!).
 */

#include "a-map-reduce-library/amr.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include "the-io-library/io.h"
#include "the-io-library/io_in.h"
#include "the-io-library/io_out.h"

#include <stdio.h>
#include <stdlib.h>

/* ================================================================
 * Stage 1: The Router (Branching Logic)
 * ================================================================ */
static void router_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins, io_out_t **outs, size_t num_outs) {
    (void)ins; (void)num_ins;
    (void)num_outs;

    size_t partition = amr_worker_partition(w);
    aml_pool_t *scratch = amr_worker_scratch_pool(w);

    /* * amr_worker_skip_output is a CONTROL FLOW toggle, not a data toggle.
     * We only need to call it once per partition to kill a downstream branch.
     */
    if (partition == 0) {
        // P0: Take the Even path.
        amr_worker_skip_output(w, 1);

        char *msg = aml_pool_strdupf(scratch, "P0 -> Routed to EVEN branch only");
        amr_string_singleton_t rec = { msg };
        amr_worker_serialize(w, 0, outs[0], &rec);

    } else if (partition == 1) {
        // P1: Take the Odd path.
        amr_worker_skip_output(w, 0);

        char *msg = aml_pool_strdupf(scratch, "P1 -> Routed to ODD branch only");
        amr_string_singleton_t rec = { msg };
        amr_worker_serialize(w, 1, outs[1], &rec);

    } else if (partition == 2) {
        // P2: Take BOTH paths! We skip nothing.
        char *msg_e = aml_pool_strdupf(scratch, "P2 -> Routed to BOTH (Even Payload)");
        amr_string_singleton_t rec_e = { msg_e };
        amr_worker_serialize(w, 0, outs[0], &rec_e);

        char *msg_o = aml_pool_strdupf(scratch, "P2 -> Routed to BOTH (Odd Payload)");
        amr_string_singleton_t rec_o = { msg_o };
        amr_worker_serialize(w, 1, outs[1], &rec_o);

    } else {
        // P3: Skip EVERYTHING!
        // We write 0 records, but more importantly, we explicitly kill the DAG edges.
        // The downstream `converge` task for Partition 3 will starve and skip itself!
        amr_worker_skip_output(w, 0);
        amr_worker_skip_output(w, 1);
    }
}

static bool router_setup(amr_task_t *t) {
    amr_task_output(t, "path_even.bin", 1.0);
    amr_task_output_type(t, "StringSingleton");

    amr_task_output(t, "path_odd.bin", 1.0);
    amr_task_output_type(t, "StringSingleton");

    amr_task_io_transform(t, "", "path_even.bin|path_odd.bin", router_runner);
    return true;
}

/* ================================================================
 * Stage 2a: The "Even" Branch
 * ================================================================ */
static void process_even_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    amr_string_singleton_t *in = amr_worker_deserialize(w, 0, r);
    char *buf = aml_pool_strdupf(amr_worker_scratch_pool(w), "[EVEN] %s", in->str);
    amr_string_singleton_t out = { buf };
    amr_worker_serialize(w, 0, outs[0], &out);
}

static bool process_even_setup(amr_task_t *t) {
    amr_task_input_from_task_partition(t, "router", "path_even.bin", 1.0);
    amr_task_input_expect_type(t, "StringSingleton");

    amr_task_output(t, "processed_even.bin", 1.0);
    amr_task_output_type(t, "StringSingleton");

    amr_task_transform(t, "path_even.bin", "processed_even.bin", process_even_runner);
    return true;
}

/* ================================================================
 * Stage 2b: The "Odd" Branch
 * ================================================================ */
static void process_odd_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    amr_string_singleton_t *in = amr_worker_deserialize(w, 0, r);
    char *buf = aml_pool_strdupf(amr_worker_scratch_pool(w), "[ODD]  %s", in->str);
    amr_string_singleton_t out = { buf };
    amr_worker_serialize(w, 0, outs[0], &out);
}

static bool process_odd_setup(amr_task_t *t) {
    amr_task_input_from_task_partition(t, "router", "path_odd.bin", 1.0);
    amr_task_input_expect_type(t, "StringSingleton");

    amr_task_output(t, "processed_odd.bin", 1.0);
    amr_task_output_type(t, "StringSingleton");

    amr_task_transform(t, "path_odd.bin", "processed_odd.bin", process_odd_runner);
    return true;
}

/* ================================================================
 * Stage 3: Convergence (The Union Edge)
 * ================================================================ */
static void converge_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    amr_string_singleton_t *in = amr_worker_deserialize(w, 0, r);
    aml_buffer_t *bh = amr_worker_buffer(w);

    aml_buffer_setf(bh, "Convergence Reached: %s", in->str);
    io_out_write_record(outs[0], aml_buffer_data(bh), aml_buffer_length(bh));
}

static bool converge_setup(amr_task_t *t) {
    // UNION EDGE: Funnels BOTH physical outputs into a single logical stream.
    amr_task_input_from_task_partition(t, "process_even|process_odd",
                                          "processed_even.bin|processed_odd.bin", 1.0);
    amr_task_input_expect_type(t, "StringSingleton");

    amr_task_output(t, "final_output.txt", 1.0);
    amr_task_output_format(t, io_delimiter('\n'));

    // Reference the truncated alias generated by the union
    amr_task_transform(t, "processed_even.bin", "final_output.txt", converge_runner);
    return true;
}

/* ================================================================
 * Main: Executing the DAG
 * ================================================================ */
int main(int argc, char **argv) {
    // Hardcode 4 partitions so we can test the 4 distinct routing states
    size_t parts = 4;
    int shift = 1;

    amr_t *sched = amr_init(argc - shift, argv + shift, parts, 4, 1024);
    amr_register_common_datatypes(sched);

    amr_task(sched, "router",       true, router_setup);
    amr_task(sched, "process_even", true, process_even_setup);
    amr_task(sched, "process_odd",  true, process_odd_setup);
    amr_task(sched, "converge",     true, converge_setup);

    bool success = amr_run(sched, amr_worker_complete);
    amr_destroy(sched);

    if (!success) {
        fprintf(stderr, "[ERROR] DAG Execution Failed!\n");
        return 1;
    }

    printf("\nDone! Try running:\n");
    printf("  cat tasks/converge_?/final_output.txt_?\n");
    printf("Notice that Partition 3 produced NO file because it starved and pruned itself!\n");

    return 0;
}
