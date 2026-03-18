// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "a-map-reduce-library/amr.h"
#include "a-map-reduce-library/amr_common_datatypes.h"
#include "the-io-library/io.h"
#include "the-io-library/io_in.h"
#include "the-io-library/io_out.h"

#include <stdio.h>
#include <stdlib.h>

// Global pipeline pointer to share between main and setup callbacks
static amr_pipeline_t *branching_pipe = NULL;

/* ================================================================
 * THE PIPELINE: Encapsulating the diverged branches & union
 * ================================================================ */
static void process_even_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    amr_string_singleton_t *in = amr_worker_deserialize(w, 0, r);
    char *buf = aml_pool_strdupf(amr_worker_scratch_pool(w), "[PIPELINE: EVEN] %s", in->str);
    amr_string_singleton_t out = { buf };
    amr_worker_serialize(w, 0, outs[0], &out);
}

static bool process_even_setup(amr_task_t *t) {
    amr_task_input_from_pipeline_port_partition(t, "in_even", 1.0);
    amr_task_input_expect_type(t, "StringSingleton");
    amr_task_output(t, "pipe_even.bin", 1.0);
    amr_task_output_type(t, "StringSingleton");
    amr_task_transform(t, "in_even", "pipe_even.bin", process_even_runner);
    return true;
}

static void process_odd_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    amr_string_singleton_t *in = amr_worker_deserialize(w, 0, r);
    char *buf = aml_pool_strdupf(amr_worker_scratch_pool(w), "[PIPELINE: ODD]  %s", in->str);
    amr_string_singleton_t out = { buf };
    amr_worker_serialize(w, 0, outs[0], &out);
}

static bool process_odd_setup(amr_task_t *t) {
    amr_task_input_from_pipeline_port_partition(t, "in_odd", 1.0);
    amr_task_input_expect_type(t, "StringSingleton");
    amr_task_output(t, "pipe_odd.bin", 1.0);
    amr_task_output_type(t, "StringSingleton");
    amr_task_transform(t, "in_odd", "pipe_odd.bin", process_odd_runner);
    return true;
}

static void converge_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    amr_string_singleton_t *in = amr_worker_deserialize(w, 0, r);
    char *buf = aml_pool_strdupf(amr_worker_scratch_pool(w), "Pipeline Unified: %s", in->str);
    amr_string_singleton_t out = { buf };
    amr_worker_serialize(w, 0, outs[0], &out);
}

static bool converge_setup(amr_task_t *t) {
    // UNION EDGE: Internally merging the two diverged pipeline tasks
    // The patched sibling function will seamlessly handle the '|' character!
    amr_task_input_from_sibling_partition(t, "process_even|process_odd",
                                             "pipe_even.bin|pipe_odd.bin", 1.0);

    amr_task_input_expect_type(t, "StringSingleton");
    amr_task_output(t, "unified.bin", 1.0);
    amr_task_output_type(t, "StringSingleton");
    amr_task_transform(t, "pipe_even.bin", "unified.bin", converge_runner);
    return true;
}

// The Pipeline Registration
static bool branching_pipeline_setup(amr_pipeline_t *p) {
    amr_pipeline_task(p, "process_even", true, process_even_setup);
    amr_pipeline_task(p, "process_odd",  true, process_odd_setup);
    amr_pipeline_task(p, "converge",     true, converge_setup);

    // Expose the single, unified output port to the outside world
    amr_pipeline_bind_output(p, "out_unified", "converge", "unified.bin");
    return true;
}


/* ================================================================
 * THE APP LAYER: Orchestrating the data flow
 * ================================================================ */
static void router_runner(amr_worker_t *w, io_in_t **ins, size_t num_ins, io_out_t **outs, size_t num_outs) {
    (void)ins; (void)num_ins; (void)num_outs;
    size_t partition = amr_worker_partition(w);
    aml_pool_t *scratch = amr_worker_scratch_pool(w);

    if (partition == 0) {
        amr_worker_skip_output(w, 1);
        char *msg = aml_pool_strdupf(scratch, "P0 -> Routed to EVEN branch only");
        amr_string_singleton_t rec = { msg };
        amr_worker_serialize(w, 0, outs[0], &rec);
    } else if (partition == 1) {
        amr_worker_skip_output(w, 0);
        char *msg = aml_pool_strdupf(scratch, "P1 -> Routed to ODD branch only");
        amr_string_singleton_t rec = { msg };
        amr_worker_serialize(w, 1, outs[1], &rec);
    } else if (partition == 2) {
        char *msg_e = aml_pool_strdupf(scratch, "P2 -> Routed to BOTH (Even Payload)");
        amr_string_singleton_t rec_e = { msg_e };
        amr_worker_serialize(w, 0, outs[0], &rec_e);

        char *msg_o = aml_pool_strdupf(scratch, "P2 -> Routed to BOTH (Odd Payload)");
        amr_string_singleton_t rec_o = { msg_o };
        amr_worker_serialize(w, 1, outs[1], &rec_o);
    } else {
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

static void printer_runner(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
    amr_string_singleton_t *in = amr_worker_deserialize(w, 0, r);
    aml_buffer_t *bh = amr_worker_buffer(w);
    aml_buffer_setf(bh, "App Layer Received: %s", in->str);
    io_out_write_record(outs[0], aml_buffer_data(bh), aml_buffer_length(bh));
}

static bool printer_setup(amr_task_t *t) {
    // The App layer has no idea the data diverged. It just reads the pipeline port!
    amr_task_input_from_pipeline_partition(t, branching_pipe, "out_unified", 1.0);
    amr_task_input_expect_type(t, "StringSingleton");

    amr_task_output(t, "final_output.txt", 1.0);
    amr_task_output_format(t, io_delimiter('\n'));
    amr_task_transform(t, "out_unified", "final_output.txt", printer_runner);
    return true;
}

/* ================================================================
 * Main: Executing the DAG
 * ================================================================ */
int main(int argc, char **argv) {
    size_t parts = 4;
    int shift = 1;

    amr_t *sched = amr_init(argc - shift, argv + shift, parts, 4, 1024);
    amr_set_workspace_dir(sched, "pipeline_tasks");
    amr_register_common_datatypes(sched);

    // 1. App Layer: The Router
    amr_task(sched, "app_router", true, router_setup);

    // 2. The Branching Pipeline
    branching_pipe = amr_pipeline_create(sched, "branching_pipe", branching_pipeline_setup, NULL);
    amr_pipeline_bind_input(branching_pipe, "in_even", "app_router", "path_even.bin");
    amr_pipeline_bind_input(branching_pipe, "in_odd",  "app_router", "path_odd.bin");

    // 3. App Layer: The Printer
    amr_task(sched, "app_printer", true, printer_setup);

    bool success = amr_run(sched, amr_worker_complete);
    amr_destroy(sched);

    if (!success) {
        fprintf(stderr, "[ERROR] Pipeline DAG Execution Failed!\n");
        return 1;
    }

    printf("\nDone! Try running:\n");
    printf("  cat pipeline_tasks/app_printer_?/final_output.txt_?\n");
    return 0;
}
