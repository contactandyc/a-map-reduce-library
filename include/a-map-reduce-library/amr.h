// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef AMR_H
#define AMR_H

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * A-MAP-REDUCE LIBRARY (AMR)
 * ========================================================================
 * This is the umbrella header. The API is divided into domain-specific
 * headers to clearly separate setup logic from execution context.
 *
 * - amr_core.h: Scheduler lifecycle, CLI, and shared callback types.
 * - amr_types.h: Datatype registry and typed I/O.
 * - amr_task.h: DAG construction and task setup (Stateful Builder API).
 * - amr_worker.h: Worker runtime execution context.
 * - amr_pipeline.h: Reusable sub-graphs.
 * - amr_common_datatypes.h: Pre-registered convenience schemas.
 * ======================================================================== */

#include "a-map-reduce-library/amr_core.h"
#include "a-map-reduce-library/amr_types.h"
#include "a-map-reduce-library/amr_task.h"
#include "a-map-reduce-library/amr_worker.h"
#include "a-map-reduce-library/amr_pipeline.h"
#include "a-map-reduce-library/amr_common_datatypes.h"

/*
========================================================================
HELLO WORLD: IDENTITY COPY
========================================================================
A tiny example of a complete AMR application. It reads an input,
applies an identity transform (copies data), and writes to an output.

bool hello_setup(amr_task_t *task) {
    amr_task_input_files(task, "input.txt", 1.0, NULL);
    amr_task_output(task, "output.txt", 1.0);
    amr_task_default_runner(task);
    // Passing NULL for the runner uses the default identity copy
    amr_task_transform(task, "input.txt", "output.txt", NULL);
    return true;
}

int main(int argc, char **argv) {
    // 16 partitions, 4 CPUs, 1024 MB RAM
    amr_t *sched = amr_init(argc, argv, 16, 4, 1024);
    amr_task(sched, "hello_task", true, hello_setup);

    // amr_worker_complete provides built-in telemetry out of the box.
    // Check the return value to catch execution failures!
    if (!amr_run(sched, amr_worker_complete)) {
        fprintf(stderr, "DAG Execution Failed!\n");
        amr_destroy(sched);
        return 1;
    }

    amr_destroy(sched);
    return 0;
}
========================================================================
*/

#ifdef __cplusplus
}
#endif

#endif /* AMR_H */