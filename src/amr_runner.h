// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef AMR_RUNNER_H
#define AMR_RUNNER_H

#include "amr_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Worker execution lifecycle */
void setup_worker(amr_worker_t *w);
bool run_worker(amr_worker_t *w);
void destroy_worker(amr_worker_t *w);

/* * Exposed so the scheduler can check if a task is using the default runner
 * during `schedule_setup` and debugging list outputs.
 */
bool in_out_runner(amr_worker_t *w);

#ifdef __cplusplus
}
#endif

#endif /* AMR_RUNNER_H */
