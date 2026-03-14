// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef AMR_DEBUG_H
#define AMR_DEBUG_H

#include "amr_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Debug & Diagnostic Entry Points */
void amr_print(amr_t *h);
void debug_task(amr_thread_t *h);
void list_selected_tasks(void *arg);
void dump_selected_tasks(void *arg);

/* * Hooks into the core scheduler (amr.c).
 * These must have their `static` keywords removed in amr.c.
 */
extern amr_worker_t *create_worker(aml_pool_t *pool, amr_task_t *task, size_t partition);
extern void get_ack_time(amr_worker_t *w);
extern amr_worker_t *get_next_worker(amr_thread_t *t);
extern amr_worker_t *worker_complete(amr_worker_t *w, time_t when);
extern void mark_as_done(amr_t *scheduler);

#ifdef __cplusplus
}
#endif

#endif /* AMR_DEBUG_H */
