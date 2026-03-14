// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef AMR_TASK_H
#define AMR_TASK_H

#include "amr_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Graph building and validation phases called by amr.c during setup */
void ensure_outputs_for_transforms(amr_t *sched);
void resolve_pipeline_ports(amr_t *sched);
void wire_graph(amr_t *sched);
void validate_partitions(amr_t *sched);
void resolve_transforms(amr_t *sched);

/* Internal link helpers */
void link_state(amr_t *h, amr_task_state_link_t *state, size_t partition);

#ifdef __cplusplus
}
#endif

#endif /* AMR_TASK_H */
