// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "a-map-reduce-library/amr_core.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ========================================================================
 * PIPELINE / SUB-GRAPH API
 * ========================================================================
 * Pipelines allow you to encapsulate a sequence of tasks into a reusable,
 * modular sub-graph. Tasks inside a pipeline map to logical input and
 * output ports, hiding their internal implementations from downstream consumers.
 * ======================================================================== */

/* Context: [Setup | Main Thread]
 * Define the setup function signature for configuring a pipeline. */
typedef bool (*amr_pipeline_cb)(amr_pipeline_t *pipe);

/* Context: [Init | Main Thread]
 * Instantiates a top-level pipeline. */
amr_pipeline_t *amr_pipeline_create(amr_t *sched, const char *ns, amr_pipeline_cb setup, void *config);

/* Context: [Setup | Main Thread]
 * Instantiates a child pipeline INSIDE a parent pipeline. Its namespace will
 * automatically be prefixed by the parent's namespace to avoid collisions. */
amr_pipeline_t *amr_pipeline_create_nested(amr_pipeline_t *parent, const char *ns, amr_pipeline_cb setup, void *config);

/* Context: [Setup | Main Thread]
 * Maps an external task's physical output to a logical pipeline input port. */
void amr_pipeline_bind_input(amr_pipeline_t *pipe, const char *port_name, const char *ext_task, const char *ext_out);

/* Context: [Setup | Main Thread]
 * Exposes a child pipeline's logical input port to the outside world via the parent. */
void amr_pipeline_expose_input(amr_pipeline_t *parent, const char *parent_port,
                               amr_pipeline_t *child, const char *child_port);

/* Context: [Setup | Main Thread] (Called INSIDE pipeline setup)
 * Creates a task whose name is automatically prefixed by the pipeline namespace. */
amr_task_t *amr_pipeline_task(amr_pipeline_t *pipe, const char *task_base_name, bool partitioned, amr_task_cb setup);

/* Context: [Any Phase | Any Thread]
 * Retrieves the parent pipeline encapsulating this task. Returns NULL if un-pipelined. */
amr_pipeline_t *amr_task_pipeline(amr_task_t *task);

/* Context: [Setup | Main Thread] (Called INSIDE task setup)
 * Wires a task to consume from the pipeline's logical input port. */
void amr_task_input_from_pipeline_port_first(amr_task_t *task, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_port_partition(amr_task_t *task, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_port_shuffle(amr_task_t *task, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_port_all_to_all(amr_task_t *task, const char *port_name, double in_ram_pct);

/* Context: [Setup | Main Thread] (Called INSIDE task setup)
 * Wires a task to a sibling task within the same pipeline (auto-resolves namespace). */
void amr_task_input_from_sibling_first(amr_task_t *task, const char *sibling_base_name, const char *out_name, double in_ram_pct);
void amr_task_input_from_sibling_partition(amr_task_t *task, const char *sibling_base_name, const char *out_name, double in_ram_pct);
void amr_task_input_from_sibling_shuffle(amr_task_t *task, const char *sibling_base_name, const char *out_name, double in_ram_pct);
void amr_task_input_from_sibling_all_to_all(amr_task_t *task, const char *sibling_base_name, const char *out_name, double in_ram_pct);

/* Context: [Setup | Main Thread] (Called INSIDE pipeline setup)
 * Exposes an internal task's physical output as a logical port on the pipeline. */
void amr_pipeline_bind_output(amr_pipeline_t *pipe, const char *port_name,
                              const char *internal_task_base, const char *internal_out);

/* Context: [Setup | Main Thread]
 * Exposes a child pipeline's logical output port to the outside world via the parent. */
void amr_pipeline_expose_output(amr_pipeline_t *parent, const char *parent_port,
                                amr_pipeline_t *child, const char *child_port);

/* Context: [Setup | Main Thread] (Called INSIDE a downstream task's setup)
 * Wires a task to consume from a pipeline's exposed logical output port. */
void amr_task_input_from_pipeline_first(amr_task_t *task, amr_pipeline_t *pipe, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_partition(amr_task_t *task, amr_pipeline_t *pipe, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_shuffle(amr_task_t *task, amr_pipeline_t *pipe, const char *port_name, double in_ram_pct);
void amr_task_input_from_pipeline_all_to_all(amr_task_t *task, amr_pipeline_t *pipe, const char *port_name, double in_ram_pct);

/* Context: [Setup | Main Thread]
 * Wires two pipelines together, mapping an output port directly to an input port. */
void amr_pipeline_bind_link(amr_pipeline_t *dest_pipe, const char *dest_in_port,
                            amr_pipeline_t *src_pipe,  const char *src_out_port);

/* Retrieves the root scheduler attached to this pipeline. */
amr_t *amr_pipeline_scheduler(amr_pipeline_t *pipe);

/* Retrieves the configuration payload assigned during pipeline creation. */
void *amr_pipeline_config(amr_pipeline_t *pipe);

/* Context: [Runtime | Worker Thread]
 * Retrieves the pipeline context this worker is executing within. */
amr_pipeline_t *amr_worker_pipeline(amr_worker_t *w);

#ifdef __cplusplus
}
#endif
