// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "a-map-reduce-library/amr.h"
#include "a-memory-library/aml_alloc.h"
#include "a-memory-library/aml_pool.h"
#include "the-macro-library/macro_map.h"
#include "amr_internal.h"
#include "amr_task.h"
#include "amr_worker.h"
#include "amr_datatypes.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* --- Internal Map Methods --- */
int compare_task_for_find(const char *key, const amr_task_t *node) {
  return strcmp(key, node->task_name);
}

int compare_task_for_insert(const amr_task_t *a, const amr_task_t *b) {
  return strcmp(a->task_name, b->task_name);
}

static macro_map_find_kv(_task_find, char, amr_task_t, compare_task_for_find);
static macro_map_insert(_task_insert, amr_task_t, compare_task_for_insert);

amr_task_t *amr_find_task(amr_t *h, const char *task_name) {
  return _task_find(h->task_root, task_name);
}

const char *amr_task_name(const amr_task_t *task) { return task->task_name; }

amr_t *amr_task_schedule(const amr_task_t *task) {
  return task->scheduler;
}

/* --- Task Creation & Core Properties --- */
amr_task_t *amr_task(amr_t *h, const char *task_name,
                     bool partitioned, amr_task_cb setup) {
  amr_task_t *node = _task_find(h->task_root, task_name);
  if (!node) {
    size_t num_partitions = 1;
    if (partitioned)
      num_partitions = h->num_partitions;

    node = (amr_task_t *)aml_pool_zalloc(
        h->pool, sizeof(amr_task_t) + strlen(task_name) + 1 +
                     (num_partitions * sizeof(amr_task_state_link_t)));
    node->scheduler = h;
    node->do_nothing = false;
    node->run_everytime = false;
    node->setup = setup;
    node->state_linkage = (amr_task_state_link_t *)(node + 1);
    node->num_partitions = num_partitions;
    node->task_name = (char *)(node->state_linkage + num_partitions);
    strcpy(node->task_name, task_name);
    _task_insert(&(h->task_root), node);
    for (size_t i = 0; i < num_partitions; i++) {
      node->state_linkage[i].task = node;
      node->state_linkage[i].waiting_on_others = false;
      node->state_linkage[i].completed = 0;
      node->state_linkage[i].ack_time = -1;
      link_state(h, node->state_linkage + i, i);
    }
    node->next = NULL;
    if (!h->head)
      h->head = h->tail = node;
    else {
      h->tail->next = node;
      h->tail = node;
    }
  }
  return node;
}

void amr_task_run_everytime(amr_task_t *task) { task->run_everytime = true; }
void amr_task_do_nothing(amr_task_t *task) { task->do_nothing = true; }

void amr_task_runner(amr_task_t *task, amr_worker_cb runner) {
  task->runner = runner;
}

/* --- Graph Linkage & Dependencies --- */
static void add_task_link(amr_task_t *task, amr_task_link_t **link, amr_task_t *to_add) {
  amr_task_link_t *n = *link;
  while (n) {
    if (n->task == to_add) return;
    n = n->next;
  }
  amr_t *scheduler = task->scheduler;
  n = (amr_task_link_t *)aml_pool_zalloc(scheduler->pool, sizeof(amr_task_link_t));
  n->task = to_add;
  n->next = *link;
  *link = n;
}

static bool _amr_task_dependency(amr_task_t *task, const char *dependency, bool partial) {
  amr_t *scheduler = task->scheduler;
  if (strchr(dependency, '|')) {
    aml_pool_clear(scheduler->tmp_pool);
    char **dep = aml_pool_split2(scheduler->tmp_pool, NULL, '|', dependency);
    for (size_t i = 0; dep[i] != NULL; i++) {
      if (!_amr_task_dependency(task, dep[i], partial)) return false;
    }
    return true;
  }
  amr_task_t *d = amr_find_task(scheduler, dependency);
  if (!d) {
    fprintf(stderr, "%s not found in scheduler\n", dependency);
    return false;
  }
  if (!task || !d) return false;

  if (partial) {
    add_task_link(task, &task->partial_dependencies, d);
    add_task_link(d, &d->reverse_partial_dependencies, task);
  } else {
    add_task_link(task, &task->dependencies, d);
    add_task_link(d, &d->reverse_dependencies, task);
  }
  return true;
}

bool amr_task_dependency(amr_task_t *task, const char *dependency) {
  return _amr_task_dependency(task, dependency, false);
}

bool amr_task_partial_dependency(amr_task_t *task, const char *dependency) {
  return _amr_task_dependency(task, dependency, true);
}

/* --- Inputs and Outputs Configuration --- */
amr_worker_input_t *amr_task_find_input(amr_task_t *task, const char *name) {
  amr_worker_input_t *n = task->inputs;
  while (n) {
    if (!strcmp(n->name, name)) return n;
    n = n->next;
  }
  return NULL;
}

amr_worker_output_t *amr_task_find_output(amr_task_t *task, const char *name) {
  amr_worker_output_t *n = task->outputs;
  while (n) {
    if (!strcmp(n->name, name)) return n;
    n = n->next;
  }
  return NULL;
}

static amr_worker_input_t *_amr_task_input(amr_task_t *task, const char *name,
                                           amr_worker_output_t *src, double pct,
                                           amr_worker_file_info_cb file_info) {
  amr_t *scheduler = task->scheduler;
  aml_pool_t *pool = scheduler->pool;

  amr_worker_input_t *ti = (amr_worker_input_t *)aml_pool_zalloc(pool, sizeof(*ti));
  io_in_options_init(&(ti->options));
  ti->name = aml_pool_strdup(pool, name);
  ti->ram_pct = pct;
  ti->task = task;
  ti->src = src;
  ti->file_info = file_info;
  ti->edge_flags = AMR_EDGE_ALL_TO_ALL; /* Default to squaring the process */

  if (src) {
    ti->options.format = src->options.format;
  }
  if (task->inputs) {
    amr_worker_input_t *n = task->inputs;
    while (n->next) n = n->next;
    ti->id = n->id + 1;
    n->next = ti;
  } else {
    ti->id = 0;
    task->inputs = ti;
  }
  return ti;
}

/* Isolate the edge topology bits (Bits 0, 1, 2, 3) */
#define AMR_EDGE_MASK (AMR_EDGE_ALL_TO_ALL | AMR_EDGE_FIRST | AMR_EDGE_SHUFFLE | AMR_EDGE_PARTITION)

void amr_task_input_files(amr_task_t *task, const char *name, double pct,
                          amr_worker_file_info_cb file_info) {
  amr_t *scheduler = task->scheduler;
  aml_pool_t *pool = scheduler->pool;
  if (!file_info) file_info = file_info_name;

  task->current_input = NULL;
  amr_worker_input_t *ti = _amr_task_input(task, name, NULL, pct, file_info);
  if (ti) {
    amr_task_input_link_t *inp_link =
        (amr_task_input_link_t *)aml_pool_alloc(pool, sizeof(*inp_link));
    inp_link->input = ti;
    inp_link->next = task->current_input;
    task->current_input = inp_link;
  }
}

void amr_task_output(amr_task_t *task, const char *name, double out_ram_pct) {
  amr_t *scheduler = task->scheduler;
  aml_pool_t *pool = scheduler->pool;

  amr_worker_output_t *to = aml_pool_zalloc(pool, sizeof(*to));
  io_out_options_init(&(to->options));
  io_out_ext_options_init(&(to->ext_options));

  to->name           = aml_pool_strdup(pool, name);
  to->task           = task;
  to->ram_pct        = out_ram_pct;
  to->flags          = 0; /* Default to standard non-shuffled write */
  to->num_partitions = task->num_partitions;

  to->cleaned_up_parts = aml_pool_zalloc(pool, sizeof(bool)   * to->num_partitions);
  to->refcount_parts   = aml_pool_zalloc(pool, sizeof(size_t) * to->num_partitions);

  if (task->outputs) {
    amr_worker_output_t *n = task->outputs;
    while (n->next) n = n->next;
    to->id = n->id + 1;
    n->next = to;
  } else {
    to->id = 0;
    task->outputs = to;
  }

  task->current_output = to;
  task->current_input = NULL;
}

/* ========================================================================
 * STATEFUL BUILDER API (OPAQUE)
 * ======================================================================== */
void amr_task_output_opaque(amr_task_t *task) {
    if (task->current_output) task->current_output->flags |= AMR_OUTPUT_OPAQUE;
}

void amr_task_input_opaque(amr_task_t *task) {
    amr_task_input_link_t *inp = task->current_input;
    while (inp) {
        inp->input->edge_flags |= AMR_INPUT_OPAQUE;
        inp = inp->next;
    }
}

/* ========================================================================
 * STATEFUL BUILDER API (SHUFFLING)
 * ======================================================================== */
static size_t round_robin_part(const io_record_t *r, size_t np, void *arg) {
    static __thread size_t counter = 0;
    return counter++ % np;
}

void amr_task_output_shuffle(amr_task_t *task) {
    if (!task || !task->current_output) return;
    task->current_output->flags |= AMR_WRITE_SHUFFLE;
    io_out_ext_options_partition(&(task->current_output->ext_options), round_robin_part, NULL);
}

void amr_task_output_shuffle_by(amr_task_t *t, const char *partitioner_name, void *arg) {
    if (!t || !t->current_output) return;
    t->current_output->flags |= AMR_WRITE_SHUFFLE;

    amr_datatype_t *dt = t->current_output->datatype;
    if (!dt) {
        fprintf(stderr, "[ERROR] Task '%s' output '%s' must call amr_task_output_type() before amr_task_output_shuffle_by()!\n", t->task_name, t->current_output->name);
        abort();
    }
    io_partition_cb part_fn = amr_datatype_get_partition(dt, partitioner_name);
    if (!part_fn) {
        fprintf(stderr, "[ERROR] Task '%s' requested partitioner '%s' for type '%s', but it is not registered!\n", t->task_name, partitioner_name, dt->name);
        abort();
    }

    io_out_ext_options_partition(&(t->current_output->ext_options), part_fn, arg);
}

void amr_task_output_shuffle_with(amr_task_t *task, io_partition_cb part, void *arg) {
    if (!task || !task->current_output) return;
    task->current_output->flags |= AMR_WRITE_SHUFFLE;
    io_out_ext_options_partition(&(task->current_output->ext_options), part, arg);
}

/* ========================================================================
 * STATEFUL BUILDER API (OUTPUT SORTING & REDUCING)
 * ======================================================================== */

void amr_task_output_sort_by(amr_task_t *t, const char *comparator_name, void *arg) {
    if (!t || !t->current_output) return;
    amr_datatype_t *dt = t->current_output->datatype;
    if (!dt) {
        fprintf(stderr, "[ERROR] Task '%s' output '%s' must call amr_task_output_type() before amr_task_output_sort_by()!\n", t->task_name, t->current_output->name);
        abort();
    }

    io_compare_cb cmp_fn = amr_datatype_get_compare(dt, comparator_name);
    if (!cmp_fn) {
        fprintf(stderr, "[ERROR] Task '%s' requested comparator '%s' for type '%s', but it is not registered!\n", t->task_name, comparator_name, dt->name);
        abort();
    }

    io_out_ext_options_compare(&(t->current_output->ext_options), cmp_fn, arg);
}

void amr_task_output_sort_with(amr_task_t *task, io_compare_cb compare, void *arg) {
    if (task->current_output)
        io_out_ext_options_compare(&(task->current_output->ext_options), compare, arg);
}

void amr_task_output_reduce_by(amr_task_t *t, const char *reducer_name, void *arg) {
    if (!t || !t->current_output) return;
    amr_datatype_t *dt = t->current_output->datatype;
    if (!dt) {
        fprintf(stderr, "[ERROR] Task '%s' output '%s' must call amr_task_output_type() before amr_task_output_reduce_by()!\n", t->task_name, t->current_output->name);
        abort();
    }

    io_reducer_cb red_fn = amr_datatype_get_reducer(dt, reducer_name);
    if (!red_fn) {
        fprintf(stderr, "[ERROR] Task '%s' requested reducer '%s' for type '%s', but it is not registered!\n", t->task_name, reducer_name, dt->name);
        abort();
    }

    io_out_ext_options_reducer(&(t->current_output->ext_options), red_fn, arg);
}

void amr_task_output_reduce_with(amr_task_t *task, io_reducer_cb reducer, void *arg) {
    if (task->current_output)
        io_out_ext_options_reducer(&(task->current_output->ext_options), reducer, arg);
}

void amr_task_output_reduce_by_keeping_first(amr_task_t *task) {
    amr_task_output_reduce_with(task, io_keep_first, NULL);
}

void amr_task_output_intermediate_sort_by(amr_task_t *t, const char *comparator_name, void *arg) {
    if (!t || !t->current_output) return;
    amr_datatype_t *dt = t->current_output->datatype;
    if (!dt) { abort(); }

    io_compare_cb cmp_fn = amr_datatype_get_compare(dt, comparator_name);
    if (!cmp_fn) {
        fprintf(stderr, "[ERROR] Task '%s' requested intermediate comparator '%s' for type '%s', but it is not registered!\n", t->task_name, comparator_name, dt->name);
        abort();
    }

    io_out_ext_options_intermediate_compare(&(t->current_output->ext_options), cmp_fn, arg);
}

void amr_task_output_intermediate_sort_with(amr_task_t *task, io_compare_cb compare, void *arg) {
    if (task->current_output)
        io_out_ext_options_intermediate_compare(&(task->current_output->ext_options), compare, arg);
}

void amr_task_output_intermediate_reduce_by(amr_task_t *t, const char *reducer_name, void *arg) {
    if (!t || !t->current_output) return;
    amr_datatype_t *dt = t->current_output->datatype;
    if (!dt) { abort(); }

    io_reducer_cb red_fn = amr_datatype_get_reducer(dt, reducer_name);
    if (!red_fn) {
        fprintf(stderr, "[ERROR] Task '%s' requested intermediate reducer '%s' for type '%s', but it is not registered!\n", t->task_name, reducer_name, dt->name);
        abort();
    }

    io_out_ext_options_intermediate_reducer(&(t->current_output->ext_options), red_fn, arg);
}

void amr_task_output_intermediate_reduce_with(amr_task_t *task, io_reducer_cb reducer, void *arg) {
    if (task->current_output)
        io_out_ext_options_intermediate_reducer(&(task->current_output->ext_options), reducer, arg);
}

/* ========================================================================
 * STATEFUL BUILDER API (INPUT SORTING & REDUCING)
 * ======================================================================== */

void amr_task_input_sort_by(amr_task_t *t, const char *comparator_name, void *arg) {
    if (!t || !t->current_input) return;

    amr_task_input_link_t *inp = t->current_input;
    while (inp) {
        amr_datatype_t *dt = inp->input->datatype;
        if (!dt) {
            fprintf(stderr, "[ERROR] Task '%s' must amr_task_input_expect_type() before amr_task_input_sort_by()!\n", t->task_name);
            abort();
        }

        io_compare_cb cmp_fn = amr_datatype_get_compare(dt, comparator_name);
        if (!cmp_fn) {
            fprintf(stderr, "[ERROR] Task '%s' input requested comparator '%s' for type '%s', but it is not registered!\n", t->task_name, comparator_name, dt->name);
            abort();
        }

        inp->input->compare = cmp_fn;
        inp->input->compare_arg = arg;
        inp = inp->next;
    }
}

void amr_task_input_sort_with(amr_task_t *task, io_compare_cb compare, void *arg) {
    amr_task_input_link_t *inp = task->current_input;
    while (inp) {
        inp->input->compare = compare;
        inp->input->compare_arg = arg;
        inp = inp->next;
    }
}

void amr_task_input_reduce_by(amr_task_t *t, const char *reducer_name, void *arg) {
    if (!t || !t->current_input) return;

    amr_task_input_link_t *inp = t->current_input;
    while (inp) {
        amr_datatype_t *dt = inp->input->datatype;
        if (!dt) {
            fprintf(stderr, "[ERROR] Task '%s' must amr_task_input_expect_type() before amr_task_input_reduce_by()!\n", t->task_name);
            abort();
        }

        io_reducer_cb red_fn = amr_datatype_get_reducer(dt, reducer_name);
        if (!red_fn) {
            fprintf(stderr, "[ERROR] Task '%s' input requested reducer '%s' for type '%s', but it is not registered!\n", t->task_name, reducer_name, dt->name);
            abort();
        }

        inp->input->reducer = red_fn;
        inp->input->reducer_arg = arg;
        inp = inp->next;
    }
}

void amr_task_input_reduce_with(amr_task_t *task, io_reducer_cb reducer, void *arg) {
    amr_task_input_link_t *inp = task->current_input;
    while (inp) {
        inp->input->reducer = reducer;
        inp->input->reducer_arg = arg;
        inp = inp->next;
    }
}

void amr_task_input_reduce_by_keeping_first(amr_task_t *task) {
    amr_task_input_reduce_with(task, io_keep_first, NULL);
}

/* --- Transforms Configuration --- */
static amr_transform_t *_amr_task_transform(amr_task_t *task, const char *inp, const char *outp) {
  aml_pool_t *pool = task->scheduler->pool;
  amr_transform_t *t = (amr_transform_t *)aml_pool_zalloc(pool, sizeof(*t));

  size_t num_inputs = 0;
  char **inputs = aml_pool_split2(pool, &num_inputs, '|', inp);
  t->num_inputs = num_inputs;
  if (num_inputs) {
    t->input_names = aml_pool_strdupan(pool, inputs, num_inputs);
    t->inputs = (amr_worker_input_t **)
        aml_pool_zalloc(pool, sizeof(amr_worker_input_t *) * num_inputs);
  }

  size_t num_outputs = 0;
  char **outputs = aml_pool_split2(pool, &num_outputs, '|', outp);
  t->num_outputs = num_outputs;
  if (num_outputs) {
    t->output_names = aml_pool_strdupan(pool, outputs, num_outputs);
    t->outputs = (amr_worker_output_t **)
        aml_pool_zalloc(pool, sizeof(amr_worker_output_t *) * num_outputs);
  }

  if (!task->transforms) task->transforms = t;
  else {
    amr_transform_t *n = task->transforms;
    while (n->next) n = n->next;
    n->next = t;
  }
  return t;
}

void amr_task_transform(amr_task_t *task, const char *inp, const char *outp, amr_runner_cb runner) {
  amr_transform_t *t = _amr_task_transform(task, inp, outp);
  t->runner = runner;
}

void amr_task_io_transform(amr_task_t *task, const char *inp, const char *outp, amr_io_runner_cb runner) {
  amr_transform_t *t = _amr_task_transform(task, inp, outp);
  t->io_runner = runner;
}

void amr_task_group_transform(amr_task_t *task, const char *inp, const char *outp,
                              amr_group_runner_cb runner, io_compare_cb compare) {
  amr_transform_t *t = _amr_task_transform(task, inp, outp);
  t->group_runner = runner;
  t->group_compare = compare;
}

void amr_task_transform_data(amr_task_t *task, amr_worker_data_cb create,
                             amr_destroy_worker_data_cb destroy) {
  if (!task->transforms)
    return;

  /* Apply to the most recently added transform */
  amr_transform_t *t = task->transforms;
  while (t->next)
    t = t->next;

  t->create_data = create;
  t->destroy_data = destroy;
}

void amr_task_group_compare_arg(amr_task_t *task, amr_worker_data_cb create,
                                amr_destroy_worker_data_cb destroy) {
  if (!task->transforms)
    return;

  /* Apply to the most recently added transform */
  amr_transform_t *t = task->transforms;
  while (t->next)
    t = t->next;

  t->create_group_compare_arg = create;
  t->destroy_group_compare_arg = destroy;
}

/* --- Graph Wiring and Validation (Called by Scheduler) --- */
static inline int _amr_count_edge_bits(size_t flags) {
  int n = 0;
  if (flags & AMR_EDGE_FIRST)     n++;
  if (flags & AMR_EDGE_PARTITION) n++;
  if (flags & AMR_EDGE_SHUFFLE)   n++;
  if (flags & AMR_EDGE_ALL_TO_ALL)  n++;
  return n;
}

void amr_task_input_from_task_mode(amr_task_t *consumer, const char *producer_task_name,
                                   const char *producer_output, const char *local_alias,
                                   double in_ram_pct, size_t edge_flags) {
  amr_t *sched = consumer->scheduler;

  /* If NO topology bits are passed, inject the default ALL_TO_ALL bit */
  if ((edge_flags & AMR_EDGE_MASK) == 0) {
    edge_flags |= AMR_EDGE_ALL_TO_ALL;
  }

  if (_amr_count_edge_bits(edge_flags) > 1) {
    fprintf(stderr, "[ERROR] Conflicting input modes for %s <- %s::%s\n",
            consumer->task_name, producer_task_name, producer_output);
    abort();
  }

  consumer->current_input = NULL;

  // THE FIX: Register the input locally using the ALIAS, not the producer's physical name!
  const char *name_to_use = local_alias ? local_alias : producer_output;
  amr_worker_input_t *ti = _amr_task_input(consumer, name_to_use, NULL, in_ram_pct, file_info_name);
  ti->edge_flags = edge_flags;

  amr_task_input_link_t *link = (amr_task_input_link_t*)aml_pool_alloc(sched->pool, sizeof(*link));
  link->input = ti; link->next = consumer->current_input;
  consumer->current_input = link;

  amr_pending_edge_t *e = aml_pool_zalloc(sched->pool, sizeof(*e));
  e->producer_task_name = aml_pool_strdup(sched->pool, producer_task_name);

  // THE FIX: Tell the DAG resolver to find the exact physical output on the producer!
  e->output_name        = aml_pool_strdup(sched->pool, producer_output);
  e->consumer           = consumer;
  e->input_stub         = ti;
  e->in_ram_pct         = in_ram_pct;
  e->next               = sched->pending_edges;
  sched->pending_edges  = e;
}

void amr_task_input_from_task_first(amr_task_t *c, const char *prod, const char *out, double pct) {
  amr_task_input_from_task_mode(c, prod, out, out, pct, AMR_EDGE_FIRST);
}

void amr_task_input_from_task_partition(amr_task_t *c, const char *prod, const char *out, double pct) {
  amr_task_input_from_task_mode(c, prod, out, out, pct, AMR_EDGE_PARTITION);
}

void amr_task_input_from_task_shuffle(amr_task_t *c, const char *prod, const char *out, double pct) {
  amr_task_input_from_task_mode(c, prod, out, out, pct, AMR_EDGE_SHUFFLE);
}

void amr_task_input_from_task_all_to_all(amr_task_t *c, const char *prod, const char *out, double pct) {
  amr_task_input_from_task_mode(c, prod, out, out, pct, AMR_EDGE_ALL_TO_ALL);
}

static amr_worker_output_t *find_or_create_output(amr_task_t *producer, const char *out_name) {
  amr_worker_output_t *out = amr_task_find_output(producer, out_name);
  if (!out) {
    amr_task_output(producer, out_name, 1.0);
    out = amr_task_find_output(producer, out_name);
  }
  return out;
}

void ensure_outputs_for_transforms(amr_t *sched) {
  for (amr_task_t *t = sched->head; t; t = t->next) {
    for (amr_transform_t *tr = t->transforms; tr; tr = tr->next) {
      for (size_t i = 0; i < tr->num_outputs; i++) {
        const char *name = tr->output_names[i];
        if (name && !amr_task_find_output(t, name)) {
          amr_task_output(t, name, 1.0);
        }
      }
    }
  }
}

static void attach_input_to_output(amr_t *sched, amr_worker_output_t *out,
                                   amr_worker_input_t  *ti, double in_ram_pct,
                                   amr_task_t *consumer) {
  size_t mode = ti->edge_flags;

  if (mode & AMR_EDGE_SHUFFLE) {
    out->flags |= AMR_WRITE_SHUFFLE;
    if (out->ext_options.num_partitions == 0)
      io_out_ext_options_num_partitions(&(out->ext_options), consumer->num_partitions);
  }

  ti->ram_pct        = (in_ram_pct > 0.0) ? in_ram_pct : 1.0;
  ti->options.format = out->options.format;
  ti->compare        = out->ext_options.compare;
  ti->compare_arg    = out->ext_options.compare_arg;
  ti->reducer        = out->ext_options.reducer;
  ti->reducer_arg    = out->ext_options.reducer_arg;
  ti->src            = out;

  /* Inherit the Type and Dumper from the Producer! */
  if (out->datatype && !ti->datatype) {
      ti->expected_type = out->type_name;
      ti->datatype = out->datatype;

      // Inherit the dump behavior if the consumer didn't override it
      if (!ti->dump && out->dump) {
          ti->dump = out->dump;
          ti->dump_arg = out->dump_arg;
      }
  }

  if      (mode & AMR_EDGE_SHUFFLE)     ti->file_info = file_info_shuffle;
  else if (mode & AMR_EDGE_FIRST)       ti->file_info = file_info_first;
  else if (mode & AMR_EDGE_PARTITION)   ti->file_info = file_info_partition;
  else if (mode & AMR_EDGE_ALL_TO_ALL)  ti->file_info = file_info_all_to_all;
  else abort(); /* Defensive guard */

  if (mode & AMR_EDGE_PARTITION)
    amr_task_partial_dependency(consumer, out->task->task_name);
  else
    amr_task_dependency(consumer, out->task->task_name);

  /* Set up the deletion reference counts based on the topology */
  if (mode & AMR_EDGE_FIRST) {
    out->refcount_parts[0] += consumer->num_partitions;
  } else if (mode & AMR_EDGE_PARTITION) {
    for (size_t i = 0; i < out->num_partitions; i++)
      out->refcount_parts[i] += 1;
  } else if (mode & AMR_EDGE_SHUFFLE) {
    size_t N = out->ext_options.num_partitions;
    if (!out->refcount_buckets) {
      out->refcount_buckets = aml_pool_zalloc(sched->pool, sizeof(size_t) * N);
    }
    for (size_t i = 0; i < N; i++) {
      out->refcount_buckets[i] += 1;
    }
  } else if (mode & AMR_EDGE_ALL_TO_ALL) {
    for (size_t i = 0; i < out->num_partitions; i++)
      out->refcount_parts[i] += consumer->num_partitions;
  } else {
      abort(); /* Defensive guard */
  }
}

void wire_graph(amr_t *sched) {
  for (amr_pending_edge_t *e = sched->pending_edges; e; e = e->next) {
    e->producer = _task_find(sched->task_root, e->producer_task_name);
    if (!e->producer) {
      fprintf(stderr, "[ERROR] Unknown producer task '%s' (required by %s)\n",
              e->producer_task_name, e->consumer->task_name);
      abort();
    }
    e->output = find_or_create_output(e->producer, e->output_name);
    attach_input_to_output(sched, e->output, e->input_stub, e->in_ram_pct, e->consumer);
  }
}

void validate_partitions(amr_t *sched) {
  for (amr_pending_edge_t *e = sched->pending_edges; e; e = e->next) {
    amr_worker_output_t *out = e->output;
    amr_worker_input_t  *ti  = e->input_stub;
    size_t mode = ti->edge_flags;

    /* ==================================================================
     * NEW STRICT TOPOLOGY GUARDRAILS
     * ================================================================== */
    bool producer_shuffled = (out->flags & AMR_WRITE_SHUFFLE) != 0;
    bool consumer_shuffled = (mode & AMR_EDGE_SHUFFLE) != 0;

    if (producer_shuffled && !consumer_shuffled) {
        fprintf(stderr, "\n[FATAL TOPOLOGY ERROR] Dropped Buckets Detected!\n"
                        "Producer Task : '%s'\n"
                        "Output Artifact: '%s'\n"
                        "Consumer Task : '%s'\n\n"
                        "The producer SHUFFLED this data into network buckets, but the consumer "
                        "is trying to read it using a non-shuffle edge (like _partition). "
                        "This will silently drop data! Use amr_task_input_from_*_shuffle().\n\n",
                out->task->task_name, out->name, e->consumer->task_name);
        abort();
    }

    if (!producer_shuffled && consumer_shuffled) {
        fprintf(stderr, "\n[FATAL TOPOLOGY ERROR] Missing Network Buckets!\n"
                        "Producer Task : '%s'\n"
                        "Output Artifact: '%s'\n"
                        "Consumer Task : '%s'\n\n"
                        "The consumer expects this input to be SHUFFLED, but the producer "
                        "wrote it as a direct un-shuffled partition. "
                        "Use amr_task_output_shuffle_by() on the producer, or change "
                        "the consumer edge to _partition.\n\n",
                out->task->task_name, out->name, e->consumer->task_name);
        abort();
    }
    /* ================================================================== */

    if (ti->expected_type) {
        if (!out->type_name) {
             fprintf(stderr, "[ERROR] Type Mismatch on edge %s -> %s: Consumer expects '%s', but Producer didn't declare a type.\n",
                     out->task->task_name, e->consumer->task_name, ti->expected_type);
             abort();
        }
        if (strcmp(ti->expected_type, out->type_name) != 0) {
             fprintf(stderr, "[ERROR] Type Mismatch on edge %s -> %s: Producer outputs '%s', but Consumer expects '%s'.\n",
                     out->task->task_name, e->consumer->task_name, out->type_name, ti->expected_type);
             abort();
        }
    }
    if (mode & AMR_EDGE_PARTITION) {
      if (out->task->num_partitions != e->consumer->num_partitions) {
        fprintf(stderr, "[ERROR] PARTITION edge mismatch: %s(%zu) -> %s(%zu)\n",
                out->task->task_name, out->task->num_partitions,
                e->consumer->task_name, e->consumer->num_partitions);
        abort();
      }
    }

    if (mode & AMR_EDGE_SHUFFLE) {
      if (!out->ext_options.partition) {
        fprintf(stderr, "[ERROR] SHUFFLE edge requires a partitioner: %s::%s -> %s\n",
                out->task->task_name, out->name, e->consumer->task_name);
        abort();
      }
      size_t np = out->ext_options.num_partitions;
      if (np == 0) {
        io_out_ext_options_num_partitions(&(out->ext_options), e->consumer->num_partitions);
      } else if (np != e->consumer->num_partitions) {
        fprintf(stderr, "[ERROR] SHUFFLE mismatch for %s::%s (have %zu, need %zu)\n",
                out->task->task_name, out->name, np, e->consumer->num_partitions);
        abort();
      }
    }
  }
}

void resolve_transforms(amr_t *sched) {
  for (amr_task_t *t = sched->head; t; t = t->next) {
    for (amr_transform_t *tr = t->transforms; tr; tr = tr->next) {
      for (size_t i = 0; i < tr->num_inputs; i++) {
        if (!tr->inputs[i] && tr->input_names[i]) {
          tr->inputs[i] = amr_task_find_input(t, tr->input_names[i]);
          if (!tr->inputs[i]) {
            fprintf(stderr, "Unresolved input '%s' for task %s\n", tr->input_names[i], t->task_name);
            abort();
          }
        }
      }
      for (size_t i = 0; i < tr->num_outputs; i++) {
        if (!tr->outputs[i] && tr->output_names[i]) {
          for (amr_worker_output_t *out = t->outputs; out; out = out->next) {
            if (!strcmp(out->name, tr->output_names[i])) {
              tr->outputs[i] = out;
              break;
            }
          }
          if (!tr->outputs[i]) {
            fprintf(stderr, "Unresolved output '%s' for task %s\n", tr->output_names[i], t->task_name);
            abort();
          }
        }
      }
    }
  }
}

void amr_task_output_keep(amr_task_t *task) {
  if (!task->current_output)
    return;
  task->current_output->keep_output = true;
}

void amr_task_dump_text(amr_worker_t *w, io_record_t *r, aml_buffer_t *bh,
                        void *arg) {
  aml_buffer_appends(bh, r->record);
}

void amr_task_output_dump(amr_task_t *task, amr_task_dump_cb dump, void *arg) {
  if (!task->current_output)
    return;

  task->current_output->dump = dump;
  task->current_output->dump_arg = arg;
}

void amr_task_output_group_size(amr_task_t *task, size_t num_per_group,
                               size_t start) {
  if (!task->current_output)
    return;

  io_out_ext_options_intermediate_group_size(
      &(task->current_output->ext_options), num_per_group);
}

void amr_task_output_use_extra_thread(amr_task_t *task) {
  if (!task->current_output)
    return;

  io_out_ext_options_use_extra_thread(&(task->current_output->ext_options));
}

void amr_task_output_dont_compress_tmp(amr_task_t *task) {
  if (!task->current_output)
    return;

  io_out_ext_options_dont_compress_tmp(&(task->current_output->ext_options));
}

void amr_task_output_sort_before_partitioning(amr_task_t *task) {
  if (!task->current_output)
    return;

  io_out_ext_options_sort_before_partitioning(
      &(task->current_output->ext_options));
}

void amr_task_output_sort_while_partitioning(amr_task_t *task) {
  if (!task->current_output)
    return;

  io_out_ext_options_sort_while_partitioning(
      &(task->current_output->ext_options));
}

void amr_task_output_sort_after_partitioning(amr_task_t *task) {
  if (task->current_output) {
      task->current_output->ext_options.sort_before_partitioning = false;
  }
}

void amr_task_output_num_sort_threads(amr_task_t *task, size_t num_sort_threads) {
  if (!task->current_output)
    return;

  io_out_ext_options_num_sort_threads(&(task->current_output->ext_options),
                                      num_sort_threads);
}

void amr_task_output_format(amr_task_t *task, io_format_t format) {
  if (!task->current_output)
    return;

  io_out_options_format(&(task->current_output->options), format);
}

void amr_task_output_safe_mode(amr_task_t *task) {
  if (!task->current_output)
    return;

  io_out_options_safe_mode(&(task->current_output->options));
}

void amr_task_output_write_ack_file(amr_task_t *task) {
  if (!task->current_output)
    return;

  io_out_options_write_ack_file(&(task->current_output->options));
}

void amr_task_output_gz(amr_task_t *task, int level) {
  if (!task->current_output)
    return;

  io_out_options_gz(&(task->current_output->options), level);
}

void amr_task_output_lz4(amr_task_t *task, int level, lz4_block_size_t size,
                        bool block_checksum, bool content_checksum) {
  if (!task->current_output)
    return;

  io_out_options_lz4(&(task->current_output->options), level, size,
                     block_checksum, content_checksum);
}

/* The amr_task_input... option methods apply to the most recently declared
   inputs (either amr_task_input_files(...) or amr_task_input_from_task(...)). */
void amr_task_input_format(amr_task_t *task, io_format_t format) {
  amr_task_input_link_t *inp = task->current_input;
  while (inp) {
    io_in_options_format(&(inp->input->options), format);
    inp = inp->next;
  }
}

void amr_task_input_dump(amr_task_t *task, amr_task_dump_cb dump, void *arg) {
  amr_task_input_link_t *inp = task->current_input;
  while (inp) {
    inp->input->dump = dump;
    inp->input->dump_arg = arg;
    inp = inp->next;
  }
}

void amr_task_input_compressed_buffer_size(amr_task_t *task, size_t buffer_size) {
  amr_task_input_link_t *inp = task->current_input;
  while (inp) {
    io_in_options_compressed_buffer_size(&(inp->input->options), buffer_size);
    inp = inp->next;
  }
}

void amr_task_input_limit(amr_task_t *task, size_t limit) {
  amr_task_input_link_t *inp = task->current_input;
  while (inp) {
    inp->input->limit = limit;
    inp = inp->next;
  }
}

void amr_task_input_load_into_memory(amr_task_t *task) {
  amr_task_input_link_t *inp = task->current_input;
  while (inp) {
    inp->input->load_into_memory = true;
    inp = inp->next;
  }
}

void amr_task_input_from_previous_run(amr_task_t *consumer,
                                      const char *prev_task_name,
                                      const char *output_name,
                                      double in_ram_pct,
                                      size_t edge_flags) {
  amr_t *sched = consumer->scheduler;

  /* If NO topology bits are passed, inject the default ALL_TO_ALL bit */
  if ((edge_flags & AMR_EDGE_MASK) == 0) {
    edge_flags |= AMR_EDGE_ALL_TO_ALL;
  }

  if (_amr_count_edge_bits(edge_flags) > 1) {
    fprintf(stderr, "[ERROR] Conflicting input modes for %s <- PREV_RUN::%s::%s\n",
            consumer->task_name, prev_task_name, output_name);
    abort();
  }

  consumer->current_input = NULL;

  /* Select the correct custom callback based on the edge type */
  amr_worker_file_info_cb cb = NULL;
  if (edge_flags & AMR_EDGE_FIRST) cb = file_info_prev_run_first;
  else if (edge_flags & AMR_EDGE_PARTITION) cb = file_info_prev_run_partition;
  else if (edge_flags & AMR_EDGE_SHUFFLE) cb = file_info_prev_run_shuffle;
  else if (edge_flags & AMR_EDGE_ALL_TO_ALL) cb = file_info_prev_run_all_to_all;
  else abort(); /* Defensive guard */

  amr_worker_input_t *ti = _amr_task_input(consumer, output_name, NULL, in_ram_pct, cb);
  ti->edge_flags = edge_flags;
  ti->is_previous_run = true;
  ti->prev_task_name = aml_pool_strdup(sched->pool, prev_task_name);

  /* Hardwire the run target logic */
  if (sched->parsed_args.run_number > 0) {
    ti->prev_run = sched->parsed_args.run_number - 1;
  } else {
    ti->prev_run = 0;
  }

  amr_task_input_link_t *link = (amr_task_input_link_t*)aml_pool_alloc(sched->pool, sizeof(*link));
  link->input = ti;
  link->next = consumer->current_input;
  consumer->current_input = link;
}

/* ========================================================================
 * Type Registry Integration
 * ======================================================================== */

/* Universal bridge that connects the CLI --dump flag to your datatype's to_string */
static void amr_typed_dump_cb(amr_worker_t *w, io_record_t *r, aml_buffer_t *bh, void *arg) {
    amr_datatype_t *dt = (amr_datatype_t *)arg;

    /* Fallback if incomplete type implementation */
    if (!dt || !dt->deserialize || !dt->to_string) {
        aml_buffer_appends(bh, (char*)r->record); // Assume it's text
        return;
    }

    // 1. Hydrate the struct using the worker's fast scratch pool
    void *obj = dt->deserialize(w->pool, r->record, r->length);

    if (obj) {
        // 2. Ask the struct to print itself into the framework's output buffer!
        dt->to_string(obj, bh);
    } else {
        aml_buffer_appends(bh, "[DESERIALIZATION FAILED]");
    }
}

/* ----- Output Configuration ----- */
void amr_task_output_type(amr_task_t *t, const char *type_name) {
  if (!t || !t->current_output || !type_name) return;

  amr_datatype_t *dt = amr_datatype_find(t->scheduler->types, type_name);
  if (!dt) {
    fprintf(stderr, "[ERROR] Task '%s' output '%s' tried to set unknown type: '%s'\n",
            t->task_name, t->current_output->name, type_name);
    abort();
  }

  t->current_output->type_name = aml_pool_strdup(t->scheduler->pool, type_name);
  t->current_output->datatype = dt; /* Cache the pointer directly! */

  /* AUTO-WIRE THE DUMPER! */
  if (dt->to_string && dt->deserialize) {
      t->current_output->dump = amr_typed_dump_cb;
      t->current_output->dump_arg = dt;
  }
}

/* ----- Input Configuration ----- */
void amr_task_input_expect_type(amr_task_t *t, const char *type_name) {
  if (!t || !t->current_input || !type_name) return;

  amr_datatype_t *dt = amr_datatype_find(t->scheduler->types, type_name);
  if (!dt) {
    fprintf(stderr, "[ERROR] Task '%s' input tried to expect unknown type: '%s'\n",
            t->task_name, type_name);
    abort();
  }

  amr_task_input_link_t *inp = t->current_input;
  while (inp) {
    inp->input->expected_type = aml_pool_strdup(t->scheduler->pool, type_name);
    inp->input->datatype = dt; /* Cache the pointer directly! */
    /* AUTO-WIRE THE DUMPER! */
    if (dt->to_string && dt->deserialize) {
        inp->input->dump = amr_typed_dump_cb;
        inp->input->dump_arg = dt;
    }
    inp = inp->next;
  }
}

amr_t *amr_pipeline_scheduler(amr_pipeline_t *p) {
    return p->scheduler;
}
