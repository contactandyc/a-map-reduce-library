// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "a-map-reduce-library/amr.h"

#include "a-memory-library/aml_alloc.h"
#include "a-memory-library/aml_pool.h"
#include "a-memory-library/aml_buffer.h"
#include "the-io-library/io.h"
#include "the-io-library/io_out.h"
#include "the-io-library/io_in.h"
#include "the-macro-library/macro_map.h"
#include "the-macro-library/macro_time.h"

#include "amr_internal.h"
#include "amr_cli.h"
#include "amr_datatypes.h"
#include "amr_task.h"
#include "amr_worker.h"
#include "amr_runner.h"
#include "amr_debug.h"

#include <math.h>
#include <pthread.h>
#include <unistd.h>

io_file_info_t *file_info_name(amr_worker_t *w, size_t *num_files,
                               amr_worker_input_t *inp);
static inline void _amr_push_current_input(amr_task_t *task,
                                           amr_worker_input_t *ti);

static bool is_worker_selected(amr_worker_t *w) {
  if (w->task->selected && w->task->selected[w->partition])
    return true;
  return false;
}

static amr_task_state_link_t *available_tasks(amr_t *h,
                                             size_t partition) {
  return h->state[partition].available_tasks;
}

static amr_task_state_link_t *completed_tasks(amr_t *h,
                                             size_t partition) {
  return h->state[partition].completed_tasks;
}

static amr_task_state_link_t *tasks_to_finish(amr_t *h,
                                             size_t partition) {
  return h->state[partition].tasks_to_finish;
}

void amr_custom_args(amr_t *h, void (*custom_usage)(),
                     amr_parse_args_cb parse_args, amr_finish_args_cb finish_args,
                     void *arg) {
  h->parse_args = parse_args;
  h->finish_args = finish_args;
  h->parse_args_arg = arg;
  h->custom_usage = custom_usage;
}

void *amr_task_custom_arg(amr_task_t *task) {
  return task->scheduler->parse_args_arg;
}

amr_t *amr_init(int argc, char **args, size_t num_partitions,
                                size_t cpus, size_t ram) {
  size_t disk_space = 1000; // not really used at the moment
  aml_pool_t *pool = aml_pool_init(32768);
  amr_t *h = (amr_t *)aml_pool_zalloc(
      pool, sizeof(amr_t) + (num_partitions * sizeof(amr_task_state_t)));
  pthread_mutex_init(&(h->mutex), NULL);
  pthread_cond_init(&(h->cond), NULL);

  h->types = amr_datatype_registry_init();

  amr_register_common_datatypes(h);

  h->started_threads = false;
  h->num_partitions = num_partitions;
  h->state = (amr_task_state_t *)(h + 1);
  h->ram = ram * 1024;
  h->cpus = cpus;
  h->disk_space = disk_space * 1024;
  h->pool = pool;
  h->tmp_pool = aml_pool_init(4096);
  h->args = argc > 0 ? aml_pool_strdupan(pool, args, argc) : NULL;
  h->argc = argc > 0 ? argc : 0;
  return h;
}

void amr_use_runs(amr_t *h) {
  h->use_runs = true;
  size_t run_num = 0;
  char **p = h->args;
  char **ep = p + h->argc;

  /* Pre-scan for --run so the app can use it immediately in main() */
  while (p < ep) {
    if (!strcmp(*p, "--run") && p + 1 < ep) {
      sscanf(*(p + 1), "%zu", &run_num);
      break;
    }
    p++;
  }
  h->parsed_args.run_number = run_num;

  if (!h->task_dir) {
    h->task_dir = aml_pool_strdupf(h->pool, "tasks/run_%zu", run_num);
  }
}

size_t amr_current_run(amr_t *h) {
  return h->parsed_args.run_number;
}

void amr_ack_dir(amr_t *h, const char *ack_dir) {
  h->ack_dir = aml_pool_strdup(h->pool, ack_dir);
}

void amr_task_dir(amr_t *h, const char *task_dir) {
  h->task_dir = aml_pool_strdup(h->pool, task_dir);
}

void amr_destroy(amr_t *h) {
  pthread_mutex_destroy(&h->mutex);
  pthread_cond_destroy(&h->cond);
  amr_datatype_registry_destroy(h->types);
  aml_pool_destroy(h->tmp_pool);
  aml_pool_t *pool = h->pool;
  aml_pool_destroy(pool);
}

void unlink_state(amr_t *h, amr_task_state_link_t *state,
                  size_t partition) {
  if (state->previous) {
    state->previous->next = state->next;
    if (state->next)
      state->next->previous = state->previous;
  } else {
    amr_task_state_link_t **root = &(h->state[partition].available_tasks);
    if (state->waiting_on_others)
      root = &(h->state[partition].tasks_to_finish);
    else if (state->completed)
      root = &(h->state[partition].completed_tasks);

    *root = state->next;
    if (state->next)
      state->next->previous = NULL;
  }
  state->next = state->previous = NULL;
  if (!state->waiting_on_others && !state->completed)
    h->num_available--;
  else if (state->waiting_on_others)
    h->num_tasks_to_run--;
}

void link_state(amr_t *h, amr_task_state_link_t *state,
                size_t partition) {
  amr_task_state_link_t **root = &(h->state[partition].available_tasks);
  if (state->waiting_on_others)
    root = &(h->state[partition].tasks_to_finish);
  else if (state->completed)
    root = &(h->state[partition].completed_tasks);

  state->next = *root;
  state->previous = NULL;
  if (*root)
    (*root)->previous = state;

  *root = state;
  if (!state->waiting_on_others && !state->completed)
    h->num_available++;
  else if (state->waiting_on_others)
    h->num_tasks_to_run++;
}

static void mark_task_complete(amr_worker_t *w,
                               amr_task_state_link_t *state_link,
                               size_t partition, time_t when);
static bool is_dependencies_complete(amr_task_t *task, size_t partition);

static amr_worker_t *take_worker(amr_t *scheduler, aml_pool_t *pool,
                                amr_task_t *task, size_t partition) {
  amr_task_state_link_t *avail = available_tasks(scheduler, partition);
  while (avail) {
    if (!task || avail->task == task) {
      unlink_state(scheduler, avail, partition);
      amr_worker_t *w = (amr_worker_t *)aml_pool_zalloc(pool, sizeof(amr_worker_t));
      w->task = avail->task;
      w->partition = partition;
      w->num_partitions = w->task->num_partitions;
      w->ack_time = -1;
      w->state_link = avail;
      return w;
    }
    avail = avail->next;
  }
  return NULL;
}

amr_worker_t *create_worker(aml_pool_t *pool, amr_task_t *task,
                            size_t partition) {
  amr_worker_t *w = (amr_worker_t *)aml_pool_zalloc(pool, sizeof(amr_worker_t));
  w->task = task;
  w->partition = partition;
  w->num_partitions = task->num_partitions;
  w->ack_time = -1;
  w->state_link = NULL;
  return w;
}

static void get_ack_time_for_task(amr_task_t *task) {
  for (size_t i = 0; i < task->num_partitions; i++) {
    time_t *ack = &(task->state_linkage[i].ack_time);
    if (*ack == -1) {
      char *filename =
          aml_pool_strdupf(task->scheduler->pool, "%s/%s_%zu",
                          task->scheduler->ack_dir, task->task_name, i);
      *ack = io_modified(filename);
    }
  }
}

static void schedule_setup(amr_t *h) {
  if (!h->task_dir) h->task_dir = (char *)"tasks";
  if (!h->ack_dir)  h->ack_dir  = aml_pool_strdupf(h->pool, "%s/ack", h->task_dir);

  aml_buffer_t *bh = aml_buffer_init(100);
  io_make_directory(h->ack_dir);

  // (1) run user setup()s
  for (amr_task_t *n = h->head; n; n = n->next) {
    get_ack_time_for_task(n);

    if (n->setup) {
      if (!n->setup(n)) {
        fprintf(stderr, "[ERROR] Task '%s' setup failed!\n", n->task_name);
        abort();
      }
      if (n->runner == in_out_runner && !n->transforms) {
        amr_worker_input_t *i = n->inputs;
        char *inp = NULL;
        if (i) {
          aml_buffer_clear(bh);
          while (i) {
            aml_buffer_appends(bh, i->name);
            if (i->next) aml_buffer_appendc(bh, '|');
            i = i->next;
          }
          inp = aml_strdup(aml_buffer_data(bh));
        }
        amr_worker_output_t *o = n->outputs;
        aml_buffer_clear(bh);
        while (o) {
          aml_buffer_appends(bh, o->name);
          if (o->next) aml_buffer_appendc(bh, '|');
          o = o->next;
        }
        amr_task_transform(n, inp, aml_buffer_data(bh), NULL);
        if (inp) aml_free(inp);
      }
    }

    if (n->transforms && !n->runner) {
        n->runner = in_out_runner;
    }

    for (size_t i = 0; i < n->num_partitions; i++) {
      aml_buffer_setf(bh, "%s/%s_%zu", h->task_dir, n->task_name, i);
      io_make_directory(aml_buffer_data(bh));
    }
  }
  aml_buffer_destroy(bh);

  // (2) ensure outputs exist for any transform-declared names
  ensure_outputs_for_transforms(h);

  // (2.5) Resolve logical pipeline ports into physical DAG edges
  resolve_pipeline_ports(h);

  // (3) wire consumer-declared edges
  wire_graph(h);

  // (4) resolve transform names to pointers
  resolve_transforms(h);

  // (5) validate partitioning (SHUFFLE/PARTITION)
  validate_partitions(h);

  // (5.5) Initialize dependency queues to enforce DAG execution
  for (amr_task_t *n = h->head; n; n = n->next) {
    for (size_t i = 0; i < n->num_partitions; i++) {
      if (!is_dependencies_complete(n, i)) {
        unlink_state(h, n->state_linkage + i, i);
        n->state_linkage[i].waiting_on_others = true;
        link_state(h, n->state_linkage + i, i);
      }
    }
  }

  // (6) now it's safe to pre-complete do-nothing tasks
  time_t now = time(NULL);
  for (amr_task_t *n = h->head; n; n = n->next) {
    if (n->do_nothing) {
      for (size_t i = 0; i < n->num_partitions; i++) {
        if (is_dependencies_complete(n, i)) {
          aml_pool_clear(h->tmp_pool);
          amr_worker_t *w = take_worker(h, h->tmp_pool, n, i);
          if (w) mark_task_complete(w, w->state_link, i, now);
        }
      }
    }
  }
  aml_pool_clear(h->tmp_pool);
}


static bool task_available(amr_task_t *task, size_t partition) {
  amr_t *scheduler = task->scheduler;
  amr_task_state_link_t *avail = available_tasks(scheduler, partition);
  while (avail) {
    if (avail->task == task)
      return true;
    avail = avail->next;
  }
  return false;
}

static bool is_schedule_running(amr_worker_t *w) {
  parsed_args_t *p = &(w->task->scheduler->parsed_args);
  if (p->dump || p->list || p->help)
    return false;
  return true;
}

static void write_ack(amr_worker_t *w) {
  if (!is_schedule_running(w))
    return;

  char *filename = aml_pool_strdupf(w->schedule_thread->pool, "%s/%s_%zu",
                                   w->task->scheduler->ack_dir,
                                   w->task->task_name, w->partition);
  // printf("%s\n", filename);
  FILE *out = fopen(filename, "wb");
  fclose(out);
}

void get_ack_time(amr_worker_t *w) {
  time_t *ack = &(w->task->state_linkage[w->partition].ack_time);
  if (*ack == -1) {
    char *filename = aml_pool_strdupf(w->schedule_thread->pool, "%s/%s_%zu",
                                     w->task->scheduler->ack_dir,
                                     w->task->task_name, w->partition);
    *ack = io_modified(filename);
  }
  w->ack_time = *ack;
}

amr_worker_t *worker_complete(amr_worker_t *w, time_t when) {
  if (when > w->ack_time && when > 1)
    write_ack(w);

  // amr_worker_t *next = NULL;
  amr_t *scheduler = w->task->scheduler;
  pthread_mutex_lock(&(scheduler->mutex));
  size_t num_available = scheduler->num_available;
  if (is_worker_selected(w)) {
    scheduler->parsed_args.num_selected--;
    if (!scheduler->parsed_args.num_selected)
      scheduler->done = true;
  }
  mark_task_complete(w, w->state_link, w->partition, when);
  // if (w->next_task && take_available(w->next_task, w->partition))
  //  next = take_worker(w->next_task, w->partition);
  if ((!num_available && scheduler->num_available) || scheduler->done)
    pthread_cond_broadcast(&scheduler->cond);
  pthread_mutex_unlock(&(scheduler->mutex));
  return NULL;
}

amr_worker_t *get_next_worker(amr_thread_t *t) {
  aml_pool_t *pool = t->pool;
  amr_worker_t *w = NULL;
  amr_t *scheduler = t->scheduler;
  // printf("Attempting to get task for %zu\n", t->thread_id);
  pthread_mutex_lock(&(scheduler->mutex));
  if (scheduler->done) {
    pthread_mutex_unlock(&(scheduler->mutex));
    return NULL;
  }
  scheduler->num_running--;
  while (!scheduler->done && !scheduler->num_available &&
         scheduler->num_tasks_to_run)
    pthread_cond_wait(&scheduler->cond, &scheduler->mutex);

  if (scheduler->num_available) {
    for (size_t p = 0; p < scheduler->num_partitions; p++) {
      size_t px = (p + t->partition) % scheduler->num_partitions;
      w = take_worker(scheduler, pool, NULL, px);
      if (w) {
        scheduler->num_running++;
        w->running = scheduler->num_running + scheduler->num_available;
        if (w->running > scheduler->cpus)
          w->running = scheduler->cpus;
        break;
      }
    }
  }
  pthread_mutex_unlock(&(scheduler->mutex));
  if (w) {
    w->thread_id = t->thread_id;
    w->schedule_thread = t;
  }
  // if (!w) {
  //  printf("failed to get task for %zu\n", t->thread_id);
  // }
  return w;
}

time_t get_ack_time_for_task_and_partition(amr_task_t *task, size_t partition) {
  if (partition >= task->num_partitions)
    return 0;
  time_t ack_time = task->state_linkage[partition].ack_time;
  time_t completed = task->state_linkage[partition].completed;

  return ack_time > completed ? ack_time : completed;
}

static bool task_run_per_args(amr_worker_t *w) {
  amr_t *scheduler = w->task->scheduler;
  if (scheduler->parsed_args.only_run_selected) {
    if (scheduler->parsed_args.select_all)
      return true;
    if (w->task->selected && w->task->selected[w->partition])
      return true;
    return false;
  }
  if (!scheduler->parsed_args.select_all &&
      !scheduler->parsed_args.num_selected)
    return false;
  return true;
}

static bool worker_needs_to_run(amr_worker_t *w) {
  if (w->ack_time == 0)
    return true;

  if (w->task->run_everytime)
    return true;

  if (w->task->scheduler->parsed_args.force) {
    if (w->task->scheduler->parsed_args.select_all)
      return true;
    if (w->task->selected && w->task->selected[w->partition])
      return true;
  }

  amr_task_t *task = w->task;
  amr_task_link_t *link = task->dependencies;
  while (link) {
    for (size_t i = 0; i < link->task->num_partitions; i++) {
      time_t ack_time = get_ack_time_for_task_and_partition(link->task, i);
      if (ack_time > w->ack_time)
        return true;
    }
    link = link->next;
  }
  link = task->partial_dependencies;
  while (link) {
    time_t ack_time =
        get_ack_time_for_task_and_partition(link->task, w->partition);
    if (ack_time > w->ack_time)
      return true;
    link = link->next;
  }

  if (w->inputs) {
    amr_worker_input_t *n = w->inputs;
    while (n) {
      for (size_t i = 0; i < n->num_files; i++) {
        if (n->files[i].last_modified > w->ack_time)
          return true;
      }
      n = n->next;
    }
  }
  return false;
}

void mark_as_done(amr_t *scheduler) {
  pthread_mutex_lock(&(scheduler->mutex));
  if (!scheduler->done) {
    scheduler->done = true;
    pthread_cond_broadcast(&scheduler->cond);
  }
  pthread_mutex_unlock(&(scheduler->mutex));
}

void *schedule_thread(void *arg) {
  amr_thread_t *t = (amr_thread_t *)arg;
  t->pool = aml_pool_init(65536);
  t->bh = aml_buffer_init(200);
  aml_pool_t *tmp_pool = aml_pool_init(65536);
  aml_buffer_t *bh = aml_buffer_init(1024);
  while (true) {
    aml_pool_clear(t->pool);
    amr_worker_t *w = get_next_worker(t);
    if (!w)
      break;

    w->worker_pool = t->pool;
    aml_pool_clear(tmp_pool);
    w->pool = tmp_pool;
    aml_buffer_clear(bh);
    w->bh = bh;
    get_ack_time(w);
    clone_inputs_and_outputs(w);
    fill_inputs(w);

    if (worker_needs_to_run(w)) {
      if (!task_run_per_args(w)) {
        worker_complete(w, w->ack_time ? w->ack_time : 1);
      } else {
        setup_worker(w);
        if (!run_worker(w)) {
          pthread_mutex_lock(&(t->scheduler->mutex));
          t->scheduler->success = false;
          t->scheduler->done = true;
          pthread_cond_broadcast(&(t->scheduler->cond));
          pthread_mutex_unlock(&(t->scheduler->mutex));
          destroy_worker(w);
          break;
        }
        if (t->scheduler->on_complete) {
          if (!t->scheduler->on_complete(w)) {
            pthread_mutex_lock(&(t->scheduler->mutex));
            t->scheduler->success = false;
            t->scheduler->done = true;
            pthread_cond_broadcast(&(t->scheduler->cond));
            pthread_mutex_unlock(&(t->scheduler->mutex));
            destroy_worker(w);
            break;
          }
        }
        worker_complete(w, time(NULL));
        destroy_worker(w);
      }
    } else
      worker_complete(w, w->ack_time ? w->ack_time : 1);
  }
  aml_pool_destroy(t->pool);
  aml_pool_destroy(tmp_pool);
  aml_buffer_destroy(bh);
  aml_buffer_destroy(t->bh);
  mark_as_done(t->scheduler);
  return NULL;
}

void amr_keep_intermediate_files(amr_t *h) {
  h->parsed_args.keep_files = true;
}

bool amr_run(amr_t *h, amr_worker_cb on_complete) {
  h->success = true;
  schedule_setup(h);
  parse_args(h);
  if (h->parsed_args.cpus) {
    h->cpus = h->parsed_args.cpus;
    // TODO: fix this so that you can have more cpus
    // if (h->cpus > h->num_partitions)
    //   h->cpus = h->num_partitions;
  }
  if (h->parsed_args.ram)
    h->ram = h->parsed_args.ram * 1024;

  if (h->parsed_args.help || h->parsed_args.dump || h->parsed_args.list)
    h->cpus = 1;

  h->threads = (amr_thread_t *)aml_pool_zalloc(
      h->pool, sizeof(amr_thread_t) * h->cpus);
  for (size_t i = 0; i < h->cpus; i++) {
    amr_thread_t *a = h->threads + i;
    a->thread_id = i;
    a->scheduler = h;
  }

  if (h->parsed_args.debug_task) {
    if (h->parsed_args.dump) {
      h->num_running = h->cpus;
      h->on_complete = NULL;
      h->done = false;
      dump_selected_tasks(h->threads + 0);
    } else
      debug_task(h->threads + 0);
    return h->success;
  }

  if (h->finish_args) {
    if (!h->finish_args(h->argc, h->args, h->parse_args_arg))
      h->parsed_args.help = true;
  }

  if (h->parsed_args.help) {
    schedule_usage(h);
    return h->success;
  } else if (h->parsed_args.dump) {
    h->num_running = h->cpus;
    h->on_complete = NULL;
    h->done = false;
    dump_selected_tasks(h->threads);
  } else if (h->parsed_args.list) {
    h->num_running = h->cpus;
    h->on_complete = NULL;
    h->done = false;
    list_selected_tasks(h->threads);
  } else {
    h->num_running = h->cpus;
    h->on_complete = on_complete;
    h->done = false;
    for (size_t i = 0; i < h->cpus; i++)
      pthread_create(&(h->threads[i].thread), NULL, schedule_thread,
                     h->threads + i);
    for (size_t i = 0; i < h->cpus; i++)
      pthread_join(h->threads[i].thread, NULL);
  }

  return h->success;
}


static bool is_task_complete(amr_task_t *task) {
  for (size_t i = 0; i < task->num_partitions; i++) {
    if (task->state_linkage[i].waiting_on_others ||
        !task->state_linkage[i].completed)
      return false;
  }
  return true;
}

static bool is_worker_complete(amr_task_t *task, size_t partition) {
  if (partition >= task->num_partitions)
    return is_worker_complete(task, 0);

  if (!task->state_linkage[partition].waiting_on_others &&
      task->state_linkage[partition].completed)
    return true;
  return false;
}

static bool is_dependencies_complete(amr_task_t *task, size_t partition) {
  // dependencies
  amr_task_link_t *n = task->dependencies;
  while (n) {
    if (!is_task_complete(n->task))
      return false;
    n = n->next;
  }

  // partial_dependencies
  n = task->partial_dependencies;
  while (n) {
    if (!is_worker_complete(n->task, partition))
      return false;
    n = n->next;
  }
  return true;
}

/* Internal helper: push a just-created input into task->current_input
   so that amr_task_input_* option setters (format/compare/reducer/limit/…)
   continue to affect the most recently declared inputs. */
static inline void _amr_push_current_input(amr_task_t *task,
                                           amr_worker_input_t *ti) {
  amr_task_input_link_t *link =
      (amr_task_input_link_t *)aml_pool_alloc(task->scheduler->pool,
                                              sizeof(*link));
  link->input = ti;
  link->next  = task->current_input;
  task->current_input = link;
}

static void check_task(amr_worker_t *w,
                       amr_t *scheduler,
                       amr_task_state_link_t *state_link, size_t partition,
                       time_t when) {
  if (state_link->waiting_on_others &&
      is_dependencies_complete(state_link->task, partition)) {
    unlink_state(scheduler, state_link, partition);
    state_link->waiting_on_others = false;
    if (state_link->task->do_nothing)
      mark_task_complete(w, state_link, partition, when);
    else
      link_state(scheduler, state_link, partition);
  }
}

static void mark_task_complete(amr_worker_t *w,
                               amr_task_state_link_t *state_link,
                               size_t partition, time_t when) {
  amr_task_t *task = state_link->task;
  amr_t *scheduler = task->scheduler;
  state_link->waiting_on_others = false;
  state_link->completed = when;
  link_state(scheduler, state_link, partition);

  bool global_keep = scheduler->parsed_args.keep_files;

  /* FILE DELETION LOGIC (Unlink when refcount hits 0) */
  for (amr_worker_input_t *inp = w->inputs; inp; inp = inp->next) {
    amr_worker_output_t *src = inp->src;
    if (!src) continue; /* Ignore external files or previous run boundaries */

    bool keep = global_keep || src->keep_output;
    size_t mode = inp->edge_flags;

    if (mode & AMR_EDGE_PARTITION) {
      size_t i = partition;
      if (src->refcount_parts[i] > 0 && --src->refcount_parts[i] == 0) {
        src->cleaned_up_parts[i] = true;
        if (!keep && inp->num_files > 0) unlink(inp->files[0].filename);
      }
    } else if (mode & AMR_EDGE_FIRST) {
      if (src->refcount_parts[0] > 0 && --src->refcount_parts[0] == 0) {
        src->cleaned_up_parts[0] = true;
        if (!keep && inp->num_files > 0) unlink(inp->files[0].filename);
      }
    } else if (mode & AMR_EDGE_SHUFFLE) {
      size_t bucket = partition;
      if (src->refcount_buckets && src->refcount_buckets[bucket] > 0) {
        if (--src->refcount_buckets[bucket] == 0) {
          if (!keep) {
            for (size_t f = 0; f < inp->num_files; f++) {
              unlink(inp->files[f].filename);
            }
          }
        }
      }
    } else if (mode & AMR_EDGE_ALL_TO_ALL) {
      for (size_t i = 0; i < src->num_partitions; i++) {
        if (src->refcount_parts[i] > 0 && --src->refcount_parts[i] == 0) {
          src->cleaned_up_parts[i] = true;
          if (!keep && i < inp->num_files) unlink(inp->files[i].filename);
        }
      }
    } else {
      /* Defensive guard: Catches bit corruption or missing edge flags */
      abort();
    }
  }

  /* DAG RESOLUTION LOGIC */
  bool partially_complete = false;
  for (size_t i = 0; i < task->num_partitions; i++) {
    if (!task->state_linkage[i].completed) { partially_complete = true; break; }
  }

  if (!partially_complete) {
    for (amr_task_link_t *link = task->reverse_dependencies; link; link = link->next)
      for (size_t i = 0; i < link->task->num_partitions; i++)
        check_task(w, scheduler, link->task->state_linkage + i, i, when);

    for (amr_task_link_t *link = task->reverse_partial_dependencies; link; link = link->next)
      for (size_t i = 0; i < link->task->num_partitions; i++)
        check_task(w, scheduler, link->task->state_linkage + i, i, when);
  } else {
    for (amr_task_link_t *link = task->reverse_partial_dependencies; link; link = link->next) {
      size_t i = partition;
      if (i < link->task->num_partitions)
        check_task(w, scheduler, link->task->state_linkage + i, i, when);
    }
  }
}

char *amr_run_file_path(amr_t *h, size_t run, const char *task_name,
                        size_t partition, const char *file_base) {
  aml_buffer_t *bh = aml_buffer_init(256);

  /* Assume default root "tasks/" for runs */
  aml_buffer_setf(bh, "tasks/run_%zu/%s_%zu/", run, task_name, partition);

  if (io_extension(file_base, "lz4")) {
    aml_buffer_append(bh, file_base, strlen(file_base) - 4);
    aml_buffer_appendf(bh, "_%zu.lz4", partition);
  } else if (io_extension(file_base, "gz")) {
    aml_buffer_append(bh, file_base, strlen(file_base) - 3);
    aml_buffer_appendf(bh, "_%zu.gz", partition);
  } else {
    aml_buffer_appends(bh, file_base);
    aml_buffer_appendf(bh, "_%zu", partition);
  }

  char *res = aml_pool_strdup(h->pool, aml_buffer_data(bh));
  aml_buffer_destroy(bh);
  return res;
}

/* ========================================================================
 * PIPELINE / SUBGRAPH IMPLEMENTATION
 * ======================================================================== */

static amr_pipeline_t *_pipeline_create(amr_t *sched, amr_pipeline_t *parent, const char *ns, amr_pipeline_cb setup, void *config) {
    amr_pipeline_t *p = aml_pool_zalloc(sched->pool, sizeof(*p));
    if (parent) {
        p->ns = aml_pool_strdupf(sched->pool, "%s_%s", parent->ns, ns);
        p->parent = parent;
    } else {
        p->ns = aml_pool_strdup(sched->pool, ns);
    }
    p->scheduler = sched;
    p->config = config;

    if (setup && !setup(p)) {
        fprintf(stderr, "[ERROR] Pipeline '%s' setup failed!\n", p->ns);
        abort();
    }
    return p;
}

amr_pipeline_t *amr_pipeline_create(amr_t *sched, const char *ns, amr_pipeline_cb setup, void *config) {
    return _pipeline_create(sched, NULL, ns, setup, config);
}

amr_pipeline_t *amr_pipeline_create_nested(amr_pipeline_t *parent, const char *ns, amr_pipeline_cb setup, void *config) {
    return _pipeline_create(parent->scheduler, parent, ns, setup, config);
}

void amr_pipeline_bind_input(amr_pipeline_t *p, const char *port_name, const char *ext_task, const char *ext_out) {
    aml_pool_t *pool = p->scheduler->pool;
    amr_pipeline_port_t *port = aml_pool_zalloc(pool, sizeof(*port));
    port->port_name = aml_pool_strdup(pool, port_name);
    port->mapped_task = aml_pool_strdup(pool, ext_task);
    port->mapped_output = aml_pool_strdup(pool, ext_out);
    port->next = p->inputs;
    p->inputs = port;
}

void amr_pipeline_expose_input(amr_pipeline_t *parent, const char *parent_port, amr_pipeline_t *child, const char *child_port) {
    aml_pool_t *pool = parent->scheduler->pool;
    amr_pipeline_port_t *port = aml_pool_zalloc(pool, sizeof(*port));
    port->port_name = aml_pool_strdup(pool, child_port);
    port->alias_pipe = parent;
    port->alias_port = aml_pool_strdup(pool, parent_port);
    port->next = child->inputs;
    child->inputs = port;
}

static void amr_pipeline_bind_output_exact(amr_pipeline_t *p, const char *port_name, const char *exact_task, const char *exact_out) {
    aml_pool_t *pool = p->scheduler->pool;
    amr_pipeline_port_t *port = aml_pool_zalloc(pool, sizeof(*port));
    port->port_name = aml_pool_strdup(pool, port_name);
    port->mapped_task = aml_pool_strdup(pool, exact_task);
    port->mapped_output = aml_pool_strdup(pool, exact_out);
    port->next = p->outputs;
    p->outputs = port;
}

void amr_pipeline_bind_output(amr_pipeline_t *p, const char *port_name, const char *internal_task_base, const char *internal_out) {
    char *exact_task = aml_pool_strdupf(p->scheduler->pool, "%s_%s", p->ns, internal_task_base);
    amr_pipeline_bind_output_exact(p, port_name, exact_task, internal_out);
}

void amr_pipeline_expose_output(amr_pipeline_t *parent, const char *parent_port, amr_pipeline_t *child, const char *child_port) {
    amr_pipeline_port_t *out_port = child->outputs;
    while (out_port) {
        if (strcmp(out_port->port_name, child_port) == 0) break;
        out_port = out_port->next;
    }
    if (!out_port) {
        fprintf(stderr, "[ERROR] Cannot expose child output '%s'. Port not found in child pipeline '%s'.\n", child_port, child->ns);
        abort();
    }
    /* Bubbles the physical resolution straight up to the parent! */
    amr_pipeline_bind_output_exact(parent, parent_port, out_port->mapped_task, out_port->mapped_output);
}

amr_task_t *amr_pipeline_task(amr_pipeline_t *p, const char *base_name, bool partitioned, amr_task_cb setup) {
    char *full_name = aml_pool_strdupf(p->scheduler->pool, "%s_%s", p->ns, base_name);
    amr_task_t *t = amr_task(p->scheduler, full_name, partitioned, setup);
    t->pipeline = p; // Tag the task with its pipeline context
    return t;
}

/* ========================================================================
 * PIPELINE ROUTING EDGES
 * ======================================================================== */

// 1. INTERNAL PORTS (Tasks reading from the Pipeline's logical entrance)
void amr_task_input_from_pipeline_port_first(amr_task_t *t, const char *port, double pct) {
    amr_task_input_from_task_first(t, port, port, pct);
    t->scheduler->pending_edges->is_pipeline_port = true;
}

void amr_task_input_from_pipeline_port_partition(amr_task_t *t, const char *port, double pct) {
    amr_task_input_from_task_partition(t, port, port, pct);
    t->scheduler->pending_edges->is_pipeline_port = true;
}

void amr_task_input_from_pipeline_port_shuffle(amr_task_t *t, const char *port, double pct) {
    amr_task_input_from_task_shuffle(t, port, port, pct);
    t->scheduler->pending_edges->is_pipeline_port = true;
}

void amr_task_input_from_pipeline_port_all_to_all(amr_task_t *t, const char *port, double pct) {
    amr_task_input_from_task_all_to_all(t, port, port, pct);
    t->scheduler->pending_edges->is_pipeline_port = true;
}

// 2. SIBLING LINKS (Tasks connecting to other tasks inside the same pipeline)
void amr_task_input_from_sibling_first(amr_task_t *t, const char *sibling, const char *out, double pct) {
    char *full_name = aml_pool_strdupf(t->scheduler->pool, "%s_%s", t->pipeline->ns, sibling);
    amr_task_input_from_task_first(t, full_name, out, pct);
}

void amr_task_input_from_sibling_partition(amr_task_t *t, const char *sibling, const char *out, double pct) {
    char *full_name = aml_pool_strdupf(t->scheduler->pool, "%s_%s", t->pipeline->ns, sibling);
    amr_task_input_from_task_partition(t, full_name, out, pct);
}

void amr_task_input_from_sibling_shuffle(amr_task_t *t, const char *sibling, const char *out, double pct) {
    char *full_name = aml_pool_strdupf(t->scheduler->pool, "%s_%s", t->pipeline->ns, sibling);
    amr_task_input_from_task_shuffle(t, full_name, out, pct);
}

void amr_task_input_from_sibling_all_to_all(amr_task_t *t, const char *sibling, const char *out, double pct) {
    char *full_name = aml_pool_strdupf(t->scheduler->pool, "%s_%s", t->pipeline->ns, sibling);
    amr_task_input_from_task_all_to_all(t, full_name, out, pct);
}

// 3. EXTERNAL ALIASING (Consumer tasks reading from the Pipeline's logical exit)
void amr_task_input_from_pipeline_first(amr_task_t *t, amr_pipeline_t *p, const char *port_name, double in_ram_pct) {
    amr_pipeline_port_t *port = p->outputs;
    while (port) { if (strcmp(port->port_name, port_name) == 0) break; port = port->next; }
    if (!port) abort();
    amr_task_input_from_task_mode(t, port->mapped_task, port->mapped_output, port_name, in_ram_pct, AMR_EDGE_FIRST);
}

void amr_task_input_from_pipeline_partition(amr_task_t *t, amr_pipeline_t *p, const char *port_name, double in_ram_pct) {
    amr_pipeline_port_t *port = p->outputs;
    while (port) { if (strcmp(port->port_name, port_name) == 0) break; port = port->next; }
    if (!port) abort();
    amr_task_input_from_task_mode(t, port->mapped_task, port->mapped_output, port_name, in_ram_pct, AMR_EDGE_PARTITION);
}

void amr_task_input_from_pipeline_shuffle(amr_task_t *t, amr_pipeline_t *p, const char *port_name, double in_ram_pct) {
    amr_pipeline_port_t *port = p->outputs;
    while (port) { if (strcmp(port->port_name, port_name) == 0) break; port = port->next; }
    if (!port) abort();
    amr_task_input_from_task_mode(t, port->mapped_task, port->mapped_output, port_name, in_ram_pct, AMR_EDGE_SHUFFLE);
}

void amr_task_input_from_pipeline_all_to_all(amr_task_t *t, amr_pipeline_t *p, const char *port_name, double in_ram_pct) {
    amr_pipeline_port_t *port = p->outputs;
    while (port) { if (strcmp(port->port_name, port_name) == 0) break; port = port->next; }
    if (!port) abort();
    amr_task_input_from_task_mode(t, port->mapped_task, port->mapped_output, port_name, in_ram_pct, AMR_EDGE_ALL_TO_ALL);
}

void amr_pipeline_bind_link(amr_pipeline_t *dest_pipe, const char *dest_in_port,
                            amr_pipeline_t *src_pipe,  const char *src_out_port) {

    // Find what internal task/output the source pipeline exposed
    amr_pipeline_port_t *out_port = src_pipe->outputs;
    while (out_port) {
        if (strcmp(out_port->port_name, src_out_port) == 0) break;
        out_port = out_port->next;
    }

    if (!out_port) {
        fprintf(stderr, "[ERROR] Source pipeline '%s' has no output port '%s'\n",
                src_pipe->ns, src_out_port);
        abort();
    }

    // Bind it to the destination pipeline's input!
    amr_pipeline_bind_input(dest_pipe, dest_in_port, out_port->mapped_task, out_port->mapped_output);
}

/* Helper to return an empty stream for unbound optional ports */
static io_file_info_t *file_info_empty(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp) {
    *num_files = 0;
    return NULL;
}

/* Recursive helper to resolve nested port aliases */
static bool resolve_logical_port(amr_pipeline_t *p, const char *logical_port, char **phys_task, char **phys_out) {
    amr_pipeline_port_t *port = p->inputs;
    while (port) {
        if (strcmp(port->port_name, logical_port) == 0) {
            // Did this port get forwarded up to a parent pipeline? Ask the parent!
            if (port->alias_pipe) {
                return resolve_logical_port(port->alias_pipe, port->alias_port, phys_task, phys_out);
            } else if (port->mapped_task) {
                *phys_task = port->mapped_task;
                *phys_out = port->mapped_output;
                return true;
            } else {
                return false; // Safely unbound
            }
        }
        port = port->next;
    }
    return false; // Port not declared
}

/* The Magic Resolution Step - Now handles recursion! */
void resolve_pipeline_ports(amr_t *sched) {
    amr_pending_edge_t **prev = &sched->pending_edges;
    amr_pending_edge_t *e = sched->pending_edges;

    while (e) {
        if (e->is_pipeline_port) {
            amr_pipeline_t *p = e->consumer->pipeline;
            char *phys_task = NULL;
            char *phys_out = NULL;

            bool resolved = resolve_logical_port(p, e->producer_task_name, &phys_task, &phys_out);

            if (!resolved) {
                /* OPTIONAL PORT LOGIC: Turn unbound port into an empty stream */
                e->input_stub->file_info = file_info_empty;

                /* Remove from the linked list so wire_graph() ignores it completely! */
                *prev = e->next;
                e = e->next;
                continue;
            }

            // Overwrite the fake edge with the recursively resolved physical path!
            e->producer_task_name = phys_task;
            e->output_name = phys_out;
            e->is_pipeline_port = false;
        }
        prev = &e->next;
        e = e->next;
    }
}

void *amr_pipeline_config(amr_pipeline_t *p) {
    return p->config;
}

amr_pipeline_t *amr_task_pipeline(amr_task_t *t) {
    return t->pipeline;
}
