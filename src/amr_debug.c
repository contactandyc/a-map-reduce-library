// SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-map-reduce-library/amr.h"
#include "a-memory-library/aml_alloc.h"
#include "a-memory-library/aml_pool.h"
#include "a-memory-library/aml_buffer.h"
#include "the-io-library/io_out.h"

#include "amr_internal.h"
#include "amr_debug.h"
#include "amr_worker.h"
#include "amr_runner.h"

#include <stdio.h>
#include <string.h>

/* --- Simple Diagnostic Printers --- */

static void print_state_link(amr_task_state_link_t *tasks) {
  amr_task_state_link_t *n = tasks;
  while (n) {
    printf(" %s", n->task->task_name);
    n = n->next;
  }
  printf("\n");
}

void amr_print(amr_t *h) {
  printf("%zu partitions\n", h->num_partitions);
  printf("%zu ram\n", h->ram);
  printf("%zu cpus\n", h->cpus);
  printf("%zu disk space\n", h->disk_space);

  for (size_t i = 0; i < h->num_partitions; i++) {
    printf("Completed[%zu]: ", i);
    print_state_link(h->state[i].completed_tasks);
    printf("Available[%zu]: ", i);
    print_state_link(h->state[i].available_tasks);
    printf("Waiting[%zu]: ", i);
    print_state_link(h->state[i].tasks_to_finish);
  }
}

static void print_task_link(const char *s, amr_task_link_t *tl) {
  if (!tl) return;
  printf("%s", s);
  while (tl) {
    printf(" %s[%zu]", tl->task->task_name, tl->task->num_partitions);
    tl = tl->next;
  }
  printf("\n");
}

/* --- Plan / Graph Output (--list) --- */

void list_selected_tasks(void *arg) {
  amr_thread_t *t = (amr_thread_t *)arg;
  t->pool = aml_pool_init(65536);
  t->bh = aml_buffer_init(200);
  aml_pool_t *tmp_pool = aml_pool_init(65536);
  aml_buffer_t *bh = aml_buffer_init(1024);
  bool show_files = t->scheduler->parsed_args.show_files;

  while (!t->scheduler->done) {
    aml_pool_clear(t->pool);
    amr_worker_t *w = get_next_worker(t);
    if (!w) break;

    w->worker_pool = t->pool;
    aml_pool_clear(tmp_pool);
    w->pool = tmp_pool;
    aml_buffer_clear(bh);
    w->bh = bh;

    get_ack_time(w);
    if ((w->task->scheduler->parsed_args.select_all ||
         (w->task->selected && w->task->selected[w->partition])) &&
        (show_files || w->partition == (w->num_partitions - 1))) {
      clone_inputs_and_outputs(w);
      fill_inputs(w);
      printf("task: %s [%zu/%zu]\n", w->task->task_name, w->partition,
             w->task->num_partitions);

      print_task_link("  dependencies: ", w->task->dependencies);
      print_task_link("  reverse dependencies: ", w->task->reverse_dependencies);
      print_task_link("  partial dependencies: ", w->task->partial_dependencies);
      print_task_link("  reverse partial dependencies: ", w->task->reverse_partial_dependencies);

      if (w->task->runner == in_out_runner) {
        size_t id = 0;
        printf("  in_out_runner\n");
        amr_transform_t *transforms = (amr_transform_t *)w->data;
        while (transforms) {
          printf("    transform: %zu\n", id);
          id++;
          size_t num_outs = transforms->num_outputs;
          size_t num_ins = transforms->num_inputs;
          for (size_t i = 0; i < num_ins; i++) {
            amr_worker_input_t *n = transforms->inputs[i];
            printf("      input[%zu]: %s (%zu)\n", i, n->name, n->num_files);
            if (show_files) {
              for (size_t j = 0; j < n->num_files; j++)
                printf("        %s (%zu)\n", n->files[j].filename, n->files[j].size);
            }
          }
          for (size_t i = 0; i < num_outs; i++) {
            amr_worker_output_t *n = transforms->outputs[i];
            printf("      output[%zu]: %s %s\n", i, n->name,
                   (n->flags & AMR_WRITE_SHUFFLE) ? "shuffle" : "all_to_all");
            if (show_files) {
              char *base_name = amr_worker_output_base(w, n);
              size_t num_partitions = 0;
              if ((n->flags & AMR_WRITE_SHUFFLE) && n->ext_options.partition) {
                num_partitions = n->ext_options.num_partitions
                               ? n->ext_options.num_partitions
                               : n->task->scheduler->num_partitions;
              }
              if (!num_partitions)
                printf("        %s\n", base_name);
              char *filename = (char *)aml_pool_alloc(w->pool, strlen(base_name) + 20);
              for (size_t j = 0; j < num_partitions; j++) {
                io_out_partition_filename(filename, base_name, j);
                printf("        %s\n", filename);
              }
            }
          }
          transforms = transforms->next;
        }
      } else {
        printf("  custom runner\n");
        {
          amr_worker_input_t *n = w->inputs;
          size_t i = 0;
          while (n) {
            printf("      input[%zu]: %s (%zu)\n", i, n->name, n->num_files);
            i++;
            if (show_files) {
              for (size_t j = 0; j < n->num_files; j++)
                printf("        %s (%zu)\n", n->files[j].filename, n->files[j].size);
            }
            n = n->next;
          }
        }
        {
          amr_worker_output_t *n = w->outputs;
          size_t i = 0;
          while (n) {
            printf("      output[%zu]: %s %s\n", i, n->name,
                   (n->flags & AMR_WRITE_SHUFFLE) ? "shuffle" : "all_to_all");
            if (show_files) {
              char *base_name = amr_worker_output_base(w, n);
              size_t num_partitions = 0;
              if ((n->flags & AMR_WRITE_SHUFFLE) && n->ext_options.partition) {
                num_partitions = n->ext_options.num_partitions
                               ? n->ext_options.num_partitions
                               : n->task->scheduler->num_partitions;
              }
              if (!num_partitions)
                printf("        %s\n", base_name);
              char *filename = (char *)aml_pool_alloc(w->pool, strlen(base_name) + 20);
              for (size_t j = 0; j < num_partitions; j++) {
                io_out_partition_filename(filename, base_name, j);
                printf("        %s\n", filename);
              }
            }
            n = n->next;
          }
        }
      }
    }
    worker_complete(w, time(NULL));
  }
  aml_pool_destroy(t->pool);
  aml_pool_destroy(tmp_pool);
  aml_buffer_destroy(bh);
  aml_buffer_destroy(t->bh);
  mark_as_done(t->scheduler);
}

/* --- Dump Tasks & Debug (--dump, --sample, --debug) --- */

void dump_selected_tasks(void *arg) {
  amr_thread_t *t = (amr_thread_t *)arg;
  t->pool = aml_pool_init(65536);
  t->bh = aml_buffer_init(200);
  aml_pool_t *tmp_pool = aml_pool_init(65536);
  aml_buffer_t *bh = aml_buffer_init(1024);
  char **files = t->scheduler->parsed_args.files;
  size_t num_files = 0;

  if (files) {
    for (size_t i = 0; files[i] != NULL; i++)
      num_files++;
  }

  /* Exit safely if running standard dump without files */
  if (!num_files && !t->scheduler->parsed_args.sample) {
    aml_pool_destroy(t->pool);
    aml_pool_destroy(tmp_pool);
    aml_buffer_destroy(bh);
    aml_buffer_destroy(t->bh);
    return;
  }

  if (t->scheduler->parsed_args.debug_task) {
    amr_worker_t *w = create_worker(t->pool, t->scheduler->parsed_args.debug_task,
                                    t->scheduler->parsed_args.debug_partition);
    w->running = 1;
    w->worker_pool = t->pool;
    w->pool = tmp_pool;
    w->bh = bh;
    w->schedule_thread = t;
    get_ack_time(w);
    clone_inputs_and_outputs(w);
    fill_inputs(w);

    if (t->scheduler->parsed_args.sample) {
      scan_output_sample(w);
    } else {
      scan_output(w, files, num_files);
    }
  } else {
    while (!t->scheduler->done) {
      aml_pool_clear(t->pool);
      amr_worker_t *w = get_next_worker(t);
      if (!w) break;

      w->worker_pool = t->pool;
      aml_pool_clear(tmp_pool);
      w->pool = tmp_pool;
      aml_buffer_clear(bh);
      w->bh = bh;

      get_ack_time(w);
      clone_inputs_and_outputs(w);
      fill_inputs(w);

      /* Route to the correct visualizer tool */
      if (t->scheduler->parsed_args.sample) {
        /* 1. Only sample if the task was explicitly requested via `-t` (or all are requested) */
        bool is_selected = t->scheduler->parsed_args.select_all ||
                           (w->task->selected && w->task->selected[w->partition]);

        /* 2. Only run the sample report ONCE per task (on partition 0) to avoid 16x spam */
        if (is_selected && w->partition == 0) {
          scan_output_sample(w);
        }
      } else {
        scan_output(w, files, num_files);
      }

      worker_complete(w, time(NULL));
    }
  }
  aml_pool_destroy(t->pool);
  aml_pool_destroy(tmp_pool);
  aml_buffer_destroy(bh);
  aml_buffer_destroy(t->bh);
  if (!t->scheduler->parsed_args.debug_task)
    mark_as_done(t->scheduler);
}

void debug_task(amr_thread_t *h) {
  aml_pool_t *pool = aml_pool_init(65536);
  h->bh = aml_buffer_init(200);
  aml_pool_t *tmp_pool = aml_pool_init(65536);
  aml_buffer_t *bh = aml_buffer_init(1024);
  amr_worker_t *w = create_worker(pool, h->scheduler->parsed_args.debug_task,
                                  h->scheduler->parsed_args.debug_partition);
  w->running = 1;
  w->worker_pool = pool;
  w->pool = tmp_pool;
  w->bh = bh;
  w->schedule_thread = h;
  get_ack_time(w);
  clone_inputs_and_outputs(w);
  fill_inputs(w);

  setup_worker(w);
  run_worker(w);
  destroy_worker(w);

  aml_pool_destroy(pool);
  aml_pool_destroy(tmp_pool);
  aml_buffer_destroy(h->bh);
  aml_buffer_destroy(w->bh);
}
