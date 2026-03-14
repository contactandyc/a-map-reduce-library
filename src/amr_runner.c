// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "a-map-reduce-library/amr.h"
#include "a-memory-library/aml_alloc.h"
#include "a-memory-library/aml_pool.h"
#include "a-memory-library/aml_buffer.h"
#include "the-io-library/io.h"
#include "the-macro-library/macro_time.h"

#include "amr_internal.h"
#include "amr_runner.h"
#include "amr_worker.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>

void amr_task_default_runner(amr_task_t *task) { task->runner = in_out_runner; }

void setup_worker(amr_worker_t *w) {}

void destroy_worker(amr_worker_t *w) {}

bool run_worker(amr_worker_t *w) {
  size_t n = 0;
  amr_worker_input_t *inp = w->inputs;
  while (inp) {
    if (inp->compare) {
      io_in_t *in = amr_worker_in(w, n);
      aml_buffer_t *bh = aml_buffer_init(1000);
      io_record_t cr;
      io_record_t *r;
      if ((r = io_in_advance(in)) != NULL) {
        aml_buffer_set(bh, r->record, r->length);
        cr = *r;
        cr.record = aml_buffer_data(bh);
        while ((r = io_in_advance(in)) != NULL) {
          if (inp->compare(&cr, r, inp->compare_arg) > 0) {
            printf("%s test failed!\n", inp->name);
            abort();
          }
          aml_buffer_set(bh, r->record, r->length);
          cr = *r;
          cr.record = aml_buffer_data(bh);
        }
      }
      io_in_destroy(in);
      aml_buffer_destroy(bh);
    }
    n++;
    inp = inp->next;
  }

  bool r = true;
  uint64_t start = macro_now();
  if (w->task->runner)
    r = w->task->runner(w);
  w->elapsed_ns = macro_now() - start;
  amr_allocs_t *a = w->schedule_thread->allocs;
  while (a) {
    aml_free(a->d);
    a = a->next;
  }
  w->schedule_thread->allocs = NULL;
  return r;
}

bool in_out_runner(amr_worker_t *w) {
  amr_transform_t *transforms = (amr_transform_t *)w->data;
  io_in_t *in = NULL;
  while (transforms) {
    size_t num_outs = transforms->num_outputs;
    size_t num_ins = transforms->num_inputs + (in ? 1 : 0);
    io_in_t **ins = NULL;
    amr_worker_input_t *inp = NULL;
    if (!num_ins) {
      ins = (io_in_t **)aml_pool_zalloc(w->worker_pool, sizeof(io_in_t *));
      ins[0] = io_in_empty();
    } else {
      ins = (io_in_t **)aml_pool_zalloc(w->worker_pool,
                                        sizeof(io_in_t *) * num_ins);
      size_t ibase = 0;
      if (in) {
        ins[0] = in;
        ibase = 1;
      } else
        inp = amr_worker_input(w, transforms->inputs[0]->id);
      for (size_t i = ibase; i < num_ins; i++)
        ins[i] = amr_worker_in(w, transforms->inputs[i - ibase]->id);
    }
    io_out_t **outs = (io_out_t **)aml_pool_zalloc(
        w->worker_pool, sizeof(io_out_t *) * num_outs);
    for (size_t i = 0; i < num_outs; i++) {
      amr_worker_output_t *out_def = transforms->outputs[i];
      // Check if the user wants an opaque/external artifact
      if (out_def->flags & AMR_OUTPUT_OPAQUE) {
        outs[i] = NULL;
      } else {
        outs[i] = amr_worker_out(w, out_def->id);
      }
    }

    if (transforms->create_data)
      w->transform_data = transforms->create_data(w);

    io_record_t *r;
    if (num_ins == 1) {
      if (transforms->runner) {
        while ((r = io_in_advance(ins[0])) != NULL) {
          aml_pool_clear(w->pool);
          transforms->runner(w, r, outs);
        }
      } else if (transforms->group_runner) {
        void *compare_arg = NULL;
        if (transforms->create_group_compare_arg)
          compare_arg = transforms->create_group_compare_arg(w);
        size_t num_r = 0;
        bool more_records = false;
        while ((r = io_in_advance_group(ins[0], &num_r, &more_records,
                                        transforms->group_compare,
                                        compare_arg)) != NULL) {
          aml_pool_clear(w->pool);
          transforms->group_runner(w, r, num_r, outs);
        }
        if (transforms->destroy_group_compare_arg)
          transforms->destroy_group_compare_arg(w, compare_arg);
      } else if (transforms->io_runner) {
        transforms->io_runner(w, ins, num_ins, outs, num_outs);
      } else {
        if (inp && inp->src && inp->src->dump &&
            w->task->scheduler->parsed_args.debug_dump_input) {
          aml_buffer_t *bh = aml_buffer_init(1000);
          while ((r = io_in_advance(ins[0])) != NULL) {
            aml_buffer_clear(bh);
            inp->src->dump(w, r, bh, inp->dump_arg);
            if (aml_buffer_length(bh))
              printf("%s\n", aml_buffer_data(bh));
            for (size_t i = 0; i < num_outs; i++)
              io_out_write_record(outs[i], r->record, r->length);
          }
          aml_buffer_destroy(bh);
        } else {
          while ((r = io_in_advance(ins[0])) != NULL) {
            for (size_t i = 0; i < num_outs; i++)
              io_out_write_record(outs[i], r->record, r->length);
          }
        }
      }
    } else {
      if (transforms->io_runner) {
        transforms->io_runner(w, ins, num_ins, outs, num_outs);
      } else if (transforms->group_runner) {
          void *compare_arg = NULL;
          if (transforms->create_group_compare_arg)
            compare_arg = transforms->create_group_compare_arg(w);

          io_in_options_t opts; io_in_options_init(&opts);
          amr_worker_input_t *inp0 = amr_worker_input(w, transforms->inputs[0]->id);
          if (inp0) opts = inp0->options;

          io_in_t *ext = io_in_ext_init(transforms->group_compare, compare_arg, &opts);

          for (size_t i = 0; i < num_ins; i++) {
            io_in_ext_add(ext, ins[i], 0);
            ins[i] = NULL;
          }

          io_record_t *g;
          size_t num_r = 0; bool more = false;
          while ((g = io_in_advance_group(ext, &num_r, &more,
                                          transforms->group_compare, compare_arg))) {
            aml_pool_clear(w->pool);
            transforms->group_runner(w, g, num_r, outs);
          }

          io_in_destroy(ext);
          if (transforms->destroy_group_compare_arg)
            transforms->destroy_group_compare_arg(w, compare_arg);
      }
      else {
          if (num_ins == 0) {
              fprintf(stderr, "[ERROR] Task '%s' has 0 inputs. Data generators must be defined using amr_task_io_transform(), not standard runners.\n", w->task->task_name);
          } else {
              fprintf(stderr, "[ERROR] Task '%s' has %zu inputs. Multi-input tasks require an io_runner or group_runner.\n", w->task->task_name, num_ins);
          }
          abort();
      }
    }
    for (size_t i = 0; i < num_ins; i++)
      if(ins[i]) io_in_destroy(ins[i]);
    in = NULL;
    if (num_outs > 0) {
      if (transforms->next) {
        // If chaining transforms, we can only pipe standard outputs
        if (outs[0]) {
          in = io_out_in(outs[0]);
        }
      } else {
        if (outs[0]) {
          io_out_destroy(outs[0]);
        }
      }
      for (size_t i = 1; i < num_outs; i++) {
        if (outs[i]) {
          io_out_destroy(outs[i]);
        }
      }
    }
    if (transforms->destroy_data)
      transforms->destroy_data(w, w->transform_data);

    transforms = transforms->next;
  }
  return true;
}

bool amr_worker_complete(amr_worker_t *w) {
  // 1. Get current time with microsecond precision
  struct timeval tv;
  gettimeofday(&tv, NULL);

  // 2. Convert to a thread-safe local time structure
  struct tm tm_info;
  localtime_r(&tv.tv_sec, &tm_info);

  // 3. Extract the milliseconds
  int millis = tv.tv_usec / 1000;

  if (w->worker_pool) {
    size_t used = aml_pool_used(w->worker_pool);
    size_t max_used  = aml_pool_max_used(w->worker_pool);

    fprintf(stderr, "%04d:%02d:%02dT%02d:%02d:%02d.%03d %s[%zu] %.3fms thread %zu pool %zu/%zu\n",
            tm_info.tm_year + 1900,
            tm_info.tm_mon + 1,
            tm_info.tm_mday,
            tm_info.tm_hour,
            tm_info.tm_min,
            tm_info.tm_sec,
            millis,
            amr_task_name(w->task),
            w->partition,
            w->elapsed_ns / 1.0e6,
            w->thread_id,
            used,
            max_used);
  } else {
    fprintf(stderr, "%04d:%02d:%02dT%02d:%02d:%02d.%03d %s[%zu] %.3fms thread %zu\n",
            tm_info.tm_year + 1900,
            tm_info.tm_mon + 1,
            tm_info.tm_mday,
            tm_info.tm_hour,
            tm_info.tm_min,
            tm_info.tm_sec,
            millis,
            amr_task_name(w->task),
            w->partition,
            w->elapsed_ns / 1.0e6,
            w->thread_id);
  }

  return true;
}
