// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include "a-map-reduce-library/amr.h"
#include "a-memory-library/aml_alloc.h"
#include "a-memory-library/aml_pool.h"
#include "a-memory-library/aml_buffer.h"
#include "the-io-library/io.h"

#include "amr_internal.h"
#include "amr_worker.h"
#include "amr_datatypes.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <fcntl.h>
#include <unistd.h>

/* --- Opaque Accessors --- */
aml_pool_t *amr_worker_pool(amr_worker_t *w) { return w->worker_pool; }
aml_pool_t *amr_worker_scratch_pool(amr_worker_t *w) { return w->pool; }
aml_buffer_t *amr_worker_buffer(amr_worker_t *w) { return w->bh; }

size_t amr_worker_partition(const amr_worker_t *w) { return w->partition; }
size_t amr_worker_num_partitions(const amr_worker_t *w) { return w->num_partitions; }
size_t amr_worker_thread_id(const amr_worker_t *w) { return w->thread_id; }

bool amr_worker_debug(const amr_worker_t *w) {
  return w->task->scheduler->parsed_args.debug_task ? true : false;
}

void *amr_worker_custom_arg(const amr_worker_t *w) {
  return w->task->scheduler->parse_args_arg;
}

amr_t *amr_worker_schedule(const amr_worker_t *w) {
  return w->task->scheduler;
}

void *amr_worker_transform_data(const amr_worker_t *w) {
  return w->transform_data;
}

void *amr_worker_data(const amr_worker_t *w) {
  return w->data;
}

/* --- Worker Input/Output Fetchers --- */
amr_worker_output_t *amr_worker_output(amr_worker_t *w, size_t pos) {
  amr_worker_output_t *r = w->outputs;
  while (r && pos) {
    pos--;
    r = r->next;
  }
  if (pos) return NULL;
  return r;
}

amr_worker_input_t *amr_worker_input(amr_worker_t *w, size_t pos) {
  amr_worker_input_t *r = w->inputs;
  while (r && pos) {
    pos--;
    r = r->next;
  }
  if (pos) return NULL;
  return r;
}

/* --- Core Worker Config & Limits --- */
size_t amr_worker_ram(amr_worker_t *w, double pct) {
  if (pct <= 0.0) pct = 1e-6;
  if (pct > 1.0)  pct = 1.0;
  double total = w->task->scheduler->ram;
  double running = w->running > 0 ? w->running : 1.0;
  return (size_t)round((total * pct) / running) * 1024;
}

/* --- Path Generation --- */

char *amr_worker_input_name_for_src(const amr_worker_t *w, const amr_worker_input_t *inp, const amr_worker_output_t *src, size_t partition) {
  const char *base = src->name;
  aml_buffer_t *bh = w->schedule_thread->bh;
  aml_buffer_setf(bh, "%s/%s_%zu/", w->task->scheduler->task_dir,
                  src->task->task_name, partition);

  if (src->flags & AMR_WRITE_SHUFFLE) {
    if (io_extension(base, "lz4")) {
      aml_buffer_append(bh, base, strlen(base) - 4);
      aml_buffer_appendf(bh, "_%zu_%zu.lz4", partition, w->partition);
    } else if (io_extension(base, "gz")) {
      aml_buffer_append(bh, base, strlen(base) - 3);
      aml_buffer_appendf(bh, "_%zu_%zu.gz", partition, w->partition);
    } else {
      aml_buffer_appends(bh, base);
      aml_buffer_appendf(bh, "_%zu_%zu", partition, w->partition);
    }
  } else {
    if (io_extension(base, "lz4")) {
      aml_buffer_append(bh, base, strlen(base) - 4);
      aml_buffer_appendf(bh, "_%zu.lz4", partition);
    } else if (io_extension(base, "gz")) {
      aml_buffer_append(bh, base, strlen(base) - 3);
      aml_buffer_appendf(bh, "_%zu.gz", partition);
    } else {
      aml_buffer_appends(bh, base);
      aml_buffer_appendf(bh, "_%zu", partition);
    }
  }
  return aml_pool_strdup(w->worker_pool, aml_buffer_data(bh));
}

char *amr_worker_input_name(const amr_worker_t *w, const amr_worker_input_t *inp, size_t partition) {
  if (!inp || inp->num_srcs == 0) return NULL;
  return amr_worker_input_name_for_src(w, inp, inp->srcs[0], partition);
}

/* --- File Info Strategies --- */

io_file_info_t *file_info_shuffle(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp) {
  size_t valid_count = 0;
  for (size_t i = 0; i < inp->num_srcs; i++) {
    size_t num_partitions = inp->srcs[i]->task->num_partitions;
    for (size_t p = 0; p < num_partitions; p++) {
      amr_task_state_link_t *st = &inp->srcs[i]->task->state_linkage[p];
      if (!st->skipped && !(st->skipped_outputs_mask & (1ULL << inp->srcs[i]->id))) valid_count++;
    }
  }

  *num_files = valid_count;
  if (valid_count == 0) return NULL;

  io_file_info_t *res = (io_file_info_t *)aml_pool_zalloc(w->worker_pool, sizeof(*res) * valid_count);
  size_t idx = 0;
  for (size_t i = 0; i < inp->num_srcs; i++) {
    size_t num_partitions = inp->srcs[i]->task->num_partitions;
    for (size_t p = 0; p < num_partitions; p++) {
      amr_task_state_link_t *st = &inp->srcs[i]->task->state_linkage[p];
      if (!st->skipped && !(st->skipped_outputs_mask & (1ULL << inp->srcs[i]->id))) {
        res[idx].filename = amr_worker_input_name_for_src(w, inp, inp->srcs[i], p);
        res[idx].tag = p;
        io_file_info(&res[idx]);
        idx++;
      }
    }
  }
  return res;
}

io_file_info_t *file_info_all_to_all(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp) {
  size_t valid_count = 0;
  for (size_t i = 0; i < inp->num_srcs; i++) {
    size_t num_partitions = inp->srcs[i]->task->num_partitions;
    for (size_t p = 0; p < num_partitions; p++) {
      amr_task_state_link_t *st = &inp->srcs[i]->task->state_linkage[p];
      if (!st->skipped && !(st->skipped_outputs_mask & (1ULL << inp->srcs[i]->id))) valid_count++;
    }
  }

  *num_files = valid_count;
  if (valid_count == 0) return NULL;

  io_file_info_t *res = (io_file_info_t *)aml_pool_zalloc(w->worker_pool, sizeof(*res) * valid_count);
  size_t idx = 0;
  for (size_t i = 0; i < inp->num_srcs; i++) {
    size_t num_partitions = inp->srcs[i]->task->num_partitions;
    for (size_t p = 0; p < num_partitions; p++) {
      amr_task_state_link_t *st = &inp->srcs[i]->task->state_linkage[p];
      if (!st->skipped && !(st->skipped_outputs_mask & (1ULL << inp->srcs[i]->id))) {
        res[idx].filename = amr_worker_input_name_for_src(w, inp, inp->srcs[i], p);
        res[idx].tag = p;
        io_file_info(&res[idx]);
        idx++;
      }
    }
  }
  return res;
}

io_file_info_t *file_info_first(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp) {
  size_t valid_count = 0;
  for (size_t i = 0; i < inp->num_srcs; i++) {
    amr_task_state_link_t *st = &inp->srcs[i]->task->state_linkage[0];
    if (!st->skipped && !(st->skipped_outputs_mask & (1ULL << inp->srcs[i]->id))) valid_count++;
  }

  *num_files = valid_count;
  if (valid_count == 0) return NULL;

  io_file_info_t *res = (io_file_info_t *)aml_pool_zalloc(w->worker_pool, sizeof(*res) * valid_count);
  size_t idx = 0;
  for (size_t i = 0; i < inp->num_srcs; i++) {
    amr_task_state_link_t *st = &inp->srcs[i]->task->state_linkage[0];
    if (!st->skipped && !(st->skipped_outputs_mask & (1ULL << inp->srcs[i]->id))) {
        res[idx].filename = amr_worker_input_name_for_src(w, inp, inp->srcs[i], 0);
        res[idx].tag = 0;
        io_file_info(&res[idx]);
        idx++;
    }
  }
  return res;
}

io_file_info_t *file_info_partition(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp) {
  size_t valid_count = 0;
  for (size_t i = 0; i < inp->num_srcs; i++) {
    size_t src_part = w->partition < inp->srcs[i]->task->num_partitions ? w->partition : 0;
    amr_task_state_link_t *st = &inp->srcs[i]->task->state_linkage[src_part];
    if (!st->skipped && !(st->skipped_outputs_mask & (1ULL << inp->srcs[i]->id))) {
        valid_count++;
    }
  }

  *num_files = valid_count;
  if (valid_count == 0) return NULL;

  io_file_info_t *res = (io_file_info_t *)aml_pool_zalloc(w->worker_pool, sizeof(*res) * valid_count);
  size_t idx = 0;
  for (size_t i = 0; i < inp->num_srcs; i++) {
    size_t src_part = w->partition < inp->srcs[i]->task->num_partitions ? w->partition : 0;
    amr_task_state_link_t *st = &inp->srcs[i]->task->state_linkage[src_part];
    if (!st->skipped && !(st->skipped_outputs_mask & (1ULL << inp->srcs[i]->id))) {
        res[idx].filename = amr_worker_input_name_for_src(w, inp, inp->srcs[i], src_part);
        res[idx].tag = w->partition;
        io_file_info(&res[idx]);
        idx++;
    }
  }
  return res;
}

io_file_info_t *file_info_name(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp) {
  io_file_info_t *res = (io_file_info_t *)aml_pool_zalloc(w->worker_pool, sizeof(*res));
  res->filename = inp->name;
  res->tag = 0;
  *num_files = 1;
  io_file_info(res);
  return res;
}

io_file_info_t *file_info_prev_run_first(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp) {
  if (w->task->scheduler->parsed_args.run_number == 0) {
    *num_files = 0; return NULL;
  }
  io_file_info_t *res = (io_file_info_t *)aml_pool_zalloc(w->worker_pool, sizeof(*res));
  res->filename = amr_run_file_path(w->task->scheduler, inp->prev_run, inp->prev_task_name, 0, inp->name);
  res->tag = 0;
  *num_files = 1;
  io_file_info(res);
  return res;
}

io_file_info_t *file_info_prev_run_partition(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp) {
  if (w->task->scheduler->parsed_args.run_number == 0) {
    *num_files = 0; return NULL;
  }
  io_file_info_t *res = (io_file_info_t *)aml_pool_zalloc(w->worker_pool, sizeof(*res));
  res->filename = amr_run_file_path(w->task->scheduler, inp->prev_run, inp->prev_task_name, w->partition, inp->name);
  res->tag = w->partition;
  *num_files = 1;
  io_file_info(res);
  return res;
}

io_file_info_t *file_info_prev_run_all_to_all(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp) {
  if (w->task->scheduler->parsed_args.run_number == 0) {
    *num_files = 0; return NULL;
  }
  size_t num_partitions = w->task->scheduler->num_partitions;
  io_file_info_t *res = (io_file_info_t *)aml_pool_zalloc(w->worker_pool, sizeof(*res) * num_partitions);
  for (size_t i = 0; i < num_partitions; i++) {
    res[i].filename = amr_run_file_path(w->task->scheduler, inp->prev_run, inp->prev_task_name, i, inp->name);
    res[i].tag = i;
    io_file_info(res + i);
  }
  *num_files = num_partitions;
  return res;
}

static char *amr_prev_run_shuffle_path(amr_worker_t *w, amr_worker_input_t *inp, size_t prod_partition) {
  aml_buffer_t *bh = w->schedule_thread->bh;
  aml_buffer_setf(bh, "%s/run_%zu/%s_%zu/",
                  w->task->scheduler->workspace_dir, inp->prev_run,
                  inp->prev_task_name, prod_partition);
  const char *base = inp->name;
  if (io_extension(base, "lz4")) {
    aml_buffer_append(bh, base, strlen(base) - 4);
    aml_buffer_appendf(bh, "_%zu_%zu.lz4", prod_partition, w->partition);
  } else if (io_extension(base, "gz")) {
    aml_buffer_append(bh, base, strlen(base) - 3);
    aml_buffer_appendf(bh, "_%zu_%zu.gz", prod_partition, w->partition);
  } else {
    aml_buffer_appends(bh, base);
    aml_buffer_appendf(bh, "_%zu_%zu", prod_partition, w->partition);
  }
  return aml_pool_strdup(w->worker_pool, aml_buffer_data(bh));
}

io_file_info_t *file_info_prev_run_shuffle(amr_worker_t *w, size_t *num_files, amr_worker_input_t *inp) {
  if (w->task->scheduler->parsed_args.run_number == 0) {
    *num_files = 0; return NULL;
  }
  size_t num_partitions = w->task->scheduler->num_partitions;
  io_file_info_t *res = (io_file_info_t *)aml_pool_zalloc(w->worker_pool, sizeof(*res) * num_partitions);
  for (size_t i = 0; i < num_partitions; i++) {
    res[i].filename = amr_prev_run_shuffle_path(w, inp, i);
    res[i].tag = i;
    io_file_info(res + i);
  }
  *num_files = num_partitions;
  return res;
}

/* --- I/O Instantiation --- */

io_out_t *amr_worker_out(amr_worker_t *w, size_t n) {
  amr_worker_output_t *o = amr_worker_output(w, n);
  if (!o) return NULL;

  io_out_options_buffer_size(&(o->options), amr_worker_ram(w, o->ram_pct));

  if (o->flags & AMR_WRITE_SHUFFLE) {
    if (!o->ext_options.partition) {
      char *base_name = amr_worker_output_base(w, o);
      printf("%s from %s is configured\n  to be shuffled, but does not specify a "
             "partition method!  Exiting early!\n",
             base_name, w->task->task_name);
      abort();
    }
    if (o->ext_options.num_partitions == 0)
      io_out_ext_options_num_partitions(&(o->ext_options),
                                        w->task->scheduler->num_partitions);
    return io_out_ext_init(amr_worker_output_base(w, o),
                           &(o->options), &(o->ext_options));
  }

  o->ext_options.partition = NULL;
  return io_out_ext_init(amr_worker_output_base(w, o),
                         &(o->options), &(o->ext_options));
}

int amr_worker_out_as_file(amr_worker_t *w, size_t n) {
  amr_worker_output_t *outp = amr_worker_output(w, n);
  if (!outp) return -1;

  char *filename = amr_worker_output_base(w, outp);
  if (!filename) return -1;

  // Ask the I/O layer to ensure all parent directories exist
  io_make_path_valid(filename);

  return open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
}

io_in_t *amr_worker_in(amr_worker_t *w, size_t n) {
  amr_worker_input_t *inp = amr_worker_input(w, n);
  if (!inp || !inp->num_files) return NULL;

  /* BYPASS IO_IN_T FOR CUSTOM ARTIFACTS OR MEMORY LOADS */
  if (inp->edge_flags & AMR_INPUT_OPAQUE) return NULL;
  if (inp->load_into_memory) return NULL;

  io_in_t *in = NULL;
  if (inp->compare && inp->num_files > 1) {
    io_in_options_buffer_size(&(inp->options),
                              amr_worker_ram(w, inp->ram_pct / inp->num_files));
    in = io_in_ext_init(inp->compare, inp->compare_arg, &(inp->options));
    if (inp->reducer)
      io_in_ext_reducer(in, inp->reducer, inp->reducer_arg);
    for (size_t i = 0; i < inp->num_files; i++)
      io_in_ext_add(in, io_in_init(inp->files[i].filename, &(inp->options)),
                    inp->files[i].tag);
  } else {
    io_in_options_buffer_size(&(inp->options), amr_worker_ram(w, inp->ram_pct));
    if (inp->num_files > 1)
      in = io_in_init_from_list(inp->files, inp->num_files, &(inp->options));
    else {
      io_in_options_t opts = inp->options;
      if (opts.buffer_size > inp->files[0].size)
        opts.buffer_size = inp->files[0].size;
      opts.tag = inp->files[0].tag;
      in = io_in_init(inp->files[0].filename, &opts);
    }
  }
  if (inp->limit) io_in_limit(in, inp->limit);
  return in;
}

static void add_worker_allocation(amr_worker_t *w, void *d, size_t len) {
  amr_allocs_t *a =
      (amr_allocs_t *)aml_pool_alloc(w->worker_pool, sizeof(*a));
  a->d = d;
  a->length = len;
  a->next = w->schedule_thread->allocs;
  w->schedule_thread->allocs = a;
}

char *amr_worker_read(amr_worker_t *w, size_t *num_records, char **endp, size_t pos) {
  *num_records = 0;
  amr_worker_input_t *inp = amr_worker_input(w, pos);
  if (!inp || inp->num_files != 1) return NULL;

  size_t len = 0;
  char *buf = io_read_file(&len, inp->files[0].filename);
  if (!buf) return NULL;

  if (endp) *endp = buf + len;

  if (inp->options.format == 0) {
    char *p = buf, *ep = buf + len;
    size_t num = 0;
    while (p < ep) {
      if ((size_t)(ep - p) < sizeof(uint32_t)) break;
      uint32_t l = *(uint32_t *)p;
      p += sizeof(uint32_t);
      if ((size_t)(ep - p) < l) break;
      p += l;
      num++;
    }
    *num_records = num;
  } else if (inp->options.format < 0) {
    char delim = (char)(-inp->options.format - 1);
    size_t num = 0;
    for (size_t i = 0; i < len; i++)
      if (buf[i] == delim) num++;
    if (len && buf[len-1] != delim) num++;
    *num_records = num;
  } else {
    *num_records = len / inp->options.format;
  }

  add_worker_allocation(w, buf, len);
  return buf;
}

amr_loaded_data_t *amr_worker_loaded_data(amr_worker_t *w, size_t n, size_t *num_data) {
  amr_worker_input_t *inp = amr_worker_input(w, n);

  if (!inp || !inp->load_into_memory) {
    if (num_data) *num_data = 0;
    return NULL;
  }

  // Lazy load: Only read the files if we haven't already done it!
  if (!inp->loaded_data && inp->num_files > 0) {
    inp->num_loaded_data = inp->num_files;
    inp->loaded_data = (amr_loaded_data_t *)aml_pool_zalloc(w->worker_pool, sizeof(amr_loaded_data_t) * inp->num_files);

    for (size_t i = 0; i < inp->num_files; i++) {
      size_t len = 0;

      // NEW: Read directly into the worker's persistent pool!
      char *buf = io_pool_read_file(w->worker_pool, &len, inp->files[i].filename);

      if (buf) {
        // No manual add_worker_allocation tracking needed anymore!
        inp->loaded_data[i].buffer = buf;
        inp->loaded_data[i].length = len;
        inp->loaded_data[i].partition = inp->files[i].tag; // Preserves the source partition!
      }
    }
  }

  if (num_data) *num_data = inp->num_loaded_data;
  return inp->loaded_data;
}

amr_loaded_data_t *amr_worker_loaded_data_for_partition(amr_worker_t *w, size_t n, size_t partition) {
  size_t num_data = 0;
  amr_loaded_data_t *data = amr_worker_loaded_data(w, n, &num_data);

  if (!data) return NULL;

  for (size_t i = 0; i < num_data; i++) {
    if (data[i].partition == partition) {
      return &data[i];
    }
  }

  return NULL;
}


static char *_amr_worker_output_base(const amr_worker_t *w, const amr_worker_output_t *outp,
                                     const char *suffix) {
  if (!outp) return NULL;
  aml_buffer_t *bh = w->schedule_thread->bh;
  if (w->task->scheduler->parsed_args.debug_path) {
    aml_buffer_sets(bh, w->task->scheduler->parsed_args.debug_path);
    if (aml_buffer_length(bh) > 0 &&
        aml_buffer_data(bh)[aml_buffer_length(bh) - 1] != '/')
      aml_buffer_appendc(bh, '/');
    if (aml_buffer_length(bh) == 0) {
      printf("[ERROR] Debug path seems to be invalid!\n");
      abort();
    }
  } else {
    aml_buffer_setf(bh, "%s/%s_%zu/", w->task->scheduler->task_dir,
                    w->task->task_name, w->partition);
  }
  const char *base = outp->name;
  if (io_extension(base, "lz4")) {
    aml_buffer_append(bh, base, strlen(base) - 4);
    if (suffix) aml_buffer_appends(bh, suffix);
    aml_buffer_appendf(bh, "_%zu.lz4", w->partition);
  } else if (io_extension(base, "gz")) {
    aml_buffer_append(bh, base, strlen(base) - 3);
    if (suffix) aml_buffer_appends(bh, suffix);
    aml_buffer_appendf(bh, "_%zu.gz", w->partition);
  } else {
    aml_buffer_appends(bh, base);
    if (suffix) aml_buffer_appends(bh, suffix);
    aml_buffer_appendf(bh, "_%zu", w->partition);
  }
  return aml_pool_strdup(w->worker_pool, aml_buffer_data(bh));
}

char *amr_worker_output_base(const amr_worker_t *w, const amr_worker_output_t *outp) {
  return _amr_worker_output_base(w, outp, NULL);
}

char *amr_worker_output_base2(const amr_worker_t *w, const amr_worker_output_t *outp, const char *suffix) {
  return _amr_worker_output_base(w, outp, suffix);
}

char *amr_worker_input_params(amr_worker_t *w, size_t n) {
  amr_worker_input_t *inp = amr_worker_input(w, n);
  if (!inp) return (char *)"";
  if (inp->num_files == 1) return inp->files[0].filename;

  size_t len = 1;
  for (size_t i = 0; i < inp->num_files; i++) {
    len += strlen(inp->files[i].filename) + 1;
  }
  char *res = (char *)aml_pool_alloc(w->pool, len);
  char *p = res;
  for (size_t i = 0; i < inp->num_files; i++) {
    strcpy(p, inp->files[i].filename);
    p += strlen(inp->files[i].filename);
    if (i + 1 < inp->num_files) *p++ = ' ';
  }
  return res;
}

char *amr_worker_output_params(amr_worker_t *w, size_t n) {
  amr_worker_output_t *outp = amr_worker_output(w, n);
  if (!outp) return NULL;

  char *base = amr_worker_output_base(w, outp);
  if (outp->flags & AMR_WRITE_SHUFFLE) {
    size_t np = outp->ext_options.num_partitions
              ? outp->ext_options.num_partitions
              : w->task->scheduler->num_partitions;
    return aml_pool_strdupf(w->pool, "%s %zu", base, np);
  }
  return base;
}

/* --- Internal Lifecycle & Utilities --- */

void fill_inputs(amr_worker_t *w) {
  if (w->inputs) {
    amr_worker_input_t *n = w->inputs;
    while (n) {
      n->num_files = 0;
      n->files = n->file_info(w, &(n->num_files), n);
      n = n->next;
    }
  }
}

amr_transform_t *clone_transforms(amr_worker_t *w) {
  amr_transform_t *head = NULL;
  amr_transform_t *tail = NULL;
  amr_transform_t *n = w->task->transforms;

  while (n) {
    if (!head)
      head = tail = (amr_transform_t *)aml_pool_dup(w->worker_pool, n, sizeof(*n));
    else {
      tail->next = (amr_transform_t *)aml_pool_dup(w->worker_pool, n, sizeof(*n));
      tail = tail->next;
    }

    /* 1. Clone External Inputs (Safe against NULLs) */
    if (n->inputs) {
      tail->inputs = (amr_worker_input_t **)aml_pool_zalloc(
          w->worker_pool, sizeof(amr_worker_input_t *) * tail->num_inputs);
      for (size_t i = 0; i < tail->num_inputs; i++) {
        if (n->inputs[i]) {
            tail->inputs[i] = amr_worker_input(w, n->inputs[i]->id);
        }
      }
    }

    /* 2. Clone Internal Pipes (NEW) */
    if (n->internal_inputs) {
      tail->internal_inputs = (amr_worker_output_t **)aml_pool_zalloc(
          w->worker_pool, sizeof(amr_worker_output_t *) * tail->num_inputs);
      for (size_t i = 0; i < tail->num_inputs; i++) {
        if (n->internal_inputs[i]) {
            tail->internal_inputs[i] = amr_worker_output(w, n->internal_inputs[i]->id);
        }
      }
    }

    /* 3. Clone Outputs */
    if (n->outputs) {
      tail->outputs = (amr_worker_output_t **)aml_pool_zalloc(
          w->worker_pool, sizeof(amr_worker_output_t *) * tail->num_outputs);
      for (size_t i = 0; i < tail->num_outputs; i++) {
        if (n->outputs[i]) {
            tail->outputs[i] = amr_worker_output(w, n->outputs[i]->id);
        }
      }
    }

    tail->next = NULL;
    n = n->next;
  }
  return head;
}


void clone_inputs_and_outputs(amr_worker_t *w) {
  amr_worker_input_t *i_head = NULL;
  amr_worker_input_t *i_tail = NULL;
  amr_worker_input_t *i_n = w->task->inputs;
  while (i_n) {
    if (!i_head)
      i_head = i_tail =
          (amr_worker_input_t *)aml_pool_dup(w->worker_pool, i_n, sizeof(*i_n));
    else {
      i_tail->next =
          (amr_worker_input_t *)aml_pool_dup(w->worker_pool, i_n, sizeof(*i_n));
      i_tail = i_tail->next;
    }
    i_tail->next = NULL;
    i_n = i_n->next;
  }
  w->inputs = i_head;

  amr_worker_output_t *o_head = NULL;
  amr_worker_output_t *o_tail = NULL;
  amr_worker_output_t *o_n = w->task->outputs;
  while (o_n) {
    if (!o_head)
      o_head = o_tail =
          (amr_worker_output_t *)aml_pool_dup(w->worker_pool, o_n, sizeof(*o_n));
    else {
      o_tail->next =
          (amr_worker_output_t *)aml_pool_dup(w->worker_pool, o_n, sizeof(*o_n));
      o_tail = o_tail->next;
    }
    o_tail->next = NULL;
    o_n = o_n->next;
  }
  w->outputs = o_head;

  w->data = clone_transforms(w);
}

/* --- Diagnostic and Scanning Support --- */

static bool match_filename(const char *query, const char *path) {
  if (!strcmp(query, path)) return true;
  if (strstr(path, query) != NULL) return true;
  return false;
}

static void dump_file(amr_worker_t *w, const char *filename,
                      io_format_t format, amr_task_dump_cb dump,
                      void *dump_arg) {
  io_in_options_t opts;
  io_in_options_init(&opts);
  io_in_options_format(&opts, format);
  io_in_t *in = io_in_init(filename, &opts);
  if (!in) return;

  io_record_t *r;
  aml_buffer_t *bh = aml_buffer_init(1000);
  size_t record_index = 0;
  bool print_extra = w->task->scheduler->parsed_args.prefix;
  const char *match = w->task->scheduler->parsed_args.sample_match;
  const char *printed_name = filename;
  const char *slash = strrchr(filename, '/');
  if (slash) printed_name = slash + 1;

  while ((r = io_in_advance(in)) != NULL) {
    aml_buffer_clear(bh);
    dump(w, r, bh, dump_arg);
    aml_buffer_appendc(bh, '\0');

    if (match != NULL) {
        if (strstr(aml_buffer_data(bh), match) == NULL) {
            record_index++;
            continue;
        }
    }

    if (print_extra) {
      printf("%s:%zu\t", printed_name, record_index);
    } else {
      printf("[%zu] ", record_index);
    }

    printf("%s\n", aml_buffer_data(bh));
    record_index++;
  }
  aml_buffer_destroy(bh);
  io_in_destroy(in);
}

void scan_output(amr_worker_t *w, char **files, size_t num_files) {
  amr_worker_input_t *inp = w->inputs;
  while (inp) {
    if (inp->dump) {
      for (size_t i = 0; i < num_files; i++) {
        if (!files[i]) continue;
        for (size_t j = 0; j < inp->num_files; j++) {
          if (match_filename(files[i], inp->files[j].filename)) {
            aml_pool_clear(w->pool);
            dump_file(w, inp->files[j].filename, inp->options.format, inp->dump, inp->dump_arg);
          }
        }
      }
    }
    inp = inp->next;
  }

  amr_worker_output_t *o = w->outputs;
  while (o) {
    if (o->dump) {
      char *base_name = amr_worker_output_base(w, o);
      size_t num_partitions = 0;
      if ((o->flags & AMR_WRITE_SHUFFLE) && o->ext_options.partition) {
          num_partitions = o->ext_options.num_partitions
                         ? o->ext_options.num_partitions
                         : o->task->scheduler->num_partitions;
      }
      for (size_t i = 0; i < num_files; i++) {
        if (!files[i]) continue;
        if (!num_partitions) {
          if (match_filename(files[i], base_name)) {
            aml_pool_clear(w->pool);
            dump_file(w, base_name, o->options.format, o->dump, o->dump_arg);
          }
        } else {
          for (size_t j = 0; j < num_partitions; j++) {
            aml_pool_clear(w->pool);
            char *filename = (char *)aml_pool_alloc(w->pool, strlen(base_name) + 20);
            io_out_partition_filename(filename, base_name, j);
            if (match_filename(files[i], filename)) {
              dump_file(w, filename, o->options.format, o->dump, o->dump_arg);
            }
          }
        }
      }
    }
    o = o->next;
  }
}

void amr_worker_dump_input(amr_worker_t *w, io_record_t *r, size_t n) {
  amr_worker_input_t *inp = amr_worker_input(w, n);
  if (inp && inp->dump) {
    aml_buffer_clear(w->bh);
    inp->dump(w, r, w->bh, inp->dump_arg);
    printf("%s\n", aml_buffer_data(w->bh));
  } else if (inp && inp->num_srcs > 0 && inp->srcs[0] && inp->srcs[0]->dump) {
    aml_buffer_clear(w->bh);
    inp->srcs[0]->dump(w, r, w->bh, inp->srcs[0]->dump_arg);
    printf("%s\n", aml_buffer_data(w->bh));
  }
}

char **amr_worker_opaque_inputs(amr_worker_t *w, size_t n, size_t *num_paths) {
  amr_worker_input_t *inp = amr_worker_input(w, n);
  if (!inp) {
    *num_paths = 0;
    return NULL;
  }

  *num_paths = inp->num_files;
  char **paths = (char **)aml_pool_alloc(w->worker_pool, sizeof(char *) * inp->num_files);
  for (size_t i = 0; i < inp->num_files; i++) {
    paths[i] = inp->files[i].filename;
  }
  return paths;
}

char **amr_worker_opaque_outputs(amr_worker_t *w, size_t n, size_t *num_paths) {
  amr_worker_output_t *outp = amr_worker_output(w, n);
  if (!outp) {
    *num_paths = 0;
    return NULL;
  }

  char *base = amr_worker_output_base(w, outp);

  if (outp->flags & AMR_WRITE_SHUFFLE) {
    size_t np = outp->ext_options.num_partitions
                ? outp->ext_options.num_partitions
                : w->task->scheduler->num_partitions;
    *num_paths = np;
    char **paths = (char **)aml_pool_alloc(w->worker_pool, sizeof(char *) * np);
    for (size_t i = 0; i < np; i++) {
      paths[i] = aml_pool_strdupf(w->worker_pool, "%s_%zu", base, i);
    }
    return paths;
  } else {
    *num_paths = 1;
    char **paths = (char **)aml_pool_alloc(w->worker_pool, sizeof(char *));
    paths[0] = base;
    return paths;
  }
}

FILE **amr_worker_open_opaque_inputs(amr_worker_t *w, size_t n, const char *mode, size_t *num_files) {
  char **paths = amr_worker_opaque_inputs(w, n, num_files);
  if (!paths || !*num_files) return NULL;

  FILE **files = (FILE **)aml_pool_alloc(w->worker_pool, sizeof(FILE *) * (*num_files));
  for (size_t i = 0; i < *num_files; i++) {
    files[i] = fopen(paths[i], mode);
  }
  return files;
}

FILE **amr_worker_open_opaque_outputs(amr_worker_t *w, size_t n, const char *mode, size_t *num_files) {
  char **paths = amr_worker_opaque_outputs(w, n, num_files);
  if (!paths || !*num_files) return NULL;

  FILE **files = (FILE **)aml_pool_alloc(w->worker_pool, sizeof(FILE *) * (*num_files));
  for (size_t i = 0; i < *num_files; i++) {
    files[i] = fopen(paths[i], mode);
  }
  return files;
}

void amr_worker_close_opaque_files(FILE **files, size_t num_files) {
  if (!files) return;
  for (size_t i = 0; i < num_files; i++) {
    if (files[i]) {
      fclose(files[i]);
    }
  }
}

static void dump_file_limited(amr_worker_t *w, const char *filename,
                              io_format_t format, amr_task_dump_cb dump,
                              void *dump_arg, size_t limit) {
  io_in_options_t opts;
  io_in_options_init(&opts);
  io_in_options_format(&opts, format);
  io_in_t *in = io_in_init(filename, &opts);
  if (!in) return;

  io_record_t *r;
  aml_buffer_t *bh = aml_buffer_init(1000);
  size_t count = 0;
  size_t record_index = 0;
  const char *match = w->task->scheduler->parsed_args.sample_match;

  while (count < limit && (r = io_in_advance(in)) != NULL) {
    aml_buffer_clear(bh);
    dump(w, r, bh, dump_arg);
    aml_buffer_appendc(bh, '\0');

    if (match != NULL) {
        if (strstr(aml_buffer_data(bh), match) == NULL) {
            record_index++;
            continue;
        }
    }

    printf("%s\n", aml_buffer_data(bh));
    count++;
    record_index++;
  }
  aml_buffer_destroy(bh);
  io_in_destroy(in);
}

void scan_output_sample(amr_worker_t *w) {
  amr_worker_output_t *o = w->outputs;
  size_t sample_recs = w->task->scheduler->parsed_args.sample_records;
  size_t sample_parts = w->task->scheduler->parsed_args.sample_partitions;

  static bool seeded = false;
  if (!seeded) {
    srand(time(NULL));
    seeded = true;
  }

  while (o) {
    if (!o->dump) {
      o = o->next;
      continue;
    }

    size_t M = o->task->num_partitions;
    size_t N = o->ext_options.num_partitions ? o->ext_options.num_partitions : w->task->scheduler->num_partitions;

    bool *is_empty = aml_pool_zalloc(w->pool, M * sizeof(bool));
    size_t *active_indices = aml_pool_alloc(w->pool, M * sizeof(size_t));
    size_t num_active = 0;

    for (size_t i = 0; i < M; i++) {
      aml_buffer_t *bh = w->schedule_thread->bh;

      const char *base = o->name;
      char *name_i;

      if (io_extension(base, "lz4"))
        name_i = aml_pool_strdupf(w->pool, "%.*s_%zu.lz4", (int)strlen(base)-4, base, i);
      else if (io_extension(base, "gz"))
        name_i = aml_pool_strdupf(w->pool, "%.*s_%zu.gz", (int)strlen(base)-3, base, i);
      else
        name_i = aml_pool_strdupf(w->pool, "%s_%zu", base, i);

      aml_buffer_setf(bh, "%s/%s_%zu/%s", w->task->scheduler->task_dir, w->task->task_name, i, name_i);
      char *path = aml_pool_strdup(w->pool, aml_buffer_data(bh));

      if (o->flags & AMR_WRITE_SHUFFLE) {
        path = aml_pool_alloc(w->pool, strlen(path) + 20);
        io_out_partition_filename(path, aml_buffer_data(bh), i % N);
      }

      io_in_options_t opts;
      io_in_options_init(&opts);
      io_in_options_format(&opts, o->options.format);
      io_in_t *in = io_in_init(path, &opts);

      if (!in || io_in_advance(in) == NULL) {
        is_empty[i] = true;
      } else {
        active_indices[num_active++] = i;
      }
      if (in) io_in_destroy(in);
    }

    aml_buffer_t *ranges = aml_buffer_init(256);
    bool first = true;
    size_t s = 0;
    while (s < M) {
      if (!is_empty[s]) { s++; continue; }
      size_t e = s;
      while (e + 1 < M && is_empty[e + 1]) e++;
      if (!first) aml_buffer_appendc(ranges, ',');
      if (s == e) aml_buffer_appendf(ranges, "%zu", s);
      else aml_buffer_appendf(ranges, "%zu-%zu", s, e);
      first = false;
      s = e + 1;
    }

    printf("\n============================================================\n");
    printf("TASK: %-15s | OUTPUT: %s\n", w->task->task_name, o->name);
    printf("------------------------------------------------------------\n");
    printf("HEALTH: %zu partitions. %zu have 0 records.\n", M, M - num_active);
    if (M > num_active) {
      printf("EMPTY RECORD PARTITIONS: %s\n", (char *)aml_buffer_data(ranges));
    }
    aml_buffer_destroy(ranges);

    if (num_active > 0) {
      size_t to_sample = (sample_parts < num_active) ? sample_parts : num_active;
      printf("SAMPLING: %zu records from %zu random active partitions...\n\n", sample_recs, to_sample);

      for (size_t i = num_active - 1; i > 0; i--) {
        size_t j = rand() % (i + 1);
        size_t t = active_indices[i];
        active_indices[i] = active_indices[j];
        active_indices[j] = t;
      }

      for (size_t i = 0; i < to_sample; i++) {
        size_t idx = active_indices[i];
        aml_buffer_t *bh = w->schedule_thread->bh;

        const char *base = o->name;
        char *name_idx;
        if (io_extension(base, "lz4")) name_idx = aml_pool_strdupf(w->pool, "%.*s_%zu.lz4", (int)strlen(base)-4, base, idx);
        else if (io_extension(base, "gz")) name_idx = aml_pool_strdupf(w->pool, "%.*s_%zu.gz", (int)strlen(base)-3, base, idx);
        else name_idx = aml_pool_strdupf(w->pool, "%s_%zu", base, idx);

        aml_buffer_setf(bh, "%s/%s_%zu/%s", w->task->scheduler->task_dir, w->task->task_name, idx, name_idx);
        char *path = aml_pool_strdup(w->pool, aml_buffer_data(bh));

        if (o->flags & AMR_WRITE_SHUFFLE) {
          path = aml_pool_alloc(w->pool, strlen(path) + 20);
          io_out_partition_filename(path, aml_buffer_data(bh), idx % N);
        }

        printf("--- Partition %zu ---\n", idx);
        dump_file_limited(w, path, o->options.format, o->dump, o->dump_arg, sample_recs);
        printf("\n");
      }
    } else {
      printf("SAMPLING: No records found to sample.\n");
    }
    printf("============================================================\n");
    o = o->next;
  }
}


/* --- Type Registry Runtime Helpers --- */
void* amr_worker_deserialize(amr_worker_t *w, size_t local_idx, const io_record_t *r) {
    amr_transform_t *tr = w->current_transform;
    amr_datatype_t *dt = NULL;

    if (tr && local_idx < tr->num_inputs) {
        if (tr->inputs && tr->inputs[local_idx]) {
            // It's a standard external DAG input
            dt = tr->inputs[local_idx]->datatype;
        } else if (tr->internal_inputs && tr->internal_inputs[local_idx]) {
            // It's an internal pipelined output! The datatype lives on the output definition.
            dt = tr->internal_inputs[local_idx]->datatype;
        }
    }

    if (!dt || !dt->deserialize) {
        fprintf(stderr, "[ERROR] amr_worker_deserialize: No type registered for local input %zu.\n", local_idx);
        abort();
    }

    return dt->deserialize(w->pool, r->record, r->length);
}

void amr_worker_serialize(amr_worker_t *w, size_t local_idx, io_out_t *out, const void *obj) {
    amr_transform_t *tr = w->current_transform;

    if (!tr || local_idx >= tr->num_outputs || !tr->outputs[local_idx]) {
        fprintf(stderr, "[ERROR] amr_worker_serialize: Invalid local output index %zu.\n", local_idx);
        abort();
    }

    amr_datatype_t *dt = tr->outputs[local_idx]->datatype;

    if (!dt || !dt->serialize) {
        fprintf(stderr, "[ERROR] amr_worker_serialize: No type (or serialize function) registered for local output %zu.\n", local_idx);
        abort();
    }

    aml_buffer_clear(w->bh);
    dt->serialize(obj, w->bh);
    io_out_write_record(out, aml_buffer_data(w->bh), aml_buffer_length(w->bh));
}

amr_pipeline_t *amr_worker_pipeline(amr_worker_t *w) {
  return w->task->pipeline;
}

/* --- Callback Getters --- */

io_compare_cb amr_worker_input_compare(amr_worker_t *w, size_t n) {
    amr_worker_input_t *inp = amr_worker_input(w, n);
    return inp ? inp->compare : NULL;
}
void *amr_worker_input_compare_arg(amr_worker_t *w, size_t n) {
    amr_worker_input_t *inp = amr_worker_input(w, n);
    return inp ? inp->compare_arg : NULL;
}

io_reducer_cb amr_worker_input_reducer(amr_worker_t *w, size_t n) {
    amr_worker_input_t *inp = amr_worker_input(w, n);
    return inp ? inp->reducer : NULL;
}
void *amr_worker_input_reducer_arg(amr_worker_t *w, size_t n) {
    amr_worker_input_t *inp = amr_worker_input(w, n);
    return inp ? inp->reducer_arg : NULL;
}

io_compare_cb amr_worker_output_compare(amr_worker_t *w, size_t n) {
    amr_worker_output_t *out = amr_worker_output(w, n);
    return out ? out->ext_options.compare : NULL;
}
void *amr_worker_output_compare_arg(amr_worker_t *w, size_t n) {
    amr_worker_output_t *out = amr_worker_output(w, n);
    return out ? out->ext_options.compare_arg : NULL;
}

io_partition_cb amr_worker_output_partition(amr_worker_t *w, size_t n) {
    amr_worker_output_t *out = amr_worker_output(w, n);
    return out ? out->ext_options.partition : NULL;
}
void *amr_worker_output_partition_arg(amr_worker_t *w, size_t n) {
    amr_worker_output_t *out = amr_worker_output(w, n);
    return out ? out->ext_options.partition_arg : NULL;
}

io_reducer_cb amr_worker_output_reducer(amr_worker_t *w, size_t n) {
    amr_worker_output_t *out = amr_worker_output(w, n);
    return out ? out->ext_options.reducer : NULL;
}
void *amr_worker_output_reducer_arg(amr_worker_t *w, size_t n) {
    amr_worker_output_t *out = amr_worker_output(w, n);
    return out ? out->ext_options.reducer_arg : NULL;
}

void amr_worker_skip_output(amr_worker_t *w, size_t output_idx) {
    if (output_idx < 64) {
        w->skipped_outputs_mask |= (1ULL << output_idx);
    }
}
