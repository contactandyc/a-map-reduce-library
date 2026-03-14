// SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#ifndef INTERNAL_AMR_INTERNAL_H
#define INTERNAL_AMR_INTERNAL_H

#include "a-map-reduce-library/amr.h"
#include "the-macro-library/macro_map.h"
#include <pthread.h>

/* --- Forward Declarations --- */
struct amr_task_state_s;
typedef struct amr_task_state_s amr_task_state_t;

struct amr_task_link_s;
typedef struct amr_task_link_s amr_task_link_t;

struct amr_allocs_s;
typedef struct amr_allocs_s amr_allocs_t;

struct amr_task_input_link_s;
typedef struct amr_task_input_link_s amr_task_input_link_t;

struct amr_transform_s;
typedef struct amr_transform_s amr_transform_t;

typedef struct amr_pending_edge_s amr_pending_edge_t;

struct amr_datatype_registry_s;
typedef struct amr_datatype_registry_s amr_datatype_registry_t;

struct amr_datatype_s;
typedef struct amr_datatype_s amr_datatype_t;

struct amr_pipeline_port_s;
typedef struct amr_pipeline_port_s amr_pipeline_port_t;

/* Internal Edge/Output flags (moved from amr.h) */
#define AMR_WRITE_SHUFFLE    (1u << 1)
#define AMR_OUTPUT_OPAQUE    (1u << 5)
#define AMR_INPUT_OPAQUE     (1u << 6)
#define AMR_OUTPUT_DIR       (1u << 7)

#define AMR_EDGE_ALL_TO_ALL  (1u << 0)
#define AMR_EDGE_FIRST       (1u << 2)
#define AMR_EDGE_SHUFFLE     (1u << 3)
#define AMR_EDGE_PARTITION   (1u << 4)

struct amr_pipeline_port_s {
    char *port_name;
    char *mapped_task;
    char *mapped_output;

    amr_pipeline_t *alias_pipe;
    char *alias_port;

    amr_pipeline_port_t *next;
};

struct amr_pipeline_s {
    char *ns;
    amr_t *scheduler;
    void *config;
    amr_pipeline_port_t *inputs;
    amr_pipeline_port_t *outputs;

    amr_pipeline_t *parent;
    amr_pipeline_t *next;
};


/* --- Struct Definitions --- */
struct amr_task_link_s {
  amr_task_t *task;
  amr_task_link_t *next;
};

struct amr_worker_input_s {
  size_t id;
  char *name;
  amr_worker_file_info_cb file_info;
  io_file_info_t *files;
  size_t num_files;
  double ram_pct;

  const char *expected_type;
  amr_datatype_t *datatype;
  size_t edge_flags;

  /* --- NEW: In-Memory Loading State --- */
  bool load_into_memory;
  amr_loaded_data_t *loaded_data;
  size_t num_loaded_data;

  /* Previous Run Variables */
  bool is_previous_run;
  size_t prev_run;
  char *prev_task_name;

  io_in_options_t options;
  io_compare_cb compare;
  void *compare_arg;
  io_reducer_cb reducer;
  void *reducer_arg;
  size_t limit;

  amr_task_dump_cb dump;
  void *dump_arg;

  amr_task_t *task;
  amr_worker_output_t *src;
  amr_worker_input_t *next;
};

struct amr_worker_output_s {
  size_t id;
  char *name;
  double ram_pct;
  size_t flags;
  size_t num_partitions;

  const char *type_name;
  amr_datatype_t *datatype;

  amr_task_dump_cb dump;
  void *dump_arg;

  io_out_options_t options;
  io_out_ext_options_t ext_options;

  bool cleaned_up;
  bool keep_output;

  /* this can be num_partitions * dest num_partitions if split */
  bool *cleaned_up_parts;
  size_t *refcount_parts;
  size_t *refcount_buckets; /* Added to track Shuffle deletions safely */

  amr_task_t *task;
  amr_worker_output_t *next;
};

struct amr_task_state_link_s {
  bool waiting_on_others;
  amr_task_t *task;
  time_t ack_time;
  amr_task_state_link_t *next;
  amr_task_state_link_t *previous;
  time_t completed;
};

struct amr_task_state_s {
  amr_task_state_link_t *completed_tasks;
  amr_task_state_link_t *available_tasks;
  amr_task_state_link_t *tasks_to_finish;
};

struct amr_allocs_s {
  void *d;
  size_t length;
  amr_allocs_t *next;
};

struct amr_thread_s {
  pthread_t thread;
  amr_t *scheduler;
  aml_pool_t *pool;
  aml_buffer_t *bh;
  amr_allocs_t *allocs;
  size_t thread_id;
  size_t partition;
};

/* ========================================================================
 * WORKER RUNTIME CONTEXT
 * ========================================================================
 * The amr_worker_t is the execution context passed to your runner callbacks.
 * It represents a single thread processing a single partition of a task.
 * ======================================================================== */
struct amr_worker_s {
  /* --- Memory Management (Allocated by Framework) ---
     worker_pool: Survives the ENTIRE execution of this worker partition.
     Use it for allocating state, lookup tables, or file paths that you need
     across the whole stream. Never call aml_pool_clear() on this!

     pool: Your scratch memory for inner loops. In standard runners, this is
     cleared for you automatically. If writing custom runners, you MUST call
     aml_pool_clear(w->pool) repeatedly (e.g., at the start of processing
     each group or record) to prevent out-of-memory errors.

     bh: A reusable byte buffer for constructing strings or binary records.
     Call aml_buffer_clear(w->bh) before writing a new record.
  */
  aml_pool_t *worker_pool;
  aml_pool_t *pool;
  aml_buffer_t *bh;

  /* --- Task Context (Read-Only) ---
     task: Pointer back to the parent task definition. You can use this to
     extract global arguments set via amr_custom_args using amr_task_custom_arg().
  */
  amr_task_t *task;

  /* Internal I/O. Do not use directly. Use amr_worker_in(w, n) instead. */
  amr_worker_input_t *inputs;
  amr_worker_output_t *outputs;

  /* --- Custom State (Managed by You) ---
     data: Internal framework use (holds transform pipeline). Ignore.

     transform_data: This is YOUR local state for this specific worker instance.
     It is automatically populated by the framework IF you registered a
     creation callback via amr_task_transform_data(). Cast this to your custom
     struct (e.g., my_regex_t *re = w->transform_data).
  */
  void *data;
  void *transform_data;

  /* --- Execution State (Read-Only) ---
     partition: The specific data slice ID this worker is handling (0 to num-1).
     num_partitions: Total number of partitions for the parent task.
     thread_id: The physical CPU thread running this worker.
     running: The current number of active threads in the whole system.
     elapsed_ns: Populated after the worker finishes.
  */
  uint64_t elapsed_ns;
  size_t partition;
  size_t num_partitions;
  size_t running;
  size_t thread_id;
  time_t ack_time;

  /* Internal scheduler state (Do not modify) */
  amr_thread_t *schedule_thread;
  amr_task_state_link_t *state_link;
};

typedef struct {
  amr_task_t *task;
  bool *partitions;
} selected_task_t;

typedef struct {
  bool dump;
  bool prefix;
  bool list;
  bool show_files;
  bool help;
  bool only_run_selected;
  bool force;
  bool select_all;
  bool keep_files; /* Added for lifecycle management */

  bool sample;
  size_t sample_records;
  size_t sample_partitions;

  const char *sample_match;

  size_t run_number; /* Added for iterative runs */

  size_t ram;
  size_t cpus;

  amr_task_t *debug_task;
  size_t debug_partition;
  char *debug_path;
  bool debug_dump_input;

  char **files;
  selected_task_t *tasks;
  size_t num_tasks;
  size_t num_selected;
} parsed_args_t;

struct amr_pending_edge_s {
  char *producer_task_name;
  char *output_name;
  amr_task_t *producer;
  amr_worker_output_t *output;

  bool is_pipeline_port;

  amr_task_t *consumer;
  amr_worker_input_t *input_stub;

  double in_ram_pct;

  amr_pending_edge_t *next;
};

struct amr_s {
  macro_map_t *task_root;
  aml_pool_t *pool;
  aml_pool_t *tmp_pool;

  char **args;
  int argc;


  amr_parse_args_cb parse_args;
  amr_finish_args_cb finish_args;
  void *parse_args_arg;
  void (*custom_usage)();

  char *ack_dir;
  char *task_dir;
  bool use_runs; /* Added for iterative runs */

  size_t num_available;
  size_t num_tasks_to_run;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  size_t num_running;

  amr_thread_t *threads;
  bool started_threads;

  amr_worker_cb on_complete;
  bool done;
  bool success;

  size_t num_partitions;
  amr_task_state_t *state;

  amr_pending_edge_t *pending_edges;

  amr_task_t *head;
  amr_task_t *tail;

  size_t ram;
  size_t cpus;
  size_t disk_space;

  parsed_args_t parsed_args;
  amr_datatype_registry_t *types;
};

struct amr_task_input_link_s {
  amr_worker_input_t *input;
  amr_task_input_link_t *next;
};

struct amr_transform_s {
  amr_worker_input_t **inputs;
  size_t num_inputs;
  amr_worker_output_t **outputs;
  size_t num_outputs;

  char **input_names;
  char **output_names;

  amr_io_runner_cb io_runner;
  amr_runner_cb runner;
  amr_group_runner_cb group_runner;
  io_compare_cb group_compare;
  amr_worker_data_cb create_group_compare_arg;
  amr_destroy_worker_data_cb destroy_group_compare_arg;
  amr_worker_data_cb create_data;
  amr_destroy_worker_data_cb destroy_data;
  amr_transform_t *next;
};

struct amr_task_s {
  macro_map_t node;

  char *task_name;
  size_t num_partitions;

  bool do_nothing;
  bool run_everytime;

  amr_t *scheduler;

  amr_task_cb setup;
  amr_worker_cb runner;

  amr_transform_t *transforms;

  amr_task_input_link_t *current_input;
  amr_worker_output_t *current_output;

  amr_worker_input_t *inputs;
  amr_worker_output_t *outputs;
  amr_task_link_t *dependencies;
  amr_task_link_t *partial_dependencies;
  amr_task_link_t *reverse_dependencies;
  amr_task_link_t *reverse_partial_dependencies;

  void *global_arg;
  size_t global_size;

  void *local_arg;
  size_t local_size;

  bool *selected;

  amr_task_state_link_t *state_linkage;

  amr_pipeline_t *pipeline;

  amr_task_t *next;
};

amr_task_t *amr_find_task(amr_t *h, const char *task_name);
void amr_task_input_from_task_mode(amr_task_t *consumer,
                                   const char *producer_task_name,
                                   const char *producer_output,
                                   const char *local_alias,
                                   double in_ram_pct,
                                   size_t edge_flags);

#endif /* AMR_INTERNAL_H */
