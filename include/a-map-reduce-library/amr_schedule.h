#ifndef AMR_SCHEDULE_H
#define AMR_SCHEDULE_H

#include "a-memory-library/aml_alloc.h"
#include "a-memory-library/aml_pool.h"
#include "a-memory-library/aml_buffer.h"
#include "the-io-library/io.h"
#include "the-io-library/io_out.h"
#include "the-io-library/io_in.h"
#include "the-macro-library/macro_map.h"

#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

struct amr_schedule_s;
typedef struct amr_schedule_s amr_schedule_t;

struct amr_task_s;
typedef struct amr_task_s amr_task_t;

struct amr_worker_s;
typedef struct amr_worker_s amr_worker_t;

struct amr_worker_input_s;
typedef struct amr_worker_input_s amr_worker_input_t;

typedef bool (*amr_task_cb)(amr_task_t *task);
typedef bool (*amr_worker_cb)(amr_worker_t *w);

typedef void *(*amr_worker_data_cb)(amr_worker_t *w);
typedef void (*amr_destroy_worker_data_cb)(amr_worker_t *w, void *d);

typedef void (*amr_task_dump_cb)(amr_worker_t *w, io_record_t *r,
                               aml_buffer_t *bh, void *arg);

void amr_task_dump_text(amr_worker_t *w, io_record_t *r, aml_buffer_t *bh,
                       void *arg);

typedef void (*amr_runner_cb)(amr_worker_t *w, io_record_t *r, io_out_t **out);
typedef void (*amr_group_runner_cb)(amr_worker_t *w, io_record_t *r,
                                  size_t num_r, io_out_t **out);

typedef void (*io_runner_cb)(amr_worker_t *w, io_in_t **ins, size_t num_ins,
                               io_out_t **outs, size_t num_outs);

typedef io_file_info_t *(*amr_worker_file_info_cb)(amr_worker_t *w,
                                                    size_t *num_files,
                                                    amr_worker_input_t *inp);

/* This function should advance past used args and return < 0 on error.
   Make sure it doesn't extend beyond argc. */
typedef int (*parse_args_cb)(int argc, char **argv, void *arg);

typedef bool (*finish_args_cb)(int argc, char **argv, void *arg);

/**************************************************************************
The following functions should be called to setup the scheduler.  Once all
of the tasks are scheduled, call amr_schedule_run.  This will cause the setup
functions to be called in the order that they were scheduled.  The scheduler
allows you to define a number of partitions, cpus, ram, and disk space.  It
then attempts to keep your process operating fully within that space.
***************************************************************************/

/* Create the scheduler and specify how many partitions, cpus, ram (in MB),
   and disk space.  Disk space is currently unused - but I have plans for it. */
amr_schedule_t *amr_schedule_init(int argc, char **args, size_t num_partitions,
                                size_t cpus, size_t ram);

/* Define where the ack directory should be (default is tasks/ack)*/
void amr_schedule_ack_dir(amr_schedule_t *h, const char *ack_dir);

/* Define where the tasks directory should be output to (default is tasks) */
void amr_schedule_task_dir(amr_schedule_t *h, const char *task_dir);

/* Define custom usage - make sure your args don't conflict with amr_schedule.
   The parse_args method will be called for every argument that isn't part of
   amr_schedule's basic arguments.  If it returns NULL, there is an error.
   parse_args will be called one last time after all arguments have been
   parsed with a NULL first argument.  Make sure to return eargs if everything
   is good.  Otherwise, return NULL. */
void amr_schedule_custom_args(amr_schedule_t *h, void (*custom_usage)(),
                             parse_args_cb parse_args, finish_args_cb finish_args,
                             void *arg);

/* Add a task to the scheduler with an associated name and define whether it
   is partitioned or not.  The setup function will further describe the task
   once all of the tasks have been added to the scheduler. */
amr_task_t *amr_schedule_task(amr_schedule_t *h, const char *task_name,
                            bool partitioned, amr_task_cb setup);

/* An on_complete method that prints completion of each task to stderr */
bool amr_worker_complete(amr_worker_t *w);

/* Run all of the tasks */
void amr_schedule_run(amr_schedule_t *h, amr_worker_cb on_complete);

/* Destroy the scheduler */
void amr_schedule_destroy(amr_schedule_t *h);

/**************************************************************************
The following functions should be called from the setup callback methods
specified by amr_schedule_task.
***************************************************************************/

/* This retrieves the arg passed in via amr_schedule_custom_args */
void *amr_task_custom_arg(amr_task_t *task);

/* Define what should run for the given task */
void amr_task_runner(amr_task_t *task, amr_worker_cb runner);

void amr_task_default_runner(amr_task_t *task);

void amr_task_transform(amr_task_t *task, const char *inp, const char *outp,
                       amr_runner_cb runner);

void amr_task_io_transform(amr_task_t *task, const char *inp, const char *outp,
                          io_runner_cb runner);

void amr_task_group_transform(amr_task_t *task, const char *inp, const char *outp,
                             amr_group_runner_cb runner, io_compare_cb compare);

void amr_task_group_compare_arg(amr_task_t *task, amr_worker_data_cb create,
                               amr_destroy_worker_data_cb destroy);

void amr_task_transform_data(amr_task_t *task, amr_worker_data_cb create,
                            amr_destroy_worker_data_cb destroy);

/* Define outside input files to a task.  Inputs from other tasks are auto
   configured through amr_task_output. Inputs are named for convenience with
   the expectation that the check method would confirm if data has changed.

   The ram_pct is what percentage of ram this input should use for the given
   task (and should range from 0-1).
   */
void amr_task_input_files(amr_task_t *task, const char *name, double ram_pct,
                         amr_worker_file_info_cb file_info);

/* Once intermediate files are no longer required, they are removed unless
   AMR_OUTPUT_KEEP is defined. */
static const size_t AMR_OUTPUT_KEEP = 1;

/* Files are typically assumed to not be split and meant to be used from
   one partition to the next.  For example, if partition 3 of a task produces
   an output to destination task, it is assumed that the output is meant for
   partition 3 of the destination task.

   AMR_OUTPUT_USE_FIRST would cause the destination task to use the first
   partition of the source task.

   AMR_OUTPUT_SPLIT would cause the destination task to use all of the source
   partitions data.

   AMR_OUTPUT_PARTITION causes the data to not be split and the destination task
   to use data from the same input partition.  For example, if partition 3 of a
   task produces an output to destination task, it is assumed that the output
   is meant for partition 3 of the destination task.
*/

static const size_t AMR_OUTPUT_NORMAL = 0;
static const size_t AMR_OUTPUT_FIRST = 2;
static const size_t AMR_OUTPUT_SPLIT = 4;
static const size_t AMR_OUTPUT_PARTITION = 8;

/* Defines an output which will use name as the base name.  The destinations
   are a list of output tasks which can be NULL to specify none, or multiple if
   separated by vertical bars.
*/
void amr_task_output(amr_task_t *task, const char *name, const char *destinations,
                    double out_ram_pct, double in_ram_pct, size_t flags);

void amr_task_output_dump(amr_task_t *task, amr_task_dump_cb dump, void *arg);

/* These amr_task_output_... methods must be called after amr_task_output and
   will apply to the previous amr_task_output call. */
void amr_task_output_partition(amr_task_t *task, io_partition_cb part,
                              void *arg);

void amr_task_output_compare(amr_task_t *task, io_compare_cb compare,
                            void *compare_tag);

void amr_task_output_intermediate_compare(amr_task_t *task,
                                         io_compare_cb compare,
                                         void *compare_tag);

void amr_task_output_keep_first(amr_task_t *task);

void amr_task_output_reducer(amr_task_t *task, io_reducer_cb reducer,
                            void *reducer_tag);

void amr_task_output_intermediate_reducer(amr_task_t *task,
                                         io_reducer_cb reducer,
                                         void *reducer_tag);

void amr_task_output_group_size(amr_task_t *task, size_t num_per_group,
                               size_t start);

void amr_task_output_use_extra_thread(amr_task_t *task);

void amr_task_output_dont_compress_tmp(amr_task_t *task);

void amr_task_output_sort_before_partitioning(amr_task_t *task);

void amr_task_output_sort_while_partitioning(amr_task_t *task);

void amr_task_output_num_sort_threads(amr_task_t *task, size_t num_sort_threads);

void amr_task_output_format(amr_task_t *task, io_format_t format);

void amr_task_output_safe_mode(amr_task_t *task);

void amr_task_output_write_ack_file(amr_task_t *task);

void amr_task_output_gz(amr_task_t *task, int level);

void amr_task_output_lz4(amr_task_t *task, int level, lz4_block_size_t size,
                        bool block_checksum, bool content_checksum);

/* The amr_task_input... methods apply to the previous amr_task_input_files or
   amr_task_output call.  If the previous amr_task_output call doesn't specify
   one or more destinations, the calls are silently ignored. */
void amr_task_input_format(amr_task_t *task, io_format_t format);

void amr_task_input_dump(amr_task_t *task, amr_task_dump_cb dump, void *arg);

void amr_task_input_compare(amr_task_t *task, io_compare_cb compare,
                           void *compare_tag);

void amr_task_input_keep_first(amr_task_t *task);

void amr_task_input_reducer(amr_task_t *task, io_reducer_cb reducer,
                           void *reducer_tag);

void amr_task_input_compressed_buffer_size(amr_task_t *task, size_t buffer_size);

void amr_task_input_limit(amr_task_t *task, size_t limit);

/* Use this if the dependency must finish completely prior to task running.
   Vertical bars can seperate dependencies.
*/
bool amr_task_dependency(amr_task_t *task, const char *dependency);

/* Use this if the task can run partition by partition if dependency is filled.
   Vertical bars can seperate dependencies.
 */
bool amr_task_partial_dependency(amr_task_t *task, const char *dependency);

/* this allows a task to do nothing and can be used as a checkpoint or
   dependency for other tasks */
void amr_task_do_nothing(amr_task_t *task);

/* this forces a task to run everytime no matter what */
void amr_task_run_everytime(amr_task_t *task);

/**************************************************************************
The following structures are primarily used within the runner
***************************************************************************/

struct amr_task_state_link_s;
typedef struct amr_task_state_link_s amr_task_state_link_t;

struct amr_worker_output_s;
typedef struct amr_worker_output_s amr_worker_output_t;

struct amr_task_link_s;
typedef struct amr_task_link_s amr_task_link_t;

struct amr_task_link_s {
  amr_task_t *task;
  amr_task_link_t *next;
};

struct amr_schedule_thread_s;
typedef struct amr_schedule_thread_s amr_schedule_thread_t;

struct amr_worker_s {
  /* The worker pool should never be cleared */
  aml_pool_t *worker_pool;
  /* The pool can be cleared by application */
  aml_pool_t *pool;
  /* The bh can be cleared by application */
  aml_buffer_t *bh;

  amr_task_t *task;
  amr_worker_input_t *inputs;
  amr_worker_output_t *outputs;
  void *data;
  void *transform_data;
  uint64_t elapsed_ns;
  size_t partition;
  size_t num_partitions;
  size_t running;
  size_t thread_id;
  time_t ack_time;
  amr_schedule_thread_t *schedule_thread;
  amr_task_state_link_t *state_link;
};

struct amr_worker_input_s {
  size_t id;
  char *name;
  amr_worker_file_info_cb file_info;
  io_file_info_t *files;
  size_t num_files;
  double ram_pct;

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
  amr_task_link_t *destinations;
  double ram_pct;
  size_t flags;
  size_t num_partitions;

  amr_task_dump_cb dump;
  void *dump_arg;

  io_out_options_t options;
  io_out_ext_options_t ext_options;

  bool cleaned_up;
  /* this can be num_partitions * dest num_partitions if split */
  bool *cleaned_up_parts;
  size_t *refcount_parts;

  amr_task_t *task;
  amr_worker_output_t *next;
};

/**************************************************************************
The following functions are meant to be used within the runner or the check
callback.
***************************************************************************/

/* Get nth output (defined in the setup callback) */
amr_worker_output_t *amr_worker_output(amr_worker_t *w, size_t n);

/* Form an io_out_t from the specifications for the given task */
io_out_t *amr_worker_out(amr_worker_t *w, size_t n);

/* Get nth input (defined by amr_task_input_files and by amr_task_output) */
amr_worker_input_t *amr_worker_input(amr_worker_t *w, size_t n);

/* Form an io_in_t from the specifications for the given task */
io_in_t *amr_worker_in(amr_worker_t *w, size_t n);

/* Dump a record from Nth input */
void amr_worker_dump_input(amr_worker_t *w, io_record_t *r, size_t n);

/* Load records into RAM from the specifications for the given task.  This only
   works if the input is a fixed format, the file is not compressed, and their
   is only one input file.  To use this when there are multiple files, first
   merge the multiple files into a single file.  There is no need to free the
   memory associated from amr_worker_read as it will be done after task is
   done.  */
char *amr_worker_read(amr_worker_t *w, size_t *num_records, char **endp,
                     size_t n);

/* Return the task name given a task */
const char *amr_task_name(amr_task_t *task);

/* Get the scheduler given a task - shouldn't be needed much if at all */
amr_schedule_t *amr_task_schedule(amr_task_t *task);

/* Return an actual amount of ram given the task/partition and percentage */
size_t amr_worker_ram(amr_worker_t *w, double pct);

/* Returns true if in debug mode */
bool amr_worker_debug(amr_worker_t *w);

/* Return the output base name based upon a task/partition and an output */
char *amr_worker_output_base(amr_worker_t *w, amr_worker_output_t *outp);

/* Same as above except adding a suffix.  Suffix is somewhat complicated in
   the case where filenames have a compression extension. */
char *amr_worker_output_base2(amr_worker_t *w, amr_worker_output_t *outp,
                             const char *suffix);

/* Returns a name and number for split, otherwise just a name */
char *amr_worker_output_params(amr_worker_t *w, size_t n);

/* Returns the names of the input files as a string */
char *amr_worker_input_params(amr_worker_t *w, size_t n);

/* Returns NULL if no input from the given source partition.  Otherwise,
   this will return the name of the file that was output to this partition
   given the task/part, input, and source partition */
char *amr_worker_input_name(amr_worker_t *w, amr_worker_input_t *inp,
                           size_t partition);

/**************************************************************************
Other helper functions
**************************************************************************/

/* Debugging - print the internals */
void amr_schedule_print(amr_schedule_t *h);

#ifdef __cplusplus
}
#endif

#endif  /* AMR_SCHEDULE_H */

