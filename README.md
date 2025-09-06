# a-map-reduce-library (AMR)

A small, single-host Map/Reduce-style scheduler for building reproducible, partitioned data pipelines in C. AMR orchestrates *tasks* connected by typed *inputs* and *outputs*, runs them in parallel within a CPU/RAM budget, persists completion with *ack* files, and automatically cleans up intermediates.

> **License:** Apache-2.0
> **Headers:** `include/a-map-reduce-library/amr_schedule.h`
> **Core:** `src/amr_schedule.c`

---

## Why use AMR?

* **Deterministic pipelines**: Task graph + acknowledgements (ack files) decide what must re-run.
* **Parallelism with limits**: You declare available CPUs/RAM; AMR schedules accordingly.
* **Partition-aware**: Write once, run across N partitions; supports fan-in, shuffle, and same-partition wiring.
* **I/O helpers**: Plugs into *the-io-library* (streaming I/O, formats, compression).
* **Debuggability**: CLI options to list plans, dump files, and run an isolated task partition.

AMR depends on:

* **a-memory-library** (`aml_pool`, `aml_buffer`, alloc helpers)
* **the-io-library** (`io_in`, `io_out`, formats, compression, partitioning)
* **the-macro-library** (maps, timers)

---

## Quick start

### 1) Define tasks and wires

```c
#include "a-map-reduce-library/amr_schedule.h"
#include "the-io-library/io.h"
#include "the-io-library/io_out.h"
#include "the-io-library/io_in.h"

static bool setup_ingest(amr_task_t *t) {
  // External input files consumed by this task (“ingest”)
  amr_task_input_files(t, "input.txt", /*ram_pct*/0.25, NULL);
  // Produce “raw.lz4” for the next task (“transform”)
  amr_task_output(t, "raw.lz4", "transform", 0.25, 0.25, AMR_OUTPUT_PARTITION);
  // Use default runner (transform chain)
  amr_task_default_runner(t);
  // Pass-through: input -> output
  amr_task_transform(t, "input.txt", "raw.lz4", /*runner*/NULL);
  return true;
}

static void to_upper(amr_worker_t *w, io_record_t *r, io_out_t **out) {
  // simple example: uppercase the record in-place copy
  aml_buffer_t *tmp = w->bh; // reusable scratch buffer
  aml_buffer_clear(tmp);
  aml_buffer_append(tmp, r->record, r->length);
  char *p = aml_buffer_data(tmp);
  for (size_t i = 0; i < r->length; ++i) if (p[i] >= 'a' && p[i] <= 'z') p[i] -= 32;
  io_out_write_record(out[0], p, r->length);
}

static bool setup_transform(amr_task_t *t) {
  // Wire from ingest’s output; emit “clean.lz4”
  amr_task_output(t, "clean.lz4", NULL, 0.25, 0.25, 0);
  amr_task_default_runner(t);
  amr_task_transform(t, "raw.lz4", "clean.lz4", to_upper);
  // Example: compressed output (LZ4)
  amr_task_output_lz4(t, /*level*/9, /*block*/LZ4_1MB, /*blkCk*/true, /*cntCk*/true);
  return true;
}

int main(int argc, char **argv) {
  // 8 partitions, 4 CPUs, 32 GB RAM budget
  amr_schedule_t *S = amr_schedule_init(argc, argv, /*partitions*/8, /*cpus*/4, /*ramMB*/32768);

  // Optional: customize on-disk layout
  amr_schedule_task_dir(S, "tasks");
  amr_schedule_ack_dir(S, "tasks/ack");

  // Create tasks
  amr_task_t *ingest    = amr_schedule_task(S, "ingest",    /*partitioned*/true,  setup_ingest);
  amr_task_t *transform = amr_schedule_task(S, "transform", /*partitioned*/true,  setup_transform);

  // Make transform wait for ingest (same-partition via AMR_OUTPUT_PARTITION above)
  (void)ingest; (void)transform; // dependencies are established by outputs

  // Run (prints a completion line per task)
  amr_schedule_run(S, amr_worker_complete);
  amr_schedule_destroy(S);
}
```

### 2) Build (example)

Link against your local builds of **a-memory-library**, **the-io-library**, **the-macro-library**, and `pthread`.

```cmake
add_executable(pipeline main.c)
target_include_directories(pipeline PRIVATE include) # this repo's include/
target_link_libraries(pipeline PRIVATE the_io a_memory the_macro pthread)
```

### 3) Run

```bash
./pipeline -c 4 -r 32768               # override cpus/ram if desired
./pipeline -t transform:0-3            # run a subset of partitions
./pipeline -o -t transform:2           # only the selected task/part runs
./pipeline --debug transform:2 /tmp/dbg  # run one partition in isolation
./pipeline -l                          # list the execution plan
./pipeline -s                          # list + show input/output files
```

---

## Concept model

### Scheduler

```c
amr_schedule_t *amr_schedule_init(int argc,char**argv,size_t num_partitions,size_t cpus,size_t ramMB);
void amr_schedule_run(amr_schedule_t*, amr_worker_cb on_complete);
void amr_schedule_destroy(amr_schedule_t*);
```

* **Partitions**: Global number of data partitions across which tasks may run.
* **CPUs/RAM**: Upper bounds; AMR spreads work to honor these (per-worker RAM is `ram * pct / running`).
* **Ack files**: On completion, AMR touches `tasks/ack/<task>_<part>` (configurable). A task partition re-runs if:

    * Its ack is missing, or
    * Any dependency (full or same-partition) has a newer ack, or
    * Any declared input file is newer, or
    * `amr_task_run_everytime()` was set, or `--force` was used.

### Tasks & runners

```c
amr_task_t *amr_schedule_task(amr_schedule_t*, const char *name, bool partitioned, amr_task_cb setup);
void amr_task_default_runner(amr_task_t*);           // chain of transforms (see below)
void amr_task_runner(amr_task_t*, amr_worker_cb);    // fully custom worker
void amr_task_do_nothing(amr_task_t*);               // pure “checkpoint”
void amr_task_run_everytime(amr_task_t*);            // always re-run
```

### Inputs & outputs

Declare *external* inputs:

```c
void amr_task_input_files(amr_task_t*, const char *name, double ram_pct, amr_worker_file_info_cb file_info /*or NULL*/);
```

Declare *outputs* and wire to downstream tasks by name (pipe-separated for many):

```c
void amr_task_output(amr_task_t*, const char *name, const char *destinations_or_NULL,
                     double out_ram_pct, double in_ram_pct, size_t flags);
```

**Output flags (semantics):**

* `AMR_OUTPUT_NORMAL` (default): *fan-in*. Each destination partition reads **all** source partitions’ files:
  `.../<srcTask>_<i>/<name>_<i>` for all `i`.
* `AMR_OUTPUT_SPLIT`: *shuffle* (all-to-all). Source partitions produce per-destination files
  `.../<srcTask>_<i>/<name>_<i>_<dst>`. Each destination partition `dst` reads all `i`.
  You **must** set a partitioner via `amr_task_output_partition(...)`.
* `AMR_OUTPUT_PARTITION`: *same-partition*. Destination partition `p` reads only source `p`:
  `.../<srcTask>_<p>/<name>_<p>`.
* `AMR_OUTPUT_FIRST`: destination reads only source partition `0`.

> **Tip:** Despite an older comment suggesting the default is “same-partition,” the implementation’s default (`NORMAL`) is **fan-in**. Use `AMR_OUTPUT_PARTITION` if you want “same partition in -> same partition out”.

**Compression, format & options** (apply to the *most recent* output declaration):

```c
void amr_task_output_format(amr_task_t*, io_format_t fmt);   // also propagates to inputs of destinations
void amr_task_output_gz(amr_task_t*, int level);
void amr_task_output_lz4(amr_task_t*, int level, lz4_block_size_t size, bool block_ck, bool content_ck);
void amr_task_output_safe_mode(amr_task_t*);
void amr_task_output_write_ack_file(amr_task_t*);
void amr_task_output_use_extra_thread(amr_task_t*);
void amr_task_output_sort_before_partitioning(amr_task_t*);
void amr_task_output_sort_while_partitioning(amr_task_t*);
void amr_task_output_num_sort_threads(amr_task_t*, size_t n);
void amr_task_output_partition(amr_task_t*, io_partition_cb part, void *arg); // required for SPLIT
void amr_task_output_compare(amr_task_t*, io_compare_cb cmp, void *tag);      // also applied to inputs
void amr_task_output_reducer(amr_task_t*, io_reducer_cb red, void *tag);      // also applied to inputs
void amr_task_output_keep_first(amr_task_t*);                                  // convenience reducer
void amr_task_output_group_size(amr_task_t*, size_t num_per_group, size_t start);
void amr_task_output_dump(amr_task_t*, amr_task_dump_cb dump, void *arg);     // for debugging “cat” output
```

**Input options** (apply to the most recent `amr_task_input_files` or `amr_task_output`):

```c
void amr_task_input_format(amr_task_t*, io_format_t);
void amr_task_input_compare(amr_task_t*, io_compare_cb cmp, void *tag); // enables merged sorted reads
void amr_task_input_reducer(amr_task_t*, io_reducer_cb red, void *tag);
void amr_task_input_keep_first(amr_task_t*);
void amr_task_input_compressed_buffer_size(amr_task_t*, size_t sz);
void amr_task_input_limit(amr_task_t*, size_t limit);
void amr_task_input_dump(amr_task_t*, amr_task_dump_cb dump, void *arg);      // debugging
```

**Automatic cleanup of intermediates**
AMR tracks per-partition downstream refcounts. Once all consumers have finished with a partitioned intermediate, it unlinks it **unless** you requested to keep it.

* Keep: set a keep bit in your `flags` when you call `amr_task_output` (the library defines `AMR_OUTPUT_KEEP = 1`).

### Transforms with the default runner

The *default runner* processes a chain of transforms you declare:

```c
// record-by-record transform
void amr_task_transform(amr_task_t*, const char *inputs /*'a|b'*/, const char *outputs /*'x|y'*/,
                        amr_runner_cb run /*(w, r, outs)*/);

// grouped transform (records with same key seen together)
void amr_task_group_transform(amr_task_t*, const char *inputs, const char *outputs,
                              amr_group_runner_cb run /*(w, first, num_r, outs)*/,
                              io_compare_cb compare /*group comparator*/);
void amr_task_group_compare_arg(amr_task_t*, amr_worker_data_cb create, amr_destroy_worker_data_cb destroy);

// full-control transform (you drive reads/writes)
void amr_task_io_transform(amr_task_t*, const char *inputs, const char *outputs,
                           io_runner_cb run /*(w, ins[], num_ins, outs[], num_outs)*/);

// optional per-transform state
void amr_task_transform_data(amr_task_t*, amr_worker_data_cb create, amr_destroy_worker_data_cb destroy);
```

> If you set `amr_task_default_runner()` and did *not* add any transforms, AMR will auto-create a pass-through transform wiring all declared inputs to all outputs.

### Dependencies

You rarely need to declare dependencies manually—wiring outputs to destinations sets them up. For ad‑hoc barriers:

```c
bool amr_task_dependency(amr_task_t*, const char *upstream_names_pipe_separated);
bool amr_task_partial_dependency(amr_task_t*, const char *upstream_names);
// Full dependency = wait for all partitions of upstream.
// Partial dependency = only wait for the *same* partition of upstream.
```

---

## File layout & naming

AMR creates per-task/partition directories and files under `task_dir` (default `tasks/`):

```
tasks/
  ack/<task>_<part>            # ack files (mtime used for staleness)
  <task>_<part>/name_<part>    # NORMAL/PARTITION/ FIRST outputs
  <task>_<part>/name_<src>_<dst>  # SPLIT outputs (shuffle)
```

Compression suffixes are appended appropriately (`.gz`, `.lz4`).

---

## CLI (built-in)

* `-t|--task <t[:parts]>[|t2[:parts]]` select tasks/partitions. Ranges like `1-3`, lists `1,3,5`.
* `-o` only run the selected tasks (no automatic upstream runs).
* `-f|--force` re-run selected tasks even if inputs/acks say “up-to-date”.
* `-c|--cpus <n>` override CPU slots.
* `-r|--ram <MB>` override RAM budget (affects per-worker buffers).
* `-l|--list` print the execution plan.
* `-s|--show-files` plan + concrete input/output file paths.
* `-d|--dump f1[,f2...]` dump matched input/output files’ contents.
* `-p|--prefix ...` like `--dump` but prefix each line with `file:line`.
* `--debug <task:part> <dir> [dump_input]` run a single partition in isolation, writing to `<dir>`.
* `--new-args` (custom args; see below)
* `-h|--help` help.

**Custom CLI args & reproducibility**

You can let AMR parse your own arguments and persist them to `tasks/custom_args`. On next runs AMR will compare current args with the stored set and fail if they differ, unless `--new-args` is provided.

```c
void amr_schedule_custom_args(amr_schedule_t*,
    void (*custom_usage)(),
    int  (*parse_args)(int argc, char **argv, void *arg),  // return the # of consumed argv entries (>0)
    bool (*finish_args)(int argc, char **argv, void *arg),
    void *arg);

// Retrieve your custom pointer inside setup/runner callbacks:
void *amr_task_custom_arg(amr_task_t*);
```

---

## Inside a runner (helpers you’ll use)

```c
// Resolve declared inputs/outputs for this worker (by index 0..)
amr_worker_input_t  *amr_worker_input(amr_worker_t*, size_t n);
amr_worker_output_t *amr_worker_output(amr_worker_t*, size_t n);

// Open streaming handles based on your declarations & formats
io_in_t  *amr_worker_in(amr_worker_t*, size_t n);
io_out_t *amr_worker_out(amr_worker_t*, size_t n);

// Convenience: load a whole (single, uncompressed, fixed-format) input into RAM
char *amr_worker_read(amr_worker_t*, size_t *num_records, char **endp, size_t input_index);

// Output/input parameter strings (for logging)
char *amr_worker_output_params(amr_worker_t*, size_t n);
char *amr_worker_input_params(amr_worker_t*, size_t n);

// Budget
size_t amr_worker_ram(amr_worker_t*, double pct /*0..1*/);

// Debug
bool   amr_worker_debug(amr_worker_t*);
void   amr_worker_dump_input(amr_worker_t*, io_record_t *r, size_t input_index);

// Names
const char *amr_task_name(amr_task_t*);
```

**I/O formats** (via `io_format_t`, from the I/O library)
AMR recognizes three patterns in `amr_worker_read` based on `io_in_options.format`:

* `0` → length-prefixed records (`uint32_t len | payload`)
* `>0` → fixed width of `format` bytes
* `<0` → delimited (delimiter char is `-(format + 1)`)

---

## Performance & memory

* Each worker’s buffers are sized with `amr_worker_ram(w, pct)`, where `pct` is the fraction you supplied when declaring inputs/outputs. The effective division considers how many workers are concurrently active (`w->running`).
* For multi-file inputs:

    * If you attach a comparator (`amr_task_input_compare`), AMR creates a *merged* input (`io_in_ext_*`) honoring sort order.
    * Otherwise it creates a sequential iterator over the list.

---

## Debugging & inspection

* `-l` / `-s` show dependencies, reverse dependencies, and concrete file names (including SPLIT variants).
* `--debug <task:part> <dir> [dump_input]` runs one partition in isolation, overriding the output path. With `dump_input`, AMR will print the transformed inputs before writing.
* `amr_schedule_print(S)` dumps internal state (for advanced debugging).

---

## Gotchas & tips

* **SPLIT requires a partitioner**: Always call `amr_task_output_partition` when you set `AMR_OUTPUT_SPLIT`.
* **Default wiring is fan-in**: If you expected “same partition → same partition,” use `AMR_OUTPUT_PARTITION`.
* **Keep vs. auto-clean**: Intermediates are deleted when no longer referenced; mark outputs with the keep bit if you need them retained.
* **Ack semantics**: Ack mtime drives staleness. If you generate side files *outside* AMR, include them as explicit inputs or they won’t trigger re-runs.
* **Transforms are chained**: If you add multiple transforms to one task, the first output becomes the next transform’s primary input automatically.

---

## Minimal “map → shuffle → reduce” sketch

```c
// Map: split output by downstream partition
static void map_cb(amr_worker_t *w, io_record_t *r, io_out_t **outs) {
  // choose outs[0]; partitioning happens inside io_out based on the partitioner you set
  io_out_write_record(outs[0], r->record, r->length);
}

static bool map_setup(amr_task_t *t) {
  amr_task_input_files(t, "lines.txt", 0.3, NULL);
  amr_task_output(t, "kv.lz4", "reduce", 0.3, 0.3, AMR_OUTPUT_SPLIT);
  amr_task_output_partition(t, my_partitioner, my_part_arg); // required
  amr_task_default_runner(t);
  amr_task_transform(t, "lines.txt", "kv.lz4", map_cb);
  return true;
}

// Reduce: fan-in all map partitions for my partition
static void reduce_group(amr_worker_t *w, io_record_t *first, size_t num_r, io_out_t **outs) {
  // first..first+(num_r-1) share the same key (comparison set below)
  // emit reduced record
  io_out_write_record(outs[0], first->record, first->length);
}

static bool reduce_setup(amr_task_t *t) {
  amr_task_output(t, "result.lz4", NULL, 0.4, 0.4, 0);
  amr_task_default_runner(t);
  amr_task_group_transform(t, "kv.lz4", "result.lz4", reduce_group, my_key_compare);
  amr_task_output_sort_before_partitioning(t); // if you need sorted intermediates
  return true;
}
```

---

## API reference at a glance

* **Scheduler**: `amr_schedule_init`, `amr_schedule_run`, `amr_schedule_destroy`, `amr_schedule_ack_dir`, `amr_schedule_task_dir`, `amr_schedule_custom_args`, `amr_schedule_print`
* **Tasks**: `amr_schedule_task`, `amr_task_default_runner`, `amr_task_runner`, `amr_task_do_nothing`, `amr_task_run_everytime`, `amr_task_dependency`, `amr_task_partial_dependency`, `amr_task_name`, `amr_task_schedule`
* **Transforms**: `amr_task_transform`, `amr_task_group_transform`, `amr_task_io_transform`, `amr_task_group_compare_arg`, `amr_task_transform_data`
* **I/O wiring**:

    * Outputs: `amr_task_output` + options (`_partition`, `_format`, `_gz`, `_lz4`, `_compare`, `_reducer`, `_keep_first`, `_group_size`, `_use_extra_thread`, `_dont_compress_tmp`, `_sort_before_partitioning`, `_sort_while_partitioning`, `_num_sort_threads`, `_safe_mode`, `_write_ack_file`, `_dump`)
    * Inputs:   `amr_task_input_files` + options (`_format`, `_compare`, `_reducer`, `_keep_first`, `_compressed_buffer_size`, `_limit`, `_dump`)
* **Runner helpers**: `amr_worker_in`, `amr_worker_out`, `amr_worker_input`, `amr_worker_output`, `amr_worker_read`, `amr_worker_dump_input`, `amr_worker_ram`, `amr_worker_output_params`, `amr_worker_input_params`, `amr_worker_output_base(2)`, `amr_worker_input_name`, `amr_worker_debug`
* **Defaults**: `amr_worker_complete` (stderr timing line)

---

## Contributing

This README describes the API as implemented in `amr_schedule.c`. If you spot mismatches or want multi-host scheduling, open an issue/PR (or extend the scheduler threads to remote workers).

Enjoy building fast, partitioned C pipelines!
