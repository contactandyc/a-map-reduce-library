# A-Map-Reduce Library (AMR)

**A-Map-Reduce (AMR)** is a single-node, partitioned DAG (Directed Acyclic Graph) engine for out-of-core batch processing in C. 

It is designed to process datasets that exceed available system RAM by providing disk-backed shuffle, sort, and merge operations across multi-threaded worker partitions.

---

## 🏗️ Engine Characteristics

* **Partitioned Execution:** Workloads are split into discrete partitions and processed concurrently by a bounded pool of CPU threads.
* **Out-of-Core I/O:** Automatically spills to disk and manages multi-way merges when sorts or shuffles exceed the configured RAM limits.
* **Incremental Caching:** Operates with "Make-like" behavior, comparing file modification times against `.ack` files to intelligently skip partitions that do not need to be rebuilt.
* **Schema-Registered Binary Streams:** An internal datatype registry maps C structs to serialization, partitioning, and sorting callbacks to centralize serialization logic and reduce direct raw-buffer manipulation in task code.
* **Encapsulated Sub-Graphs:** Complex topologies can be wrapped into reusable `amr_pipeline_t` modules with explicit logical input and output ports.

### 🛑 When NOT to use AMR
* **Sub-millisecond latency:** AMR is a batch processing engine heavily reliant on file I/O and topological barriers. It is not a real-time stream processor.
* **Multi-node clusters:** AMR scales vertically on a single machine. It does not handle network partitions or distributed consensus.
* **Purely in-memory tasks:** If your entire dataset fits comfortably in RAM and doesn't require complex routing, standard `pthread` pools or OpenMP will be simpler and faster.

---

## 🛤️ The Core API Path

Every AMR application follows a static setup and execution lifecycle. The DAG must be completely defined before execution begins.

1. **Initialize:** Call `amr_init()` to define total Partitions, CPUs, and RAM.
2. **Register Types:** Register your structs and their behaviors via `amr_register_datatype()`.
3. **Define Tasks:** Register setup callbacks using `amr_task()`.
4. **Wire the Graph:** Inside the setup callbacks, use the Stateful Builder API:
   * Declare an input (`amr_task_input_...`).
   * Declare an output (`amr_task_output...`).
   * Bind them with a runner (`amr_task_transform()`).
   * *Note: Configuration modifiers (like sorting or formatting) apply exclusively to the most recently declared input or output port.*
5. **Execute:** Call `amr_run()` to finalize the graph, evaluate the cache, and spawn worker threads.

---

## ⚡ Hello World: Identity Copy

Here is a minimal, complete example of an AMR application that copies a raw file from input to output.

```c
#include "a-map-reduce-library/amr.h"
#include <stdio.h>

// 1. Define the task setup
bool copy_setup(amr_task_t *task) {
    // Declare an external input file
    amr_task_input_files(task, "input.txt", 1.0, NULL);
    
    // Declare the output file
    amr_task_output(task, "output.txt", 1.0);
    
    // Use the default runner (an identity byte-copy)
    amr_task_default_runner(task);
    amr_task_transform(task, "input.txt", "output.txt", NULL);
    
    return true;
}

int main(int argc, char **argv) {
    // Initialize: 16 total partitions, 4 CPU threads, 1024 MB RAM limit
    amr_t *sched = amr_init(argc, argv, 16, 4, 1024);
    
    // Register the task into the DAG
    amr_task(sched, "copy_task", true, copy_setup);

    // Finalize the graph and execute
    if (!amr_run(sched, amr_worker_complete)) {
        fprintf(stderr, "DAG Execution Failed!\n");
        amr_destroy(sched);
        return 1;
    }

    amr_destroy(sched);
    return 0;
}

```

---

## 🔀 Topology Patterns

The way you wire inputs dictates how data moves between worker boundaries. AMR provides explicit primitives for partitioned topologies:

* **1-to-1 Pipelining (`_partition`)**
* Consumer partition *X* reads exclusively from Producer partition *X*. Guarantees parallel isolation with zero cross-partition I/O overhead.


* **The Global Shuffle (`_shuffle`)**
* The producer scatters its output into *N* hash buckets. Consumer partition *X* gathers bucket *X* from all upstream producers. Used for grouping data by a specific key.


* **Many-to-Many / All-to-All (`_all_to_all`)**
* EVERY consumer partition reads ALL producer partitions. Used for matrix math or Cartesian products.


* **Many-to-One / Global Gather (`_all_to_all` to 1 Partition)**
* A specific application of the all-to-all primitive where the consumer task is configured with exactly `1` partition. Concatenates all upstream producer partitions into a single global stream.


* **Map-Side Join / Read First (`_first`)**
* Every parallel consumer partition safely reads ONLY the file produced by producer Partition `0`. Used to broadcast a global dictionary.



---

## 📦 The Datatype Registry

To use AMR's sorting and shuffling engines with structured binary records, you map your C structs into the framework using the registry.

```c
// 1. Register the struct and its serialization rules
amr_register_datatype(sched, "UserScore", "uint32 + double", 
                      score_serialize, score_deserialize, score_to_string);

// 2. Attach specific behaviors
amr_datatype_add_partition(sched, "UserScore", "Hash_ID", hash_id_func);
amr_datatype_add_compare(sched, "UserScore", "Sort_Score_Desc", sort_desc_func);

// 3. Apply them to an output in your task setup
amr_task_output_type(task, "UserScore");
amr_task_output_shuffle_by(task, "Hash_ID", NULL);
amr_task_output_sort_by(task, "Sort_Score_Desc", NULL);

```

Registering a `to_string` method allows your binary artifacts to be rendered meaningfully by inspection tools such as `--dump` and `--sample`.

---

## ⚙️ Advanced Execution Semantics

* **Failure Modes:**
* *Setup Phase:* Invalid graph topologies, unbound pipeline ports, and type mismatches fail fast during graph construction (typically triggering `abort()`).
* *Execution Phase:* A custom `amr_worker_cb` can halt the DAG execution by returning `false`.


* **Garbage Collection:** Intermediate task outputs are automatically unlinked (deleted) based on downstream reference counting. Terminal outputs, or outputs explicitly flagged with `amr_task_output_keep()`, survive execution.
* **The Sort-Merge Optimization:** If an intermediate pipe output is flagged for sorting, AMR bypasses writing the final sorted artifact to disk. Instead, the temporary sorted chunks are merged directly into the memory space of the consuming transform.
* **In-Memory Broadcast Cache:** `amr_task_input_load_into_memory()` forces AMR to read an entire input partition into RAM before the worker starts, enabling `O(1)` lookups for map-side joins.

---

## 🛠️ The Built-In CLI

Every AMR executable automatically inherits a diagnostic CLI without requiring changes to your `main()` logic.

**Execution Control:**

* `-c <N>`, `--cpus <N>`: Override the number of CPU threads.
* `-r <MB>`, `--ram <MB>`: Override the total RAM limit.
* `-t <tasks>`: Run ONLY specific tasks/partitions (e.g., `-t reduce_pairs:0,2-4`).
* `-o`: Strict mode; only run tasks specified by `-t`, ignoring upstream cache invalidation.
* `-f`, `--force`: Force tasks to run, bypassing `.ack` file timestamps.
* `--run <N>`: Isolate the execution context to a specific run directory (`tasks/run_<N>/`).
* `--new-args`: Overwrite the persisted `custom_args` cache file.

**Inspection & Diagnostics (Forces cpus = 1):**

* `-l`, `--list`: Print the logical DAG execution plan and exit.
* `-s`, `--show-files`: Print the execution plan along with all resolved physical file paths.
* `-d`, `--dump <files>`: Print the human-readable contents of specific output files.
* `--sample <recs>:<parts>`: Evaluate data skew and randomly sample *N* records from *M* active partitions.
* `--match <str>`: Lazily filter `--sample` and `--dump` outputs to only include records matching a substring.
* `--debug <task:partition> <dir>`: Run a single task partition in strict isolation, writing outputs to a custom directory.
* `--keep-files`: Globally disable intermediate file garbage collection for this run.

---

## 📚 API Documentation

The primary include for the framework, [`amr.h`](https://www.google.com/search?q=include/a-map-reduce-library/amr.h), acts as the umbrella header. The API is divided into domain-specific sub-headers (`amr_core.h`, `amr_task.h`, `amr_worker.h`, etc.) which are extensively documented.

Inside the headers, you will find:

* Detailed explanations of the **Stateful Builder API**.
* In-depth documentation on **Execution Lifecycle Constraints** (caching, thread safety, garbage collection).
* Clear function contracts for every graph topology modifier, input/output configuration, and worker context accessor.

---

## 🧪 Learning by Example

The repository includes a progressive set of examples in the `examples/` directory that demonstrate AMR features scaling from simple metrics to billion-scale recommender systems.

### Getting the Data

Most of the examples build upon real-world Amazon clickstream datasets. Before running examples `02` through `07`, run the download script in the `data/` directory to fetch the logs:

```bash
cd data
./download_data.sh
# Follow the prompts to select a dataset (e.g. 2 for amazon_2023, then 1 for Gift_Cards).
cd ..

```

### Example Directory

1. **`01_word_count`**: The MapReduce classic. Introduces DAG wiring, cross-partition shuffles, local reduction (squashing duplicates), global gathering, and text formatting.
2. **`02_streaming_recommender`**: A partitioned merge-join pipeline. Demonstrates custom datatypes, multi-input streams (`amr_task_io_transform`), and $O(1)$ memory joins for massive dictionaries.
3. **`03_pipeline_recommender`**: Introduces **Sub-Graphs**. Refactors Example 02 to show how to encapsulate complex logic into reusable, composable `amr_pipeline_t` modules with abstract ports.
4. **`04_inverted_recommender`**: Billion-scale graph processing. Overhauls the math engine to demonstrate string-to-integer encoding, bipartite graphs, and the zero-shuffle network optimization.
5. **`05_complements_recommender`**: First-order cosine similarity math. Highlights `amr_task_input_load_into_memory()` to broadcast dense dictionaries into a partitioned in-memory cache for ultra-fast lookups.
6. **`06_substitutes_recommender`**: Second-order co-occurrence. Demonstrates TF-IDF, L2 normalization, and advanced mathematical feature-vector intersections.
7. **`07_buddies`**: Reciprocal Nearest Neighbors (RNN). Enforces deterministic ordering, top-1 extraction, and complex grouped validation to find mutually exclusive pairings.
8. **`08_conditional_branching`**: Dynamic DAG routing. Introduces the **Cascading Skip** and **Union Edges** (`|`) to gracefully handle IF/ELSE control flow and safely prune starved downstream tasks at runtime.
9. **`09_chaining`**: Chained transformations. Demonstrates explicit internal routing (the "Pipe Stash"), building Map-Side Combiners, and processing contiguous arrays natively using a `group_runner`.
