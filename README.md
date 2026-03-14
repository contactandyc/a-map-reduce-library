# A-Map-Reduce Library (AMR)

**A-Map-Reduce (AMR)** is a highly optimized, single-node, multi-core DAG (Directed Acyclic Graph) execution engine for out-of-core data processing in C.

Think of it as a lightweight, blazing-fast "Apache Spark for a single machine." It is designed to handle datasets much larger than your available RAM by automatically managing multi-threaded execution, disk-spilling, sorting, and merging—all with the bare-metal performance of pure C.

---

## 🚀 Key Features

* **Multi-Core Execution:** Automatically scales across available CPU threads to process data partitions concurrently.
* **Out-of-Core Processing:** Natively handles large-scale sorting, shuffles, and merges using efficient disk-backed temp files when datasets exceed RAM limits.
* **Strongly Typed Binary Streams:** A flexible `amr_datatype_t` registry allows for zero-overhead struct serialization, custom comparators, and localized reduction logic.
* **Modular DAGs & Pipelines:** Wire together complex topologies using individual Tasks or encapsulate them into reusable Pipelines.
* **Incremental Caching:** Operates with "Make-like" behavior, intelligently skipping partitions if the upstream data hasn't changed.
* **Powerful Built-in CLI:** Every AMR application automatically inherits a robust command-line interface for testing, sampling, and debugging specific graph nodes.

---

## 🧠 Core Architecture

AMR relies on three foundational concepts:

1. **The Scheduler (`amr_t`):** The global orchestrator. You allocate it, define the total resources (RAM, CPUs, Partitions), register your Tasks, and call `amr_run()`.
2. **The Task (`amr_task_t`):** A logical step in the DAG (e.g., "tokenize_text" or "reduce_counts"). Tasks declare what inputs they need, what outputs they produce, and the function that processes the data.
3. **The Worker (`amr_worker_t`):** The physical thread executing a slice of a Task. If a Task has 16 partitions, AMR spawns 16 Workers. Workers open physical files, read records, and write outputs concurrently.

---

## ⚡ Hello World: Word Count

Here is a minimal, complete example of an AMR application that copies an input file to an output file.

```c
#include "a-map-reduce-library/amr.h"
#include <stdio.h>

// 1. Define the task behavior
bool hello_setup(amr_task_t *t) {
    // Declare an input file
    amr_task_input_files(t, "input.txt", 1.0, NULL);
    
    // Declare an output file
    amr_task_output(t, "output.txt", 1.0);
    
    // Use the default runner (an identity copy)
    amr_task_default_runner(t);
    amr_task_transform(t, "input.txt", "output.txt", NULL);
    
    return true;
}

int main(int argc, char **argv) {
    // Initialize: 16 partitions, 4 CPUs, 1024 MB RAM
    amr_t *sched = amr_init(argc, argv, 16, 4, 1024);
    
    // Register the task
    amr_task(sched, "hello_task", true, hello_setup);

    // Execute the DAG
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

## 🔀 Common Data Flow Patterns (Topologies)

The true power of AMR lies in how you wire Tasks together. AMR provides different edge types to route your data across workers:

* **1-to-1 Pipelining (`_partition`)**
* *Use Case:* Sequential mapping (parse -> enrich -> filter).
* *Behavior:* Consumer Worker X reads exclusively from Producer Worker X. This provides perfect parallel isolation with zero network/shuffle overhead.


* **The Global Shuffle (`_shuffle`)**
* *Use Case:* Grouping data by a specific key (e.g., User ID or ASIN).
* *Behavior:* The producer scatters its output into N hash buckets. Consumer X then gathers bucket X from all M producers.


* **Distributed Broadcast (`_all_to_all`)**
* *Use Case:* Matrix math or comparing every item against every other item.
* *Behavior:* EVERY consumer worker reads ALL M producer files.


* **Map-Side Join / Read First (`_first`)**
* *Use Case:* Broadcasting a small global dictionary to all workers.
* *Behavior:* Every parallel consumer thread safely reads ONLY the file produced by producer Partition 0.



---

## 📦 The Datatype Registry & Type Safety

AMR abstracts away raw byte manipulation through a centralized type registry. Instead of manually casting `void*` buffers in every task, you register your C structs along with their serialization, deserialization, partitioning, and sorting rules.

Once registered, the framework handles the disk I/O, routing, and sorting automatically:

```c
// Register your type once during setup...
amr_register_datatype(sched, "MyStruct", "Description", my_ser, my_des, my_to_string);
amr_datatype_add_partition(sched, "MyStruct", "Hash_ID", my_hash_func);
amr_datatype_add_compare(sched, "MyStruct", "Sort_Score_Desc", my_sort_func);

// ...and use it anywhere in your DAG!
amr_task_output_type(t, "MyStruct");
amr_task_output_shuffle_by(t, "Hash_ID", NULL);
amr_task_output_sort_by(t, "Sort_Score_Desc", NULL);

```

*Bonus: Because you register a `to_string` method, the built-in `--dump` and `--sample` CLI commands instantly know how to print your binary out-of-core files as human-readable text.*

---

## 🧩 Reusable Pipelines (Sub-Graphs)

For complex applications, defining a single massive DAG in `main()` quickly becomes unmanageable. AMR solves this with **Pipelines**—allowing you to encapsulate a sequence of tasks into a reusable, modular sub-graph.

Pipelines expose **logical input and output ports**, meaning the internal task implementations are completely hidden from the outside world. You wire pipelines together just like individual tasks:

```c
// Create a reusable enrichment sub-graph
amr_pipeline_t *enrich_pipe = amr_pipeline_create(sched, "enrich", pipeline_enrich_setup, NULL);

// Bind the pipeline's logical input port to an upstream task's physical output
amr_pipeline_bind_input(enrich_pipe, "in_dict", "app_ingest_items", "items.dict");

// Downstream tasks can consume straight from the pipeline's logical output port
amr_task_input_from_pipeline_partition(t, enrich_pipe, "out_enriched", 0.5);

```

---

## ⚙️ Advanced Execution & I/O Optimizations

AMR provides fine-grained control over how data is materialized, merged, and processed:

* **Group Runners (`amr_group_runner_cb`):** Instead of processing one record at a time, AMR can feed your worker an array of all records that share the exact same key, making custom reducers and distributed joins trivial to implement.
* **Direct In-Memory Merging:** When chaining tasks that require sorting, AMR optimizes away unnecessary disk I/O. Instead of writing a final, fully sorted file to disk only to immediately read it back, AMR performs a multi-way merge of the temporary sorted chunks directly into the RAM of the downstream task.
* **In-Memory Distributed Cache:** Need to broadcast a small lookup table? Use `amr_task_input_load_into_memory(t)` to force AMR to read the entire partition into RAM before your worker starts, allowing for `O(1)` memory lookups during streaming joins.

---

## 🛠️ The Built-In CLI

When you build an AMR application, it automatically includes a powerful CLI for inspecting and debugging your data pipelines without changing your code.

**Execution Control:**

* `-c <N>`, `--cpus <N>`: Override the number of CPU threads at runtime.
* `-r <MB>`, `--ram <MB>`: Override the total RAM limit.
* `-t <task[:partitions]>`: Run ONLY specific tasks/partitions (e.g., `-t reduce_pairs:0,2-4`).
* `-f`, `--force`: Force tasks to run, bypassing the incremental cache.

**Inspection & Debugging:**

* `-l`, `--list`: Print the DAG execution plan (resolving all logical ports and physical paths) and exit.
* `-d`, `--dump <files>`: Print the human-readable contents of output files utilizing the registered type's `to_string` method.
* `--sample <recs>:<parts>`: Efficiently peek into task outputs, report data skew (empty partitions), and dump N records from M randomly selected active partitions.
* `--match <string>`: Lazily filter `--sample` and `--dump` outputs to only include records matching a specific substring.

---

## 📚 API Documentation

The primary include for the framework, [`amr.h`](https://www.google.com/search?q=include/a-map-reduce-library/amr.h), is extensively documented. It serves as the definitive reference manual for the library.

Inside `amr.h`, you will find:

* Detailed explanations of the **Stateful Builder API**.
* In-depth documentation on **Execution Lifecycle Gotchas** (caching, thread safety, garbage collection).
* Clear function contracts for every graph topology modifier, input/output configuration, and worker context accessor.

---

## 🧪 Learning by Example

The repository includes a progressive set of examples in the `examples/` directory that demonstrate AMR features scaling from simple counts to billion-scale recommender systems.

1. **`01_word_count`**: The "Hello World" of MapReduce. Demonstrates basic file ingestion, network shuffles, local reduction (squashing duplicates), global gathering, and text formatting.
2. **`02_streaming_recommender`**: A distributed merge-join pipeline. Demonstrates registering custom datatypes (`HalfEnriched`, `FullEnriched`), handling multi-input streams (`amr_task_io_transform`), and processing groups of records sharing the same key (`amr_task_group_transform`).
3. **`03_pipeline_recommender`**: Introduces **Sub-Graphs**. Shows how to encapsulate the complex join logic from Example 02 into reusable, composable `amr_pipeline_t` modules.
4. **`04_inverted_recommender`**: Billion-scale graph processing. Demonstrates string-to-integer encoding, generating Inverted Indexes (bipartite graphs), zero-shuffle optimization, and passing dynamic configurations into pipelines.
5. **`05_complements_recommender`**: First-order cosine similarity math. Highlights the use of `amr_task_input_load_into_memory()` to broadcast a dense dictionary into a distributed in-memory cache for ultra-fast `O(1)` lookups.
6. **`06_substitutes_recommender`**: Second-order TF-IDF weighting and L2 normalization. Demonstrates advanced mathematical reductions, custom state passing to workers (`amr_task_transform_data`), and highly complex graph traversals.
