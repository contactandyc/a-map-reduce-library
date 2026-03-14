# Pipeline Recommender (MapReduce DAG)

This project executes the exact same "Frequently Bought Together" distributed string-join algorithm as the previous example. However, it refactors the monolithic `main.c` application into a modular architecture using **Pipelines**.

As MapReduce applications grow in complexity, defining every task, datatype, and runner in a single file becomes unmanageable. The pipeline API allows you to group related tasks into encapsulated sub-DAGs (Directed Acyclic Graphs) that expose clean input and output ports.

## What is a Pipeline?

In this framework, a Pipeline is an isolated collection of MapReduce tasks that define a specific logical operation. Instead of hardcoding file dependencies (e.g., "read from `sessions.bin`"), tasks inside a pipeline read from abstract input ports (e.g., `in_sessions`) and write to abstract output ports (e.g., `out_pairs`).

The orchestrator (`main.c`) is responsible for wiring physical files or other pipelines to these abstract ports.

## The Refactored Architecture

This example splits the recommendation engine into three distinct layers:

### 1. The Application Layer (`main.c`)

* **What it does:** Handles data ingestion (`app_ingest_items`, `app_ingest_events`) and final JSON formatting (`app_format_json`).
* **Why:** The application layer is responsible for the specific domain logic. It knows how to parse JSON lines and format the final output, but it delegates the actual math and distributed joins to the underlying pipelines.

### 2. Pipeline 1: Co-Frequency (`pipeline_co_freq.c`)

* **What it does:** Encapsulates the `generate_pairs` and `reduce_pairs` tasks.
* **Ports:** * Input: `in_sessions` (Expects a user-to-item dataset).
* Output: `out_pairs` (Emits reduced co-occurrence counts).


* **Why:** This isolates the $O(n^2)$ pair generation and reduction logic. This pipeline is now a reusable module; it simply calculates graph edges and does not care if the items are ASINs, words, or user IDs.

### 3. Pipeline 2: Enrichment (`pipeline_enrich.c`)

* **What it does:** Encapsulates the complex Distributed Sort-Merge Joins (`filter_route_b`, `join_b`, `join_a`).
* **Ports:**
* Inputs: `in_pairs` (The raw scores) and `in_dict` (The master dictionary).
* Output: `out_enriched` (The final data containing text titles).


* **Why:** It isolates the complex string-joining logic. Furthermore, internal datatypes required strictly for the multi-stage join (`HalfEnriched` and `FullEnriched`) are defined and registered *inside* this pipeline setup, preventing namespace pollution in the main application.

## Pipeline Mechanics

The wiring of the DAG is handled in `main.c` using three primary binding mechanisms:

1. **Binding Task to Pipeline (`amr_pipeline_bind_input`):**
   Connects the physical output of an application task to the abstract input of a pipeline.
```c
amr_pipeline_bind_input(co_freq, "in_sessions", "app_ingest_events", "sessions.bin");

```


2. **Binding Pipeline to Pipeline (`amr_pipeline_bind_link`):**
   Chains two pipelines together. The framework routes the `out_pairs` port of the first pipeline directly into the `in_pairs` port of the second.
```c
amr_pipeline_bind_link(enrich_pipeline, "in_pairs", co_freq, "out_pairs");

```


3. **Reading from a Pipeline (`amr_task_input_from_pipeline_partition`):**
   Allows a standard application task (like the JSON formatter) to pull data from a completed pipeline's output port.
```c
amr_task_input_from_pipeline_partition(t, enrich_pipeline, "out_enriched", 0.5);

```



---

## Build and Run

Ensure you have your raw JSON lines logs (`meta.jsonl` and `reviews.jsonl`) in the `data/` directory.

**1. Compile the DAG:**

```bash
./build.sh clean && ./build.sh

```

**2. Execute the Pipeline:**
Run the executable. You can optionally specify the number of MapReduce partitions (defaults to 4).

```bash
./build/pipeline_recommender 4

```

**3. Inspect the Output:**
The final recommendations are identical to the non-pipelined version, but achieved through a modular architecture.

```bash
cat tasks/app_format_json_0/recommendations.jsonl_0

```