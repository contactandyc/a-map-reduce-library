# Conditional Branching & Union Inputs (MapReduce DAG)

This project demonstrates how to dynamically route data through different logical branches of a Directed Acyclic Graph (DAG) at runtime, skipping unneeded tasks and converging the results back into a single stream. 

In a strict, statically-defined DAG, handling IF/ELSE logic is traditionally difficult because downstream tasks expect files to exist. This framework solves this elegantly using the **Cascading Skip**, **Union Edges**, and **Pipelines**.

This directory contains two examples that achieve the exact same routing logic but use different architectural patterns:
1. `conditional_branching` (Built from `main.c`): Demonstrates raw task-to-task wiring.
2. `conditional_pipeline_branching` (Built from `main_pipeline.c`): Demonstrates encapsulating complex routing inside a reusable Pipeline.

---

## Core Concepts

* **The Cascading Skip:** By calling `amr_worker_skip_output(w, output_index)` inside a runner, you dynamically drop a physical artifact at runtime without altering the static DAG topology. If a downstream task depends *exclusively* on that skipped artifact, the scheduler intercepts the missing dependency and safely "starves" (skips) the downstream task to save CPU cycles.
* **Union Edges (`|`):** By separating input names with a pipe character (e.g., `"process_even|process_odd"`), you create a Union Edge. The framework binds the input port to *both* upstream branches, safely ignoring any branch that was dynamically skipped.

---

## Example 1: Raw DAG Routing (`conditional_branching`)
Executes four distinct logical phases, evaluating a routing condition independently for every worker partition.

1.  **The Router (Branching Logic):** Partition 0 routes to the EVEN path, Partition 1 to ODD, Partition 2 to BOTH, and Partition 3 skips BOTH.
2.  **The Diverged Branches:** Two independent tasks (`process_even` and `process_odd`) handle their specific payloads. If a branch is skipped, these tasks safely starve.
3.  **Convergence:** A final task uses a Union Edge to funnel both physical outputs back into a single logical stream.

## Example 2: Pipeline Encapsulation (`conditional_pipeline_branching`)
Demonstrates how to hide messy branching logic from the main application using the `amr_pipeline_t` API.

* **The App Layer:** Consists of `app_router` and `app_printer`. The app layer has no idea that the data diverges into Even and Odd paths. It simply passes data into the pipeline's logical input ports and reads the final result from the pipeline's single exposed `out_unified` port.
* **The Pipeline:** Encapsulates `process_even`, `process_odd`, and the `converge` task into a self-contained sub-graph. 

---

## Build and Run

**1. Compile the Examples:**
```bash
./build.sh clean && ./build.sh

```

**2. Run Example 1 (Raw Branching):**
Watch the partitions route themselves to different branches.

```bash
./build/conditional_branching

```

Inspect the output. Notice that Partition 3 produced NO file because it starved and pruned itself!

```bash
cat tasks/converge_?/final_output.txt_?

```

**3. Run Example 2 (Pipeline Branching):**
Executes the pipeline-encapsulated version.

```bash
./build/conditional_pipeline_branching

```

Inspect the app-layer output. The result is identical, but the messy internals were completely abstracted away.

```bash
cat pipeline_tasks/app_printer_?/final_output.txt_?

```
