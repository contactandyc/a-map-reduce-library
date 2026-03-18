# Chained Transformations & Map-Side Combiners

This project demonstrates how to build complex, multi-step sub-graphs inside a single MapReduce task. By chaining transformations, you can drastically reduce disk I/O, apply Map-Side Combiners to compress data early, and process grouped records natively in C.

This directory contains two examples showcasing advanced internal data routing:
1. `chained_transforms`: Demonstrates chaining sorts to build a Frequency Counter.
2. `chained_grouping`: Demonstrates a Map-Side Combiner feeding into a `group_runner` for Clickstream Analysis.

---

## Core Concepts

### 1. Explicit Internal Routing (The Pipe Stash)
Instead of writing temporary files to the permanent DAG layer, you can route data *internally* between transforms within the same task.
* **Declaration:** Mark an output as temporary using `amr_task_output_internal(t)`.
* **Routing:** The framework dynamically captures the output of Transform 1 and stashes it in memory. 
* **Ingestion:** Pass an empty string `""` as the input to Transform 2. The framework will automatically inject the internal pipe into the runner's `ins[0]` slot.
* **Automatic GC:** As soon as the task finishes, the framework automatically `unlink()`s the internal files, keeping your disk pristine.

### 2. The Map-Side Combiner
If you apply an output sort (`amr_task_output_sort_by`) and an output reducer (`amr_task_output_reduce_by`) to an internal pipe, the framework acts as a Map-Side Combiner. It sorts and squashes duplicates *before* handing the data to the next step, turning massive datasets into tiny, highly compressed streams.

### 3. The Group Runner
Standard runners process one record at a time. A `group_runner` hands your callback a contiguous array (`io_record_t *r, size_t num_r`) of all records that share the exact same key. 
* **Requirement:** The data fed into a `group_runner` **MUST** be pre-sorted by the grouping key.

---

## Example 1: Chained Sorts (`chained_transforms`)
A classic Frequency Counter (Word Count) that requires two entirely different sorting axes.
1. **Transform 1 (The Combiner):** Maps raw words to a weight of `1.0`. It outputs to an internal pipe sorted **Alphabetically** (`Sort_Str`) and reduced via summation (`Sum_W`). This squashes all duplicate words into single, aggregated records.
2. **Transform 2 (The Re-Sort):** Takes the compressed internal pipe and completely changes the sorting axis to **Frequency Descending** (`Sort_W_Desc`). It uses the framework's implicit identity runner (passing `NULL` for the callback) to let the I/O layer do all the heavy lifting.

## Example 2: Complex Grouping (`chained_grouping`)
A Clickstream Aggregator that simulates millions of raw `(Category, URL)` clicks.
1. **Transform 1 (The Combiner):** Maps clicks to a weight of `1.0`. It outputs to an internal pipe sorted by **Category AND URL** (`Sort_A_B`) and reduced (`Sum_W`). 
2. **Transform 2 (The Group Runner):** Ingests the highly compressed internal pipe. Because the data is already sorted by Category, the `group_runner` receives perfectly contiguous arrays of URLs for each Category, allowing it to easily iterate and print a formatted summary report.

---

## Build and Run

**1. Compile the Examples:**
```bash
./build.sh clean && ./build.sh

```

**2. Run Example 1 (Chained Sorts):**

```bash
./build/chained_transforms

```

Use the framework's dump tool to see the automatically re-sorted output:

```bash
./build/chained_transforms --dump chained_tasks/analyzer_0/top_words.bin_0

```

**3. Run Example 2 (Chained Grouping):**

```bash
./build/chained_grouping

```

Inspect the final text report to see how the `group_runner` formatted the pre-compressed arrays:

```bash
cat chained_group_tasks/analyzer_0/final_report.txt_0

```
