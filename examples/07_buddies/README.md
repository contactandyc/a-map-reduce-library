# Reciprocal Nearest Neighbors Recommender (MapReduce DAG)

This project extends the "Substitutes" mathematical engine to find "Buddies" or **Reciprocal Nearest Neighbors (RNN)**. 

A standard recommendation is often one-directional (e.g., Item A recommends Item B, but Item B might recommend Item C). Two items are considered *Reciprocal* Nearest Neighbors if Item A's absolute best substitute is Item B, **AND** Item B's absolute best substitute is Item A. This pipeline identifies these mutually exclusive pairings across a massive catalog.

## The Pipeline Architecture

This DAG aggressively composes existing pipelines (`amazon`, `inv_freq`, `substitutes`) to handle the heavy mathematical lifting, then applies a specialized filtering module to extract the reciprocal pairs.

### 1. Top-1 Extraction & Deterministic Ordering (`top1`)

* **What it does:** The worker reads the fully calculated substitute scores. Since the data arrives partitioned by Target A and sorted by Weight Descending, the worker simply grabs the very first record (the #1 substitute) and drops the rest. 
* **The Transformation:** Before emitting the pair, the worker enforces a deterministic ordering where `ID_A` is always less than `ID_B`. 
* **Why:** Sorting the IDs guarantees that if A chose B `(A, B)` and B chose A `(B, A)`, they will both be emitted identically as `(A, B)`.

### 2. Reciprocity Validation (`reciprocal`)

* **What it does:** The framework shuffles and sorts the `top1` stream by the newly enforced `A, B` grouping. 
* **The Transformation:** The worker evaluates the group sizes. If the group size is exactly `2`, it mathematically proves that both items selected each other as their #1 substitute. The worker averages their similarity scores and emits the final validated pair.

### 3. Flat JSON Formatting (`format_rnn`)

* **What it does:** The application layer builds a local dense dictionary of `[Integer_ID -> ASIN, Title]`. The formatting task loads this dictionary into contiguous memory alongside the global review counts.
* **Why:** The final runner streams the validated reciprocal pairs and resolves the text titles via instantaneous memory array lookups, outputting a rich, human-readable JSON lines file containing both items and their mutual similarity score.

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
./build/buddies 4

```

**3. Inspect the Output:**
The final recommendations will be written as clean JSON lines, showcasing pairs of items that share a uniquely tight, mutually exclusive feature similarity.

```bash
cat tasks/format_rnn_0/reciprocal_neighbors.jsonl_0

```
