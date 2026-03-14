# Substitutes Recommender (MapReduce DAG)

This project calculates "Substitutes" (competitor or alternative items) rather than "Complements" (items frequently bought together).

To find substitutes, the engine uses **Second-Order Co-occurrence**. Two items are considered substitutes not if they are bought together, but if they are bought in the *same context* (i.e., they share the same neighbors). This is calculated by treating a target item's first-order neighbors as a feature vector, applying TF-IDF to penalize ubiquitous "hub" items, and then calculating the Cosine Similarity between different items' feature vectors.

## The Pipeline Architecture

This DAG is composed of the base `pipeline_inv_freq` engine and the specialized `pipeline_substitutes` module. It executes in five distinct logical phases.

### 1. First-Order Intersection (`pipeline_inv_freq`)

* **What it does:** The orchestrator configures the universal graph engine to output localized, first-order raw co-frequencies (`INV_FREQ_OUT_A_WDESC`) with a minimum overlap threshold.
* **Why:** This generates the foundational dataset. Instead of using these raw counts as the final recommendation, the substitutes module treats these neighbor counts as the starting feature vectors for each item.

### 2. TF-IDF & L2 Normalization (`norm`)

* **What it does:** This task loads the global item frequencies into memory. For every item's feature vector, it applies a TF-IDF (Term Frequency-Inverse Document Frequency) penalty. If a neighbor is universally popular across the catalog, its weight is logarithmically reduced. The worker then calculates the L2 norm (the Euclidean length) of the vector, divides each feature by that norm, and truncates the vector to the top 50 strongest features.
* **Why:** TF-IDF ensures that generic items do not artificially inflate similarity scores. L2 Normalization scales every vector to a length of 1.0, meaning the subsequent dot product of two vectors will naturally yield their Cosine Similarity. Truncating to 50 features strictly bounds the $O(n^2)$ computational complexity of the final intersection.

### 3. Feature Graph Extraction (`extract`)

* **What it does:** The worker reads the normalized vectors and pivots them into a secondary bipartite graph. It outputs an inverted index (`item_to_feat.bin`) and a partitioned forward index (`partial_feat_to_item.bin`).
* **Why:** This structures the data for the final matrix multiplication, allowing the framework to efficiently find items that share the exact same features.

### 4. Second-Order Intersection (`intersect`)

* **What it does:** The worker loads its localized feature matrix into memory and streams the global inverted feature index from disk. It calculates the overlap of features between items, multiplying their normalized weights and summing the results.
* **Why:** Because the input vectors are L2 normalized, this sum of products is the exact Cosine Similarity. The zero-shuffle optimization is applied here: the worker computes the full matrix overlap but strictly emits edges originating from its local partition, resulting in an output natively sorted by Target A and Weight descending.

### 5. O(1) Memory Formatting (`main.c`)

* **What it does:** The application layer builds a `dense_dict.bin` containing the `[Integer_ID -> ASIN, Title]` mapping. The formatting task loads this dictionary and the global `item_counts` into contiguous memory arrays.
* **Why:** The final runner iterates through the sorted substitute scores and resolves the text titles via $O(1)$ array lookups (e.g., `dict[item_b]`), outputting the structured JSON lines sequentially to disk.

---

## Build and Run

Ensure your raw JSON lines logs (`meta.jsonl` and `reviews.jsonl`) are present in the `data/` directory.

**1. Compile the DAG:**

```bash
./build.sh clean && ./build.sh

```

**2. Execute the Pipeline:**
Run the executable. You can optionally specify the number of MapReduce partitions (defaults to 4).

```bash
./build/substitutes_recommender

```

**3. Inspect the Output:**
The final recommendations will be written as clean JSON lines, showcasing structurally similar alternative items rather than complementary accessories.

```bash
cat tasks/format_subs_0/substitutes.jsonl_0

```
