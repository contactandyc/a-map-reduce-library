# Complements Recommender (MapReduce DAG)

This project calculates "Frequently Bought Together" recommendations, but shifts the mathematical focus from **Raw Co-Frequency** to **First-Order Cosine Similarity**.

Raw co-frequency has a flaw: it heavily biases towards universally popular items (e.g., if everyone buys bananas, bananas will be recommended with everything). Cosine similarity corrects this by penalizing items that are popular everywhere, surfacing true *complements*—items that have a unique, highly specific relationship to the target item.

Architecturally, this project demonstrates **DAG Composability**. Instead of rewriting the complex data-prep logic from scratch, it imports the previous pipeline (`04_inverted_recommender`) as a Universal Graph Engine library, skipping its standard intersection and applying a custom math module to the raw data.

## The Modular Architecture

This DAG executes in three highly decoupled phases:

### 1. Universal Graph Extraction (`pipeline_inv_freq`)

* **What it does:** The orchestrator spins up the Inverted Index pipeline but configures it to run in `INV_FREQ_INDEX_ONLY` mode.
* **Why:** This tells the universal engine to perform the heavy lifting of String-to-Integer encoding and Bipartite Graph Extraction, but to halt execution *before* running its standard frequency intersection. It exposes the raw `user_to_partial_items` and `item_to_users` network graphs directly to our new custom math engine, saving massive amounts of redundant code and processing time.

### 2. Optimized Complements Scoring (`pipeline_complements`)

* **What it does:** This dedicated math pipeline links directly to the universal engine's exposed graph ports. It streams the inverted index against the fragmented forward index to find the exact overlap ($f_{AB}$) between items. It then fetches the global totals for both items ($f_A$ and $f_B$) and applies the Cosine Similarity formula:
  $\text{Cosine} = \frac{f_{AB}}{\sqrt{f_A} \times \sqrt{f_B}}$
* **Why (The Zero-Shuffle Optimization):** Just like the optimized inverted engine, this pipeline trades CPU for Network I/O. It computes the entire overlap matrix across all workers, but strictly emits edges originating from its local partition. This guarantees the output emerges perfectly partitioned and sorted by `A`, completely bypassing the network shuffle phase.

### 3. O(1) Memory Formatting (`main.c`)

* **What it does:** The application layer compiles a `dense_dict.bin` containing the `[Integer_ID -> ASIN, Title]` mapping. When the final JSON formatter spins up, it loads both the dense dictionary and the global `item_counts` into contiguous memory arrays.
* **Why:** Because the custom math engine outputs the data natively sorted by `A` and `Weight Descending`, the runner simply iterates through the top 20 scores. It resolves the text titles and review counts via instantaneous $O(1)$ RAM array lookups (e.g., `dict[item_b]`), outputting the rich JSON without requiring a localized `qsort` or any expensive distributed disk joins.

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
./build/complements 4

```

**3. Inspect the Output:**
The final recommendations will be written as clean JSON lines. You will notice that generic, highly popular items have been filtered out in favor of items with stronger relative correlations.

```bash
cat tasks/format_comps_0/complements.jsonl_0

```
