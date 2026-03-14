# Streaming String-Join Recommender (MapReduce DAG)

This project demonstrates how to build a distributed "Frequently Bought Together" recommendation engine in C.

In large-scale systems, an item dictionary may be too large to load entirely into a single worker's RAM for text lookups. This pipeline addresses that memory constraint by using **Distributed Sort-Merge Joins**. This allows the framework to enrich raw co-frequency data with human-readable product titles sequentially, keeping the memory footprint strictly bounded.

## The Pipeline Architecture

This Directed Acyclic Graph (DAG) executes in 6 distinct logical phases.

### 1. Data Ingestion

* **What it does:** Workers read the raw JSONL logs (`meta.jsonl` and `reviews.jsonl`). The items task builds a master dictionary of `[ASIN, Title]`. The events task parses the clickstream into `[User_ID, ASIN]`.
* **Why:** This creates the foundational datasets. Partitioning, sorting, and reducing are applied immediately at this stage to deduplicate the raw data before it enters the heavy computational phases.

### 2. Pair Generation (The Map Phase)

* **What it does:** The framework streams user sessions, grouped by `User_ID`. For every item in a user's cart, the runner uses nested loops to emit all possible pair combinations: `[Item_A, Item_B, 1.0]`.
* **Why (The Shuffle):** The output is partitioned by `Hash_A`. This guarantees that all pairs originating from the same target item (`Item_A`) are routed across the network to the exact same worker node for accurate grouping.

### 3. Co-Frequency Reduction

* **What it does:** Workers sort their local bins by `[Item_A, Item_B]`. The framework's reduction engine collapses consecutive identical pairs into a single, global raw co-frequency score.
* **Why:** This efficiently aggregates millions of scattered `1.0` weights into actual integer counts without requiring a global memory state.

### 4. Distributed Merge Join 1 (Enriching "B")

* **What it does:** Task `filter_route_b` physically re-routes the paired data over the network based on `Hash_B` and sorts it alphabetically by `Item_B`. Task `join_b` then executes a streaming merge against the master dictionary, outputting a `HalfEnriched` record.
* **Why:** Because the master dictionary is *also* partitioned and sorted alphabetically by ASIN, both files can be streamed simultaneously. The worker only advances the dictionary pointer when the paired stream requests a new item, successfully attaching the title for `Item_B` without loading the full dictionary into RAM.

### 5. Distributed Merge Join 2 (Enriching "A")

* **What it does:** The framework routes the `HalfEnriched` records back to `Hash_A` and sorts them. The worker streams the data against the sorted master dictionary a second time to append the title for `Item_A`.
* **Why:** This completes the enrichment process. The output is a `FullEnriched` record containing the raw score, both ASINs, and both titles, executed sequentially to maintain the $O(1)$ memory footprint.

### 6. JSON Formatting

* **What it does:** The fully enriched stream arrives sorted by `Item_A` and descending weight. The worker groups the stream by `Item_A`, limits the output to the top 20 highest-weighted neighbors, and writes a nested JSONL file.
* **Why:** Truncating the stream at 20 items limits the final file size, while formatting the binary structures back into clean JSON ensures the output is immediately ready for external consumption.

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
./build/streaming_recommender 4

```

**3. Inspect the Output:**
The final recommendations will be written as clean JSON lines.

```bash
cat tasks/app_format_json_0/recommendations.jsonl_0

```