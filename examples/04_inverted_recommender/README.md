# Inverted Index Recommender (MapReduce DAG)

This project calculates "Frequently Bought Together" recommendations by completely overhauling the underlying mathematical engine.

Previous pipelines used a naive "Market Basket" approach: generating all possible $O(n^2)$ combinations of items within a user's cart. This can fail in real-world scenarios due to users with exceptionally large carts, which instantly flood the network with millions of intermediate pair records. Furthermore, routing and comparing raw 32-byte string ASINs across a cluster consumes excessive RAM and CPU.

This pipeline solves these bottlenecks using **String-to-Integer Translation** and **Dual Inverted Indices** (Bipartite Projection), encapsulated in a highly configurable, universal graph engine.

## The Core Pipeline (`pipeline_inv_freq`)

The mathematical engine executes in distinct logical phases:

### 1. String-to-Integer Encoding

* **Tasks:** `assign_users` and `assign_items`
* **What it does:** The framework reads the raw `[User_String, Item_String]` clickstream. It hashes and sorts the strings, assigning a dense, contiguous `uint32_t` integer ID to every unique User and Item.
* **Why:** Transforming arbitrary strings into contiguous integers shrinks the data footprint. It allows all downstream math and network shuffling to operate purely on 4-byte integers, bypassing expensive string comparisons entirely.

### 2. Bipartite Graph Extraction

* **Task:** `extract_graphs`
* **What it does:** The worker groups the binary session data to build two distinct adjacency lists:
1. `item_to_users` (Inverted Index): Maps an `Item_ID` to a list of all `User_IDs` who bought it.
2. `user_to_partial_items` (Fragmented Forward Index): Maps a `User_ID` to a partial list of `Item_IDs`. Because the data was previously partitioned by Item, this list only contains the specific fragmented slice of the user's shopping cart that was routed to this local worker.


* **Why:** To perform algebraic graph intersections, the framework needs the topology defined in both directions. Using a fragmented forward index prevents memory bottlenecks, as no single worker is ever forced to hold a user's complete purchase history in RAM.

### 3. The Dual Index Intersection (Zero-Shuffle Optimization)

* **Task:** `intersect`
* **What it does:** The worker loads its `user_to_partial_items` index into memory. It then streams the `item_to_users` inverted index from disk. For every Item (X), it computes the overlap against the local target items.
* **Why:** This computes the co-frequency mathematically via Bipartite Projection. By trading CPU compute for Network I/O, the worker evaluates the complete intersection matrix locally for its assigned items. This "Zero-Shuffle Optimization" completely eliminates the need to route and reduce intermediate edges over the network.

### 4. Configurable Output Geometry

* **What it does:** The pipeline accepts a configuration struct (`inv_freq_config_t`) allowing the caller to define the exact output state of the DAG:
* `INV_FREQ_OUT_A_WDESC`: Outputs locally, sorted by Target A and Weight descending (ideal for $O(1)$ memory formatting).
* `INV_FREQ_OUT_B_A`: Pre-shuffles the output across the network by Neighbor B (ideal for billion-scale distributed merge-joins).
* `INV_FREQ_INDEX_ONLY`: Halts the pipeline before intersection, exposing the raw bipartite graphs for custom downstream math.


* **Why:** This decoupling turns the recommender into a Universal Graph Engine. Downstream applications can reuse the complex data-prep phases without executing redundant tasks or network shuffles.

## The Billion-Scale Application Layer (`main.c`)

The application orchestrating this specific DAG demonstrates how to resolve titles for a massive catalog by utilizing the `INV_FREQ_OUT_B_A` configuration.

### Distributed Integer Merge-Joins

* **What it does:** The `build_master_dict` task creates a master dictionary of `[Integer_ID -> ASIN, Title]`. Because the mathematical pipeline output is pre-shuffled by `B`, task `join_b` executes a streaming merge to attach the neighbor titles. The data is then routed by `A`, allowing `join_a` to stream and attach the target titles.
* **Why:** Streaming the dictionaries directly from disk guarantees the memory footprint remains strictly bounded ($O(1)$ RAM), preventing Out-Of-Memory errors regardless of how large the catalog grows.

### JSON Formatting

* **What it does:** The fully enriched integer stream arrives natively sorted by `A` and descending weight. The `app_format_json` task pulls the top 20 items per group and formats the output.
* **Why:** Because the pipeline and join tasks perfectly preserve the sorting geometry, the final formatter requires no internal sorting algorithms, streaming the highest-weighted recommendations directly to disk.

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
./build/inverted_recommender 4

```

**3. Inspect the Output:**
The final recommendations are identical to the previous pipelined version, achieved through highly optimized graph math and integer joins.

```bash
cat tasks/app_format_json_0/recommendations.jsonl_0

```