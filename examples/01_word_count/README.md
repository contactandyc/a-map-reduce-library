# Word Count MapReduce Pipeline

This project demonstrates how to structure a multi-stage Directed Acyclic Graph (DAG) using the MapReduce framework to perform a globally sorted frequency count of words in a dataset.

## The Problem

Counting items across a large dataset requires distributing the workload. However, if two different workers encounter the word "apple" independently, their local counts will be incomplete. The system must route identical words to the same worker, aggregate their counts, and merge the final results into a globally sorted list.

## The Pipeline Transformations

This pipeline accomplishes the task in four distinct stages.

### Stage 1: Ingest & Shuffle (`ingest`)

* **The Operation:** The worker reads the input file, parses the text into alphanumeric tokens, and emits a `StringWeight` record for each word with a starting weight of `1.0`.
* **The Transformation:** `amr_task_output_shuffle_by(t, "Hash_Str")` hashes the string to a specific integer and uses modulo arithmetic to assign the record to a partition. This guarantees that all instances of a specific word (e.g., "apple") are sent to the exact same worker, regardless of where they appeared in the input file.

### Stage 2: Local Aggregation (`reduce`)

* **The Operation:** Workers receive their shuffled partitions containing millions of raw `1.0` records and aggregate them into total counts.
* **The Transformations:**
* `amr_task_output_sort_by(t, "Sort_Str")`: Before the data is processed, the framework sorts the records alphabetically in memory. This groups identical strings together.
* `amr_task_output_reduce_by(t, "SumWeights")`: The framework iterates through the sorted records. When it detects consecutive identical strings, it invokes the custom `sum_weights_reducer` to combine them into a single record (e.g., merging fifty `1.0` records into a single `50.0` record) before writing to disk.



### Stage 3: Global Sort (`global_sort`)

* **The Operation:** The pipeline gathers the individually counted worker partitions and merges them into a single, cohesive ranking.
* **The Transformations:**
* `amr_task_input_from_task_all_to_all`: A worker streams the output files from every partition simultaneously.
* `amr_task_output_sort_by(t, "Sort_W_Desc")`: Instead of sorting alphabetically, this stage applies a descending sort based on the numeric weight, placing the most frequent words at the top of the file.



### Stage 4: Text Formatting (`format_text`)

* **The Operation:** The final stage translates the binary `StringWeight` types back into human-readable text. It uses `aml_buffer_setf` to cleanly format the word and its total count separated by a tab, letting the I/O layer handle the line breaks for the final `.txt` artifact.

## Build and Run

**1. Compile the DAG:**
Use the provided build script to compile the framework and the example.

```bash
./build.sh

```

**2. Create a Sample File (if you don't have one):**

```bash
echo "the quick brown fox jumps over the lazy dog but the dog is not lazy the fox is quick" > sample.txt

```

**3. Execute the Pipeline:**
Run the compiled executable and pass it your input file. You can optionally specify the number of worker partitions (defaults to 4).

```bash
./build/word_count sample.txt

```

**4. Inspect the Output:**
The final human-readable word counts will be located in the final task's output directory.

```bash
cat tasks/format_text_0/word_counts.txt_0

```
