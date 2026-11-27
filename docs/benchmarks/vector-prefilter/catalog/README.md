# Catalog pre-filter materialization on 5 GB HNSW segments

For recall across raw float32, raw int8, scalar and binary graphs on matched product IDs, see the [codec comparison](codec-comparison/README.md).

The decision criterion is 500 ms to prepare a pre-filter on one roughly 5 GB segment. Each observation times the first iterator-backed `AcceptDocs.cost()` on a fresh Lucene weight. All qualifying observations count, including isolated slow runs.

**0 of 700 measured materializations reached 500 ms.** The slowest took 18.22 ms after file eviction was requested. The highest per-filter p95 was 11.81 ms. Both occurred on the second leaf for the conjunction of price $5–$100, rating ≥4, at least ten reviews, and category other than Books. Across the seven predicates, actual acceptance ranged from about 11% to 75%. The accepted counts for every timed run matched the source metadata.

| Physical leaf | Committed bytes | Live products / HNSW nodes | Graph levels | Observations ≥500 ms |
| --- | ---: | ---: | ---: | ---: |
| 0 | 5,373,653,634 | 312,000 | 4 | 0 / 350 |
| 1 | 5,373,628,005 | 312,000 | 4 | 0 / 350 |

The two leaves are each 5.37 GB (5.00 GiB). **99.83% of each leaf is vector-format files**, including the raw vectors, quantized values and graph. All other segment files together occupy about 8.8 MiB per leaf. That distinction matters for workload selection: a large vector payload contributes to segment size without requiring the filter to read those bytes. These controls give no evidence for a streaming crossover at any acceptance fraction.

To reproduce a slow production case, match its filter expression, live vector count and filter-field footprint as well as its total segment size. The cache state and storage medium also matter. A public option for higher document density is the Big ANN filtered track, with 10 million 192-dimensional uint8 CLIP vectors, real image tags, and supplied filtered queries. It exercises a different kind of predicate and has not been timed here. [Big ANN filtered track](https://big-ann-benchmarks.com/neurips23.html)

[All filter counts and p50/p95/max times](summary.csv) · [Leaf 0 samples](leaf0.csv) · [Leaf 1 samples](leaf1.csv)

The [retained-int8 experiment](int8/README.md) fits 1.13 million products per 5 GiB leaf and repeats the same seven filters.

## Corpus and index

Superlinked publishes 10.53 million products derived from Amazon Reviews 2023, with price, rating, review count, category and precomputed 4,154-dimensional multimodal embeddings. The vectors include text, image and structured attributes, so vector neighborhoods can reflect the filter attributes. We used the publisher’s `benchmark-10M` split at revision `9baf92ae5fb8f4746ed227fa6c88e66c7783fa23`. [Dataset and embedding description](https://huggingface.co/datasets/superlinked/external-benchmarking/blob/9baf92ae5fb8f4746ed227fa6c88e66c7783fa23/README.md)

In shard-name and row order, take the first 624,000 distinct parent ASINs and divide them into two disjoint cohorts of 312,000. Verify each shard against its published SHA-256. Keep the supplied embeddings in dimension order, cast to float32, and index the supplied filter values. About 40% of products in each cohort lack a price. Missing fields stay missing.

Build one committed Lucene segment per cohort with the pinned 10.4.0-5 fork. Both carry the original float32 vectors and a single-bit quantized HNSW index (M=16, construction beam width 100). Verify the segment bytes, one graph node per document, and the presence of graph levels on reopen. Index scalar numeric fields as points and numeric doc values, and category as an exact term. Each document has one embedding and there are no deletes.

## Timing protocol

- Run a single query at a time on local NVMe, OpenJDK 21, Linux and an Intel Core i5-12600K. Disable Lucene query caching. Run both segments after index construction finishes.
- Use seven predicates drawn from the catalog fields. Per leaf, run three warmup rounds followed by 25 measured rounds, shuffling predicate order within each round. Before each pair of trials, close the reader and request eviction of just this index's files with Linux `POSIX_FADV_DONTNEED`. Reopen the reader, time the first materialization, then repeat with a fresh weight on the warm reader. Eviction is advisory and storage-controller caches may remain warm.
- Build the Lucene filter with vector-field existence outside the timer. Time scorer creation, consuming its iterator and obtaining the accepted-set cardinality inside `cost()`. Exclude reader opening, query rewrite, weight creation, graph traversal, and response handling. This isolates the Lucene preparation step under discussion. Report wall-clock time and compare accepted counts to independent predicates over the source TSVs. The original product metadata stays paired with each vector throughout.

## Reproduce

Use Linux, JDK 21 and Python with NumPy and PyArrow. Set `LUCENE_CORE_JAR` to the pinned fork jar. Run from the repository root. Use a new working directory with space for the source shards, exported vectors, staging files and final indexes. Both index builds must finish before timing.

```sh
work=$(mktemp -d)
src=scripts/perf/catalog_prefilter
python "$src/prepare.py" "$work/cache" "$work/cohorts"
javac -cp "$LUCENE_CORE_JAR" -d "$work" "$src/CatalogPrefilter.java"
run_catalog() {
  java -Xms1g -Xmx5g --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
    -cp "$work:$LUCENE_CORE_JAR" CatalogPrefilter "$@"
}
for leaf in 0 1; do
  run_catalog build "$work/cohorts/$leaf" "$work/staging$leaf" "$work/leaf$leaf"
done
mkdir "$work/results"
for leaf in 0 1; do
  run_catalog measure "$work/leaf$leaf" "$work/results/leaf$leaf.csv" "$src/evict.py"
done
python "$src/summarize.py" "$work/cohorts" "$work/results"
```
