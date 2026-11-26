# Filter materialization with retained int8 product embeddings

The [float32 control](../README.md) already had a one-bit HNSW graph scorer. Its raw vector file still consumed 16,616 bytes per product. Here the same 4,154 coordinates are retained as signed bytes, using 4,154 bytes per product. Mongot’s binary-quantized byte-vector codec builds the graph from their signs. The physical leaf size stays near 5 GiB while the number of products and filter values increases.

**0 of 700 materializations reached 500 ms.** Across the two leaves, the seven filters actually admitted 125,958 to 851,502 products per leaf (11.1% to 75.4%). The highest per-filter p95 was 25.13 ms and the slowest observation was 29.43 ms. Both were for the combined price, rating, review-count and category predicate after requesting file eviction.

| Leaf | Committed segment bytes | Distinct live products | HNSW levels | Bytes outside vector-format files |
| --- | ---: | ---: | ---: | ---: |
| 0 | 5,374,893,791 | 1,130,000 | 5 | 32,820,292 |
| 1 | 5,375,123,636 | 1,130,000 | 5 | 32,838,553 |

At the same approximate segment size, int8 retention fit **3.62× as many distinct products**. The corresponding non-vector footprint rose from about 9.2 MB to 32.8 MB per leaf. This includes IDs, numeric points and doc values, terms, and segment metadata. It is an upper bound on the files relevant to a single predicate. Both density levels still fall below the target latency on this storage and filter mix. A slow production leaf’s live vector count, filter expression, and filter-field footprint would provide a stronger sizing target than segment bytes alone.

[Per-filter counts and timing percentiles](summary.csv) · [Leaf 0 observations](leaf0.csv) · [Leaf 1 observations](leaf1.csv)

## Compound filters from the slow plans

Two production explains showed compound filters with weighted match shares of 9.6% and 78.7% across the reported documents. On the same two committed leaves we timed three public-catalog analogues, using their existing metadata. All three require at least one review, include a set of categories and exclude a numeric rating of 1. The price-capped case requires either of two category terms and price ≤ $9. The three-category case preserves the broad plan's three-term membership operator. Expanding it to six categories more closely matches the observed acceptance. Stock, website restrictions, multivalued numeric exclusions and production segment concurrency still need a representative replay. [Production plan measurements](../../../../streaming-filtered-hnsw-design.md#two-slow-catalog-queries)

| Filter | Documents passing, leaf 0 / leaf 1 | Normal p95, leaf 0 / leaf 1 | Profiled p95, leaf 0 / leaf 1 |
| --- | ---: | ---: | ---: |
| Two categories and price ceiling | 9.34% / 9.27% | 5.32 / 4.96 ms | 10.16 / 9.82 ms |
| Three categories | 65.73% / 65.69% | 18.00 / 16.91 ms | 33.22 / 32.56 ms |
| Six categories | 77.43% / 77.48% | 24.98 / 24.30 ms | 44.21 / 40.44 ms |

The timer surrounds the first `AcceptDocs.cost()` call on a fresh weight and supplier. The profiled mode creates the filter weight with this fork's `ProfilingIndexSearcher` and asserts that its root is a `ProfileWeight`. Both modes use the same rewritten filter and reader, a disabled Lucene query cache and a warm filesystem cache. Each leaf runs in its own JVM. Discard five warm-up rounds, shuffle predicate order and alternate which mode runs first for 20 measured pairs per predicate per leaf. The table uses nearest-rank p95. Every accepted count agreed between modes and with an independent count over the cohort TSVs. Across 120 measurements per mode, the maximum was 28.78 ms normally and 48.95 ms profiled. The measured difference applies to this fork's filter profiler on these leaves. The historical query runtime, profiling overhead at 76-way fan-out and latency of the HNSW walk remain unmeasured by this experiment. [Paired observations](compound-profile.csv) · [Harness](../../../../../scripts/perf/catalog_prefilter/CatalogFilterProfile.java)

To reproduce against the two existing dense leaves, use the classpath and working directory from the reproduction instructions below.

```sh
javac -proc:none -cp "$classpath" -d "$work" "$src/CatalogPrefilter.java" "$src/CatalogFilterProfile.java"
for leaf in 0 1; do
  java -Xms1g -Xmx5g --enable-preview --add-modules jdk.incubator.vector \
    --enable-native-access=ALL-UNNAMED -cp "$work:$classpath" CatalogFilterProfile \
    "$work/leaf$leaf" > "$work/results/compound-profile-leaf$leaf.csv"
done
```

## Experiment

Take the first 2,260,000 distinct product IDs in the same pinned catalog ordering and divide them into two disjoint leaves of 1,130,000 products. The earlier control products are a subset of this larger dataset. Keep the seven predicates, missing values, metadata field representations, HNSW parameters, single-query concurrency and warm/eviction protocol from the control. The control uses Lucene’s single-bit quantizer on float32 inputs. The denser run uses mongot’s sign quantizer on int8 inputs. This comparison measures filter preparation at two physical densities. It does not measure ANN recall.

For ingestion, compute `round(256 × coordinate)` on the source float32 coordinates using nearest-even rounding. Reject out-of-range values before conversion to signed bytes. A single scale across the corpus preserves the relative weighting of dimensions to rounding precision. There is no float32 vector field in the committed dense index. On reopen, verify the byte encoding, dimension, live document and graph node counts, and one-bit quantizer. Run a byte-vector kNN query as a reader check outside the timer. [Byte-vector codec routing](../../../../../src/main/java/com/xgen/mongot/index/lucene/codec/LuceneCodec.java) · [Mongot sign quantizer](../../../../../src/main/java/com/xgen/mongot/index/lucene/quantization/BinaryQuantizer.java)

Each timed accepted count matched the independent product-metadata calculation. The two committed leaves passed the graph and encoding checks. The second graph build completed on retry from its committed staging data with a 2 GiB JVM heap. Both timed processes used the same 5 GiB heap limit after all indexing had finished.

## Reproduction

Use the environment from the [control](../README.md#reproduce) and set `MONGOT_JAR` to the standalone deploy JAR from this fork with the byte-input binary codec. The two cohorts require more source shards than the control. Existing verified Parquet shards can be reused. Set `cache_dir` to a directory for the pinned Parquet shards and run these commands from the repository root.

```sh
work=$(mktemp -d)
src=scripts/perf/catalog_prefilter
cache_dir=${cache_dir:-"$work/cache"}
python "$src/prepare.py" --int8 "$cache_dir" "$work/cohorts"
classpath="$LUCENE_CORE_JAR:$MONGOT_JAR"
javac -proc:none -cp "$classpath" -d "$work" "$src/CatalogPrefilter.java" "$src/Int8CatalogPrefilter.java"
run_dense() {
  java -Xms1g -Xmx5g --enable-preview --add-modules jdk.incubator.vector \
    --enable-native-access=ALL-UNNAMED -cp "$work:$classpath" Int8CatalogPrefilter "$@"
}
for leaf in 0 1; do
  run_dense build "$work/cohorts/$leaf" "$work/staging$leaf" "$work/leaf$leaf"
done
mkdir "$work/results"
for leaf in 0 1; do
  run_dense measure "$work/leaf$leaf" "$work/results/leaf$leaf.csv" "$src/evict.py"
done
python "$src/summarize.py" "$work/cohorts" "$work/results"
```

## Replaying the production segment counts

The single-leaf benchmark measured one bitmap on an idle worker. The broad production explain measured a search over 76 segments. To isolate the effect of scheduling and profiling, we built a filter-only index with the 76 reported live-document counts, 30,787,216 live documents in total and 38,479,608 document slots. We distributed the inferred deletes proportionally among leaves and evenly within each leaf. A second index matches the 45 reported live counts for the price plan. Its other nine leaves and per-leaf delete counts are unknown, so this control uses only the 19,553,940 reported documents and assumes zero deletes.

Both indexes cycle the same 2.26 million public catalog rows and preserve each row’s fields together. Each leaf has independent postings, points and doc values. A dense doc-values field represents vector existence. This times only filter materialization and uses no graph. Segment counts and live-document populations match the respective reported rows. Segment bytes, attribute distributions, website restrictions, mixed numeric types, array fields and production hardware remain unmatched. The broad predicate admitted 23,845,758 (77.45%) and the price predicate admitted 1,819,887 (9.31%). An independent calculation on the source rows verified every leaf’s accepted count in all measured runs.

We issued one filter request at a time on a 16-logical-CPU host with a warm filesystem cache, disabled query caching, a shared fresh filter weight per request and one task per leaf. Request wall time runs from task submission until all leaf builds finish. Leaf time surrounds the first call to `AcceptDocs.cost()` after the worker starts. Process CPU includes JVM work during the request. Each row below is the median of five requests after two warm-ups per configuration.

| Workload | Workers | Normal wall | Current filter profiler wall | Every-call filter profiler wall |
| --- | ---: | ---: | ---: | ---: |
| Broad | 1 | 963 ms | 1,670 ms | 5,457 ms |
| Broad | 8 | 154 ms | 1,762 ms | 1,025 ms |
| Broad | 16 | 109 ms | 1,263 ms | 680 ms |
| Price-capped | 1 | 82 ms | 166 ms | 645 ms |
| Price-capped | 8 | 18 ms | 154 ms | 176 ms |

The broad normal run consumed a median 1.24 s of process CPU with eight workers. Enabling the current filter profiler raised this to 13.85 s and the phase took 1.76 s on the clock. Across all normal runs the slowest broad leaf took 63.5 ms. With 16 workers and the current filter profiler, 46 of 380 broad leaf measurements reached 500 ms and the maximum was 867 ms. This reproduces second-scale filter requests and even 500 ms leaf timings in an instrumented run. The 3.95 s price-capped production capture and the broad plan’s 40.06 s of CPU remain unexplained by these controls. Matching elapsed time at a chosen worker count cannot establish which costs dominated either deployment.

The earlier source measures every iterator invocation. Commit `b0968e20` added sampling for high-frequency operations after the dated production capture. The current sampler still increments a shared atomic invocation counter on each call. To test sensitivity to timing strategy, the every-call column switches just the timer selection in today's build. The experiment measures the combined effect of profiling wrappers and timers. It does not identify contention at a particular instruction or emulate a release binary. [Timing implementation](../../../../../src/main/java/com/xgen/mongot/util/timers/SamplingInvocationCountingTimer.java) · [Request observations](fanout-requests.csv) · [Broad geometry](fanout-leaves.csv) · [Price geometry](price-leaves.csv)

### Reproduction

Use the two int8 cohorts and classpath from the dense-index reproduction above. The filter-only indexes use fresh output directories. The reports use worker counts 1, 8 and 16 for the broad query and 1 and 8 for the price query. Run each configuration sequentially with two warm-ups and five retained requests. The output includes the accepted count, start and end time of each leaf build, request wall time and process CPU.

```sh
javac -proc:none -cp "$classpath" -d "$work" "$src/CatalogPrefilter.java" \
  "$src/CatalogFilterProfile.java" "$src/CatalogFilterFanout.java"
report=docs/benchmarks/vector-prefilter/catalog/int8
run_fanout() {
  java -Xms1g -Xmx4g --enable-preview --add-modules jdk.incubator.vector \
    --enable-native-access=ALL-UNNAMED -cp "${timer_classes:-$work}:$work:$classpath" CatalogFilterFanout "$@"
}
run_fanout build "$work/broad-index" "$report/fanout-leaves.csv" "$work/cohorts"
run_fanout build "$work/price-index" "$report/price-leaves.csv" "$work/cohorts" price
run_fanout measure "$work/broad-index" 8 false 5
run_fanout measure "$work/broad-index" 8 true 5
run_fanout measure "$work/price-index" 8 true 5 price
```

For the every-call sensitivity test, compile a temporary copy of the timer class and place it first on the classpath for the profiled runs. This leaves the mongot source tree and deploy JAR intact.

```sh
timer_src=src/main/java/com/xgen/mongot/index/lucene/explain/timing/ExplainTimings.java
mkdir -p "$work/everycall/src" "$work/everycall/classes"
sed 's/boolean useSampling = HIGH_FREQUENCY_TYPES.contains(type);/boolean useSampling = false;/' \
  "$timer_src" > "$work/everycall/src/ExplainTimings.java"
javac -proc:none -cp "$classpath" -d "$work/everycall/classes" "$work/everycall/src/ExplainTimings.java"
timer_classes="$work/everycall/classes" run_fanout measure "$work/broad-index" 8 true 5
```

## Streaming stage and prefix benchmark

The six-category filter admitted 77.45% of the 2.26 million products. With the default streaming budget, search plus rescoring took 11.7 ms at p95 versus 50.0 ms for ordinary filtered kNN. Mean recall@48 fell from 61.2% to 59.9%. At 9.31% acceptance, the price-capped filter had a faster median with streaming and a slower p95. These measurements leave the production routing threshold open.

The same $5–$100 price filter admitted **exactly the same 1,124,562 products** through doc values and through a standalone point range. The default streaming p95 was 4.9 ms with doc values and 423.7 ms with points. A doc-values iterator advances to candidate IDs. A point-only range can revisit matching BKD cells for each refill. The existing path builds its accepted set once. Some legacy mongot vector-index numeric filters use this point-only representation even for scalar input values.

| Filter | Acceptance | Baseline p50 / p95 (ms) | Streaming p50 / p95 (ms) | Mean recall@48, baseline / streaming |
| --- | ---: | ---: | ---: | ---: |
| Two categories with price cap | 9.31% | 23.1 / 105.9 | 7.9 / 189.5 | 50.5% / 47.4% |
| Price range using doc values | 49.76% | 4.1 / 6.2 | 2.4 / 4.9 | 65.1% / 64.5% |
| Same price range using only points | 49.76% | 4.3 / 8.1 | 189.5 / 423.7 | 65.1% / 64.5% |
| Three categories | 65.71% | 29.9 / 74.0 | 4.3 / 53.4 | 58.5% / 56.1% |
| Six categories | 77.45% | 42.6 / 50.0 | 5.2 / 11.7 | 61.2% / 59.9% |

The separate resumable cursor orders the known passing candidates by full retained-int8 similarity. Across the first 100 hits for the six-category filter, increasing the warm-up from width 32 and 256 visits to width 64 and 1,024 visits reduced mean inverted pairs from 12.86% to 0.53%. An inversion means an earlier hit had strictly lower similarity than a later hit. Mean recall@10 rose from 58.8% to 78.8%. At the larger budget the weakest query had 30% recall@10, and mean recall@100 was 62.8%. Good ordering within a prefix and finding the true nearest neighbors are different quality measures. [Prefix observations](streaming-prefix.csv)

**Method.** The stage harness uses the two physical int8 leaves above, 16 distinct held-out real query vectors, one query at a time, serial leaf execution, warm files and a disabled Lucene query cache. Each case uses 500 candidates and limit 48. Time starts at the Lucene search call and ends after both stages of mongot’s binary rescorer. BSON translation, RPC and result serialization are outside the timer. Each query and predicate gets one discarded warm-up of each mode and three measured repetitions in shuffled mode order. Each p95 is nearest-rank over 48 observations. The second streaming budget is also in the raw CSV. For the cursor experiment, each budget is sampled once per query in fixed order, including setup time. Use that experiment for prefix quality. Cold-start timing and concurrent production fan-out need separate runs. [Stage observations](streaming-stage.csv) · [Harness](../../../../../scripts/perf/catalog_prefilter/RankedStreamBenchmark.java)

Both experiments cross-check hit eligibility and uniqueness and compare IDs and scores to exhaustive filtered retained-int8 truth with document-ID tie breaking. The stage and cursor exercise different scoring pipelines. The stage uses the production two-pass rescorer after selecting its candidate pool. The cursor fully scores every admitted candidate before making it eligible for delivery. These graphs use catalog-derived binary traversal scores, and even the existing stage misses exact hits.

### Reproduce the streaming comparison

Use the dense leaves and classpath from the reproduction section, with the deploy JAR built from this implementation. Query preparation takes the first 16 unique, non-indexed products sampled every 13 rows in 128-row batches from shard 223 of the pinned release. The query file SHA-256 is `32be3c89fac9e93a6a6ad63aee02209ae721f1e84274e51bd590fc4340272792`. The file contains 16 × 4,154 signed bytes.

```sh
bazel build //:mongot_standalone_deploy.jar
classpath="$LUCENE_CORE_JAR:$PWD/bazel-bin/mongot_standalone_deploy.jar"
python "$src/prepare.py" "$work/cohorts" "$work/queries.i8" \
  --queries-from "$cache_dir"/part-00223-9336264f-afa4-46f8-b510-1aedf848eb31-c000.snappy.parquet
javac -proc:none -cp "$classpath" -d "$work" "$src/CatalogPrefilter.java" \
  "$src/CatalogFilterProfile.java" "$src/RankedStreamBenchmark.java"
for experiment in prefix stage; do
  java -Xms1g -Xmx3g --enable-preview --add-modules jdk.incubator.vector \
    --enable-native-access=ALL-UNNAMED -cp "$work:$classpath" RankedStreamBenchmark \
    "$experiment" "$work/leaf0" "$work/leaf1" "$work/queries.i8" > "$work/results/$experiment.csv"
done
```

## Continuation, filter cost and rescoring

On the six-category filter (77.45% acceptance), expanding 128 neighborhoods every 32 delivered hits reached **61.85% mean recall@48 at 18.8 ms p95**. Ordinary filtered kNN reached **61.20% at 51.7 ms p95** in the same run. This setting matched ordinary kNN's recall on 13 queries and exceeded it on three. The mean gain was 0.65 percentage points. Its paired 95% bootstrap interval was 0.00 to +1.43 points across 10,000 resamples of the 16 queries (seed 9137).

| Six-category stage | Refill every / expansions | Exact top 48 present in pool, mean | Final recall@48 | Search + rescore wall time, p50 / p95 |
| --- | ---: | ---: | ---: | ---: |
| Ordinary filtered kNN, depth trial | — | 73.70% | 61.20% | 42.8 / 51.7 ms |
| Checkpoint streaming, five-filter trial | 8 / 8 | 72.27% | 59.90% | 5.5 / 12.6 ms |
| Optimized streaming, five-filter trial | 8 / 8 | 72.66% | 59.90% | 6.0 / 12.7 ms |
| Optimized streaming, five-filter trial | 32 / 32 | 72.40% | 59.90% | 4.8 / 10.3 ms |
| Optimized streaming, depth trial | 16 / 32 | 74.48% | 61.59% | 8.1 / 16.0 ms |
| Optimized streaming, depth trial | 32 / 128 | 74.74% | 61.85% | 10.3 / 18.8 ms |

**Where the difficult query lost hits.** Query 0 reached the same first local maximum and best traversal-scored node as ordinary kNN in each leaf. The checkpoint then reopened 64 and 65 already-expanded neighborhoods in its first refill expansions. The optimized walk skips fully read level-zero neighborhoods and leaves the last one eligible if Lucene hit its visit limit, since Lucene may have truncated the final batch of scores. At the original cadence it scored 2,687 distinct nodes by candidate 500, up from 2,377, and still held only 37 of the exact top 48. The stage returned 33. At 16 / 32 it held 46 and returned 41. At 32 / 128 it held all 48 and returned 43, matching ordinary kNN. The two changes have different effects. Using 32 / 128 on the checkpoint also recovered 43. At 16 / 32 the checkpoint returned 39. [Stage observations](streaming-tuning-stage.csv)

**Point-only membership.** A 1 KiB table of candidate ID low bits lets the BKD visitor reject most scanned point IDs before binary-searching the batch. Every possible match still gets an exact ID lookup. In a dedicated point-only trial with the same continuation and 8 / 8 cadence, p95 fell from **594.0 to 67.6 ms**. At 32 / 32 it fell from **243.3 to 46.0 ms**. The pool hit count and final recall agreed on every query at each cadence. Ordinary kNN took **8.1 ms p95** in the optimized trial at **65.10% recall**, versus **64.45%** streaming. Both paths admit the same 49.76% of the catalog. Even this faster visitor scans matching cells repeatedly. At 9.31% acceptance, the price-capped compound filter also favored ordinary kNN at the tail, 108.0 ms versus 160.7 ms streaming at 16 / 32. [Point and compound trials](streaming-tuning-stage.csv)

**Rescoring the same pool.** On each six-category query, freeze the 500 candidates from optimized 16 / 32 streaming. Vary the number read from the retained int8 vectors in the existing binary rescorer and evaluate the top 48 each time. At depth 500 the final recall reached the candidate pool's 74.48% ceiling. This experiment used cached files. At 4,154 bytes per raw vector, 500 raw reads request about 2.0 MiB of vector payload, versus 0.61 MiB at depth 154. Cold reads and concurrent requests could change the cost materially. [Rescoring observations](streaming-tuning-rescore.csv)

| Full-fidelity candidates | Mean recall@48 | Rescorer alone, p50 / p95 |
| ---: | ---: | ---: |
| 154 (stage default for 500 / 48) | 61.59% | 0.93 / 1.19 ms |
| 250 | 65.50% | 1.01 / 1.15 ms |
| 500 | 74.48% | 1.22 / 1.42 ms |

**Generic cursor prefixes.** Skipping completed neighborhoods at 8 / 8 raised recall@100 from 62.8% to 67.4%, but also raised the share of inverted score pairs from 0.53% to 2.80%. Investing more work before the first delivery improved both measures. It increased the amount of retained state and the time until the first ten hits. The open-ended stream still allows a later hit to outrank an earlier one. [Warm-up and cadence sweep](streaming-tuning-prefix.csv) · [Checkpoint prefixes](streaming-prefix.csv)

| Warm-up width / visits, refill 8 / 8 | Mean recall@10 / @100 | Inverted pairs in first 100 | Mean distinct nodes at 100 | Median time to first 10 |
| --- | ---: | ---: | ---: | ---: |
| Checkpoint 64 / 1,024 | 78.8% / 62.8% | 0.53% | 1,641 | 4.1 ms |
| Optimized 64 / 1,024 | 78.8% / 67.4% | 2.80% | 2,140 | 4.2 ms |
| Optimized 128 / 2,048 | 81.9% / 71.7% | 1.78% | 2,754 | 4.4 ms |
| Optimized 256 / 4,096 | 83.8% / 75.9% | 0.37% | 3,874 | 6.2 ms |

### Reproduce the tuning trials

Use the same two leaves, query digest, JVM flags and exact truth procedure as above. Measurements used an i5-12600K and OpenJDK 21.0.13. Each implementation ran in a separate JVM with serial leaf execution and warm files. Within each stage run, the harness shuffled stock and streaming configurations for each query and repetition. Percentiles use the nearest rank over 48 stage observations per configuration. Prefix timings include open and were sampled once per query and configuration in the listed order. The observations cover two dense leaves and 16 independent query vectors. They leave cold latency, concurrent throughput, high segment fan-out and the production routing threshold open.

Compile the **current** benchmark harness once. Put its classes first on the runtime classpath. For a checkpoint run, use the deploy JAR from `c0e870c5`. For optimized runs, use the deploy JAR containing the continuation and point membership changes. To isolate point membership, extract `com/xgen/mongot/index/lucene/streaming/PointRangeMembership*.class` from the checkpoint JAR into a fresh directory (including the visitor inner class) and put that directory before the optimized JAR. An optional JVM property `-Dcatalog.filter=categories_6` or `-Dcatalog.filter=point_only_price` restricts the stage trial to one predicate.

With `run` invoking `RankedStreamBenchmark` as in the streaming reproduction, supply the following arguments after the mode and three input paths.

| Trial | Mode | Refill configurations |
| --- | --- | --- |
| Five filter shapes | stage | Checkpoint `8/8` and optimized `8/8 32/32 16/32` |
| Six-category depth | stage | Both revisions `16/32 32/128` |
| Standalone points | stage | Optimized, with each point visitor, `8/8 32/32` |
| Six-category reranking | rescore | Optimized `16/32` |
| Six-category prefix cadence | prefix | Optimized `8/8 32/32 16/32` |
| Six-category prefix warm-up | prefix | Optimized `8/8 128/2048/8/8 256/4096/8/8 64/1024/64/64` |

A pair supplies `refillEvery/refillExpansions` with warm-up 64 / 1,024. Four values also override `warmupWidth/warmupVisits`. The reranking experiment calls the production rescorer with limits 48, 125 and 500 so its geometric rule selects 154, 250 and 500 full-fidelity candidates. It times both rescoring passes and evaluates only the first 48 results. This research setting lives in the benchmark harness. [Harness](../../../../../scripts/perf/catalog_prefilter/RankedStreamBenchmark.java)
