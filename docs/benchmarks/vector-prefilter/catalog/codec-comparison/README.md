# Filtered HNSW recall across vector representations

This comparison separates rounding a stored vector, scoring neighbors during graph traversal and choosing which candidates receive a retained-vector score. Each recall number uses the fraction of exact filtered document IDs in the first 10, 20, 50 or 100 results. Document ID breaks score ties.

**The one-bit score accounts for most of the absolute recall loss on the broad catalog query.** On 2.26 million products at 500 candidates, streaming recovered 99.02% of the exact int8 top 100 using a raw int8 graph. The one-bit graph's pool contained 77.31%, and the default binary reranker returned 64.16%. These observations separate a 21.70-point difference in candidate coverage from a further 13.16 points lost when the reranker selected which candidates received a full-vector score. Raw int8 beat the default binary policy on 59 queries, tied on four and trailed on one. Ordinary kNN also shows a large binary penalty. Across the dense replay, 1,664 distinct per-query final-recall comparisons and their pool hit counts reproduced the earlier binary results. [Earlier dense benchmark](../int8/README.md#independent-holdout)

**Use the available scores differently by representation.** Raw float32 reached 99.30% recall@100 on the broad matched filter with 500 candidates. Raw int8 recovered 99.34% against exact int8, or 96.20% against exact float32. These two paths reuse their graph scores. The scalar float32 pool contained 99.19% of the original top 100, with 95.27% in the first 100 by scalar score. Full-pool scoring improved scalar recall@100 on 63 queries and tied on one. Scalar float32 now supports the same per-request full-pool rescoring switch as binary. On the binary/int8 graph, the public cursor's first 100 results reached 91.16% against float32 when it ranked passing discoveries by retained-vector score before delivery. That is a different candidate budget from reranking 500 survivors. The cursor can read many more original vectors.

**Filter acceptance also matters with full-precision traversal.** At 500 candidates, ordinary float32 kNN recovered 99.48% on the price-capped predicate. Float32 streaming recovered 79.80%, counting one failed request as zero. At 4,000 candidates it exhausted the node budget on 17 of the 64 queries. This supports keeping that filter on ordinary kNN in this fixture. An automatic crossover needs evidence across more filters and segment distributions.

The two binary codecs also see different bits. With this catalog’s 256× round-to-even conversion, a positive coordinate at or below 1/512 rounds to zero. That changes the sign bit of 6.66% of all product coordinates and 6.55% of the held-out query coordinates. The sign rankings therefore depend on which representation the index retained.

## Adaptive streaming defaults

Both search stages accept `stream: true`. This run compares the finite stage with ordinary filtered ANN using the same index, query, predicate, candidate count (500), limit (100) and default rescoring policy. The first six rows cover the same 624,000 real products, with 312,000 per segment and 77.41% acceptance. The dense pair has 1.13 million products per segment, consuming 4.77 GB each for raw int8 or 5.37 GB each for binary int8. The 50-segment row uses the first cohort repartitioned into unequal, category-clustered segments of 4,186–95,737 products.

| Index | Classic p50 | Stream p50 | Classic p95 | Stream p95 | Classic recall@100 | Stream recall@100 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Raw FP32 | 17.4 ms | 17.1 ms | 23.2 ms | 18.9 ms | 99.11% | 99.17% |
| Raw int8 | 15.8 ms | 14.0 ms | 20.6 ms | 15.7 ms | 99.05% | 99.12% |
| Scalar FP32 | 16.4 ms | 14.2 ms | 21.3 ms | 16.8 ms | 95.14% | 95.20% |
| Auto-binary FP32 | 17.6 ms | 14.6 ms | 20.1 ms | 16.5 ms | 60.88% | 61.25% |
| Auto-binary int8 | 13.4 ms | 10.5 ms | 15.9 ms | 12.5 ms | 60.70% | 61.17% |
| Packed bits | 12.5 ms | 9.1 ms | 14.4 ms | 10.4 ms | 98.06% | 98.23% |
| Dense raw int8 | 45.6 ms | 16.3 ms | 57.2 ms | 18.8 ms | 98.62% | 98.69% |
| Dense auto-binary int8 | 40.6 ms | 13.2 ms | 45.4 ms | 15.5 ms | 63.19% | 63.25% |
| 50-segment FP32 | 41.2 ms | 41.4 ms | 52.9 ms | 44.7 ms | 98.94% | 98.83% |

Recall is measured against exhaustive scoring of the retained representation. This means Euclidean distance on the original floats for scalar and automatic binary/FP32, Euclidean distance on the retained bytes for automatic binary/int8, and Hamming distance on the stored bits for packed-bit search. The two packed-bit segments each occupy 0.185 GB. Document ID breaks score ties. [All recall cutoffs and latency percentiles](stream-defaults.csv) · [Per-query observations](stream-defaults-observations.zip)

A single in-process client used serial leaf execution and disabled the Lucene query cache. Index files were read before each run. Each of 64 held-out queries ran three times per mode with interleaved trial order. The table shows the median and interpolated p95 of the 64 per-query median elapsed times. The two-segment timer sums candidate search and default rescoring. At limit 100 the binary reranker runs before any diagnostic full-pool rerank. Other cutoffs reuse the same retrieved pool, with the smaller binary reranks timed after the diagnostic rerank. The 50-segment timer covers search through the returned 100 hits. These measurements exclude RPC and downstream document materialization. Each configuration completed all measured trials.

To reproduce the two-segment default runs, follow the existing input and graph-build instructions below. Run `CatalogCodecBenchmark KIND LEAF0 LEAF1 holdout.f32 TRUTH` with `-Dcatalog.stream=true -Dcatalog.filter=categories_6 -Dcatalog.candidates=500 -Dcatalog.codecRepeats=3`. For the packed-bit index use `CatalogCodecIndex bit` on both original byte cohorts, then `CatalogCodecBenchmark bit-truth LEAF0 LEAF1 holdout.f32 bit-truth.tsv`. The build and queries pack the positive signs of the rounded int8 coordinates and zero-pad the final byte. Use this Hamming truth file with `KIND=bit`.

## Automatic routing

The acceptance and reader-size results below describe the earlier prototype. The current planner makes a separate decision for each segment. [Current per-segment sweep](#per-segment-routing)

On the two 312,000-vector FP32 segments, acceptance alone gave an overly optimistic prediction for forced streaming. At 69.70% acceptance it found 95.56% of the exact top 100, while classic found 99.03%. At 77.41% it found 99.17%, compared with classic at 99.11%. This acceptance sweep motivated the provisional 70% pass-rate threshold. It used the same catalog for calibration and replay. The size sweep below adds a minimum estimated matching count.

| Predicate | Actual acceptance | Sample estimate | Selected | Auto p50 | Auto p95 | Classic recall@100 | Forced stream recall@100 |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| Price capped | 9.37% | 9.18% | classic | 68.6 ms | 77.9 ms | 99.48% | 81.2% |
| Two categories | 54.58% | 51.56% | classic | 14.06 ms | 49.41 ms | 98.73% | 91.94% |
| Four categories | 69.70% | 66.41% | classic | 15.57 ms | 39.17 ms | 99.03% | 95.56% |
| Six categories | 77.41% | 74.41% | stream | 16.77 ms | 18.56 ms | 99.11% | 99.17% |

The replay interleaved classic, forced streaming and auto on 64 real catalog queries, three repetitions each, with 500 candidates and limit 100. Latencies cover a warm serial Lucene search, including the routing decision on auto trials. Each query contributes its median to the reported percentiles. Recall uses an exhaustive filtered FP32 reference for each predicate. On this fixture the price-capped filter had the widest streaming latency spread. The slow production prefilter workload remains a separate validation target.

To replay these queries against the current planner, run `CatalogStreamRoutingBenchmark FLOAT_LEAF0 FLOAT_LEAF1 holdout.f32` with the catalog classpath and JVM settings below. [Full latency and recall results](auto-routing.csv) · [Per-query observations](auto-routing-observations.zip)

### Earlier reader-wide routing by matching count

The size sweep holds the query vectors, four predicates, candidate budget (500), limit (100), and segment count (two) fixed. For each of the two smaller sizes, take the first 16,384 or 32,768 products and their corresponding embeddings from **each** original catalog cohort and build a fresh FP32 HNSW segment with `CatalogCodecIndex`. Run `CatalogStreamRoutingBenchmark` against both segments with the original 64 holdout queries. Each size contributes 2,304 interleaved searches. Separately run `CatalogFilterProfile` on each leaf to time filter materialization. The table uses the six-category filter, the complete live matching count across both leaves, and the median of 20 warm unprofiled materializations per leaf pooled across the two leaves.

| Vectors per reader | Matching products | Build filter per leaf, p50 ms | Classic search p50 ms | Forced stream p50 ms | Auto path | Auto recall@100 |
| ---: | ---: | ---: | ---: | ---: | --- | ---: |
| 32,768 | 25,425 | 0.70 | 3.87 | 14.48 | classic | 99.31% |
| 65,536 | 50,821 | 1.02 | 5.02 | 14.06 | classic | 99.25% |
| 624,000 | 483,068 | 6.81 | 17.91 | 16.38 | stream | 99.17% |

The earlier reader-wide implementation required both an estimated acceptance of at least 70% and at least `max(450,000, 32 × numCandidates)` estimated matching vectors on the reader. Its floor was a conservative deployment choice near the 464,343 estimated matches of the largest broad-filter case. The sweep brackets the crossover between reader sizes without locating it precisely. These warm FP32 timings do not establish the floor for other predicates, quantized indexes, cold caches or the production slow plans. [All four predicates and three modes](auto-routing-by-size.csv) · [Per-query searches and per-leaf filter timings](auto-routing-by-size-observations.zip)

## Per-segment routing

Each observation in the calibration sweep runs against one physical HNSW segment. The largest FP32 graph holds 624,000 real product vectors in one segment. The dense raw and binary int8 graphs each hold 1,130,000 products per segment. Each size uses the same four limits (10, 20, 50, 100), both the standard candidate budget (100, 160, 400, 800 respectively) and 500 candidates, and paired classic and forced-stream trials. All six formats have 32,768- and 312,000-vector segments. FP32, scalar and binary / FP32 also have 156,000-vector segments.

The table reports **classic p50 divided by forced-stream p50** for the six-category compound filter (about 77% acceptance), limit 20 and 160 candidates. A value above 1 indicates faster streaming. Empty cells indicate unmeasured configurations.

| Format | 32,768 | 156,000 | 312,000 | 624,000 | 1,130,000 |
| --- | ---: | ---: | ---: | ---: | ---: |
| FP32 | 0.25× | 0.58× | 0.94× | 1.60× | — |
| Scalar 7-bit | 0.31× | 0.72× | 1.19× | — | — |
| Binary / FP32 | 0.53× | 0.93× | 1.40× | — | — |
| Raw int8 | 0.30× | — | 1.12× | — | 3.08× |
| Binary / int8 | 0.36× | — | 1.53× | — | 3.81× |
| Packed bits | 0.31× | — | 1.59× | — | — |

**Mixed-reader verification.** A second cohort supplies the large 312,000-vector leaf alongside a 32,768-vector leaf from the first cohort. Sixteen separate query vectors (indices 32–47) and exhaustive truth over the combined population verify the global result. Auto includes planning and uses the production mongot float or byte query with its per-leaf hook and ordinary global kNN merge. Binary queries use production two-stage rescoring after merge. At limit 20 and 160 candidates, the small segment always chose classic.

| Format | Large leaf | Classic / auto p50 ms | Classic / auto p95 ms | Classic / auto recall@20 |
| --- | --- | ---: | ---: | ---: |
| FP32 | classic | 9.03 / 9.09 | 12.12 / 13.02 | 97.81% / 97.81% |
| Scalar 7-bit | stream | 8.42 / 8.60 | 10.63 / 9.61 | 94.06% / 94.38% |
| Binary / FP32 | stream | 8.71 / 7.25 | 9.60 / 8.31 | 76.88% / 77.50% |
| Raw int8 | stream | 8.35 / 8.46 | 10.61 / 9.46 | 97.81% / 98.12% |
| Binary / int8 | stream | 7.53 / 6.01 | 8.02 / 6.77 | 78.44% / 79.06% |
| Packed bits | stream | 7.08 / 5.42 | 8.32 / 6.74 | 99.69% / 99.69% |

**Provisional policy.** FP32 requires at least 450,000 estimated live matches **in one segment**. Scalar, raw int8, binary and packed-bit segments require 225,000. Small segments skip sampling. Eligible segments must also reach 70% estimated acceptance. At 156,000 vectors even binary / FP32 ranged from nearly even to slower on the compound filter. At 312,000, raw FP32's initial mixed plan took 13% longer at the two small budgets despite modest recall gains, which prompted the higher raw-FP32 floor. The first mixed FP32 trial and the replay with the final floor are both in the archive. An independent test of the final route on one 624,000-vector FP32 segment (probe indices 48–63) took 9.03 ms versus 13.47 ms for classic at k=20, with recall 97.50% versus 95.62%. At k=100 the recall difference was -0.19 percentage points.

Both the limit and the candidate budget matter. Auto covers limits 10–100 and candidate counts between 500 and the resolved default for that limit, inclusive in either order. Binary / FP32 caps this at 500 candidates: the 100/800 single-segment test at 312,000 vectors lost 0.67 recall points for roughly equal median latency. Larger budgets and other limits stay on classic until calibrated. Single positive indexed ranges also stay on classic. The 98%-passing standalone rating filter was substantially cheaper for classic across formats. A compound query with a range can still select streaming. The thresholds interpolate sparsely sampled operating points on this corpus. Filter shape, vector dimensions, similarity metric, cache state, fan-out and query concurrency can move the crossover.

**Measurement.** These are warm, serial in-process measurements on a 12th-gen Intel i5-12600K with Temurin 21.0.13, a 2 GB Java heap, the incubator vector module and Lucene query caching disabled. Each main single-segment placement uses 24 query vectors (indices 0–23), three warm-ups, two measured repetitions and a shuffled trial order. Predicates pass roughly 9%, 55% and 77%. A separate 312,000-vector sweep uses the same first 16 probes, a four-category predicate (69.8%) and a standalone positive rating range (97.9%). The mixed-reader run uses the same stage limit and candidate budget in every mode. Latency includes candidate retrieval and the normal binary rerank, and excludes exhaustive scoring, RPC and document fetches. Recall is against the exact eligible top k in the retained representation (FP32 Euclidean, retained int8 Euclidean, or Hamming for packed bits), with doc ID as the tie breaker. Recall across codecs therefore uses different references. Percentiles are over per-query medians. The summary includes paired query-bootstrap intervals for recall deltas; 16–24 queries give limited precision, particularly at p95.

**Isolated prefilter cost.** Across 500 unprofiled materializations (5 sizes × 5 predicates × 20 repeats), the maximum was 29.76 ms. On 312,000 vectors the six-category compound filter took 6.66 ms median and the standalone rating range took 0.20 ms. At 1.13 million those medians were 23.60 ms and 0.60 ms. The separate 500 explain-profiled trials are also archived. These controls did not reach the 500 ms production case. They show why a large matching population and a high pass rate alone can overpredict a streaming win.

**Reproduction.** Build graphs with the catalog inputs and JVM/classpath instructions below. For 32,768 or 156,000 vectors take that many rows and matching vector records from cohort 0. For 624,000 concatenate both complete FP32 cohorts before building a single graph. Use the existing dense int8 cohort for 1,130,000. Compile the benchmark sources against the current build. Run the following on those directories.

```sh
java $JVM -cp "$CP" CatalogLeafRoutingBenchmark KIND LEAF holdout.f32 24
java $JVM -Dcatalog.boundaryFilters=true -cp "$CP" CatalogLeafRoutingBenchmark KIND LEAF holdout.f32 16
java $JVM -cp "$CP" CatalogLeafRoutingBenchmark KIND SMALL_LEAF holdout.f32 16 LARGE_LEAF 32
java $JVM -Dcatalog.verifyRouting=true -Dcatalog.probeStart=48 -cp "$CP" CatalogLeafRoutingBenchmark f32 LARGE_FP32_LEAF holdout.f32 16
java $JVM -Dcatalog.routingFilters=true -cp "$CP" CatalogFilterProfile LEAF
python3 scripts/perf/catalog_prefilter/summarize_leaf_routing.py leaf-routing-observations.zip > leaf-routing-summary.csv
```

[Full latency and recall matrix](leaf-routing-summary.csv) · [Individual trials and isolated bitmap timings](leaf-routing-observations.zip)

## Results

The six-category predicate accepted 483,068 of 624,000 products (77.41%). The price-capped predicate accepted 58,487 (9.37%). On the dense index the corresponding counts were 1,750,428 and 210,315. Committed decimal GB per leaf, rounded to two places, were 5.20 for raw float32, 1.32 for raw int8, 6.50 for scalar, 5.37 for binary/float32 and 1.48 for binary/int8. The dense raw int8 and binary/int8 pairs used about 4.77 and 5.37 GB per leaf, respectively.

### Precision before graph search

On the broad filter, exact int8 top-100 IDs overlap the float32 reference by 96.59%. The two exhaustive binary controls below show what happens when only the sign bits determine the candidate order. All cells count original float32 top-100 IDs.

| Exhaustive sign ranking | In first 100 | In first 500 | In first 4,000 |
| --- | ---: | ---: | ---: |
| Binary / float32 | 39.80 | 70.95 | 86.84 |
| Binary / int8 | 42.05 | 76.34 | 92.38 |

### Candidate discovery and final scoring

Broad filter, 500 candidates, percentages against the original float32 top 100. The prefix uses the score returned by the finite query before reranking. The pool column counts all available exact hits. On int8 indexes, exact ranking within that pool uses the retained int8 vectors; occasionally a source-float neighbor loses its slot during this final ranking.

| Codec | Ordinary default | Streaming prefix | Streaming pool | Streaming default | Streaming full |
| --- | ---: | ---: | ---: | ---: | ---: |
| Raw float32 | 99.11 | 99.30 | 99.30 | 99.30 | 99.30 |
| Raw int8 | 96.05 | 96.20 | 99.28 | 96.20 | 96.20 |
| Scalar 7-bit / float32 | 95.14 | 95.27 | 99.19 | 95.27 | 99.19 |
| Binary / float32 | 60.88 | 39.61 | 70.36 | 61.50 | 70.36 |
| Binary / int8 | 60.42 | 42.00 | 76.12 | 60.97 | 75.52 |

Streaming with full-pool scoring on the same broad filter. Each row reports source-float recall at the requested cutoffs. The scalar row uses the opt-in retained-float rescorer.

| Codec | Pool | @10 | @20 | @50 | @100 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Raw float32 | 500 | 99.53 | 99.22 | 99.34 | 99.30 |
| Raw int8 | 500 | 95.62 | 94.84 | 96.09 | 96.20 |
| Scalar 7-bit / float32 | 500 | 99.38 | 99.22 | 99.25 | 99.19 |
| Binary / float32 | 500 | 85.47 | 80.86 | 76.03 | 70.36 |
| Binary / int8 | 500 | 87.81 | 84.53 | 80.94 | 75.52 |
| Raw float32 | 4000 | 99.69 | 99.38 | 99.59 | 99.52 |
| Raw int8 | 4000 | 95.94 | 95.00 | 96.12 | 96.36 |
| Scalar 7-bit / float32 | 4000 | 99.69 | 99.69 | 99.75 | 99.61 |
| Binary / float32 | 4000 | 92.19 | 90.47 | 88.06 | 85.77 |
| Binary / int8 | 4000 | 93.75 | 91.95 | 91.62 | 89.86 |

### Dense int8 replay

Percentages against the exact dense int8 top 100, with filtered ordinary kNN as the control. Pool size, product IDs, query vectors and exact truth match across the raw and one-bit graphs. The final two columns are the streaming and ordinary default search-plus-rerank p95 in ms, including failed attempts.

| Filter | Codec | Pool | Ordinary default | Streaming default | Streaming full | Stream capped / 64 | Stream p95 | Ordinary p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| categories_6 | Dense raw int8 | 500 | 98.62 | 99.02 | 99.02 | 0 | 24.14 | 55.89 |
| categories_6 | Dense binary / int8 | 500 | 63.19 | 64.16 | 77.31 | 0 | 17.14 | 50.36 |
| categories_6 | Dense raw int8 | 4000 | 99.44 | 99.45 | 99.45 | 0 | 79.14 | 91.73 |
| categories_6 | Dense binary / int8 | 4000 | 73.34 | 73.77 | 90.77 | 0 | 56.80 | 67.80 |
| price_capped | Dense raw int8 | 500 | 99.28 | 63.70 | 63.70 | 14 | 297.54 | 278.81 |
| price_capped | Dense binary / int8 | 500 | 52.78 | 49.31 | 62.38 | 0 | 118.26 | 105.82 |
| price_capped | Dense raw int8 | 4000 | 99.78 | 63.73 | 63.73 | 21 | 314.09 | 260.02 |
| price_capped | Dense binary / int8 | 4000 | 54.41 | 37.80 | 62.64 | 19 | 217.44 | 127.10 |

### Sensitivity to the walk and the filter

The smaller walk uses the shipped streaming defaults (width 64, 1,024 visits, 8 expansions every 8 hits). Both walks use 500 candidates on the broad matched filter. The first two recall columns use the normal finite-stage policy. The cursor column uses the first 100 emitted hits with the wider walk. All recall here is against the original floats. Timings are search-only p95 in ms for the finite walk and time through the 100th emission for the cursor.

| Codec | Default walk @100 | Wider walk @100 | Cursor @100 | Default walk ms | Wider walk ms | Cursor ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Raw float32 | 98.55 | 99.30 | 99.25 | 17.81 | 27.63 | 19.95 |
| Raw int8 | 95.67 | 96.20 | 96.19 | 12.55 | 21.66 | 16.40 |
| Scalar 7-bit / float32 | 94.73 | 95.27 | 99.20 | 14.04 | 21.34 | 118.06 |
| Binary / float32 | 60.72 | 61.50 | 85.27 | 9.44 | 13.97 | 66.63 |
| Binary / int8 | 60.22 | 60.97 | 91.16 | 9.74 | 13.78 | 19.52 |

The price-capped matched workload exposes the cost of walking toward a dense neighborhood with few passing products. These values use the 4,000-candidate pool, source-float truth and standard finite-stage ranking.

| Codec | Ordinary @100 | Streaming @100, all | Streaming @100, successful | Stream capped / 64 | Stream p95 ms | Ordinary p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Raw float32 | 100.00 | 72.94 | 99.32 | 17 | 406.05 | 41.73 |
| Raw int8 | 96.00 | 70.47 | 95.96 | 17 | 310.74 | 33.59 |
| Scalar 7-bit / float32 | 93.95 | 69.12 | 94.13 | 17 | 304.57 | 34.71 |
| Binary / float32 | 48.00 | 47.19 | 47.94 | 1 | 226.58 | 56.77 |
| Binary / int8 | 48.28 | 50.02 | 50.02 | 0 | 168.63 | 23.84 |

[All finite cutoffs, both filters and latency components](summary.csv) · [Default-walk comparison](default-walk.csv) · [Cursor prefixes](prefix-summary.csv) · [Per-query rows, exhaustive controls and truth IDs](observations.zip)

## Matched experiments

The main experiment uses the same 624,000 products, filters, Euclidean similarity and 64 held-out source queries across five independently constructed HNSW indexes. Each index has two single-segment leaves of 312,000 real products with 4,154 coordinates. Each leaf uses M = 16 and construction beam width 100. The two query workloads are a broad six-category filter and a two-category filter with a price ceiling. Accepted IDs come from the complete predicate. Index generation preserves product order. The harness checks the ASIN-to-document mapping, accepted IDs, graph sizes, vector encodings, sampled retained coordinates and quantization bits on reopen. Independent graph construction permits differences in graph topology between codecs.

| Index | Retained coordinates | Traversal score | Finite-stage default |
| --- | --- | --- | --- |
| Raw float32 | Original float32 | Float32 Euclidean | Reuse the traversal score |
| Raw int8 | Signed byte, round-to-even at 256× scale | Int8 Euclidean | Reuse the traversal score |
| Scalar 7-bit | Original float32 | Lucene scalar-quantized score | Use the returned score |
| Binary / float32 | Original float32 | Sign-bit similarity | Approximate reranking, then full-vector scoring of a subset |
| Binary / int8 | Rounded int8 | Sign-bit similarity of the rounded coordinates | Same two-stage binary reranker |

A second comparison uses the same 2,260,000 products and 64 queries as the previous dense int8 experiment. It pairs freshly built raw int8 graphs with the existing binary/int8 graphs, at 1,130,000 products per leaf. Exact retained-int8 neighbors are the reference at this density. Only the 624,000-product experiment measures recall against source float32.

## Method

**Exhaustive controls.** The matched corpus is exhaustively scored four ways over the same eligible IDs. Float32 Euclidean supplies the common reference. Int8 Euclidean isolates ingestion rounding. Exact Hamming rankings of float32 signs and int8 signs measure what one-bit scoring alone can recover, including after taking 500 or 4,000 candidates. A tie at the budget boundary is resolved by ascending document ID. Hamming overlap is a reference for that scoring policy. A graph can occasionally retrieve a better Euclidean candidate from outside that Hamming prefix.

**Finite search.** Ordinary filtered kNN and opt-in streaming return the same global pool size per trial. Streaming uses warm-up width 2,048, 8,192 visits per leaf, 256 expansions every 128 hits and a request-wide 250,000-node cache cap. The output records four distinct quantities for each k.

- Prefix recall counts exact neighbors in the first k hits by the score returned from the graph query.
- Pool recall counts exact top-k neighbors anywhere in the candidate pool.
- Default recall runs the production binary policy separately at each k. It scores at most `floor(sqrt(k * numCandidates))` candidates at full fidelity. Other codecs use the returned scores directly.
- Full-pool recall uses the production full-fidelity rescorer for binary and scalar-quantized fields or the already available raw scores. The scalar stage passed its standalone RPC test. Replaying the production full-fidelity scorer reproduced all 2,560 per-query and cutoff observations from an independent retained-float rerank, including pool overlap, final recall and cap outcomes. In every successful trial, we check that full-pool reranking recovers all exact retained-vector neighbors available in the pool.

The public resumable cursor is measured separately with the same warm-up and refill parameters. Each k uses the first k emissions from one walk. The cursor scores passing discoveries against the retained vectors before advertising its head. It can emit a stronger neighbor on a later poll.

**Timing.** Trials use one request at a time, serial leaf search and a disabled Lucene query cache. Each index's files are read before its run. Six unmeasured query warm-ups precede one measured observation per query, condition and budget. Ordinary and streaming trials and budgets are shuffled for each query. Pool retrieval is timed once and reused across cutoffs and reranking policies. Full-pool rescoring precedes the four default-policy measurements on those IDs, so the latter may benefit from warmed vector pages. The component timings exclude truth, validation, RPC and result materialization. Full-pool rescoring uses the actual `rescoreAll` branch at limit 100, then its prefix supplies smaller k. The ordinary full-pool binary and scalar rows are matched-cost benchmark controls. The stage exposes the opt-in through `streaming.rescoreAll`. Percentiles describe warm component costs on this host. The previous repeated end-to-end stage timings are a separate experiment. [Dense stage timings](../int8/README.md#independent-holdout)

**Failures.** Cache-cap failures are recorded explicitly in the raw rows. The finite summary includes their elapsed search time and assigns zero returned hits at each k. Thus its denominator includes every query. A cap failure carries no ranking information about unreturned documents.

## Reproduction

Use the pinned Superlinked shards, original float32 cohorts, dense int8 cohorts, pinned Lucene JAR and mongot deploy JAR from the preceding catalog experiments. Compile `CatalogPrefilter`, `CatalogFilterProfile`, `RankedStreamBenchmark`, `CatalogCodecIndex`, `CatalogCodecTruth` and `CatalogCodecBenchmark` from `scripts/perf/catalog_prefilter` into a fresh classes directory. Put that directory, the pinned Lucene JAR and the deploy JAR on the classpath in that order. Use Java 21 with `--enable-preview --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED`. Graph builds used a 2 GiB heap and eight merge workers. Use new staging and index directories for every build. [Catalog inputs](../README.md) · [Dense inputs and JVM setup](../int8/README.md)

1. For each matched input leaf, use its original `products.tsv` and `vectors.f32`. Generate `vectors.i8` using `rint(256 * vector)` and a signed-byte cast. The first 624,000 vectors of dense input leaf 0 match these two byte files exactly. Leaf 1 starts at input row 312,000.
2. Run `CatalogCodecIndex KIND INPUT STAGING INDEX` once per leaf and kind. Valid kinds are `f32`, `i8`, `scalar`, `f32-binary`, `i8-binary` and `bit`. For each dense raw byte leaf, use the corresponding dense inputs and append `1130000` to this build command.
3. Run `prepare.py DENSE_COHORTS all.f32 --queries-from SHARD_223 --query-count 144 --float-queries`. The measured queries are records 80 through 143 inclusive. Extract them with `dd if=all.f32 of=holdout.f32 bs=16616 skip=80 count=64`. The float32 query SHA-256 is `0eee9abb4bc7ddde78b61df8945456aa90b5f05e18702955ba27ad70d9241f53`. Their rounded bytes reproduce the archived dense holdout exactly.
4. Run `CatalogCodecTruth FLOAT_LEAF0 FLOAT_LEAF1 holdout.f32 truth.tsv oracle.csv` using leaves retaining the original floats. For the dense comparison run `CatalogCodecBenchmark byte-truth DENSE_BYTE_LEAF0 DENSE_BYTE_LEAF1 holdout.f32 dense-truth.tsv`.
5. Run `CatalogCodecBenchmark KIND LEAF0 LEAF1 holdout.f32 truth.tsv` and save stdout as CSV. Use `cursor-KIND` for the public cursor. For dense trials pass the corresponding int8 kind and `dense-truth.tsv`. For the smaller-walk control add `-Dcatalog.codecDefaultWalk=true -Dcatalog.filter=categories_6 -Dcatalog.candidates=500`. The source-float columns are empty for those trials.
