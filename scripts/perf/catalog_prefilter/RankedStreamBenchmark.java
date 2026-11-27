import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedRescoreQuery;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedVectorRescorer;
import com.xgen.mongot.index.lucene.streaming.RankedVectorCursor;
import com.xgen.mongot.index.lucene.streaming.RankedVectorCursor.Hit;
import com.xgen.mongot.index.lucene.streaming.StreamingKnnQuery;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.StreamingOptions;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Random;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NumericUtils;

/** A/B against exact filtered retained-int8 truth on two real catalog graphs. */
public class RankedStreamBenchmark {
  private static final int CANDIDATES = Integer.getInteger("catalog.candidates", 500);
  private static final List<Integer> LIMITS =
      Arrays.stream(System.getProperty("catalog.limits", "10,20,50,100").split(",", -1))
          .map(Integer::parseInt)
          .distinct()
          .toList();
  private static final boolean DEEP_PREFIX = Boolean.getBoolean("catalog.deepPrefix");
  private static final Comparator<Hit> BEST =
      Comparator.comparingDouble(Hit::score).reversed().thenComparingInt(Hit::doc);
  private static final List<StreamingOptions> BUDGETS =
      List.of(
          new StreamingOptions(32, 256, 8, 8, 250_000),
          new StreamingOptions(64, 1024, 8, 8, 250_000));
  private static final BinaryQuantizedVectorRescorer RESCORER =
      new BinaryQuantizedVectorRescorer(Optional.empty());

  private static ApproximateVectorSearchCriteria criteria(int limit) {
    return criteria(limit, Optional.empty());
  }

  private static ApproximateVectorSearchCriteria criteria(
      int limit, Optional<StreamingOptions> streaming) {
    return criteria(limit, CANDIDATES, streaming);
  }

  static ApproximateVectorSearchCriteria criteria(
      int limit, int candidates, Optional<StreamingOptions> streaming) {
    return new ApproximateVectorSearchCriteria(
        FieldPath.newRoot("vector"),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        limit,
        Optional.of(candidates),
        Optional.empty(),
        false,
        Optional.empty(),
        streaming);
  }

  record Case(String name, Query query, BitSet allowed) {}

  static Case prepare(IndexSearcher searcher, CatalogPrefilter.Case filter) throws IOException {
    var weight =
        searcher.createWeight(searcher.rewrite(filter.query()), ScoreMode.COMPLETE_NO_SCORES, 1);
    var allowed = new BitSet(searcher.getIndexReader().maxDoc());
    for (var leaf : searcher.getIndexReader().leaves()) {
      var scorer = weight.scorer(leaf);
      if (scorer == null) {
        continue;
      }
      var it = scorer.iterator();
      for (int d; (d = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS; ) {
        allowed.set(leaf.docBase + d);
      }
    }
    return new Case(filter.name(), filter.query(), allowed);
  }

  static List<Hit> exact(IndexReader reader, Case filter, byte[] q) throws IOException {
    return exact(reader, filter, q, false);
  }

  static List<Hit> exact(IndexReader reader, Case filter, byte[] q, boolean hamming)
      throws IOException {
    int depth = Math.max(DEEP_PREFIX ? 500 : 100, Collections.max(LIMITS));
    var heap = new PriorityQueue<>(depth, BEST.reversed());
    for (var leaf : reader.leaves()) {
      var values = leaf.reader().getByteVectorValues("vector");
      for (int ord = 0; ord < values.size(); ord++) {
        int doc = leaf.docBase + values.ordToDoc(ord);
        if (!filter.allowed.get(doc)) {
          continue;
        }
        var v = values.vectorValue(ord);
        float score =
            hamming
                ? 1f - org.apache.lucene.util.VectorUtil.xorBitCount(q, v) / (float) (q.length * 8)
                : VectorSimilarityFunction.EUCLIDEAN.compare(q, v);
        if (heap.size() == depth && score < heap.element().score()) {
          continue;
        }
        heap.add(new Hit(doc, score));
        if (heap.size() > depth) {
          heap.remove();
        }
      }
    }
    var sorted = new ArrayList<>(heap);
    sorted.sort(BEST);
    return sorted;
  }

  private static void fill(RankedVectorCursor cursor, List<Hit> hits, int count)
      throws IOException {
    while (hits.size() < count) {
      var batch = cursor.poll(count - hits.size(), 128);
      hits.addAll(batch.hits());
      if (batch.exhausted() && hits.size() < count) {
        throw new AssertionError("Underfilled prefix");
      }
    }
  }

  private static double recall(List<Hit> hits, List<Hit> truth, int k, Case filter) {
    var unique = new HashSet<Integer>();
    for (var hit : hits) {
      if (!unique.add(hit.doc()) || !filter.allowed.get(hit.doc())) {
        throw new AssertionError("Invalid hit " + hit);
      }
    }
    var exactScores = new HashMap<Integer, Float>();
    truth.subList(0, k).forEach(hit -> exactScores.put(hit.doc(), hit.score()));
    int found = 0;
    for (var hit : hits.subList(0, k)) {
      Float exact = exactScores.get(hit.doc());
      if (exact != null) {
        if (Float.compare(exact, hit.score()) != 0) {
          throw new AssertionError("Retained-vector score differs from exact for " + hit.doc());
        }
        found++;
      }
    }
    return (double) found / k;
  }

  /** Fraction of pairs where a later hit has strictly higher similarity. */
  private static double invertedPairs(List<Hit> hits) {
    int inversions = 0;
    for (int first = 0; first < hits.size(); first++) {
      for (int later = first + 1; later < hits.size(); later++) {
        if (hits.get(first).score() < hits.get(later).score()) {
          inversions++;
        }
      }
    }
    return 2.0 * inversions / (hits.size() * (hits.size() - 1));
  }

  private static void prefix(
      IndexSearcher searcher,
      Case filter,
      byte[] q,
      int id,
      List<Hit> truth,
      List<StreamingOptions> budgets)
      throws IOException {
    var order = new ArrayList<>(budgets);
    if (Boolean.getBoolean("catalog.shufflePrefixes")) {
      Collections.shuffle(order, new Random(9137L + id));
    }
    for (var options : order) {
      long start = System.nanoTime();
      try (var cursor = RankedVectorCursor.open(searcher, "vector", q, filter.query, options)) {
        var hits = new ArrayList<Hit>();
        fill(cursor, hits, 10);
        double first10 = (System.nanoTime() - start) / 1e6;
        fill(cursor, hits, 100);
        double first100 = (System.nanoTime() - start) / 1e6;
        int cached100 = cursor.cachedNodes();
        String deeper = "";
        if (DEEP_PREFIX) {
          fill(cursor, hits, 500);
          double first500 = (System.nanoTime() - start) / 1e6;
          deeper =
              String.format(
                  Locale.ROOT,
                  ",%.3f,%.3f,%d,%.5f",
                  recall(hits, truth, 500, filter),
                  first500,
                  cursor.cachedNodes(),
                  invertedPairs(hits));
        }
        System.out.printf(
            Locale.ROOT,
            "%d,%d,%d,%d,%d,%.2f,%.2f,%.3f,%.3f,%d,%.5f%s,%.2f,%.2f%n",
            id,
            options.warmupWidth(),
            options.warmupVisits(),
            options.refillEvery(),
            options.refillExpansions(),
            recall(hits, truth, 10, filter),
            recall(hits, truth, 100, filter),
            first10,
            first100,
            cached100,
            invertedPairs(hits.subList(0, 100)),
            deeper,
            recall(hits, truth, 20, filter),
            recall(hits, truth, 50, filter));
      }
    }
  }

  private static void stage(
      IndexSearcher searcher,
      Case filter,
      byte[] q,
      int id,
      List<Hit> truth,
      List<StreamingOptions> budgets,
      boolean recallOnly)
      throws IOException {
    record Trial(
        int mode, int limit, ApproximateVectorSearchCriteria criteria, HashSet<Integer> truthIds) {}
    var trials = new ArrayList<Trial>();
    // Optionally give ordinary kNN the identical rescorer as a matched-cost control.
    var baselineOptions =
        Boolean.getBoolean("catalog.matchRescore")
            ? Optional.of(budgets.getFirst())
            : Optional.<StreamingOptions>empty();
    for (int limit : LIMITS) {
      var truthIds = new HashSet<Integer>();
      truth.subList(0, limit).forEach(hit -> truthIds.add(hit.doc()));
      for (int mode = 0; mode <= budgets.size(); mode++) {
        var options = mode == 0 ? baselineOptions : Optional.of(budgets.get(mode - 1));
        var criteria =
            criteria(Boolean.getBoolean("catalog.rescoreAll") ? CANDIDATES : limit, options);
        trials.add(new Trial(mode, limit, criteria, truthIds));
      }
    }
    var order = new Random(9137L + id);
    // Recall is deterministic on this fixed graph. Timing trials also warm up and repeat.
    int repetitions = recallOnly ? 1 : 3;
    for (int rep = recallOnly ? 0 : -1; rep < repetitions; rep++) {
      Collections.shuffle(trials, order);
      for (var trial : trials) {
        int mode = trial.mode();
        int limit = trial.limit();
        KnnByteVectorQuery query =
            mode == 0
                ? new KnnByteVectorQuery("vector", q, CANDIDATES, filter.query)
                : new StreamingKnnQuery.Bytes(
                    "vector", q, CANDIDATES, filter.query, budgets.get(mode - 1));
        long start = System.nanoTime();
        var candidates = searcher.search(query, CANDIDATES);
        var results =
            RESCORER.rescore(
                searcher, candidates, trial.criteria(), BinaryQuantizedRescoreQuery.from(query));
        if (results.scoreDocs.length > limit) {
          results.scoreDocs = Arrays.copyOf(results.scoreDocs, limit);
        }
        double wall = (System.nanoTime() - start) / 1e6;
        if (results.scoreDocs.length != limit) {
          throw new AssertionError("Underfilled stage");
        }
        var hits = Arrays.stream(results.scoreDocs).map(sd -> new Hit(sd.doc, sd.score)).toList();
        double recall = recall(hits, truth, limit, filter);
        if (rep >= 0) {
          long poolHits =
              Arrays.stream(candidates.scoreDocs)
                  .filter(sd -> trial.truthIds().contains(sd.doc))
                  .count();
          System.out.printf(
              Locale.ROOT,
              "%d,%s,%d,%d,%s,%d,%d,%.3f,%.4f,%d,%d,%d,%d%n",
              id,
              filter.name,
              filter.allowed.cardinality(),
              rep,
              mode == 0 ? "baseline" : "streaming",
              mode == 0 ? 0 : budgets.get(mode - 1).warmupWidth(),
              mode == 0 ? 0 : budgets.get(mode - 1).warmupVisits(),
              wall,
              recall,
              mode == 0 ? 0 : budgets.get(mode - 1).refillEvery(),
              mode == 0 ? 0 : budgets.get(mode - 1).refillExpansions(),
              poolHits,
              limit);
        }
      }
    }
  }

  // Hold the candidate IDs fixed and time both production reranking passes together.
  private static void rescore(
      IndexSearcher searcher,
      Case filter,
      byte[] q,
      int id,
      List<Hit> truth,
      StreamingOptions budget,
      int resultLimit)
      throws IOException {
    var query = new StreamingKnnQuery.Bytes("vector", q, CANDIDATES, filter.query, budget);
    var candidates = searcher.search(query, CANDIDATES);
    var rescoreQuery = BinaryQuantizedRescoreQuery.from(query);
    var limits =
        new ArrayList<>(
            List.of(resultLimit, 125, CANDIDATES).stream()
                .filter(limit -> limit >= resultLimit && limit <= CANDIDATES)
                .distinct()
                .toList());
    var order = new Random(9137L + id);
    for (int rep = -1; rep < 3; rep++) {
      Collections.shuffle(limits, order);
      for (int limit : limits) {
        var criteria = criteria(limit);
        long start = System.nanoTime();
        var results = RESCORER.rescore(searcher, candidates, criteria, rescoreQuery);
        double wall = (System.nanoTime() - start) / 1e6;
        var hits =
            Arrays.stream(results.scoreDocs)
                .limit(resultLimit)
                .map(sd -> new Hit(sd.doc, sd.score))
                .toList();
        double recall = recall(hits, truth, resultLimit, filter);
        if (rep >= 0) {
          System.out.printf(
              Locale.ROOT,
              "%d,%s,%d,%d,%d,%d,%d,%d,%.3f,%.4f,%d%n",
              id,
              filter.name,
              budget.warmupWidth(),
              budget.warmupVisits(),
              budget.refillEvery(),
              budget.refillExpansions(),
              rep,
              (int) Math.sqrt(limit * CANDIDATES),
              wall,
              recall,
              resultLimit);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 4 || !List.of("prefix", "stage", "recall", "rescore").contains(args[0])) {
      throw new IllegalArgumentException(
          "Expected prefix|stage|recall|rescore leaf0 leaf1 queries.i8"
              + " [refillEvery/refillExpansions ...]");
    }
    if (Collections.min(LIMITS) < 1 || Collections.max(LIMITS) > CANDIDATES) {
      throw new IllegalArgumentException(
          "Result limits must be positive and at most the candidate count");
    }
    var budgets = new ArrayList<StreamingOptions>();
    for (int i = 4; i < args.length; i++) {
      int[] values = Arrays.stream(args[i].split("/", -1)).mapToInt(Integer::parseInt).toArray();
      budgets.add(
          switch (values.length) {
            case 2 -> new StreamingOptions(64, 1024, values[0], values[1], 250_000);
            case 4 -> new StreamingOptions(values[0], values[1], values[2], values[3], 250_000);
            default ->
                throw new IllegalArgumentException("Expected [width/visits/]every/expansions");
          });
    }
    if (budgets.isEmpty()) {
      budgets.addAll(BUDGETS);
    }
    if (Boolean.getBoolean("catalog.streamingRescoreAll")) {
      budgets.replaceAll(
          b ->
              new StreamingOptions(
                  b.warmupWidth(),
                  b.warmupVisits(),
                  b.refillEvery(),
                  b.refillExpansions(),
                  b.maxCachedNodes(),
                  true));
    }
    try (var d0 = FSDirectory.open(Path.of(args[1]));
        var d1 = FSDirectory.open(Path.of(args[2]));
        var r0 = DirectoryReader.open(d0);
        var r1 = DirectoryReader.open(d1);
        var reader = new MultiReader(new IndexReader[] {r0, r1}, false);
        var queries = Files.newInputStream(Path.of(args[3]))) {
      var searcher = new IndexSearcher(reader);
      searcher.setQueryCache(null);
      var cases = new ArrayList<Case>();
      cases.add(prepare(searcher, CatalogFilterProfile.catalogCase(6, false)));
      boolean recallOnly = args[0].equals("recall");
      if (args[0].equals("stage") || recallOnly) {
        cases.add(prepare(searcher, CatalogFilterProfile.catalogCase(3, false)));
        cases.add(prepare(searcher, CatalogFilterProfile.catalogCase(2, true)));
        cases.add(
            prepare(
                searcher,
                new CatalogPrefilter.Case(
                    "doc_values_price", CatalogPrefilter.range("price", 5, 100))));
        cases.add(
            prepare(
                searcher,
                new CatalogPrefilter.Case(
                    "point_only_price",
                    LongPoint.newRangeQuery(
                        "price",
                        NumericUtils.doubleToSortableLong(5),
                        NumericUtils.doubleToSortableLong(100)))));
      }
      String onlyFilter = System.getProperty("catalog.filter");
      if (onlyFilter != null) {
        cases.removeIf(c -> !c.name.equals(onlyFilter));
        if (cases.isEmpty()) {
          throw new IllegalArgumentException("Unknown filter for this experiment: " + onlyFilter);
        }
      }
      System.out.println(
          switch (args[0]) {
            case "prefix" ->
                "query,width,visits,refillEvery,refillExpansions,recall10,recall100,first10_ms,first100_ms,cached,inverted_pairs100"
                    + (DEEP_PREFIX ? ",recall500,first500_ms,cached500,inverted_pairs500" : "")
                    + ",recall20,recall50";
            case "rescore" ->
                "query,filter,width,visits,refillEvery,refillExpansions,repeat,full_fidelity_docs,rescore_ms,recall_at_k,k";
            default ->
                "query,filter,accepted,repeat,mode,width,visits,wall_ms,recall_at_k,refillEvery,refillExpansions,pool_hits_at_k,k";
          });
      for (int id = 0; ; id++) {
        byte[] q = queries.readNBytes(CatalogPrefilter.DIMS);
        if (q.length == 0) {
          break;
        }
        if (q.length != CatalogPrefilter.DIMS) {
          throw new IllegalArgumentException("Truncated query");
        }
        for (var filter : cases) {
          var truth = exact(reader, filter, q);
          switch (args[0]) {
            case "prefix" -> prefix(searcher, filter, q, id, truth, budgets);
            case "rescore" -> {
              for (var budget : budgets) {
                for (int limit : LIMITS) {
                  rescore(searcher, filter, q, id, truth, budget, limit);
                }
              }
            }
            default -> stage(searcher, filter, q, id, truth, budgets, recallOnly);
          }
        }
      }
    }
  }
}
