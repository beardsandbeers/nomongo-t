import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedRescoreQuery;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedVectorRescorer;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnByteQuery;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import com.xgen.mongot.index.lucene.streaming.StreamRouting;
import com.xgen.mongot.index.lucene.streaming.StreamingKnnQuery;
import com.xgen.mongot.index.query.operators.StreamingOptions;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.metrics.PerIndexMetricsFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Random;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.VectorUtil;

/**
 * Measure local candidate generation and production binary rescoring against exhaustive
 * retained-vector truth.
 */
public class CatalogLeafRoutingBenchmark {
  record Trial(int filter, int limit, int candidates, String mode) {}

  static IndexMetricsUpdater.QueryingMetricsUpdater metrics() {
    return new IndexMetricsUpdater.QueryingMetricsUpdater(
        new PerIndexMetricsFactory(
            "catalog", MeterAndFtdcRegistry.createWithSimpleRegistries(), "catalog", "catalog"));
  }

  public static void main(String[] args) throws Exception {
    String kind = args[0];
    boolean bit = kind.equals("bit"), bytes = bit || kind.startsWith("i8");
    boolean mixed = args.length == 6;
    boolean compareAuto = mixed || Boolean.getBoolean("catalog.verifyRouting");
    int offset = mixed ? Integer.parseInt(args[5]) : Integer.getInteger("catalog.probeStart", 0);
    var probes =
        CatalogCodecTruth.probes(Path.of(args[2]))
            .subList(offset, offset + Integer.parseInt(args[3]));
    if (bit) {
      probes =
          probes.stream()
              .map(
                  q ->
                      new CatalogCodecTruth.Probe(
                          q.floats(), CatalogCodecIndex.packSigns(q.bytes())))
              .toList();
    }
    try (var dir = FSDirectory.open(Path.of(args[1]));
        var second = mixed ? FSDirectory.open(Path.of(args[4])) : null;
        var reader =
            mixed
                ? new MultiReader(DirectoryReader.open(dir), DirectoryReader.open(second))
                : DirectoryReader.open(dir);
        var metrics = metrics()) {
      if (reader.leaves().size() != (mixed ? 2 : 1)) {
        throw new IllegalArgumentException("Expected one segment per directory");
      }
      var searcher = new IndexSearcher(reader);
      searcher.setQueryCache(null);
      var filters = new ArrayList<RankedStreamBenchmark.Case>();
      var cases =
          Boolean.getBoolean("catalog.boundaryFilters")
              ? List.of(
                  CatalogFilterProfile.catalogCase(4, false),
                  new CatalogPrefilter.Case(
                      "rating_at_least_two", CatalogPrefilter.range("rating", 2, Double.MAX_VALUE)))
              : List.of(
                  CatalogFilterProfile.catalogCase(2, true),
                  CatalogFilterProfile.catalogCase(2, false),
                  CatalogFilterProfile.catalogCase(6, false));
      for (var f : cases) {
        filters.add(RankedStreamBenchmark.prepare(searcher, f));
      }
      var truth = new CatalogCodecTruth.Top[filters.size()][probes.size()];
      var trials = new ArrayList<Trial>();
      for (int f = 0; f < filters.size(); f++) {
        for (int q = 0; q < probes.size(); q++) {
          truth[f][q] = new CatalogCodecTruth.Top(100);
        }
        for (int k : CatalogCodecTruth.LIMITS) {
          for (int c : new int[] {Math.max(100, 8 * k), 500}) {
            for (var mode :
                compareAuto ? List.of("classic", "stream", "auto") : List.of("classic", "stream")) {
              trials.add(new Trial(f, k, c, mode));
            }
          }
        }
      }
      for (var ctx : reader.leaves()) {
        var leaf = ctx.reader();
        var floats = bytes ? null : leaf.getFloatVectorValues("vector");
        var ints = bytes ? leaf.getByteVectorValues("vector") : null;
        for (int ord = 0; ord < (bytes ? ints.size() : floats.size()); ord++) {
          int doc = ctx.docBase + (bytes ? ints.ordToDoc(ord) : floats.ordToDoc(ord));
          if (filters.stream().noneMatch(f -> f.allowed().get(doc))) {
            continue;
          }
          var fv = bytes ? null : floats.vectorValue(ord);
          var bv = bytes ? ints.vectorValue(ord) : null;
          for (int q = 0; q < probes.size(); q++) {
            var probe = probes.get(q);
            float score =
                bit
                    ? -VectorUtil.xorBitCount(bv, probe.bytes())
                    : bytes
                        ? VectorSimilarityFunction.EUCLIDEAN.compare(bv, probe.bytes())
                        : VectorSimilarityFunction.EUCLIDEAN.compare(fv, probe.floats());
            for (int f = 0; f < filters.size(); f++) {
              if (filters.get(f).allowed().get(doc)) {
                truth[f][q].add(doc, score);
              }
            }
          }
        }
      }
      var options = StreamingOptions.defaults();
      var binaryRescorer = new BinaryQuantizedVectorRescorer(Optional.empty());
      var random = new Random(719);
      System.out.println(
          "codec,vectors,filter,matches,limit,candidates,mode,query,rep,ms,recall_hits,stream_segments");
      for (int i = -3; i < probes.size(); i++) {
        int q = Math.floorMod(i, probes.size());
        var probe = probes.get(q);
        for (int rep = 0; rep < (i < 0 ? 1 : 2); rep++) {
          Collections.shuffle(trials, random);
          for (var t : trials) {
            var filter = filters.get(t.filter);
            var criteria = RankedStreamBenchmark.criteria(t.limit, t.candidates, Optional.empty());
            var streamQuery =
                bytes
                    ? new StreamingKnnQuery.Bytes(
                        "vector", probe.bytes(), t.candidates, filter.query(), options)
                    : new StreamingKnnQuery.Floats(
                        "vector", probe.floats(), t.candidates, filter.query(), options);
            long start = System.nanoTime();
            var plan =
                t.mode.equals("auto")
                    ? StreamRouting.choose(
                        reader,
                        "vector",
                        bytes ? VectorEncoding.BYTE : VectorEncoding.FLOAT32,
                        filter.query(),
                        t.candidates,
                        t.limit,
                        true,
                        false)
                    : null;
            var hook =
                plan != null && plan.usesStream()
                    ? StreamingKnnQuery.routed(plan, streamQuery)
                    : null;
            Query query;
            if (t.mode.equals("stream")) {
              query = streamQuery;
            } else if (compareAuto) {
              query =
                  bytes
                      ? new MongotKnnByteQuery(
                          metrics, "vector", probe.bytes(), t.candidates, filter.query(), hook)
                      : new MongotKnnFloatQuery(
                          metrics,
                          FeatureFlags.getDefault(),
                          "vector",
                          probe.floats(),
                          t.candidates,
                          filter.query(),
                          hook);
            } else {
              query =
                  bytes
                      ? new KnnByteVectorQuery(
                          "vector", probe.bytes(), t.candidates, filter.query())
                      : new KnnFloatVectorQuery(
                          "vector", probe.floats(), t.candidates, filter.query());
            }
            var hits = searcher.search(query, kind.endsWith("-binary") ? t.candidates : t.limit);
            if (kind.endsWith("-binary")) {
              hits =
                  binaryRescorer.rescore(
                      searcher, hits, criteria, BinaryQuantizedRescoreQuery.from(query));
            }
            double ms = (System.nanoTime() - start) / 1e6;
            var ids = CatalogCodecBenchmark.ids(hits);
            if (ids.length != t.limit
                || java.util.Arrays.stream(ids).anyMatch(id -> !filter.allowed().get(id))) {
              throw new AssertionError("Invalid search results");
            }
            int overlap =
                CatalogCodecTruth.overlap(ids, t.limit, truth[t.filter][q].ids(), t.limit);
            if (i >= 0) {
              System.out.printf(
                  Locale.ROOT,
                  "%s,%d,%s,%d,%d,%d,%s,%d,%d,%.4f,%d,%d%n",
                  kind,
                  reader.maxDoc(),
                  filter.name(),
                  filter.allowed().cardinality(),
                  t.limit,
                  t.candidates,
                  t.mode,
                  offset + q,
                  rep,
                  ms,
                  overlap,
                  plan == null
                      ? (t.mode.equals("stream") ? reader.leaves().size() : 0)
                      : plan.segments().stream().filter(StreamRouting.Segment::stream).count());
            }
          }
        }
      }
    }
  }
}
