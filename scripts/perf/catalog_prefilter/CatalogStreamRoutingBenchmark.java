import com.xgen.mongot.index.lucene.streaming.StreamRouting;
import com.xgen.mongot.index.lucene.streaming.StreamingKnnQuery;
import com.xgen.mongot.index.query.operators.StreamingOptions;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;

/** Interleave forced and automatic routing on matched catalog queries with exact filtered truth. */
public class CatalogStreamRoutingBenchmark {
  record Trial(int filter, String mode) {}

  public static void main(String[] args) throws Exception {
    var probes = CatalogCodecTruth.probes(Path.of(args[2]));
    try (var first = FSDirectory.open(Path.of(args[0]));
        var second = FSDirectory.open(Path.of(args[1]));
        var metrics = CatalogLeafRoutingBenchmark.metrics();
        var reader = new MultiReader(DirectoryReader.open(first), DirectoryReader.open(second))) {
      var searcher = new IndexSearcher(reader);
      searcher.setQueryCache(null);
      var filters = new ArrayList<RankedStreamBenchmark.Case>();
      for (var filter :
          List.of(
              CatalogFilterProfile.catalogCase(2, true),
              CatalogFilterProfile.catalogCase(2, false),
              CatalogFilterProfile.catalogCase(4, false),
              CatalogFilterProfile.catalogCase(6, false))) {
        filters.add(RankedStreamBenchmark.prepare(searcher, filter));
      }
      var exact = new CatalogCodecTruth.Top[filters.size()][probes.size()];
      var trials = new ArrayList<Trial>();
      for (int f = 0; f < filters.size(); f++) {
        for (int q = 0; q < probes.size(); q++) {
          exact[f][q] = new CatalogCodecTruth.Top(100);
        }
        for (String mode : List.of("classic", "stream", "auto")) {
          trials.add(new Trial(f, mode));
        }
      }
      for (var leaf : reader.leaves()) {
        var vectors = leaf.reader().getFloatVectorValues("vector");
        for (int ord = 0; ord < vectors.size(); ord++) {
          int doc = leaf.docBase + vectors.ordToDoc(ord);
          if (filters.stream().noneMatch(f -> f.allowed().get(doc))) {
            continue;
          }
          var vector = vectors.vectorValue(ord);
          for (int q = 0; q < probes.size(); q++) {
            float score =
                VectorSimilarityFunction.EUCLIDEAN.compare(vector, probes.get(q).floats());
            for (int f = 0; f < filters.size(); f++) {
              if (filters.get(f).allowed().get(doc)) {
                exact[f][q].add(doc, score);
              }
            }
          }
        }
      }
      var random = new Random(424);
      var options = StreamingOptions.defaults();
      System.out.println(
          "filter,query,repeat,mode,acceptance,selected,sampled,accepted,ms,recall100");
      for (int index = -5; index < probes.size(); index++) {
        int q = Math.floorMod(index, probes.size());
        for (int rep = 0; rep < (index < 0 ? 1 : 3); rep++) {
          Collections.shuffle(trials, random);
          for (var trial : trials) {
            var filter = filters.get(trial.filter());
            var target = probes.get(q).floats();
            long start = System.nanoTime();
            var decision =
                trial.mode().equals("auto")
                    ? StreamRouting.choose(
                        reader,
                        "vector",
                        VectorEncoding.FLOAT32,
                        filter.query(),
                        500,
                        100,
                        true,
                        false)
                    : null;
            var streamQuery =
                new StreamingKnnQuery.Floats("vector", target, 500, filter.query(), options);
            var hook =
                decision != null && decision.usesStream()
                    ? StreamingKnnQuery.routed(decision, streamQuery)
                    : null;
            var query =
                trial.mode().equals("stream")
                    ? streamQuery
                    : new com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery(
                        metrics,
                        com.xgen.mongot.featureflag.FeatureFlags.getDefault(),
                        "vector",
                        target,
                        500,
                        filter.query(),
                        hook);
            var result = searcher.search(query, 100);
            double elapsed = (System.nanoTime() - start) / 1e6;
            if (index >= 0) {
              var ids = CatalogCodecBenchmark.ids(result);
              if (ids.length != 100) {
                throw new AssertionError("incomplete pool");
              }
              int hits = CatalogCodecTruth.overlap(ids, 100, exact[trial.filter()][q].ids(), 100);
              System.out.printf(
                  Locale.ROOT,
                  "%s,%d,%d,%s,%.4f,%s,%d,%d,%.3f,%d%n",
                  filter.name(),
                  q,
                  rep,
                  trial.mode(),
                  filter.allowed().cardinality() / (double) reader.numDocs(),
                  decision == null
                      ? trial.mode()
                      : decision.describe().getString("selected").getValue(),
                  decision == null
                      ? 0
                      : decision.segments().stream().mapToInt(StreamRouting.Segment::sampled).sum(),
                  decision == null
                      ? 0
                      : decision.segments().stream()
                          .mapToInt(StreamRouting.Segment::accepted)
                          .sum(),
                  elapsed,
                  hits);
            }
          }
        }
      }
    }
  }
}
