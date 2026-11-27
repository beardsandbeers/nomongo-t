import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedRescoreQuery;
import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedVectorRescorer;
import com.xgen.mongot.index.lucene.streaming.RankedVectorCursor;
import com.xgen.mongot.index.lucene.streaming.StreamingKnnQuery;
import com.xgen.mongot.index.query.operators.StreamingOptions;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

/** Compare ordinary and streaming queries against the same exact neighbors across codecs. */
public class CatalogCodecBenchmark {
  private static final StreamingOptions OPTIONS = streamOptions();

  private static StreamingOptions streamOptions() {
    if (Boolean.getBoolean("catalog.stream")) {
      var defaults = StreamingOptions.defaults();
      int visits = Integer.getInteger("catalog.warmupVisits", 0);
      return new StreamingOptions(
          0, visits, 0, 0, defaults.maxCachedNodes(), false, defaults.adaptive());
    }
    return Boolean.getBoolean("catalog.codecDefaultWalk")
        ? new StreamingOptions(64, 1024, 8, 8, 250000)
        : new StreamingOptions(2048, 8192, 128, 256, 250000);
  }

  private static final StreamingOptions FULL =
      new StreamingOptions(2048, 8192, 128, 256, 250000, true);
  private static final BinaryQuantizedVectorRescorer RESCORER =
      new BinaryQuantizedVectorRescorer(Optional.empty());

  private static final String CACHE_CAP_MESSAGE =
      "Streaming vector search exceeded maxCachedNodes=" + OPTIONS.maxCachedNodes();

  record Key(int query, String filter) {}

  record Truth(int[] source, int[] bytes) {}

  record Trial(int candidates, boolean stream, RankedStreamBenchmark.Case filter) {}

  static int[] ids(TopDocs docs) {
    return Arrays.stream(docs.scoreDocs).mapToInt(d -> d.doc).toArray();
  }

  static int[] decode(String field) {
    return Arrays.stream(field.split(" ")).mapToInt(Integer::parseInt).toArray();
  }

  static String hits(int[] ids, int depth, int[] truth, int k) {
    return truth == null ? "" : Integer.toString(CatalogCodecTruth.overlap(ids, depth, truth, k));
  }

  private static boolean usesBytes(String kind) {
    return kind.startsWith("i8") || kind.equals("bit");
  }

  static void verify(IndexReader reader, String kind, boolean bytes) throws Exception {
    String expected =
        switch (kind) {
          case "f32", "i8" -> "Lucene99HnswVectorsFormat";
          case "bit" ->
              com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBitVectorsFormat.NAME;
          case "scalar" -> "Lucene99HnswScalarQuantizedVectorsFormat";
          case "f32-binary", "i8-binary" ->
              com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBinaryQuantizedVectorsFormat
                  .NAME;
          default -> throw new IllegalArgumentException(kind);
        };
    if (reader.leaves().size() != 2) throw new IllegalStateException("Expected two leaves");
    for (var context : reader.leaves()) {
      var leaf = (CodecReader) context.reader();
      var info = leaf.getFieldInfos().fieldInfo("vector");
      var per = (PerFieldKnnVectorsFormat.FieldsReader) leaf.getVectorReader();
      var vectorReader = per.getFieldReader("vector");
      var graph = ((HnswGraphProvider) vectorReader).getGraph("vector");
      var values = bytes ? leaf.getByteVectorValues("vector") : leaf.getFloatVectorValues("vector");
      if (kind.endsWith("-binary") || kind.equals("scalar")) {
        var quantized = (org.apache.lucene.util.quantization.QuantizedVectorsReader) vectorReader;
        int bits = quantized.getQuantizationState("vector").getBits();
        if (bits != (kind.equals("scalar") ? 7 : 1))
          throw new IllegalStateException("Unexpected quantization bits: " + bits);
      }
      System.err.printf(
          "leaf=%d rows=%d levels=%d format=%s%n",
          context.ord, values.size(), graph.numLevels(), expected);
      if (!expected.equals(info.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY))
          || values.size() != leaf.maxDoc()
          || graph.size() != values.size()
          || graph.numLevels() < 2
          || values.dimension()
              != (kind.equals("bit") ? (CatalogPrefilter.DIMS + 7) / 8 : CatalogPrefilter.DIMS))
        throw new IllegalStateException("Wrong index for " + kind);
    }
  }

  static void trial(
      IndexSearcher searcher,
      String kind,
      Trial trial,
      CatalogCodecTruth.Probe probe,
      Key key,
      Truth truth,
      int repeat,
      PrintWriter out)
      throws Exception {
    boolean bytes = usesBytes(kind), binary = kind.endsWith("-binary");
    Query filter = trial.filter.query();
    Query query =
        bytes
            ? (trial.stream
                ? new StreamingKnnQuery.Bytes(
                    "vector", probe.bytes(), trial.candidates, filter, OPTIONS)
                : new KnnByteVectorQuery("vector", probe.bytes(), trial.candidates, filter))
            : (trial.stream
                ? new StreamingKnnQuery.Floats(
                    "vector", probe.floats(), trial.candidates, filter, OPTIONS)
                : new KnnFloatVectorQuery("vector", probe.floats(), trial.candidates, filter));
    long start = System.nanoTime();
    TopDocs pool;
    try {
      pool = searcher.search(query, trial.candidates);
    } catch (IllegalArgumentException e) {
      if (!trial.stream || !CACHE_CAP_MESSAGE.equals(e.getMessage())) {
        throw e;
      }
      double elapsed = (System.nanoTime() - start) / 1e6;
      if (repeat >= 0)
        for (int k : CatalogCodecTruth.LIMITS) {
          out.printf(
              Locale.ROOT,
              "%d,%s,%d,%s,%d,streaming,%d,%d,0,,,,,,,,,%.3f,,,cache_cap%n",
              key.query,
              key.filter,
              trial.filter.allowed().cardinality(),
              kind,
              trial.candidates,
              repeat,
              k,
              elapsed);
        }
      return;
    }
    long searchNanos = System.nanoTime() - start;
    int[] poolIds = ids(pool);
    if (poolIds.length != trial.candidates
        || Arrays.stream(poolIds).distinct().count() != poolIds.length
        || Arrays.stream(poolIds).anyMatch(id -> !trial.filter.allowed().get(id)))
      throw new AssertionError("Invalid pool");
    // Time the primary (k=100) binary policy before the diagnostic full-pool rerank reads the same
    // vectors.
    boolean measurePrimaryFirst = binary && OPTIONS.adaptive().isPresent();
    var primary = RankedStreamBenchmark.criteria(100, trial.candidates, Optional.empty());
    long primaryStart = System.nanoTime();
    int[] primaryResult =
        measurePrimaryFirst
            ? ids(
                RESCORER.rescore(searcher, pool, primary, BinaryQuantizedRescoreQuery.from(query)))
            : poolIds;
    long primaryNanos = System.nanoTime() - primaryStart;
    // Full-pool rescore is independent of the output limit. Keep its top 100 for all
    // cutoffs.
    long fullStart = System.nanoTime();
    int[] full;
    if (binary || kind.equals("scalar")) {
      var all = RankedStreamBenchmark.criteria(100, trial.candidates, Optional.of(FULL));
      full = ids(RESCORER.rescore(searcher, pool, all, BinaryQuantizedRescoreQuery.from(query)));
    } else {
      full = poolIds;
    }
    long fullNanos = System.nanoTime() - fullStart;
    int[] nativeTruth = bytes ? truth.bytes : truth.source;
    for (int k : CatalogCodecTruth.LIMITS) {
      var criteria = RankedStreamBenchmark.criteria(k, trial.candidates, Optional.empty());
      long defaultStart = System.nanoTime();
      int[] result =
          measurePrimaryFirst && k == 100
              ? primaryResult
              : binary
                  ? ids(
                      RESCORER.rescore(
                          searcher, pool, criteria, BinaryQuantizedRescoreQuery.from(query)))
                  : poolIds;
      long defaultNanos =
          measurePrimaryFirst && k == 100 ? primaryNanos : System.nanoTime() - defaultStart;
      int after = CatalogCodecTruth.overlap(full, k, nativeTruth, k);
      int available = CatalogCodecTruth.overlap(poolIds, trial.candidates, nativeTruth, k);
      if (after != available)
        throw new AssertionError(
            "Full rerank failed to recover native truth: " + after + " vs " + available);
      if (repeat >= 0)
        out.printf(
            Locale.ROOT,
            "%d,%s,%d,%s,%d,%s,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%.3f,%.3f,%.3f,ok%n",
            key.query,
            key.filter,
            trial.filter.allowed().cardinality(),
            kind,
            trial.candidates,
            trial.stream ? "streaming" : "ordinary",
            repeat,
            k,
            poolIds.length,
            hits(poolIds, k, nativeTruth, k),
            hits(poolIds, trial.candidates, nativeTruth, k),
            hits(result, k, nativeTruth, k),
            hits(full, k, nativeTruth, k),
            hits(poolIds, k, truth.source, k),
            hits(poolIds, trial.candidates, truth.source, k),
            hits(result, k, truth.source, k),
            hits(full, k, truth.source, k),
            searchNanos / 1e6,
            defaultNanos / 1e6,
            fullNanos / 1e6);
    }
  }

  // Public cursor ranks passing discoveries with retained-vector scores before delivery.
  private static void prefixes(
      IndexSearcher searcher,
      String kind,
      List<CatalogCodecTruth.Probe> probes,
      List<RankedStreamBenchmark.Case> filters,
      Map<Key, Truth> refs)
      throws Exception {
    var out = new PrintWriter(System.out);
    out.println(
        "query,filter,codec,k,native_hits,source_hits,cached_on_open,cached_at_k,prefix_ms,status");
    boolean bytes = usesBytes(kind);
    for (int index = -Math.min(6, probes.size()); index < probes.size(); index++) {
      int q = Math.floorMod(index, probes.size());
      var probe = probes.get(q);
      for (var filter : filters) {
        var truth = refs.get(new Key(q, filter.name()));
        long start = System.nanoTime();
        int completed = 0;
        try (var cursor =
            bytes
                ? RankedVectorCursor.open(
                    searcher, "vector", probe.bytes(), filter.query(), OPTIONS)
                : RankedVectorCursor.open(
                    searcher, "vector", probe.floats(), filter.query(), OPTIONS)) {
          int opened = cursor.cachedNodes();
          int[] docs = new int[100];
          int count = 0;
          for (int k : CatalogCodecTruth.LIMITS) {
            while (count < k) {
              var batch = cursor.poll(k - count, 128);
              for (var hit : batch.hits()) {
                if (!filter.allowed().get(hit.doc())) throw new AssertionError("Failed membership");
                docs[count++] = hit.doc();
              }
              if (batch.exhausted() && count < k) throw new AssertionError("Underfilled cursor");
            }
            double elapsed = (System.nanoTime() - start) / 1e6;
            completed = k;
            if (index >= 0)
              out.printf(
                  Locale.ROOT,
                  "%d,%s,%s,%d,%s,%s,%d,%d,%.3f,ok%n",
                  q,
                  filter.name(),
                  kind,
                  k,
                  hits(docs, k, bytes ? truth.bytes : truth.source, k),
                  hits(docs, k, truth.source, k),
                  opened,
                  cursor.cachedNodes(),
                  elapsed);
          }
          if (Arrays.stream(docs).distinct().count() != docs.length)
            throw new AssertionError("Duplicate cursor hit");
        } catch (IllegalArgumentException e) {
          if (!CACHE_CAP_MESSAGE.equals(e.getMessage())) throw e;
          for (int k : CatalogCodecTruth.LIMITS) {
            if (index >= 0 && k > completed) {
              out.printf(
                  Locale.ROOT,
                  "%d,%s,%s,%d,,,,,%.3f,cache_cap%n",
                  q,
                  filter.name(),
                  kind,
                  k,
                  (System.nanoTime() - start) / 1e6);
            }
          }
        }
      }
      out.flush();
    }
    if (out.checkError()) throw new IOException("writing cursor observations");
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 5)
      throw new IllegalArgumentException(
          "codec|byte-truth|bit-truth leaf0 leaf1 queries.f32 truth.tsv");
    boolean cursorOnly = args[0].startsWith("cursor-");
    String kind = cursorOnly ? args[0].substring("cursor-".length()) : args[0];
    var probes = CatalogCodecTruth.probes(Path.of(args[3]));
    if (kind.equals("bit") || kind.equals("bit-truth")) {
      probes =
          probes.stream()
              .map(
                  q ->
                      new CatalogCodecTruth.Probe(
                          q.floats(), CatalogCodecIndex.packSigns(q.bytes())))
              .toList();
    }
    try (var d0 = FSDirectory.open(Path.of(args[1]));
        var d1 = FSDirectory.open(Path.of(args[2]));
        var reader = new MultiReader(DirectoryReader.open(d0), DirectoryReader.open(d1))) {
      var searcher = new IndexSearcher(reader);
      searcher.setQueryCache(null);
      var filters = CatalogCodecTruth.cases(searcher);
      String digest = CatalogCodecTruth.identity(reader, filters);
      if (kind.equals("byte-truth") || kind.equals("bit-truth")) {
        try (var out = new PrintWriter(Files.newBufferedWriter(Path.of(args[4])))) {
          out.println(
              String.join("\t", digest, "-", CatalogCodecTruth.coordinates(reader, true, true)));
          for (int i = 0; i < probes.size(); i++)
            for (var f : filters) {
              String ids =
                  RankedStreamBenchmark.exact(
                          reader, f, probes.get(i).bytes(), kind.equals("bit-truth"))
                      .stream()
                      .map(h -> Integer.toString(h.doc()))
                      .collect(Collectors.joining(" "));
              out.printf("%d\t%s\t-\t%s%n", i, f.name(), ids);
            }
          if (out.checkError()) throw new IOException("writing dense truth");
        }
        return;
      }
      verify(reader, kind, usesBytes(kind));
      var lines = Files.readAllLines(Path.of(args[4]));
      String[] fingerprints = lines.getFirst().split("\t");
      boolean byteInput = usesBytes(kind);
      if (!digest.equals(fingerprints[0])
          || !CatalogCodecTruth.coordinates(reader, byteInput, byteInput)
              .equals(fingerprints[byteInput ? 2 : 1]))
        throw new IllegalStateException("Unmatched doc IDs, filters or retained coordinates");
      var refs = new HashMap<Key, Truth>();
      for (String line : lines.subList(1, lines.size())) {
        String[] a = line.split("\t");
        refs.put(
            new Key(Integer.parseInt(a[0]), a[1]),
            new Truth(a[2].equals("-") ? null : decode(a[2]), decode(a[3])));
      }
      if (cursorOnly) {
        prefixes(searcher, kind, probes, filters, refs);
        return;
      }
      String filterName = System.getProperty("catalog.filter");
      if (filterName != null) {
        filters = filters.stream().filter(filter -> filter.name().equals(filterName)).toList();
        if (filters.isEmpty()) throw new IllegalArgumentException("Unknown filter " + filterName);
      }
      int[] budgets =
          System.getProperty("catalog.candidates") == null
              ? CatalogCodecTruth.BUDGETS
              : new int[] {Integer.parseInt(System.getProperty("catalog.candidates"))};
      var trials = new ArrayList<Trial>();
      for (var f : filters)
        for (int c : budgets)
          for (boolean stream : List.of(false, true)) trials.add(new Trial(c, stream, f));
      var out = new PrintWriter(System.out);
      out.println(
          "query,filter,accepted,codec,candidates,mode,repeat,k,pool,prefix_native,pool_native,default_native,full_native,prefix_source,pool_source,default_source,full_source,search_ms,default_rescore_ms,full_rescore_ms,status");
      int reps = Integer.getInteger("catalog.codecRepeats", 1);
      for (int index = -Math.min(6, probes.size()); index < probes.size(); index++) {
        int q = Math.floorMod(index, probes.size());
        for (int rep = 0; rep < (index < 0 ? 1 : reps); rep++) {
          Collections.shuffle(trials, new Random(17L + q * 47 + rep));
          for (var trial : trials) {
            var key = new Key(q, trial.filter.name());
            trial(
                searcher,
                kind,
                trial,
                probes.get(q),
                key,
                refs.get(key),
                index < 0 ? -1 : rep,
                out);
          }
        }
        out.flush();
      }
      if (out.checkError()) throw new IOException("writing benchmark observations");
    }
  }
}
