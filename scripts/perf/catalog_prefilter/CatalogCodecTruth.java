import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;

/** Exact Euclidean and exhaustive sign-bit controls on identical IDs and metadata. */
public class CatalogCodecTruth {
  static final int[] LIMITS = {10, 20, 50, 100};
  static final int[] BUDGETS = {500, 4000};
  static final int D = CatalogPrefilter.DIMS;
  static final Comparator<ScoreDoc> BEST =
      Comparator.<ScoreDoc>comparingDouble(x -> x.score).reversed().thenComparingInt(x -> x.doc);

  record Probe(float[] floats, byte[] bytes) {}

  static byte rounded(float v) {
    double code = Math.rint(v * 256.0);
    if (!Double.isFinite(code) || Math.abs(code) > 127) throw new IllegalArgumentException("range");
    return (byte) code;
  }

  static List<Probe> probes(Path path) throws Exception {
    var buf = ByteBuffer.wrap(Files.readAllBytes(path)).order(ByteOrder.LITTLE_ENDIAN);
    var all = new ArrayList<Probe>();
    while (buf.hasRemaining()) {
      var f = new float[D];
      var b = new byte[D];
      for (int i = 0; i < D; i++) {
        f[i] = buf.getFloat();
        b[i] = rounded(f[i]);
      }
      all.add(new Probe(f, b));
    }
    if (all.isEmpty()) throw new IllegalArgumentException("Missing queries");
    return all;
  }

  static List<RankedStreamBenchmark.Case> cases(IndexSearcher searcher) throws Exception {
    return List.of(
        RankedStreamBenchmark.prepare(searcher, CatalogFilterProfile.catalogCase(6, false)),
        RankedStreamBenchmark.prepare(searcher, CatalogFilterProfile.catalogCase(2, true)));
  }

  // Include document identity and filter membership. Comparing doc IDs across indexes is safe
  // only when segment merges preserved the complete product-to-ID mapping.
  static String identity(IndexReader reader, List<RankedStreamBenchmark.Case> filters)
      throws Exception {
    var md = MessageDigest.getInstance("SHA-256");
    var number = ByteBuffer.allocate(4);
    for (var leaf : reader.leaves()) {
      var terms = leaf.reader().terms("asin").iterator();
      PostingsEnum postings = null;
      int count = 0;
      for (var term = terms.next(); term != null; term = terms.next()) {
        postings = terms.postings(postings, PostingsEnum.NONE);
        if (terms.docFreq() != 1) throw new IllegalStateException("Duplicate ASIN in leaf");
        md.update(number.putInt(0, leaf.docBase + postings.nextDoc()).array());
        md.update(number.putInt(0, term.length).array());
        md.update(term.bytes, term.offset, term.length);
        count++;
      }
      if (count != leaf.reader().maxDoc() || leaf.reader().hasDeletions())
        throw new IllegalStateException("Unexpected leaf layout");
    }
    for (var filter : filters) {
      byte[] bits = filter.allowed().toByteArray();
      md.update(number.putInt(0, bits.length).array());
      md.update(bits);
    }
    return HexFormat.of().formatHex(md.digest());
  }

  /** Sample retained coordinates across both leaves independently of the graph codec. */
  static String coordinates(IndexReader reader, boolean byteInput, boolean byteDigest)
      throws Exception {
    var digest = MessageDigest.getInstance("SHA-256");
    var buffer = ByteBuffer.allocate(D * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (var context : reader.leaves()) {
      var leaf = context.reader();
      var floats = byteInput ? null : leaf.getFloatVectorValues("vector");
      var bytes = byteInput ? leaf.getByteVectorValues("vector") : null;
      for (int ordinal = 0; ordinal < leaf.maxDoc(); ordinal += 997) {
        if (byteInput) {
          digest.update(bytes.vectorValue(ordinal));
        } else {
          buffer.clear();
          for (float value : floats.vectorValue(ordinal)) {
            if (byteDigest) buffer.put(rounded(value));
            else buffer.putFloat(value);
          }
          digest.update(buffer.array(), 0, buffer.position());
        }
      }
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  static class Top {
    final int size;
    final PriorityQueue<ScoreDoc> heap = new PriorityQueue<>(BEST.reversed());

    Top(int size) {
      this.size = size;
    }

    void add(int doc, float score) {
      // Input arrives in increasing document order, so an equal-score later hit loses a tie.
      if (heap.size() == size) {
        if (score <= heap.element().score) return;
        heap.remove();
      }
      heap.add(new ScoreDoc(doc, score));
    }

    int[] ids() {
      return heap.stream().sorted(BEST).mapToInt(x -> x.doc).toArray();
    }
  }

  static int overlap(int[] docs, int depth, int[] truth, int k) {
    var relevant = new HashSet<Integer>();
    for (int i = 0; i < k; i++) relevant.add(truth[i]);
    int hits = 0;
    for (int i = 0; i < depth; i++) if (relevant.contains(docs[i])) hits++;
    return hits;
  }

  static void signs(float[] f, byte[] b, long[] floatSigns, long[] byteSigns) {
    Arrays.fill(floatSigns, 0);
    Arrays.fill(byteSigns, 0);
    for (int i = 0; i < D; i++) {
      if (f[i] > 0) floatSigns[i / 64] |= 1L << (i % 64);
      if (b[i] > 0) byteSigns[i / 64] |= 1L << (i % 64);
    }
  }

  static int scoreBits(long[] a, long[] b) {
    int distance = 0;
    for (int i = 0; i < a.length; i++) distance += Long.bitCount(a[i] ^ b[i]);
    return -distance;
  }

  private static String encode(int[] docs) {
    return Arrays.stream(docs).mapToObj(Integer::toString).collect(Collectors.joining(" "));
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 5)
      throw new IllegalArgumentException("floatLeaf0 floatLeaf1 queries.f32 truth.tsv oracle.csv");
    var queries = probes(Path.of(args[2]));
    try (var d0 = FSDirectory.open(Path.of(args[0]));
        var d1 = FSDirectory.open(Path.of(args[1]));
        var r = new MultiReader(DirectoryReader.open(d0), DirectoryReader.open(d1));
        var truth = new PrintWriter(Files.newBufferedWriter(Path.of(args[3])));
        var oracle = new PrintWriter(Files.newBufferedWriter(Path.of(args[4])))) {
      var filters = cases(new IndexSearcher(r));
      truth.println(
          String.join(
              "\t",
              identity(r, filters),
              coordinates(r, false, false),
              coordinates(r, false, true)));
      oracle.println(
          "query,filter,k,candidates,representation,exact_native_hits,exact_source_hits,pool_native_hits,pool_source_hits");
      // Scan each original float once. Keep only top-k Euclidean and top-budget Hamming lists.
      var rankings = new Top[queries.size()][filters.size()][4];
      int signWords = (D + 63) / 64;
      long[][] floatQuerySigns = new long[queries.size()][signWords],
          byteQuerySigns = new long[queries.size()][signWords];
      for (int q = 0; q < queries.size(); q++) {
        signs(queries.get(q).floats, queries.get(q).bytes, floatQuerySigns[q], byteQuerySigns[q]);
        for (int f = 0; f < filters.size(); f++)
          for (int j = 0; j < 4; j++)
            rankings[q][f][j] = new Top(j < 2 ? 100 : BUDGETS[BUDGETS.length - 1]);
      }
      byte[] bytes = new byte[D];
      long[] floatSigns = new long[signWords], byteSigns = new long[signWords];
      for (var leaf : r.leaves()) {
        var values = leaf.reader().getFloatVectorValues("vector");
        for (int ord = 0; ord < values.size(); ord++) {
          int doc = leaf.docBase + values.ordToDoc(ord);
          if (filters.stream().noneMatch(filter -> filter.allowed().get(doc))) continue;
          float[] floats = values.vectorValue(ord);
          for (int j = 0; j < D; j++) bytes[j] = rounded(floats[j]);
          signs(floats, bytes, floatSigns, byteSigns);
          for (int q = 0; q < queries.size(); q++) {
            var query = queries.get(q);
            float raw = VectorSimilarityFunction.EUCLIDEAN.compare(floats, query.floats);
            float rawByte = VectorSimilarityFunction.EUCLIDEAN.compare(bytes, query.bytes);
            int binary = scoreBits(floatSigns, floatQuerySigns[q]),
                binaryByte = scoreBits(byteSigns, byteQuerySigns[q]);
            for (int f = 0; f < filters.size(); f++)
              if (filters.get(f).allowed().get(doc)) {
                var t = rankings[q][f];
                t[0].add(doc, raw);
                t[1].add(doc, rawByte);
                t[2].add(doc, binary);
                t[3].add(doc, binaryByte);
              }
          }
        }
      }
      for (int q = 0; q < queries.size(); q++)
        for (int f = 0; f < filters.size(); f++) {
          int[][] ids = Arrays.stream(rankings[q][f]).map(Top::ids).toArray(int[][]::new);
          String name = filters.get(f).name();
          truth.printf("%d\t%s\t%s\t%s%n", q, name, encode(ids[0]), encode(ids[1]));
          for (int k : LIMITS)
            for (int c : BUDGETS)
              for (int representation = 0; representation < 4; representation++) {
                int[] nativeTruth = ids[representation % 2];
                String label = List.of("f32", "i8", "f32-binary", "i8-binary").get(representation);
                int depth = Math.min(c, ids[representation].length);
                // Euclidean controls only retained top 100. Pool overlap is defined for binary
                // lists.
                oracle.printf(
                    "%d,%s,%d,%d,%s,%d,%d,%s,%s%n",
                    q,
                    name,
                    k,
                    c,
                    label,
                    overlap(ids[representation], k, nativeTruth, k),
                    overlap(ids[representation], k, ids[0], k),
                    representation < 2 ? "" : overlap(ids[representation], depth, nativeTruth, k),
                    representation < 2 ? "" : overlap(ids[representation], depth, ids[0], k));
              }
        }
      if (truth.checkError() || oracle.checkError())
        throw new java.io.IOException("writing controls");
    }
  }
}
