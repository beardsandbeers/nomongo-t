import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NumericUtils;

/** Build one real-catalog HNSW leaf and isolate filter materialization on that leaf. */
public class CatalogPrefilter {
  static final int DIMS = 4154;
  static final int DOCS = 312_000;

  private static IndexWriterConfig config(KnnVectorsFormat format) {
    return new IndexWriterConfig()
        .setUseCompoundFile(false)
        .setMergePolicy(NoMergePolicy.INSTANCE)
        .setRAMBufferSizeMB(192)
        .setCodec(
            new Lucene104Codec() {
              @Override
              public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return format;
              }
            });
  }

  interface FormatFactory {
    KnnVectorsFormat create(int threshold, int workers, java.util.concurrent.ExecutorService pool);
  }

  static void build(
      Path inputs,
      Path staging,
      Path index,
      int docs,
      VectorEncoding encoding,
      FormatFactory formats)
      throws Exception {
    build(inputs, staging, index, docs, encoding, formats, java.util.function.Function.identity());
  }

  static void build(
      Path inputs,
      Path staging,
      Path index,
      int docs,
      VectorEncoding encoding,
      FormatFactory formats,
      java.util.function.Function<byte[], byte[]> encodeBytes)
      throws Exception {
    boolean useBytes = encoding == VectorEncoding.BYTE;
    // Flush bounded batches; build the graph once in the final codec merge.
    var flat = formats.create(Integer.MAX_VALUE, 1, null);
    Files.createDirectory(staging);
    try (var dir = FSDirectory.open(staging);
        var writer = new IndexWriter(dir, config(flat));
        var metadata = Files.newBufferedReader(inputs.resolve("products.tsv"));
        var vectors =
            new BufferedInputStream(
                Files.newInputStream(inputs.resolve(useBytes ? "vectors.i8" : "vectors.f32")))) {
      byte[] buffer = new byte[DIMS * encoding.byteSize];
      var row = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
      float[] vector = new float[DIMS];
      int count = 0;
      for (String line; (line = metadata.readLine()) != null; ) {
        String[] values = line.split("\t", -1);
        if (values.length != 5 || vectors.readNBytes(buffer, 0, buffer.length) != buffer.length) {
          throw new IOException("Truncated input at " + count);
        }
        var doc = new Document();
        if (useBytes) {
          doc.add(
              new KnnByteVectorField(
                  "vector", encodeBytes.apply(buffer), VectorSimilarityFunction.EUCLIDEAN));
        } else {
          row.get(0, vector);
          doc.add(new KnnFloatVectorField("vector", vector, VectorSimilarityFunction.EUCLIDEAN));
        }
        doc.add(new StringField("asin", values[0], Field.Store.NO));
        if (!values[4].isEmpty()) doc.add(new StringField("category", values[4], Field.Store.NO));
        addNumber(doc, "price", values[1]);
        addNumber(doc, "rating", values[2]);
        addNumber(doc, "reviews", values[3]);
        writer.addDocument(doc);
        count++;
      }
      if (count != docs || vectors.read() != -1) throw new IOException("Unexpected input size");
    }
    Files.createDirectory(index);
    try (var pool = Executors.newFixedThreadPool(8);
        var src = FSDirectory.open(staging);
        var reader = DirectoryReader.open(src);
        var dest = FSDirectory.open(index);
        var writer =
            new IndexWriter(
                dest, config(formats.create(0, 8, pool)).setMergePolicy(new TieredMergePolicy()))) {
      CodecReader[] inputsToMerge =
          reader.leaves().stream().map(c -> (CodecReader) c.reader()).toArray(CodecReader[]::new);
      writer.addIndexes(inputsToMerge);
    }
  }

  private static KnnVectorsFormat format(
      int threshold, int workers, java.util.concurrent.ExecutorService pool) {
    return new Lucene104HnswScalarQuantizedVectorsFormat(
        ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE, 16, 100, workers, pool, threshold) {
      @Override
      public int getMaxDimensions(String field) {
        return DIMS;
      }
    };
  }

  private static void addNumber(Document doc, String field, String value) {
    if (!value.isEmpty()) {
      double v = Double.parseDouble(value);
      if (!Double.isFinite(v)) throw new IllegalArgumentException("Nonfinite " + field);
      long encoded = NumericUtils.doubleToSortableLong(v);
      doc.add(new LongPoint(field, encoded));
      doc.add(new NumericDocValuesField(field, encoded));
    }
  }

  record Case(String name, Query query) {}

  static Query range(String field, double start, double end) {
    long lower = NumericUtils.doubleToSortableLong(start);
    long upper = NumericUtils.doubleToSortableLong(end);
    return new IndexOrDocValuesQuery(
        LongPoint.newRangeQuery(field, lower, upper),
        NumericDocValuesField.newSlowRangeQuery(field, lower, upper));
  }

  private static Query and(Query... queries) {
    var q = new BooleanQuery.Builder();
    for (Query clause : queries) q.add(clause, BooleanClause.Occur.FILTER);
    return q.build();
  }

  static List<Case> cases() {
    Query price = range("price", 5, 100), rating = range("rating", 4, 5);
    Query reviews = range("reviews", 10, Double.MAX_VALUE);
    var exceptBooks =
        new BooleanQuery.Builder()
            .add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER)
            .add(new TermQuery(new Term("category", "Books")), BooleanClause.Occur.MUST_NOT)
            .build();
    return List.of(
        new Case("price_5_to_100", price),
        new Case("rating_at_least_4", rating),
        new Case("reviews_at_least_10", reviews),
        new Case("except_books", exceptBooks),
        new Case("price_and_rating", and(price, rating)),
        new Case("price_rating_reviews", and(price, rating, reviews)),
        new Case("price_rating_reviews_except_books", and(price, rating, reviews, exceptBooks)));
  }

  static void measure(Path index, Path csv, Path evict, int docs, VectorEncoding encoding)
      throws Exception {
    try (var dir = FSDirectory.open(index);
        var r = DirectoryReader.open(dir)) {
      if (r.leaves().size() != 1) throw new IOException("Expected one physical segment");
      var leaf = (SegmentReader) r.leaves().getFirst().reader();
      var perField = (PerFieldKnnVectorsFormat.FieldsReader) leaf.getVectorReader();
      var provider = (HnswGraphProvider) perField.getFieldReader("vector");
      var graph = provider.getGraph("vector");
      long bytes = leaf.getSegmentInfo().sizeInBytes();
      var field = leaf.getFieldInfos().fieldInfo("vector");
      if (field.getVectorEncoding() != encoding
          || field.getVectorDimension() != DIMS
          || r.numDocs() != docs
          || graph.size() != docs
          || graph.numLevels() < 1
          || bytes < 4_800_000_000L
          || bytes > 5_800_000_000L)
        throw new IOException(
            "Unexpected fixture geometry: "
                + r.numDocs()
                + " docs, "
                + graph.size()
                + " nodes, "
                + bytes
                + " bytes");
      System.out.println(
          "Segment bytes=" + bytes + " docs=" + r.numDocs() + " graphLevels=" + graph.numLevels());
    }
    var order = new ArrayList<>(cases());
    var random = new Random(1583);
    try (var out = new PrintWriter(Files.newBufferedWriter(csv, StandardOpenOption.CREATE_NEW))) {
      out.println("round,filter,cache,accepted,materialization_ns");
      for (int round = -3; round < 25; round++) {
        Collections.shuffle(order, random);
        for (var filter : order) {
          if (new ProcessBuilder("python3", evict.toString(), index.toString())
                  .inheritIO()
                  .start()
                  .waitFor()
              != 0) throw new IOException("Cache eviction failed");
          try (var dir = FSDirectory.open(index);
              var r = DirectoryReader.open(dir)) {
            var ctx = r.leaves().getFirst();
            var searcher = new IndexSearcher(r);
            searcher.setQueryCache(null);
            Query query = and(filter.query(), new FieldExistsQuery("vector"));
            for (String cache : List.of("eviction_requested", "warm")) {
              Weight w =
                  searcher.rewrite(query).createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
              var accepted =
                  AcceptDocs.fromIteratorSupplier(
                      () -> {
                        var scorer = w.scorer(ctx);
                        return scorer == null ? DocIdSetIterator.empty() : scorer.iterator();
                      },
                      ctx.reader().getLiveDocs(),
                      ctx.reader().maxDoc());
              long start = System.nanoTime();
              int count = accepted.cost();
              long elapsed = System.nanoTime() - start;
              if (round >= 0)
                out.printf("%d,%s,%s,%d,%d%n", round, filter.name, cache, count, elapsed);
            }
            out.flush();
          }
        }
      }
      if (out.checkError()) throw new IOException("Output write failed");
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      throw new IllegalArgumentException(
          "Expected build <inputs> <staging> <index> or measure <index> <csv> <evict.py>");
    }
    switch (args[0]) {
      case "build" ->
          build(
              Path.of(args[1]),
              Path.of(args[2]),
              Path.of(args[3]),
              DOCS,
              VectorEncoding.FLOAT32,
              CatalogPrefilter::format);
      case "measure" ->
          measure(
              Path.of(args[1]), Path.of(args[2]), Path.of(args[3]), DOCS, VectorEncoding.FLOAT32);
      default -> throw new IllegalArgumentException("Use build or measure");
    }
  }
}
