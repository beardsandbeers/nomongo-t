import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.SplittableRandom;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.FSDirectory;

/** Standalone benchmark of the iterator-backed AcceptDocs used in filtered kNN search. */
public class VectorPrefilterBenchmark {
  record Segment(int docs, int dims, boolean graph) {
    String name() {
      return docs + "-" + dims + (graph ? "-hnsw" : "-flat");
    }
  }

  record Filter(String name, int priceBelow, int ratingAtLeast) {
    Query predicate() {
      var builder = new BooleanQuery.Builder();
      if (priceBelow > 0) {
        builder.add(range("price", 0, priceBelow - 1), BooleanClause.Occur.FILTER);
      }
      if (ratingAtLeast > 0) {
        builder.add(range("rating", ratingAtLeast, 5), BooleanClause.Occur.FILTER);
      }
      return builder.build();
    }

    Query query() {
      return new BooleanQuery.Builder()
          .add(predicate(), BooleanClause.Occur.FILTER)
          .add(new FieldExistsQuery("vector"), BooleanClause.Occur.FILTER)
          .build();
    }

    private static Query range(String field, long min, long max) {
      return new IndexOrDocValuesQuery(
          LongPoint.newRangeQuery(field, min, max),
          NumericDocValuesField.newSlowRangeQuery(field, min, max));
    }
  }

  static final List<Filter> FILTERS =
      List.of(
          new Filter("price_0.01pct", 1, 0),
          new Filter("price_1pct", 100, 0),
          new Filter("price_10pct", 1000, 0),
          new Filter("price_50pct", 5000, 0),
          new Filter("price_70pct", 7000, 0),
          new Filter("price_72pct", 7200, 0),
          new Filter("price_90pct", 9000, 0),
          new Filter("price_99pct", 9900, 0),
          new Filter("rating_ge_2", 0, 2),
          new Filter("price_90pct_and_rating_ge_2", 9000, 2));

  static void build(Path path, Segment segment) throws Exception {
    Files.createDirectory(path);
    var format = new Lucene99HnswVectorsFormat(16, 100, segment.graph ? 0 : Integer.MAX_VALUE);
    var config =
        new IndexWriterConfig()
            .setCodec(
                new Lucene104Codec() {
                  @Override
                  public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    return format;
                  }
                })
            .setUseCompoundFile(false)
            .setMergeScheduler(new SerialMergeScheduler())
            .setRAMBufferSizeMB(128);
    var prices = new SplittableRandom(1841);
    var ratings = new SplittableRandom(1063);
    var vectors = new SplittableRandom(7927);
    try (var dir = FSDirectory.open(path);
        var writer = new IndexWriter(dir, config)) {
      var doc = new Document();
      var price = new LongPoint("price", 0L);
      var rating = new LongPoint("rating", 0L);
      var priceValues = new NumericDocValuesField("price", 0);
      var ratingValues = new NumericDocValuesField("rating", 0);
      var values = new float[segment.dims];
      var vector = new KnnFloatVectorField("vector", values);
      doc.add(price);
      doc.add(rating);
      doc.add(priceValues);
      doc.add(ratingValues);
      doc.add(vector);
      for (int i = 0; i < segment.docs; i++) {
        long priceValue = prices.nextInt(10000);
        long ratingValue = 1 + ratings.nextInt(5);
        price.setLongValue(priceValue);
        rating.setLongValue(ratingValue);
        priceValues.setLongValue(priceValue);
        ratingValues.setLongValue(ratingValue);
        for (int j = 0; j < values.length; j++) {
          values[j] = vectors.nextFloat();
        }
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }
  }

  static void measure(Path path, Segment segment, PrintWriter out) throws Exception {
    try (var dir = FSDirectory.open(path);
        var reader = DirectoryReader.open(dir)) {
      if (reader.leaves().size() != 1) {
        throw new IllegalStateException("Expected one segment in " + path);
      }
      LeafReaderContext leaf = reader.leaves().getFirst();
      var sr = (SegmentReader) leaf.reader();
      var vectors = sr.getFloatVectorValues("vector");
      if (vectors.size() != segment.docs
          || vectors.dimension() != segment.dims
          || sr.numDocs() != sr.maxDoc()
          || sr.numDocs() != segment.docs) {
        throw new IllegalStateException("Unexpected segment geometry in " + path);
      }
      var perField = (PerFieldKnnVectorsFormat.FieldsReader) sr.getVectorReader();
      var provider = (HnswGraphProvider) perField.getFieldReader("vector");
      if (provider.getGraph("vector").size() != (segment.graph ? segment.docs : 0)) {
        throw new IllegalStateException("Unexpected HNSW graph size in " + path);
      }
      long bytes = sr.getSegmentInfo().sizeInBytes();
      var searcher = new IndexSearcher(reader);
      searcher.setQueryCache(null);
      var expected = new ArrayList<Integer>();
      for (var filter : FILTERS) {
        expected.add(searcher.count(filter.query()));
      }
      var order = new ArrayList<Integer>();
      for (int i = 0; i < FILTERS.size(); i++) {
        order.add(i);
      }
      var orderRandom = new Random(179);
      for (int round = -12; round < 25; round++) {
        Collections.shuffle(order, orderRandom);
        for (int i : order) {
          var query = FILTERS.get(i).query();
          Weight weight =
              searcher.rewrite(query).createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
          var accepted =
              AcceptDocs.fromIteratorSupplier(
                  () -> {
                    Scorer scorer = weight.scorer(leaf);
                    return scorer == null ? DocIdSetIterator.empty() : scorer.iterator();
                  },
                  sr.getLiveDocs(),
                  sr.maxDoc());
          long start = System.nanoTime();
          int count = accepted.cost();
          long elapsed = System.nanoTime() - start;
          if (count != expected.get(i)) {
            throw new IllegalStateException("Wrong accepted count for " + FILTERS.get(i));
          }
          if (round >= 0) {
            out.printf(
                Locale.ROOT,
                "%s,%d,%d,%d,%s,%d,%d,%d%n",
                segment.name(),
                segment.docs,
                segment.dims,
                bytes,
                FILTERS.get(i).name,
                count,
                round,
                elapsed);
          }
        }
      }
      out.flush();
      if (out.checkError()) {
        throw new IllegalStateException("Could not write benchmark samples");
      }
    }
  }

  // Time independent full filtered kNN queries on the segments with a graph.
  static void measureSearch(Path path, Segment segment, PrintWriter out) throws Exception {
    try (var dir = FSDirectory.open(path);
        var reader = DirectoryReader.open(dir)) {
      var searcher = new IndexSearcher(reader);
      searcher.setQueryCache(null);
      var order = new ArrayList<>(FILTERS);
      var orderRandom = new Random(179);
      var vectors = new SplittableRandom(1549);
      for (int round = -12; round < 25; round++) {
        Collections.shuffle(order, orderRandom);
        var target = new float[segment.dims];
        for (int j = 0; j < target.length; j++) {
          target[j] = vectors.nextFloat();
        }
        for (var filter : order) {
          var query = new KnnFloatVectorQuery("vector", target, 100, filter.predicate());
          long start = System.nanoTime();
          var hits = searcher.search(query, 10);
          long elapsed = System.nanoTime() - start;
          if (round >= 0) {
            out.printf(
                Locale.ROOT,
                "%s,%s,%d,%d,%d%n",
                segment.name(),
                filter.name,
                round,
                hits.scoreDocs.length,
                elapsed);
          }
        }
      }
      out.flush();
      if (out.checkError()) {
        throw new IllegalStateException("Could not write full search samples");
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      throw new IllegalArgumentException(
          "Supply a new index directory and new materialization and search CSV paths");
    }
    var root = Files.createDirectory(Path.of(args[0]));
    try (var searchOut =
            new PrintWriter(
                Files.newBufferedWriter(
                    Path.of(args[2]),
                    java.nio.file.StandardOpenOption.CREATE_NEW,
                    java.nio.file.StandardOpenOption.WRITE));
        var out =
            new PrintWriter(
                Files.newBufferedWriter(
                    Path.of(args[1]),
                    java.nio.file.StandardOpenOption.CREATE_NEW,
                    java.nio.file.StandardOpenOption.WRITE))) {
      searchOut.println("segment,filter,round,hits,search_ns");
      out.println("segment,docs,dims,segment_bytes,filter,accepted,round,materialization_ns");
      for (var segment :
          List.of(
              new Segment(100_000, 16, false),
              new Segment(100_000, 384, false),
              new Segment(1_000_000, 16, false),
              new Segment(2_200_000, 16, false),
              new Segment(3_000_000, 16, false),
              new Segment(10_000_000, 16, false),
              new Segment(100_000, 16, true),
              new Segment(1_000_000, 16, true))) {
        var path = root.resolve(segment.name());
        System.err.println("Building " + path);
        build(path, segment);
        System.err.println("Measuring " + path);
        measure(path, segment, out);
        if (segment.graph) {
          measureSearch(path, segment, searchOut);
        }
      }
    }
  }
}
