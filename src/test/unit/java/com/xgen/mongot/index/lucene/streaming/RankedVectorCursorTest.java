package com.xgen.mongot.index.lucene.streaming;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.query.DeadlineExceededException;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.StreamingOptions;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RankedVectorCursorTest {
  private static final float[] TARGET = {99, 0};
  private static final StreamingOptions BUDGET = new StreamingOptions(2, 8, 2, 1, 500);
  private final ByteBuffersDirectory directory = new ByteBuffersDirectory();
  private DirectoryReader reader;
  private IndexSearcher searcher;

  @Before
  public void setUp() throws IOException {
    var codec =
        new Lucene104Codec() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            // Exercise HNSW continuation even for these deliberately tiny test leaves.
            return new Lucene99HnswVectorsFormat(16, 100, 0);
          }
        };
    var config = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
    try (var writer = new IndexWriter(this.directory, config)) {
      // Two graphs, vectors missing from some documents and one leaf without the field.
      for (int id = 0; id < 200; id++) {
        var doc = new Document();
        doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
        doc.add(new StringField("group", Integer.toString(id % 3), Field.Store.NO));
        doc.add(new LongPoint("price", id));
        doc.add(new LongPoint("offers", id));
        doc.add(new LongPoint("offers", 300 - id));
        doc.add(new NumericDocValuesField("price", id));
        if (id % 7 != 0) {
          doc.add(new KnnFloatVectorField("v", new float[] {id, 0}));
        }
        writer.addDocument(doc);
        if (id == 99 || id == 199) {
          writer.commit();
        }
      }
      writer.addDocument(new Document());
      writer.deleteDocuments(new Term("id", "100"));
      writer.commit();
    }
    this.reader = DirectoryReader.open(this.directory);
    this.searcher = new IndexSearcher(this.reader);
  }

  @After
  public void tearDown() throws IOException {
    this.reader.close();
    this.directory.close();
  }

  private static List<RankedVectorCursor.Hit> drain(RankedVectorCursor cursor, int page, int work)
      throws IOException {
    var all = new ArrayList<RankedVectorCursor.Hit>();
    for (int polls = 0; polls < 1000; polls++) {
      var batch = cursor.poll(page, work);
      all.addAll(batch.hits());
      if (batch.exhausted()) {
        return all;
      }
    }
    throw new AssertionError("Cursor did not exhaust");
  }

  private static Query priceRange() {
    return new IndexOrDocValuesQuery(
        LongPoint.newRangeQuery("price", 75, 125),
        NumericDocValuesField.newSlowRangeQuery("price", 75, 125));
  }

  @Test
  public void poll_sparseFilteredSegments_drainsOnceWithStablePagingAndExactScores()
      throws Exception {
    var filter =
        new BooleanQuery.Builder()
            .add(priceRange(), Occur.FILTER)
            .add(new TermQuery(new Term("group", "1")), Occur.SHOULD)
            .add(new TermQuery(new Term("group", "2")), Occur.SHOULD)
            .setMinimumNumberShouldMatch(1)
            .build();
    var expected =
        java.util.stream.IntStream.rangeClosed(75, 125)
            .filter(id -> id % 3 != 0 && id % 7 != 0 && id != 100)
            .mapToObj(
                id ->
                    new RankedVectorCursor.Hit(
                        id,
                        VectorSimilarityFunction.EUCLIDEAN.compare(TARGET, new float[] {id, 0})))
            .toList();
    int refs = this.reader.getRefCount();

    try (var first = RankedVectorCursor.open(this.searcher, "v", TARGET, filter, BUDGET);
        var second = RankedVectorCursor.open(this.searcher, "v", TARGET, filter, BUDGET)) {
      var tiny = drain(first, 1, 1);
      var large = drain(second, 17, 56);

      assertThat(tiny).containsExactlyElementsIn(expected);
      assertThat(large).containsExactlyElementsIn(tiny).inOrder();
      assertThat(first.poll(2, 1).exhausted()).isTrue();
    }
    assertThat(this.reader.getRefCount()).isEqualTo(refs);
  }

  @Test
  public void poll_fullyExpandedWarmup_deliversAllHitsWithoutRefillWork() throws IOException {
    var limits = new StreamingOptions(300, 1000, 1, 1, 500);
    int liveVectors = 200 - 29 - 1; // multiples of seven have no vector, document 100 is deleted
    try (var cursor = RankedVectorCursor.open(this.searcher, "v", TARGET, null, limits)) {
      var batch = cursor.poll(liveVectors, liveVectors);

      assertThat(batch.hits()).hasSize(liveVectors);
      assertThat(batch.exhausted()).isTrue();
    }
  }

  @Test
  public void completedNodes_finalScoringBatchTruncated_keepsLastNeighborhoodForRefill()
      throws IOException {
    var vectors =
        StreamingVectors.open(
            this.reader.leaves().getFirst(),
            "v",
            (factory, similarity, values) ->
                factory.getRandomVectorScorer(similarity, values, TARGET),
            VectorEncoding.FLOAT32,
            false);
    var graph = new WarmupGraph(vectors.graph());
    readNeighborhood(graph, 0);
    readNeighborhood(graph, 1);

    assertThat(graph.completedNodes(true)).containsExactly(0);
  }

  private static void readNeighborhood(WarmupGraph graph, int ord) throws IOException {
    graph.seek(0, ord);
    while (graph.nextNeighbor() != DocIdSetIterator.NO_MORE_DOCS) {
      // Lucene can drain this iterator before limiting how many neighbors it scores.
    }
  }

  @Test
  public void poll_visitLimitDuringWarmup_preservesReachableNeighbors() throws IOException {
    var expected =
        java.util.stream.IntStream.range(0, 200)
            .filter(id -> id % 7 != 0 && id != 100)
            .boxed()
            .toList();
    var limits = new StreamingOptions(8, 20, 2, 1, 500);
    try (var cursor = RankedVectorCursor.open(this.searcher, "v", TARGET, null, limits)) {
      // A graphless leaf scores every ordinal on open and cannot exercise this handoff.
      assertThat(cursor.cachedNodes()).isLessThan(expected.size());
      var docs = drain(cursor, 5, 2).stream().map(RankedVectorCursor.Hit::doc).toList();

      assertThat(docs).containsExactlyElementsIn(expected);
    }
  }

  @Test
  public void poll_negatedMultivaluedPointRange_agreesWithOrdinaryLucene() throws Exception {
    var filter =
        new BooleanQuery.Builder()
            .add(priceRange(), Occur.FILTER)
            .add(LongPoint.newRangeQuery("offers", 181, 226), Occur.MUST_NOT)
            .build();
    var reference =
        new BooleanQuery.Builder()
            .add(filter, Occur.FILTER)
            .add(new org.apache.lucene.search.FieldExistsQuery("v"), Occur.FILTER)
            .build();
    var expected =
        java.util.Arrays.stream(this.searcher.search(reference, 200).scoreDocs)
            .map(hit -> hit.doc)
            .toList();

    try (var cursor = RankedVectorCursor.open(this.searcher, "v", TARGET, filter, BUDGET)) {
      var docs = drain(cursor, 3, 1).stream().map(RankedVectorCursor.Hit::doc).toList();

      assertThat(expected).isNotEmpty();
      assertThat(docs).containsExactlyElementsIn(expected);
    }
  }

  @Test
  public void open_rareRequiredTerm_scoresOnlyBoundedLead() throws Exception {
    var filter =
        new BooleanQuery.Builder()
            .add(priceRange(), Occur.FILTER)
            .add(new TermQuery(new Term("id", "101")), Occur.MUST)
            .build();
    try (var cursor =
        RankedVectorCursor.open(
            this.searcher, "v", TARGET, filter, new StreamingOptions(1, 1, 1, 1, 1))) {
      var batch = cursor.poll(3, 3);

      assertThat(batch.hits()).containsExactly(new RankedVectorCursor.Hit(101, 0.2f));
      assertThat(cursor.cachedNodes()).isEqualTo(1);
      assertThat(batch.exhausted()).isTrue();
    }
  }

  @Test
  public void open_memoryCapAndUnsupportedFilter_releaseReader() {
    int refs = this.reader.getRefCount();
    var cap = new StreamingOptions(3, 30, 1, 1, 1);

    assertThrows(
        IllegalArgumentException.class,
        () -> RankedVectorCursor.open(this.searcher, "v", TARGET, null, cap));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RankedVectorCursor.open(
                this.searcher,
                "v",
                TARGET,
                new org.apache.lucene.search.PrefixQuery(new Term("group", "1")),
                BUDGET));
    assertThat(this.reader.getRefCount()).isEqualTo(refs);
  }

  @Test
  public void poll_expiredDeadline_closesAndReleasesReader() throws Exception {
    int refs = this.reader.getRefCount();
    var expired = new java.util.concurrent.atomic.AtomicBoolean();
    this.searcher.setTimeout(expired::get);
    try (var cursor = RankedVectorCursor.open(this.searcher, "v", TARGET, null, BUDGET)) {
      expired.set(true);

      assertThrows(DeadlineExceededException.class, () -> cursor.poll(1, 1));
      assertThrows(IllegalStateException.class, () -> cursor.poll(1, 1));
    }
    assertThat(this.reader.getRefCount()).isEqualTo(refs);
  }

  @Test
  public void parse_streamingRoundTrip_preservesOptionsAndRejectsInvalidInput() throws Exception {
    String fields = "{index:'idx',path:'v',limit:2,queryVector:[1.0,0.0]";
    var baseline = VectorSearchQuery.fromBson(BsonDocument.parse(fields + "}"));
    var tuned =
        VectorSearchQuery.fromBson(BsonDocument.parse(fields + ",streaming:{refillEvery:3}}"));

    assertThat(baseline.toBson().containsKey("streaming")).isFalse();
    assertThat(((ApproximateVectorSearchCriteria) tuned.criteria()).streaming().orElseThrow())
        .isEqualTo(new StreamingOptions(64, 1024, 3, 8, 250000));
    assertThat(VectorSearchQuery.fromBson(tuned.toBson())).isEqualTo(tuned);
    assertThrows(
        BsonParseException.class,
        () -> VectorSearchQuery.fromBson(BsonDocument.parse(fields + ",streaming:{unknown:1}}")));
    assertThrows(
        BsonParseException.class,
        () ->
            VectorSearchQuery.fromBson(
                BsonDocument.parse(fields + ",streaming:{warmupVisits:0}}")));
    assertThrows(
        BsonParseException.class,
        () -> VectorSearchQuery.fromBson(BsonDocument.parse(fields + ",exact:true,streaming:{}}")));
  }
}
