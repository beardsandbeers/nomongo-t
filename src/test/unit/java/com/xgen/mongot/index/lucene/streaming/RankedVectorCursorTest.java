package com.xgen.mongot.index.lucene.streaming;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.knn.VectorSearchExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.query.custom.MongotKnnFloatQuery;
import com.xgen.mongot.index.query.DeadlineExceededException;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.StreamingOptions;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class RankedVectorCursorTest {
  private static final float[] TARGET = {99, 0};
  private static final StreamingOptions BUDGET = new StreamingOptions(2, 8, 2, 1, 500);
  private final ByteBuffersDirectory directory = new ByteBuffersDirectory();
  private DirectoryReader reader;
  private IndexSearcher searcher;

  private static Lucene104Codec codecWithGraphThreshold(int threshold) {
    return new Lucene104Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return new Lucene99HnswVectorsFormat(16, 100, threshold);
      }
    };
  }

  @Before
  public void setUp() throws IOException {
    // Exercise HNSW continuation even for these deliberately tiny test leaves.
    var config =
        new IndexWriterConfig()
            .setCodec(codecWithGraphThreshold(0))
            .setMergePolicy(NoMergePolicy.INSTANCE);
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
  public void refineCandidates_competitiveFrontier_improvesProvisionalHits() throws Exception {
    var options = new StreamingOptions(1, 1, 1000, 8, 500);
    int candidates = 8;
    try (var cursor =
        RankedVectorCursor.open(
            this.searcher,
            "v",
            TARGET,
            null,
            options,
            RankedVectorCursor.Use.FINITE_STAGE,
            candidates)) {
      var provisional = cursor.poll(candidates, 100).hits();
      var refined = new ArrayList<>(provisional);
      cursor.refineCandidates(refined);

      assertThat(refined).hasSize(candidates);
      assertThat(refined.stream().map(RankedVectorCursor.Hit::doc).distinct().count())
          .isEqualTo(candidates);
      assertThat(refined.stream().mapToDouble(RankedVectorCursor.Hit::score).sum())
          .isGreaterThan(provisional.stream().mapToDouble(RankedVectorCursor.Hit::score).sum());
    }
  }

  @Test
  public void refineCandidates_atCacheCap_keepsBestKnownHits() throws Exception {
    var options = new StreamingOptions(1, 1, 1000, 8, 2);
    try (var cursor =
        RankedVectorCursor.open(
            this.searcher, "v", TARGET, null, options, RankedVectorCursor.Use.FINITE_STAGE, 2)) {
      var provisional = cursor.poll(2, 2).hits();
      var refined = new ArrayList<>(provisional);
      cursor.refineCandidates(refined);

      assertThat(cursor.cachedNodes()).isEqualTo(2);
      assertThat(refined).containsExactlyElementsIn(provisional);
    }
  }

  @Test
  public void refineCandidates_oneCacheSlotLeft_stopsWithinFinalNeighborhood() throws Exception {
    var options = new StreamingOptions(1, 1, 1000, 8, 3);
    try (var cursor =
        RankedVectorCursor.open(
            this.searcher, "v", TARGET, null, options, RankedVectorCursor.Use.FINITE_STAGE, 2)) {
      var hits = new ArrayList<>(cursor.poll(2, 2).hits());
      cursor.refineCandidates(hits);

      assertThat(hits).hasSize(2);
      assertThat(cursor.cachedNodes()).isEqualTo(3);
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
        Arrays.stream(this.searcher.search(reference, 200).scoreDocs).map(hit -> hit.doc).toList();

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

  private static final String QUERY_FIELDS =
      "{path:'v',limit:2,numCandidates:10,queryVector:[1.0,0.0]";

  private static StreamingOptions options(String stream) throws Exception {
    var query =
        VectorSearchQuery.fromBson(
            BsonDocument.parse(QUERY_FIELDS + ",index:'idx',stream:" + stream + "}"));
    return ((ApproximateVectorSearchCriteria) query.criteria()).streaming().orElseThrow();
  }

  @Test
  public void parse_streamForms_shareDefaultsAcrossBothStages() throws Exception {
    var opts = options("true");
    assertThat(options("{}")).isEqualTo(opts);
    assertThat(opts.streamBson().asBoolean().getValue()).isTrue();
    try (var p =
        BsonDocumentParser.fromRoot(
                BsonDocument.parse(
                    QUERY_FIELDS + ",stream:{initialScoreBudget:8,lookaheadHits:3}}"))
            .build()) {
      var operator = VectorSearchOperator.fromBson(p);
      assertThat(((ApproximateVectorSearchCriteria) operator.criteria()).streaming().orElseThrow())
          .isEqualTo(options("{initialScoreBudget:8,lookaheadHits:3}"));
      assertThat(operator.operatorToBson().asDocument().getDocument("stream"))
          .isEqualTo(BsonDocument.parse("{initialScoreBudget:8,lookaheadHits:3}"));
    }
    assertThat(
            ((ApproximateVectorSearchCriteria)
                    VectorSearchQuery.fromBson(
                            BsonDocument.parse(QUERY_FIELDS + ",index:'idx',stream:false}"))
                        .criteria())
                .streaming())
        .isEmpty();
    assertThrows(BsonParseException.class, () -> options("42"));
    assertThrows(BsonParseException.class, () -> options("{unexpected:1}"));
    assertThrows(BsonParseException.class, () -> options("{initialScoreBudget:0}"));
    assertThrows(BsonParseException.class, () -> options("{lookaheadHits:11,maxCachedNodes:10}"));
    assertThrows(
        BsonParseException.class,
        () ->
            VectorSearchQuery.fromBson(
                BsonDocument.parse(QUERY_FIELDS + ",index:'idx',stream:true,streaming:{}}")));
    assertThrows(
        BsonParseException.class,
        () ->
            VectorSearchQuery.fromBson(
                BsonDocument.parse(
                    "{index:'idx',path:'v',limit:2,queryVector:[1.0],exact:true,stream:true}")));
  }

  @Test
  public void choose_thousandMatchingVectors_usesClassicWithoutSampling() throws IOException {
    var doc = new Document();
    doc.add(new KnnFloatVectorField("v", new float[] {1f}));
    doc.add(new StringField("group", "yes", Field.Store.NO));
    try (var directory = new ByteBuffersDirectory();
        var writer =
            new IndexWriter(
                directory,
                new IndexWriterConfig().setCodec(codecWithGraphThreshold(Integer.MAX_VALUE)))) {
      writer.addDocuments(java.util.Collections.nCopies(1000, doc));
      try (var reader = DirectoryReader.open(writer)) {
        var decision =
            StreamRouting.choose(
                    reader,
                    "v",
                    VectorEncoding.FLOAT32,
                    new TermQuery(new Term("group", "yes")),
                    100,
                    10,
                    true,
                    false)
                .segments()
                .getFirst();
        assertThat(decision.stream()).isFalse();
        assertThat(decision.reason()).isEqualTo("smallVectorPopulation");
        assertThat(decision.vectors()).isEqualTo(1000);
        assertThat(decision.sampled()).isEqualTo(0);
      }
    }
  }

  @Test
  public void choose_sparseVectorField_samplesActualVectorDocs() throws Exception {
    var codec = codecWithGraphThreshold(Integer.MAX_VALUE);
    try (var directory = new ByteBuffersDirectory()) {
      try (var writer = new IndexWriter(directory, new IndexWriterConfig().setCodec(codec))) {
        var withVector = new Document();
        withVector.add(new KnnFloatVectorField("v", new float[] {1f}));
        withVector.add(new StringField("group", "yes", Field.Store.NO));
        for (int i = 0; i < 520000; i++) {
          withVector.removeFields("tag");
          if (i % 5 < 3) {
            withVector.add(new StringField("tag", "borderline", Field.Store.NO));
          }
          if (i % 5 < 4) {
            withVector.add(new StringField("tag", "broad", Field.Store.NO));
          }
          if (i % 8 == 0) {
            writer.addDocument(new Document());
          }
          writer.addDocument(withVector);
        }
        writer.forceMerge(1);
      }
      try (var base = DirectoryReader.open(directory);
          var reader = new MultiReader(new IndexReader[] {this.reader, base}, false)) {
        var yes = new TermQuery(new Term("group", "yes"));
        var plan =
            StreamRouting.choose(reader, "v", VectorEncoding.FLOAT32, yes, 100, 10, true, false);
        assertThat(plan.describe().getString("selected").getValue()).isEqualTo("mixed");
        var broad = plan.segments().getLast();
        assertThat(broad.vectors()).isEqualTo(520000);
        assertThat(broad.accepted()).isEqualTo(broad.sampled());
        assertThat(broad.stream()).isTrue();
        assertThat(plan.segments().getFirst().sampled()).isEqualTo(0);
        var range = new BooleanQuery.Builder().add(priceRange(), Occur.FILTER).build();
        var cheapRange =
            StreamRouting.choose(reader, "v", VectorEncoding.FLOAT32, range, 100, 10, true, false)
                .segments()
                .getLast();
        assertThat(cheapRange.reason()).isEqualTo("singleRange");
        assertThat(cheapRange.sampled()).isEqualTo(0);
        var fewer =
            StreamRouting.choose(
                    reader,
                    "v",
                    VectorEncoding.FLOAT32,
                    new TermQuery(new Term("tag", "broad")),
                    100,
                    10,
                    true,
                    false)
                .segments()
                .getLast();
        assertThat(fewer.reason()).isEqualTo("smallMatchingPopulation");
        assertThat(fewer.describe().getDouble("estimatedAcceptance").doubleValue()).isAtLeast(0.70);
        assertThat(fewer.describe().getInt64("estimatedMatchingVectors").longValue())
            .isEqualTo(520000L * fewer.accepted() / 512);
        var borderline =
            StreamRouting.choose(
                    reader,
                    "v",
                    VectorEncoding.FLOAT32,
                    new TermQuery(new Term("tag", "borderline")),
                    100,
                    10,
                    true,
                    false)
                .segments()
                .getLast();
        assertThat(borderline.reason()).isEqualTo("selectiveOrUncertain");
        assertThat(
                StreamRouting.choose(
                        reader, "v", VectorEncoding.FLOAT32, yes, 10000, 100, true, false)
                    .segments()
                    .getLast()
                    .reason())
            .isEqualTo("uncalibratedProfile");
      }
    }
  }

  @Test
  public void search_mixedLeafRoutes_preservesGlobalDocIdsAndLimit() throws Exception {
    var filter = new TermQuery(new Term("group", "1"));
    var stats = new BsonDocument();
    var segments =
        this.reader.leaves().stream()
            .map(
                ctx ->
                    new StreamRouting.Segment(
                        ctx,
                        "fixture",
                        ctx.ord == 1 ? "broadFilter" : "smallVectorPopulation",
                        ctx.reader().maxDoc(),
                        0,
                        0,
                        0,
                        ctx.ord == 1 ? stats : null))
            .toList();
    var plan = new StreamRouting(segments, 10, 100);
    var streaming =
        new StreamingKnnQuery.Floats("v", TARGET, 100, filter, StreamingOptions.defaults());
    var query =
        new MongotKnnFloatQuery(
            Mockito.mock(IndexMetricsUpdater.QueryingMetricsUpdater.class),
            FeatureFlags.getDefault(),
            "v",
            TARGET,
            100,
            filter,
            StreamingKnnQuery.routed(plan, streaming));
    var expected = this.searcher.search(new KnnFloatVectorQuery("v", TARGET, 100, filter), 10);
    var actual = this.searcher.search(query, 10);

    assertThat(stats.getBoolean("fallbackToClassic").getValue()).isFalse();
    assertThat(Arrays.stream(actual.scoreDocs).map(d -> d.doc).toList())
        .containsExactlyElementsIn(Arrays.stream(expected.scoreDocs).map(d -> d.doc).toList())
        .inOrder();
    long fromFirst = Arrays.stream(actual.scoreDocs).filter(d -> d.doc < 100).count();
    assertThat(fromFirst).isGreaterThan(0);
    assertThat(fromFirst).isLessThan(10);
  }

  @Test
  public void parse_openEndedAndAuto_preservesModeAndLimit() throws Exception {
    var fields = "{index:'idx',path:'v',queryVector:[1.0],numCandidates:4,stream:";
    var open =
        (ApproximateVectorSearchCriteria)
            VectorSearchQuery.fromBson(BsonDocument.parse(fields + "true}")).criteria();
    assertThat(open.isUnboundedStream()).isTrue();
    assertThat(open.resolveNumCandidates()).isEqualTo(4);
    assertThat(open.toBson().containsKey("limit")).isFalse();
    var auto =
        (ApproximateVectorSearchCriteria)
            VectorSearchQuery.fromBson(BsonDocument.parse(fields + "'auto',limit:2}")).criteria();
    assertThat(auto.toBson().getString("stream").getValue()).isEqualTo("auto");
    assertThat(auto.limit()).isEqualTo(2);
    assertThrows(
        BsonParseException.class,
        () -> VectorSearchQuery.fromBson(BsonDocument.parse(fields + "'auto'}")));
    assertThrows(
        BsonParseException.class,
        () -> VectorSearchQuery.fromBson(BsonDocument.parse(fields + "'automatic',limit:2}")));
  }

  @Test
  public void poll_sharedWindowAndTinyBudget_deliversSameReachableSet() throws Exception {
    try (var c =
            RankedVectorCursor.open(
                this.searcher,
                "v",
                TARGET,
                priceRange(),
                options("{initialScoreBudget:1,lookaheadHits:4}"));
        var baseline = RankedVectorCursor.open(this.searcher, "v", TARGET, priceRange(), BUDGET)) {
      assertThat(c.cachedNodes()).isAtMost(1);
      assertThat(drain(c, 3, 1)).containsExactlyElementsIn(drain(baseline, 8, 5));
    }
  }

  @Test
  public void startPrefetch_withoutPolling_exploresWithinBudgetAndResumesAfterOneHit()
      throws Exception {
    var tasks = new ArrayDeque<Runnable>();
    var config = options("{initialScoreBudget:12,lookaheadHits:4,refillEvery:10000}");
    try (var c = RankedVectorCursor.open(this.searcher, "v", TARGET, null, config);
        var baseline = RankedVectorCursor.open(this.searcher, "v", TARGET, null, config)) {
      int initial = c.cachedNodes();
      c.startPrefetch(tasks::add);
      tasks.remove().run();

      assertThat(c.explanation(true, false).getInt32("prefetchRefills").getValue()).isEqualTo(1);
      assertThat(c.cachedNodes()).isGreaterThan(initial);
      assertThat(c.cachedNodes()).isAtMost(initial + 4);
      assertThat(tasks).isEmpty();

      var first = c.poll(1, 1);
      assertThat(first.hits()).hasSize(1);
      tasks.remove().run();
      assertThat(c.cachedNodes()).isAtMost(initial + 5);
      assertThat(tasks).isEmpty();

      var all = new ArrayList<>(first.hits());
      all.addAll(drain(c, 7, 3));
      assertThat(all).containsExactlyElementsIn(drain(baseline, 5, 2));
    }
    // A queued refill may outlive close. Running it must never touch the reader.
    tasks.forEach(Runnable::run);
  }

  @Test
  public void poll_workerBlocked_deliversReadyHitAndCloseWaitsForTraversal() throws Exception {
    int refs = this.reader.getRefCount();
    var worker = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    this.searcher.setTimeout(
        () -> {
          if (Thread.currentThread() == worker.get()) {
            entered.countDown();
            try {
              assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
          }
          return false;
        });
    var config = options("{initialScoreBudget:12,lookaheadHits:4}");
    try (var pool = Executors.newFixedThreadPool(2);
        var c = RankedVectorCursor.open(this.searcher, "v", TARGET, null, config)) {
      c.startPrefetch(
          task ->
              pool.execute(
                  () -> {
                    worker.set(Thread.currentThread());
                    task.run();
                  }));
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(c.poll(1, 1).hits()).hasSize(1);
        var closing =
            pool.submit(
                () -> {
                  c.close();
                  return true;
                });

        assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
        assertThat(this.reader.getRefCount()).isEqualTo(refs + 1);
        release.countDown();
        assertThat(closing.get(5, TimeUnit.SECONDS)).isTrue();
        assertThat(this.reader.getRefCount()).isEqualTo(refs);
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void poll_prefetchRejected_drainsAllHitsOnCaller() throws Exception {
    var config = options("{initialScoreBudget:4,lookaheadHits:4}");
    try (var c = RankedVectorCursor.open(this.searcher, "v", TARGET, priceRange(), config);
        var baseline = RankedVectorCursor.open(this.searcher, "v", TARGET, priceRange(), config)) {
      c.startPrefetch(
          task -> {
            throw new RejectedExecutionException();
          });

      assertThat(drain(c, 3, 8)).containsExactlyElementsIn(drain(baseline, 3, 8)).inOrder();
      assertThat(c.explanation(true, false).getInt32("prefetchRefills").getValue()).isEqualTo(0);
    }
  }

  @Test
  public void poll_failedPrefetch_propagatesFailureAndReleasesReader() throws Exception {
    var fail = new AtomicBoolean();
    var failure = new IllegalStateException("failed vector read");
    this.searcher.setTimeout(
        () -> {
          if (fail.get()) {
            throw failure;
          }
          return false;
        });
    int refs = this.reader.getRefCount();
    var tasks = new ArrayDeque<Runnable>();
    try (var c =
        RankedVectorCursor.open(
            this.searcher, "v", TARGET, null, options("{initialScoreBudget:4,lookaheadHits:4}"))) {
      c.startPrefetch(tasks::add);
      fail.set(true);
      tasks.remove().run();
      fail.set(false);

      assertThat(assertThrows(IllegalStateException.class, () -> c.poll(1, 1)))
          .isSameInstanceAs(failure);
      assertThat(this.reader.getRefCount()).isEqualTo(refs);
    }
  }

  @Test
  public void poll_atCacheCap_deliversBufferedHitsBeforeNeedingAnotherDiscovery() throws Exception {
    try (var c =
        RankedVectorCursor.open(
            this.searcher,
            "v",
            TARGET,
            null,
            options("{initialScoreBudget:10,maxCachedNodes:10,lookaheadHits:2,refillEvery:1}"))) {
      assertThat(c.cachedNodes()).isEqualTo(10);
      var batch = c.poll(10, 24);
      int accepted =
          c.explanation(true, false).getArray("segments").stream()
              .mapToInt(s -> s.asDocument().getInt32("filterAccepted").getValue())
              .sum();
      assertThat(accepted).isGreaterThan(0);
      assertThat(batch.hits()).hasSize(accepted);
      assertThrows(IllegalArgumentException.class, () -> c.poll(1, 24));
    }
  }

  @Test
  public void rewrite_adaptiveCacheLimit_fallsBackAndReportsWhy() throws Exception {
    try (var ignored =
        Explain.setup(Optional.of(Explain.Verbosity.EXECUTION_STATS), Optional.empty())) {
      var actual =
          this.searcher.search(
              new StreamingKnnQuery.Floats(
                  "v", TARGET, 10, priceRange(), options("{maxCachedNodes:1}")),
              10);
      var expected =
          this.searcher.search(new KnnFloatVectorQuery("v", TARGET, 10, priceRange()), 10);
      assertThat(actual.scoreDocs).hasLength(expected.scoreDocs.length);
      for (int i = 0; i < actual.scoreDocs.length; i++) {
        assertThat(actual.scoreDocs[i].doc).isEqualTo(expected.scoreDocs[i].doc);
      }
      var builder = SearchExplainInformationBuilder.newBuilder();
      Explain.getQueryInfo()
          .orElseThrow()
          .getFeatureExplainer(VectorSearchExplainer.class)
          .orElseThrow()
          .emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);
      var stats = builder.build().toBson().getDocument("streamStats");
      assertThat(stats.getString("fallbackReason").getValue()).isEqualTo("cacheLimit");
      assertThat(stats.getInt32("totalDistinctScored").getValue()).isEqualTo(1);
    }
  }

  @Test
  public void rewrite_executionExplain_reportsEffectivePlanAndSegmentWork() throws Exception {
    try (var ignored =
        Explain.setup(Optional.of(Explain.Verbosity.EXECUTION_STATS), Optional.empty())) {
      this.searcher.search(
          new StreamingKnnQuery.Floats(
              "v", TARGET, 10, priceRange(), options("{initialScoreBudget:12,lookaheadHits:4}")),
          2);
      var feature =
          Explain.getQueryInfo()
              .orElseThrow()
              .getFeatureExplainer(VectorSearchExplainer.class)
              .orElseThrow();
      var builder = SearchExplainInformationBuilder.newBuilder();
      feature.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);
      var report = builder.build().toBson().getDocument("streamStats");
      assertThat(report.getInt32("startupScored").getValue()).isAtMost(12);
      assertThat(report.getDouble("startupMillis").getValue()).isAtLeast(0.0);
      assertThat(report.getDouble("candidateCollectionMillis").getValue()).isAtLeast(0.0);
      assertThat(report.getInt32("lookaheadHits").getValue()).isEqualTo(4);
      assertThat(report.getArray("segments")).hasSize(2);
      assertThat(report.getArray("segments").get(0).asDocument().containsKey("format")).isTrue();
    }
  }

  @Test
  public void parse_streamingRoundTrip_preservesOptionsAndRejectsInvalidInput() throws Exception {
    String fields = "{index:'idx',path:'v',limit:2,queryVector:[1.0,0.0]";
    var baseline = VectorSearchQuery.fromBson(BsonDocument.parse(fields + "}"));
    var tuned =
        VectorSearchQuery.fromBson(
            BsonDocument.parse(fields + ",streaming:{refillEvery:3,rescoreAll:true}}"));

    assertThat(baseline.toBson().containsKey("streaming")).isFalse();
    assertThat(((ApproximateVectorSearchCriteria) tuned.criteria()).streaming().orElseThrow())
        .isEqualTo(new StreamingOptions(64, 1024, 3, 8, 250000, true));
    assertThat(VectorSearchQuery.fromBson(tuned.toBson())).isEqualTo(tuned);
    assertThrows(
        BsonParseException.class,
        () ->
            VectorSearchQuery.fromBson(BsonDocument.parse(fields + ",streaming:{rescoreAll:2}}")));
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
