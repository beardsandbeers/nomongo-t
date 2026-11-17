package com.xgen.mongot.standalone.coexistence;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.MongoClient;
import com.sun.management.ThreadMXBean;
import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.catalogservice.MongodTopologyMonitor;
import com.xgen.mongot.config.manager.ConfigState.SyncSourceSnapshot;
import com.xgen.mongot.config.manager.QuarantinedIndexLookup;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.ConstantBatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.extensions.join.JoinExecutionContext;
import com.xgen.mongot.extensions.join.JoinPlanner;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.SearchIndexReader.QuerySnapshot.OrdinalJoinPartition;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.codec.LuceneCodec;
import com.xgen.mongot.index.lucene.query.OrdinalJoinPreparation;
import com.xgen.mongot.index.lucene.query.OrdinalJoinQuery;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.JoinField;
import com.xgen.mongot.index.query.operators.MaterializedJoinOperator;
import com.xgen.mongot.index.query.operators.OrdinalJoinMembership;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.mongodb.ConnectionStringUtil;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/** Exercises the real coupled resolver and dependency checks, rather than a planner environment. */
@RunWith(Theories.class)
public class CoupledJoinLifecycleTest {
  private static final String ORDINAL_OPERATOR =
      "{compound:{filter:[{join:{localField:'key',foreignField:'key',"
          + "operator:{exists:{path:'key'}}}}]}}";
  private static final String ORDINAL_QUERY =
      """
      {compound:{filter:[{join:{localField:'key',foreignField:'key',
        operator:{exists:{path:'key'}}}}]},count:{type:'total'}}
      """;
  private static final String ORDINAL_FACETS =
      "{facet:{operator:"
          + ORDINAL_OPERATOR
          + ",facets:{first:{type:'string',path:'key'},second:{type:'string',path:'key'}}}}";

  public record OrdinalCase(int segments, int copies, String query) {}

  @DataPoints
  public static final OrdinalCase[] ORDINAL_CASES = {
    new OrdinalCase(1, 1, ORDINAL_QUERY),
    new OrdinalCase(3, 1, ORDINAL_QUERY),
    new OrdinalCase(1, 4, ORDINAL_FACETS),
    new OrdinalCase(3, 4, ORDINAL_FACETS)
  };

  @DataPoints
  public static final LuceneCodec.WriteFormat[] CODECS = {
    LuceneCodec.WriteFormat.LUCENE99, LuceneCodec.WriteFormat.LUCENE104
  };

  @Theory
  public void prepare_longTokenScorers_reservesActualAllocations(
      OrdinalCase scenario, LuceneCodec.WriteFormat codec) throws Exception {
    try (var data = new OrdinalData(scenario.segments(), codec);
        var fixture =
            new Fixture(
                new JoinPlanner.MemoryLimits(128L << 20, 256L << 20),
                Optional.of(data.partitions()),
                SearchQuery.fromBson(BsonDocument.parse(scenario.query())))) {
      var compound = (CompoundOperator) JoinPlanner.operator(fixture.prepared.plan.query());
      var join = (MaterializedJoinOperator) compound.filter().operators().getFirst();
      var query =
          new OrdinalJoinQuery(
              OrdinalData.FIELD, (OrdinalJoinMembership) join.membership(), Optional.empty());
      var searcher = new IndexSearcher(data.reader);
      // Warm class loading before measuring the new buffers held by each fresh set of scorers.
      allocateScorers(query, searcher, scenario.copies());

      var allocation = allocateScorers(query, searcher, scenario.copies());

      assertThat(allocation.scorers()).hasSize(scenario.segments() * scenario.copies());
      assertThat(allocation.bytes()).isGreaterThan(200_000L * allocation.scorers().size());
      assertThat(JoinPlanner.retainedBytes()).isAtLeast((double) allocation.bytes());
      assertThat(join.membership().size()).isEqualTo(50_064);
    }
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void prepare_ordinalConcurrentLimit_rejectsUntilExistingPlanCloses() throws Exception {
    try (var data = new OrdinalData(1, LuceneCodec.WriteFormat.LUCENE99);
        var fixture =
            new Fixture(
                new JoinPlanner.MemoryLimits(32L << 20, 8L << 20),
                Optional.of(data.partitions()))) {
      double retained = JoinPlanner.retainedBytes();

      var error = assertThrows(CoupledJoinService.Failure.class, fixture::prepare);

      assertThat(error).hasMessageThat().contains("Concurrent join-memory limit exceeded");
      assertThat(JoinPlanner.retainedBytes()).isEqualTo(retained);

      fixture.prepared.close();
      try (var next = fixture.prepare()) {
        assertThat(JoinPlanner.retainedBytes()).isEqualTo(retained);
      }
    }
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void prepare_ordinalFailureAndConcurrentPlans_releaseOnlyTheirOwnMemory()
      throws Exception {
    var cancelCollection = new AtomicBoolean();
    try (var directory = new ByteBuffersDirectory()) {
      try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
        String field = OrdinalJoinPreparation.field(JoinField.Type.TOKEN, FieldPath.parse("key"));
        var document = new Document();
        for (int key = 0; key < 50_000; key++) {
          document.add(new SortedSetDocValuesField(field, new BytesRef("key-" + key)));
        }
        writer.addDocument(document);
      }
      try (var reader = DirectoryReader.open(directory)) {
        var searcher = new IndexSearcher(reader);
        var partition =
            new OrdinalJoinPartition(
                reader,
                (predicate, collector, check) -> {
                  if (cancelCollection.get()) {
                    throw new CancellationException("Cancelled after ordinal admission");
                  }
                  searcher.search(new MatchAllDocsQuery(), collector);
                });
        try (var fixture =
            new Fixture(
                JoinPlanner.MemoryLimits.fromHeapBytes(2L << 30),
                Optional.of(List.of(partition)))) {
          var compound = (CompoundOperator) JoinPlanner.operator(fixture.prepared.plan.query());
          var join = (MaterializedJoinOperator) compound.filter().operators().getFirst();
          assertThat(join.membership()).isInstanceOf(OrdinalJoinMembership.class);
          assertThat(join.membership().size()).isEqualTo(50_000);
          double retained = JoinPlanner.retainedBytes();
          assertThat(retained).isGreaterThan(2 * 1024 * 1024.0);
          assertThat(retained).isLessThan(3 * 1024 * 1024.0);

          cancelCollection.set(true);
          assertThrows(CancellationException.class, fixture::prepare);
          assertThat(JoinPlanner.retainedBytes()).isEqualTo(retained);

          cancelCollection.set(false);
          try (var second = fixture.prepare()) {
            assertThat(JoinPlanner.retainedBytes()).isEqualTo(2 * retained);
          }
          assertThat(JoinPlanner.retainedBytes()).isEqualTo(retained);
        }
      }
    }
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void prepare_configuredConcurrentBudget_preservesExistingReservation() throws Exception {
    try (var fixture = new Fixture(new JoinPlanner.MemoryLimits(200000L, 200000L))) {
      double retained = JoinPlanner.retainedBytes();

      var error = assertThrows(CoupledJoinService.Failure.class, fixture::prepare);

      assertThat(error)
          .hasMessageThat()
          .contains("Concurrent join-memory limit exceeded (limit: 200000 bytes, reserved: ");
      assertThat(JoinPlanner.retainedBytes()).isEqualTo(retained);
    }
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void continuation_replicationHostChanges_preservesPinnedProducer() throws Exception {
    try (var fixture = new Fixture()) {
      var result = fixture.execute();
      var unavailable =
          fixture.config.copyWithUpdatedReplicationUris(Optional.empty(), Optional.empty());
      fixture.source.set(new SyncSourceSnapshot(0, Optional.of(unavailable)));

      result.searchBatchProducer.execute(Bytes.ofMebi(1), BatchCursorOptions.empty());

      verify(fixture.producer).execute(Bytes.ofMebi(1), BatchCursorOptions.empty());
      assertThat(JoinPlanner.retainedBytes()).isGreaterThan(0.0);
    }
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void continuation_sourceEpochChanges_rejectsOldPlan() throws Exception {
    try (var fixture = new Fixture()) {
      var result = fixture.execute();
      fixture.source.set(new SyncSourceSnapshot(1, Optional.of(fixture.config)));

      assertThrows(
          CoupledJoinService.Failure.class,
          () -> result.searchBatchProducer.execute(Bytes.ofMebi(1), BatchCursorOptions.empty()));
    }
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void continuation_generationRetires_rejectsOldPlan() throws Exception {
    try (var fixture = new Fixture()) {
      var result = fixture.execute();
      fixture.catalog.removeIndex(fixture.prepared.generation.indexId);

      assertThrows(
          CoupledJoinService.Failure.class,
          () -> result.searchBatchProducer.execute(Bytes.ofMebi(1), BatchCursorOptions.empty()));
    }
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void continuation_rebindsCancellationToCurrentRequest() throws Exception {
    try (var fixture = new Fixture()) {
      var initialCancellation = new AtomicReference<Runnable>(() -> {});
      JoinExecutionContext.run(
          () -> initialCancellation.get().run(),
          () -> {
            try {
              fixture.execute();
            } catch (Exception e) {
              throw new AssertionError(e);
            }
            return null;
          });
      initialCancellation.set(
          () -> {
            throw new CancellationException();
          });
      // Continuation must not use the now-cancelled initial invocation.
      fixture.batch.execute(Bytes.ofMebi(1), BatchCursorOptions.empty());
      verify(fixture.producer).execute(Bytes.ofMebi(1), BatchCursorOptions.empty());

      JoinExecutionContext.run(
          () -> {
            throw new CancellationException();
          },
          () -> {
            assertThrows(
                CancellationException.class,
                () -> fixture.batch.execute(Bytes.ofMebi(1), BatchCursorOptions.empty()));
            return null;
          });
    }
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  private record ScorerAllocation(List<Scorer> scorers, long bytes) {}

  private static ScorerAllocation allocateScorers(
      OrdinalJoinQuery query, IndexSearcher searcher, int copies) throws IOException {
    var allocations = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    allocations.setThreadAllocatedMemoryEnabled(true);
    long thread = Thread.currentThread().threadId();
    long before = allocations.getThreadAllocatedBytes(thread);
    var scorers = new ArrayList<Scorer>();
    for (int i = 0; i < copies; i++) {
      var weight = query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1);
      for (var leaf : searcher.getIndexReader().leaves()) {
        scorers.add(weight.scorerSupplier(leaf).get(Long.MAX_VALUE));
      }
    }
    return new ScorerAllocation(scorers, allocations.getThreadAllocatedBytes(thread) - before);
  }

  private static final class OrdinalData implements AutoCloseable {
    private static final String FIELD =
        OrdinalJoinPreparation.field(JoinField.Type.TOKEN, FieldPath.parse("key"));
    private final ByteBuffersDirectory directory = new ByteBuffersDirectory();
    private final DirectoryReader reader;

    private OrdinalData(int segments, LuceneCodec.WriteFormat codec) throws IOException {
      try (var writer =
          new IndexWriter(
              this.directory,
              new IndexWriterConfig()
                  .setCodec(new LuceneCodec(codec))
                  .setMergePolicy(NoMergePolicy.INSTANCE))) {
        var document = new Document();
        for (int key = 0; key < 50_000; key++) {
          document.add(new SortedSetDocValuesField(FIELD, new BytesRef("key-" + key)));
        }
        var random = new Random(13);
        for (int key = 0; key < 64; key++) {
          byte[] value = new byte[7000];
          for (int i = 0; i < value.length; i++) {
            value[i] = (byte) ('A' + random.nextInt(26));
          }
          document.add(new SortedSetDocValuesField(FIELD, new BytesRef(value)));
        }
        for (int segment = 0; segment < segments; segment++) {
          writer.addDocument(document);
          writer.commit();
        }
      }
      this.reader = DirectoryReader.open(this.directory);
    }

    private List<OrdinalJoinPartition> partitions() {
      var searcher = new IndexSearcher(this.reader);
      return List.of(
          new OrdinalJoinPartition(
              this.reader,
              (predicate, collector, check) ->
                  searcher.search(new MatchAllDocsQuery(), collector)));
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(this.reader, this.directory);
    }
  }

  private static final class Fixture implements AutoCloseable {
    final DefaultIndexCatalog catalog = new DefaultIndexCatalog();
    final SyncSourceConfig config;
    final AtomicReference<SyncSourceSnapshot> source;
    final BatchProducer producer = mock(BatchProducer.class);
    final CoupledJoinService service;
    final CoupledJoinService.Prepared prepared;
    final SearchIndexDefinition definition;
    final SearchQuery query;
    BatchProducer batch;

    Fixture() throws Exception {
      this(JoinPlanner.MemoryLimits.fromHeapBytes(2L << 30));
    }

    Fixture(JoinPlanner.MemoryLimits memoryLimits) throws Exception {
      this(memoryLimits, Optional.empty());
    }

    Fixture(
        JoinPlanner.MemoryLimits memoryLimits,
        Optional<List<OrdinalJoinPartition>> ordinalPartitions)
        throws Exception {
      this(memoryLimits, ordinalPartitions, query(ordinalPartitions.isPresent()));
    }

    Fixture(
        JoinPlanner.MemoryLimits memoryLimits,
        Optional<List<OrdinalJoinPartition>> ordinalPartitions,
        SearchQuery query)
        throws Exception {
      var connection = ConnectionStringUtil.toConnectionInfo("mongodb://localhost");
      this.config =
          SyncSourceConfig.builder()
              .mongodClusterReadWriteUri(connection)
              .mongodClusterReplicationUri(connection)
              .mongodSingleHostReplicationUri(connection)
              .build();
      this.source = new AtomicReference<>(new SyncSourceSnapshot(0, Optional.of(this.config)));
      this.definition =
          SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();
      var id = new GenerationId(this.definition.getIndexId(), Generation.CURRENT);
      var index = mock(InitializedSearchIndex.class);
      when(index.getDefinition()).thenReturn(this.definition);
      when(index.getGenerationId()).thenReturn(id);
      when(index.getStatus()).thenReturn(IndexStatus.steady());
      when(index.asSearchIndex()).thenReturn(index);
      var reader = mock(SearchIndexReader.class);
      var snapshot = mock(SearchIndexReader.QuerySnapshot.class);
      when(index.getReader()).thenReturn(reader);
      when(reader.acquireQuerySnapshot(anyBoolean())).thenReturn(snapshot);
      when(snapshot.partitions()).thenReturn(1);
      when(snapshot.ordinalJoinPartitions()).thenReturn(ordinalPartitions);
      when(snapshot.joinFields(any()))
          .thenReturn(List.of(new JoinField(JoinField.Type.TOKEN, StockNormalizerName.NONE)));
      if (ordinalPartitions.isPresent()) {
        // These fixtures select every document. Exercise real hash admission if ordinals fall back.
        doAnswer(
                invocation -> {
                  Consumer<BytesRef> keys = invocation.getArgument(3);
                  Runnable check = invocation.getArgument(4);
                  for (var partition : ordinalPartitions.get()) {
                    for (var leaf : partition.reader().leaves()) {
                      var values = DocValues.getSortedSet(leaf.reader(), OrdinalData.FIELD);
                      for (long ordinal = 0; ordinal < values.getValueCount(); ordinal++) {
                        check.run();
                        keys.accept(values.lookupOrd(ordinal));
                      }
                    }
                  }
                  return null;
                })
            .when(snapshot)
            .collectKeys(any(), any(), any(), any(), any());
      }
      when(snapshot.query(any(), any(), any(), any()))
          .thenReturn(
              new SearchIndexReader.SearchProducerAndMetaResults(this.producer, MetaResults.EMPTY));
      var generation = mock(IndexGeneration.class);
      when(generation.getDefinition()).thenReturn(this.definition);
      when(generation.getGenerationId()).thenReturn(id);
      when(generation.getIndex()).thenReturn(index);
      this.catalog.addIndex(generation);
      var initialized = new InitializedIndexCatalog();
      initialized.addIndex(index);
      this.service =
          new CoupledJoinService(
              this.catalog,
              initialized,
              mock(MongodTopologyMonitor.class),
              mock(QuarantinedIndexLookup.class),
              this.config,
              this.source::get,
              new SimpleMeterRegistry(),
              memoryLimits,
              mock(MongoClient.class));
      this.query = query;
      this.prepared = prepare();
    }

    private static SearchQuery query(boolean ordinal) throws Exception {
      return SearchQuery.fromBson(
          BsonDocument.parse(
              ordinal
                  ? ORDINAL_QUERY
                  : """
                  {compound:{filter:[{join:{localField:'parent',foreignField:'key',
                    operator:{equals:{path:'office',value:'Berlin'}}}}]}}
                  """));
    }

    CoupledJoinService.Prepared prepare() throws Exception {
      return this.service
          .prepare(
              this.definition.getDatabase(),
              this.definition.getCollectionUuid(),
              Optional.empty(),
              this.query)
          .orElseThrow();
    }

    SearchIndexReader.SearchProducerAndMetaResults execute() throws Exception {
      var result =
          this.prepared.query(
              this.prepared.plan.query(),
              QueryCursorOptions.empty(),
              new ConstantBatchSizeStrategy(),
              QueryOptimizationFlags.DEFAULT_OPTIONS);
      this.batch = result.searchBatchProducer;
      return result;
    }

    public void close() throws Exception {
      try {
        this.prepared.close();
      } finally {
        this.service.close();
      }
    }
  }
}
