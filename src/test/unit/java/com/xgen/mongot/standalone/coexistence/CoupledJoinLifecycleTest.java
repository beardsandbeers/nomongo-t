package com.xgen.mongot.standalone.coexistence;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.MongoClient;
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
import com.xgen.mongot.index.lucene.query.OrdinalJoinPreparation;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonDocument;
import org.junit.Test;

/** Exercises the real coupled resolver and dependency checks, rather than a planner environment. */
public class CoupledJoinLifecycleTest {
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
            new Fixture(JoinPlanner.MemoryLimits.defaults(), Optional.of(List.of(partition)))) {
          var compound = (CompoundOperator) JoinPlanner.operator(fixture.prepared.plan.query());
          var join = (MaterializedJoinOperator) compound.filter().operators().getFirst();
          assertThat(join.membership()).isInstanceOf(OrdinalJoinMembership.class);
          assertThat(join.membership().size()).isEqualTo(50_000);
          double retained = JoinPlanner.retainedBytes();
          assertThat(retained).isGreaterThan(0.0);
          assertThat(retained).isLessThan(64 * 1024.0);

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
      this(JoinPlanner.MemoryLimits.defaults());
    }

    Fixture(JoinPlanner.MemoryLimits memoryLimits) throws Exception {
      this(memoryLimits, Optional.empty());
    }

    Fixture(
        JoinPlanner.MemoryLimits memoryLimits,
        Optional<List<OrdinalJoinPartition>> ordinalPartitions)
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
      String query =
          ordinalPartitions.isPresent()
              ? """
              {compound:{filter:[{join:{localField:'key',foreignField:'key',
                operator:{exists:{path:'key'}}}}]},count:{type:'total'}}
              """
              : """
              {compound:{filter:[{join:{localField:'parent',foreignField:'key',
                operator:{equals:{path:'office',value:'Berlin'}}}}]}}
              """;
      this.query = SearchQuery.fromBson(BsonDocument.parse(query));
      this.prepared = prepare();
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
