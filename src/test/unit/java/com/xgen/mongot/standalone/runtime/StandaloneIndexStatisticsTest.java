package com.xgen.mongot.standalone.runtime;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.embedding.config.MaterializedViewCollectionMetadata.MaterializedViewSchemaMetadata;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingCompositeIndex;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.InitializedMaterializedViewIndex;
import com.xgen.mongot.index.autoembedding.UnresolvedAutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorAutoEmbedQuantization;
import com.xgen.mongot.index.lucene.vector.Similarity;
import com.xgen.mongot.index.statistics.IndexStatistics;
import com.xgen.mongot.index.statistics.LuceneStatisticsCollector;
import com.xgen.mongot.index.statistics.LuceneStatisticsSnapshot;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.GetIndexFieldStatsRequest;
import com.xgen.mongot.standalone.grpc.GetIndexStatsRequest;
import com.xgen.mongot.standalone.grpc.IndexFieldStatsResponse;
import com.xgen.mongot.standalone.grpc.IndexOwnership;
import com.xgen.mongot.standalone.grpc.IndexStats;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.StatsCollectionState;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import com.xgen.mongot.standalone.metrics.StandaloneMetrics;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionGenerationBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StandaloneIndexStatisticsTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void fields_offlineAutoEmbedding_reportsCostsOnLogicalField() throws Exception {
    assertAutoEmbeddingFieldStatistics(false, 1);
  }

  @Test
  public void fields_resolvedAutoEmbedding_reportsCostsAcrossPartitions() throws Exception {
    assertAutoEmbeddingFieldStatistics(true, 2);
  }

  private void assertAutoEmbeddingFieldStatistics(boolean resolved, int partitions)
      throws Exception {
    Similarity.load();
    try (var fixture = new Fixture()) {
      String id =
          fixture
              .runtime
              .catalog()
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("derived-vector")
                      .setType(IndexType.INDEX_TYPE_VECTOR_SEARCH)
                      .setDefinitionBson(
                          bson(
                              "{numPartitions:"
                                  + partitions
                                  + ",fields:[{type:'vector',path:'_autoEmbed.title',"
                                  + "numDimensions:3,"
                                  + "similarity:'cosine'}]}"))
                      .build())
              .getIndexId();
      fixture.runtime.catalogChanged();
      fixture.ready(id, 1);
      fixture.write(id, "{_id:'a',_autoEmbed:{title:[1.0,0.0,0.0]}}");
      fixture.write(id, "{_id:'b',_autoEmbed:{title:[0.0,1.0,0.0]}}");
      fixture.stopReconciliation();
      var physical = fixture.runtime.liveIndexes().getIndexById(new ObjectId(id)).orElseThrow();
      var logical =
          VectorIndexDefinitionBuilder.builder()
              .indexId(new ObjectId(id))
              .name("derived-vector")
              .numPartitions(partitions)
              .withDefinitionVersion(Optional.of(1L))
              .withAutoEmbedField(
                  "title",
                  "voyage-3-large",
                  3,
                  VectorAutoEmbedQuantization.FLOAT,
                  VectorSimilarity.COSINE)
              .build();
      var rawGeneration =
          VectorIndexDefinitionGenerationBuilder.builder()
              .definition(logical)
              .generation(physical.getGenerationId().generation)
              .build();
      var manager = fixture.configManager();
      var initialized = fixture.initializedIndexes();
      var derived = initialized.getIndex(physical.getGenerationId()).orElseThrow();
      var mapping = Map.of(FieldPath.parse("title"), FieldPath.parse("_autoEmbed.title"));
      IndexGeneration logicalGeneration;
      if (resolved) {
        // The external materialized-view metadata is the only mocked resource; the reader is real.
        var view = mock(InitializedMaterializedViewIndex.class);
        when(view.isCompatibleWith(logical)).thenReturn(true);
        when(view.getDefinition()).thenReturn(logical);
        when(view.getStatus()).thenReturn(IndexStatus.steady());
        when(view.isCurrentVersionQueryablePerLease()).thenReturn(true);
        when(view.getSchemaMetadata()).thenReturn(new MaterializedViewSchemaMetadata(1L, mapping));
        logicalGeneration =
            new AutoEmbeddingIndexGeneration(
                new AutoEmbeddingCompositeIndex(
                    view, physical.getIndex(), () -> Optional.of(derived)),
                rawGeneration,
                physical.getDefinitionGeneration());
      } else {
        logicalGeneration =
            UnresolvedAutoEmbeddingIndexGeneration.createStale(rawGeneration, derived, mapping);
      }
      var remote =
          new DirectReadCatalog.RemoteDefinitions() {
            @Override
            public List<IndexDefinition> snapshot() {
              return List.of(logical);
            }

            @Override
            public Optional<IndexDefinition> get(ObjectId key) {
              return key.toHexString().equals(id) ? Optional.of(logical) : Optional.empty();
            }
          };
      try (var local = new LocalIndexCatalog(this.folder.newFolder().toPath())) {
        synchronized (manager) {
          fixture.runtime.liveIndexes().addIndex(logicalGeneration);
        }
        var reads =
            new DirectReadCatalog(
                local, manager, fixture.runtime.liveIndexes(), initialized, remote, new Object());
        assertThat(reads.describe(id).getOwnership())
            .isEqualTo(IndexOwnership.INDEX_OWNERSHIP_REPLICATED);
        try (var stats =
            new StandaloneIndexStatistics(
                reads, Duration.ofSeconds(5), StandaloneMetrics.disabled())) {
          var full =
              readyFields(stats, GetIndexFieldStatsRequest.newBuilder().setIndexId(id).build());
          var selected =
              readyFields(
                  stats,
                  GetIndexFieldStatsRequest.newBuilder().setIndexId(id).addPaths("title").build());
          var physicalSelection =
              readyFields(
                  stats,
                  GetIndexFieldStatsRequest.newBuilder()
                      .setIndexId(id)
                      .addPaths("_autoEmbed.title")
                      .build());
          assertThat(full.getFieldsList()).hasSize(1);
          assertThat(selected.getFieldsList()).containsExactlyElementsIn(full.getFieldsList());
          assertThat(physicalSelection.getFieldsList()).isEmpty();
          var field = selected.getFields(0);
          assertThat(field.getPath()).isEqualTo("title");
          assertThat(field.getConfigured()).isTrue();
          assertThat(field.getObserved()).isTrue();
          assertThat(field.getAttributedDiskBytes().getValue()).isGreaterThan(0);
          assertThat(field.getDesiredVectorOffHeapBytes().getValue()).isGreaterThan(0);
          assertThat(
                  field.getRepresentationsList().stream()
                      .map(value -> value.getPhysicalName())
                      .toList())
              .containsExactly("$type:knnVector/_autoEmbed.title");
          assertThat(field.getAttributedDiskBytes().getValue())
              .isEqualTo(full.getTotalAttributedDiskBytes());
          assertThat(
                  full.getTotalAttributedDiskBytes()
                      + full.getSharedDiskBytes()
                      + full.getUnattributedDiskBytes())
              .isEqualTo(full.getIndexStats().getIndexSizeBytes());
          assertThat(full.getIndexStats().getDocumentCount()).isEqualTo(2);
          assertThat(full.getIndexStats().getGenerationId())
              .isEqualTo(physical.getGenerationId().uniqueString());
          assertThat(
                  full.getStorageBucketsList().stream()
                      .flatMap(bucket -> bucket.getOwnersList().stream())
                      .toList())
              .doesNotContain("_autoEmbed.title");
          assertThat(
                  full.getMemoryBucketsList().stream()
                      .flatMap(bucket -> bucket.getOwnersList().stream())
                      .toList())
              .doesNotContain("_autoEmbed.title");
        } finally {
          synchronized (manager) {
            fixture.runtime.liveIndexes().addIndex(physical);
          }
        }
      }
    }
  }

  private static IndexFieldStatsResponse readyFields(
      StandaloneIndexStatistics stats, GetIndexFieldStatsRequest request) throws Exception {
    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < until) {
      var response = stats.fields(request, "reader");
      if (response.getState() == StatsCollectionState.STATS_COLLECTION_STATE_READY) {
        return response;
      }
      Thread.sleep(10);
    }
    throw new AssertionError("Field statistics did not finish");
  }

  @Test
  public void summary_cacheAndRefresh_observesWritesAfterCacheExpiry() throws Exception {
    try (var fixture = new Fixture()) {
      String id = fixture.create("summary");
      fixture.write(id, "{_id:'a',title:'first',amount:7}");
      var first =
          fixture
              .statistics
              .summary(GetIndexStatsRequest.newBuilder().setIndexId(id).build())
              .get(5, TimeUnit.SECONDS);
      fixture.write(id, "{_id:'b',title:'second',amount:8}");

      var cached =
          fixture
              .statistics
              .summary(GetIndexStatsRequest.newBuilder().setIndexId(id).build())
              .get(5, TimeUnit.SECONDS);
      fixture.time.addAndGet(Duration.ofSeconds(6).toNanos());
      var fresh =
          fixture
              .statistics
              .summary(GetIndexStatsRequest.newBuilder().setIndexId(id).build())
              .get(5, TimeUnit.SECONDS);

      assertThat(cached.getSnapshotId()).isEqualTo(first.getSnapshotId());
      assertThat(cached.getDocumentCount()).isEqualTo(1);
      assertThat(fresh.getSnapshotId()).isNotEqualTo(first.getSnapshotId());
      assertThat(fresh.getDocumentCount()).isEqualTo(2);
    }
  }

  @Test
  public void fields_tokens_bindPrincipalAndExpireWithoutRetainingReaders() throws Exception {
    try (var fixture = new Fixture()) {
      String id = fixture.create("tokens");
      fixture.write(id, "{_id:'a',title:'one',amount:7}");
      var first = fixture.fields(id, 1);
      var next =
          GetIndexFieldStatsRequest.newBuilder()
              .setIndexId(id)
              .setPageSize(1)
              .setPageToken(first.getNextPageToken())
              .build();

      assertThat(
              assertThrows(
                      StatusRuntimeException.class, () -> fixture.statistics.fields(next, "bob"))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.INVALID_ARGUMENT);
      var generation = fixture.runtime.liveIndexes().getIndexById(new ObjectId(id)).orElseThrow();
      // A background commit may still own a lease; the unconsumed page token must not block
      // retirement after that work drains.
      fixture.runtime.ingestion().dropIndex(generation.getGenerationId()).get(30, TimeUnit.SECONDS);
      // Completed pages retain only values: retirement did not wait for these idle tokens.
    }
    try (var fixture = new Fixture()) {
      String id = fixture.create("expiry");
      fixture.write(id, "{_id:'a',title:'one',amount:7}");
      var first = fixture.fields(id, 1);
      fixture.time.addAndGet(Duration.ofMinutes(3).toNanos());

      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          fixture.statistics.fields(
                              GetIndexFieldStatsRequest.newBuilder()
                                  .setIndexId(id)
                                  .setPageSize(1)
                                  .setPageToken(first.getNextPageToken())
                                  .build(),
                              "alice"))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
    }
  }

  @Test
  public void fields_generationReplacement_rejectsOldSnapshotTokens() throws Exception {
    try (var fixture = new Fixture()) {
      String id = fixture.create("replace");
      fixture.write(id, "{_id:'a',title:'one',amount:7}");
      var first = fixture.fields(id, 1);
      fixture
          .runtime
          .catalog()
          .updateIndex(
              UpdateIndexRequest.newBuilder()
                  .setUpdateMode(
                      com.xgen.mongot.standalone.grpc.IndexUpdateMode
                          .INDEX_UPDATE_MODE_REBUILD_EMPTY)
                  .setIndexId(id)
                  .setExpectedRevision(1)
                  .setDefinitionBson(bson("{mappings:{dynamic:true}}"))
                  .build());
      fixture.runtime.catalogChanged();
      fixture.ready(id, 2);

      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          fixture.statistics.fields(
                              GetIndexFieldStatsRequest.newBuilder()
                                  .setIndexId(id)
                                  .setPageSize(1)
                                  .setPageToken(first.getNextPageToken())
                                  .build(),
                              "alice"))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
    }
  }

  @Test
  public void statisticsLease_retirement_defersCleanupUntilReleaseOutsideIngestionMonitor()
      throws Exception {
    try (var fixture = new Fixture()) {
      String id = fixture.create("lease");
      var generation = fixture.runtime.liveIndexes().getIndexById(new ObjectId(id)).orElseThrow();
      var lease = fixture.runtime.ingestion().acquireStatisticsLease(generation.getGenerationId());
      var heldMonitor = new AtomicBoolean(true);

      var retirement = fixture.runtime.ingestion().dropIndex(generation.getGenerationId());
      var continuation =
          retirement.thenRun(() -> heldMonitor.set(Thread.holdsLock(fixture.runtime.ingestion())));

      assertThat(retirement.isDone()).isFalse();
      assertThat(lease.retired()).isTrue();
      assertThat(lease.index().isClosed()).isFalse();
      lease.close();
      continuation.get(5, TimeUnit.SECONDS);
      lease.close();
      assertThat(heldMonitor.get()).isFalse();
      assertThat(retirement.isDone()).isTrue();
    }
  }

  @Test
  public void summary_queueAndCancellation_boundsWorkAndReleasesActiveGenerationLeases()
      throws Exception {
    var blocker = new BlockingCollector();
    try (var fixture = new Fixture(blocker::collect)) {
      var ids = fixture.createMany(20);
      var first =
          fixture.statistics.summary(
              GetIndexStatsRequest.newBuilder().setIndexId(ids.get(0)).build());
      var second =
          fixture.statistics.summary(
              GetIndexStatsRequest.newBuilder().setIndexId(ids.get(1)).build());
      assertThat(blocker.started.await(10, TimeUnit.SECONDS)).isTrue();
      var duplicate =
          fixture.statistics.summary(
              GetIndexStatsRequest.newBuilder().setIndexId(ids.get(0)).build());
      var queued = fixture.enqueue(ids.subList(2, 18));

      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          fixture.statistics.summary(
                              GetIndexStatsRequest.newBuilder().setIndexId(ids.get(18)).build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      assertThat(blocker.calls.get()).isEqualTo(2);
      first.cancel(false);
      assertThat(duplicate.isDone()).isFalse();
      fixture.runtime.reads().invalidate(ids.get(0));
      assertThrows(
          CompletionException.class, () -> duplicate.orTimeout(5, TimeUnit.SECONDS).join());
      cancel(queued);
      blocker.release.countDown();
      assertThat(second.get(5, TimeUnit.SECONDS).getDocumentCount()).isEqualTo(0);
    }
  }

  private static void cancel(List<? extends CompletableFuture<?>> futures) {
    futures.forEach(future -> future.cancel(false));
  }

  private static ByteString bson(String json) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json)));
  }

  private static final class BlockingCollector {
    private final CountDownLatch started = new CountDownLatch(2);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger calls = new AtomicInteger();

    private IndexStatistics collect(
        LuceneStatisticsSnapshot snapshot,
        IndexDefinition definition,
        boolean detailed,
        OptionalLong writerHeap,
        BooleanSupplier cancelled,
        Map<FieldPath, FieldPath> logicalToPhysical)
        throws IOException {
      this.calls.incrementAndGet();
      this.started.countDown();
      try {
        while (!this.release.await(10, TimeUnit.MILLISECONDS)) {
          if (cancelled.getAsBoolean()) {
            throw new CancellationException();
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CancellationException();
      }
      return LuceneStatisticsCollector.collect(
          snapshot, definition, detailed, writerHeap, cancelled, logicalToPhysical);
    }
  }

  private final class Fixture implements AutoCloseable {
    private final StandaloneRuntime runtime;
    private final StandaloneIndexStatistics statistics;
    private final AtomicLong time = new AtomicLong(1);

    private Fixture() throws Exception {
      this(LuceneStatisticsCollector::collect);
    }

    private Fixture(StandaloneIndexStatistics.Collector collector) throws Exception {
      this.runtime =
          new StandaloneRuntime(
              StandaloneConfig.development(
                  StandaloneIndexStatisticsTest.this.folder.newFolder().toPath(), 0));
      this.statistics =
          new StandaloneIndexStatistics(
              this.runtime.reads(),
              Duration.ofMinutes(1),
              StandaloneMetrics.disabled(),
              this.time::get,
              collector);
    }

    private void stopReconciliation() throws Exception {
      var field = StandaloneRuntime.class.getDeclaredField("reconciler");
      field.setAccessible(true);
      var reconciler = (java.util.concurrent.ScheduledThreadPoolExecutor) field.get(this.runtime);
      reconciler.shutdown();
      assertThat(reconciler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    private DefaultConfigManager configManager() throws Exception {
      var field = StandaloneRuntime.class.getDeclaredField("configManager");
      field.setAccessible(true);
      return (DefaultConfigManager) field.get(this.runtime);
    }

    private InitializedIndexCatalog initializedIndexes() throws Exception {
      var field = StandaloneRuntime.class.getDeclaredField("initializedIndexes");
      field.setAccessible(true);
      return (InitializedIndexCatalog) field.get(this.runtime);
    }

    private String create(String name) throws Exception {
      String id =
          this.runtime
              .catalog()
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName(name)
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(bson("{mappings:{dynamic:true}}"))
                      .build())
              .getIndexId();
      this.runtime.catalogChanged();
      ready(id, 1);
      return id;
    }

    private List<String> createMany(int count) throws Exception {
      var result = new ArrayList<String>();
      for (int i = 0; i < count; i++) {
        result.add(create("index-" + i));
      }
      return result;
    }

    private List<CompletableFuture<IndexStats>> enqueue(List<String> ids) {
      return ids.stream()
          .map(
              id ->
                  this.statistics.summary(GetIndexStatsRequest.newBuilder().setIndexId(id).build()))
          .toList();
    }

    private void ready(String id, long revision) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (System.nanoTime() < deadline) {
        var index = this.runtime.describeIndex(id);
        if (index.getLifecycleState().equals("READY") && index.getActiveRevision() == revision) {
          return;
        }
        Thread.sleep(10);
      }
      throw new AssertionError("Index is not ready");
    }

    private void write(String id, String json) {
      this.runtime
          .ingestion()
          .write(
              id,
              () -> 1L,
              "",
              List.of(bson(json)),
              false,
              DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE);
    }

    private IndexFieldStatsResponse fields(String id, int pageSize) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (System.nanoTime() < deadline) {
        var value =
            this.statistics.fields(
                GetIndexFieldStatsRequest.newBuilder().setIndexId(id).setPageSize(pageSize).build(),
                "alice");
        if (value.getState() == StatsCollectionState.STATS_COLLECTION_STATE_READY) {
          return value;
        }
        Thread.sleep(10);
      }
      throw new AssertionError("Field statistics did not finish");
    }

    @Override
    public void close() throws Exception {
      try {
        this.statistics.close();
      } finally {
        this.runtime.close();
      }
    }
  }
}
