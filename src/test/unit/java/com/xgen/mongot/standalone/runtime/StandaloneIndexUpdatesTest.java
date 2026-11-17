package com.xgen.mongot.standalone.runtime;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.DropIndexRequest;
import com.xgen.mongot.standalone.grpc.GetIndexStatsRequest;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import io.grpc.StatusRuntimeException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.bson.BsonDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Legacy copy-recovery fixtures plus regressions for the public in-place update path. */
public class StandaloneIndexUpdatesTest {
  private static final String ORIGINAL =
      "{mappings:{dynamic:false,fields:{title:{type:'string'}}}}";
  private static final String UPDATED =
      "{mappings:{dynamic:false,fields:{title:{type:'string'},category:{type:'token'}}}}";
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void update_failedTargetInitialization_retriesWithCopiedContentInsteadOfEmptyAttempt()
      throws Exception {
    var first = new java.util.concurrent.atomic.AtomicBoolean(true);
    try (var runtime =
        new StandaloneRuntime(StandaloneConfig.development(this.folder.getRoot().toPath(), 0))) {
      String id = create(runtime, "failed-target");
      write(runtime, id);
      runtime
          .updatesForTesting()
          .atCheckpoint(
              checkpoint -> {
                if (checkpoint == StandaloneIndexUpdates.Checkpoint.INSTALLED
                    && first.getAndSet(false)) {
                  var migration = runtime.catalog().migration(id).orElseThrow();
                  try (var files =
                      Files.list(IndexSeed.path(runtime.config().dataPath(), migration.target()))) {
                    Files.delete(
                        files
                            .filter(path -> path.getFileName().toString().startsWith("segments_"))
                            .findFirst()
                            .orElseThrow());
                  } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                  }
                }
              });
      runtime.updatesForTesting().startLegacyCopyForTesting(update(id, 1));
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("FAILED"));
      assertThat(runtime.describeIndex(id).getActiveRevision()).isEqualTo(1);

      runtime.updateIndex(update(id, 2));
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("READY"));

      assertThat(runtime.describeIndex(id).getActiveRevision()).isEqualTo(2);
      assertThat(
              runtime
                  .statistics()
                  .summary(
                      GetIndexStatsRequest.newBuilder().setIndexId(id).setRefresh(true).build())
                  .get(10, TimeUnit.SECONDS)
                  .getDocumentCount())
          .isEqualTo(1);
    }
  }

  @Test
  public void update_insufficientSpace_preservesSourceAndRetriesAfterRestart() throws Exception {
    var base = StandaloneConfig.development(this.folder.getRoot().toPath(), 0);
    var constrained =
        new StandaloneConfig(
            base.dataPath(),
            base.address(),
            base.documentLimits(),
            base.maxInFlightRequests(),
            base.maxInFlightBytes(),
            Long.MAX_VALUE / 2,
            base.commitInterval(),
            base.refreshInterval(),
            base.maxRpcDuration(),
            base.tls(),
            base.principals());
    String id;
    try (var runtime = new StandaloneRuntime(constrained)) {
      id = create(runtime, "space");
      write(runtime, id);

      runtime.updatesForTesting().startLegacyCopyForTesting(update(id, 1));
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("FAILED"));

      assertThat(runtime.describeIndex(id).getFailureMessage()).contains("disk headroom");
      assertThat(runtime.describeIndex(id).getActiveRevision()).isEqualTo(1);
    }

    try (var runtime = new StandaloneRuntime(base)) {
      // Active metadata can precede initialization; retry only once the source is usable.
      await(
          () ->
              runtime.describeIndex(id).getActiveRevision() == 1
                  && runtime
                      .liveIndexes()
                      .getIndexById(new org.bson.types.ObjectId(id))
                      .filter(
                          index -> runtime.ingestion().isGenerationReady(index.getGenerationId()))
                      .isPresent());
      runtime.updateIndex(update(id, 2));
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("READY"));

      assertThat(runtime.describeIndex(id).getActiveRevision()).isEqualTo(2);
      assertThat(
              runtime
                  .statistics()
                  .summary(
                      GetIndexStatsRequest.newBuilder().setIndexId(id).setRefresh(true).build())
                  .get(10, TimeUnit.SECONDS)
                  .getDocumentCount())
          .isEqualTo(1);
    }
  }

  @Test
  public void update_dropAfterSeedInstallation_doesNotPublishOrBlockUnrelatedIndexes()
      throws Exception {
    var installed = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var runtime =
        new StandaloneRuntime(StandaloneConfig.development(this.folder.getRoot().toPath(), 0))) {
      String id = create(runtime, "drop-race");
      write(runtime, id);
      runtime
          .updatesForTesting()
          .atCheckpoint(
              checkpoint -> {
                if (checkpoint == StandaloneIndexUpdates.Checkpoint.INSTALLED) {
                  installed.countDown();
                  try {
                    if (!release.await(20, TimeUnit.SECONDS)) {
                      throw new AssertionError("Drop did not complete while snapshot was paused");
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                  }
                }
              });
      runtime.updatesForTesting().startLegacyCopyForTesting(update(id, 1));
      runtime.catalogChanged();
      try {
        assertThat(installed.await(10, TimeUnit.SECONDS)).isTrue();
        var migration = runtime.catalog().migration(id).orElseThrow();

        runtime
            .ingestion()
            .coordinate(
                () ->
                    runtime
                        .catalog()
                        .dropIndex(
                            DropIndexRequest.newBuilder()
                                .setIndexId(id)
                                .setExpectedRevision(2)
                                .build()));
        runtime.catalogChanged();
        String other = create(runtime, "unrelated");

        assertThat(runtime.describeIndex(other).getLifecycleState()).isEqualTo("READY");
        assertThrows(StatusRuntimeException.class, () -> runtime.catalog().getIndex(id));
        release.countDown();
        await(() -> runtime.catalog().migration(id).isEmpty());
        assertThat(Files.exists(IndexSeed.path(this.folder.getRoot().toPath(), migration.target())))
            .isFalse();
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void update_emptyPartitions_preservesAnEmptyIndexAndAcceptsNewWrites() throws Exception {
    try (var runtime =
        new StandaloneRuntime(StandaloneConfig.development(this.folder.getRoot().toPath(), 0))) {
      String id =
          runtime
              .catalog()
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("empty")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(bson("{numPartitions:4,mappings:{dynamic:false}}"))
                      .build())
              .getIndexId();
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("READY"));

      runtime.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setIndexId(id)
              .setExpectedRevision(1)
              .setDefinitionBson(
                  bson("{numPartitions:4,mappings:{dynamic:false,fields:{title:{type:'string'}}}}"))
              .build());
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("READY"));
      write(runtime, id);

      assertThat(runtime.describeIndex(id).getActiveRevision()).isEqualTo(2);
      assertThat(
              runtime
                  .statistics()
                  .summary(
                      GetIndexStatsRequest.newBuilder().setIndexId(id).setRefresh(true).build())
                  .get(10, TimeUnit.SECONDS)
                  .getDocumentCount())
          .isEqualTo(1);
    }
  }

  private static String create(StandaloneRuntime runtime, String name) throws Exception {
    String id =
        runtime
            .catalog()
            .createIndex(
                CreateIndexRequest.newBuilder()
                    .setName(name)
                    .setType(IndexType.INDEX_TYPE_SEARCH)
                    .setDefinitionBson(bson(ORIGINAL))
                    .build())
            .getIndexId();
    runtime.catalogChanged();
    await(() -> runtime.describeIndex(id).getLifecycleState().equals("READY"));
    return id;
  }

  private static void write(StandaloneRuntime runtime, String id) {
    runtime
        .ingestion()
        .write(
            id,
            () -> runtime.catalog().getIndex(id).getDesiredRevision(),
            "",
            List.of(bson("{_id:'a',title:'coffee'}")),
            false,
            DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);
  }

  private static UpdateIndexRequest update(String id, long revision) {
    return UpdateIndexRequest.newBuilder()
        .setIndexId(id)
        .setExpectedRevision(revision)
        .setDefinitionBson(bson(UPDATED))
        .build();
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out waiting for index update");
      }
      Thread.sleep(20);
    }
  }

  private static ByteString bson(String json) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json)));
  }
}
