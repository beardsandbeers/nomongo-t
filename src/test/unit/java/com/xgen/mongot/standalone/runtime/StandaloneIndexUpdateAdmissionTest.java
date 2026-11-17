package com.xgen.mongot.standalone.runtime;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.standalone.catalog.IndexMigration;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.DropIndexRequest;
import com.xgen.mongot.standalone.grpc.GetIndexStatsRequest;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.IndexUpdateMode;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Legacy copy-recovery fixtures plus regressions for the public in-place update path. */
public class StandaloneIndexUpdateAdmissionTest {
  private static final String ORIGINAL = "{mappings:{fields:{title:{type:'string'}}}}";
  private static final String UPDATED =
      "{mappings:{fields:{title:{type:'string'},category:{type:'token'}}}}";
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void recover_legacyMultipartData_requiresCompleteCommitsWithoutInventingAuthorization()
      throws Exception {
    String id;
    com.xgen.mongot.index.definition.IndexDefinitionGeneration generation;
    try (var runtime = new StandaloneRuntime(config())) {
      id =
          runtime
              .catalog()
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("legacy")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(
                          bson("{numPartitions:2,mappings:{fields:{title:{type:'string'}}}}"))
                      .build())
              .getIndexId();
      runtime.catalogChanged();
      ready(runtime, id);
      write(runtime, id);
      generation =
          runtime
              .liveIndexes()
              .getIndexById(new ObjectId(id))
              .orElseThrow()
              .getDefinitionGeneration();
    }
    Path file = config().dataPath().resolve("indexes.json");
    var catalog = BsonDocument.parse(Files.readString(file));
    catalog.put("catalogVersion", new org.bson.BsonInt32(2));
    catalog.remove("initializations");
    Files.writeString(file, catalog.toJson());

    try (var runtime = new StandaloneRuntime(config())) {
      ready(runtime, id);
      assertThat(count(runtime, id)).isEqualTo(1);
      assertThat(runtime.catalog().initializations()).isEmpty();
      assertThat(runtime.catalog().allowsEmptyInitialization(generation.getGenerationId()))
          .isFalse();
    }
    removeCommit(IndexSeed.path(config().dataPath(), generation).resolve("0"));
    try (var runtime = new StandaloneRuntime(config())) {
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("FAILED"));
      String healthy = create(runtime, "healthy");

      assertThat(runtime.managementReady()).isTrue();
      assertThat(runtime.describeIndex(healthy).getLifecycleState()).isEqualTo("READY");
      assertThat(runtime.describeIndex(id).getGenerationId())
          .isEqualTo(generation.getGenerationId().uniqueString());
      assertThrows(
          java.io.IOException.class,
          () -> IndexSeed.verifyExisting(config().dataPath(), generation));
    }
  }

  @Test
  public void update_missingSourceCommit_rejectsWithoutChangingDesiredState() throws Exception {
    try (var runtime = new StandaloneRuntime(config())) {
      String id = create(runtime, "source");
      write(runtime, id);
      var generation =
          runtime
              .liveIndexes()
              .getIndexById(new ObjectId(id))
              .orElseThrow()
              .getDefinitionGeneration();
      removeCommit(IndexSeed.path(config().dataPath(), generation));

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () -> runtime.updatesForTesting().startLegacyCopyForTesting(update(id, 1)));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(runtime.describeIndex(id).getDesiredRevision()).isEqualTo(1);
      assertThat(runtime.catalog().migration(id)).isEmpty();
      assertThat(runtime.describeIndex(id).getLifecycleState()).isEqualTo("FAILED");
      String healthy = create(runtime, "healthy");
      assertThat(runtime.describeIndex(healthy).getLifecycleState()).isEqualTo("READY");
      assertThat(runtime.describeIndex(id).getGenerationId())
          .isEqualTo(generation.getGenerationId().uniqueString());

      runtime.updateIndex(
          update(id, 1).toBuilder()
              .setUpdateMode(IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
              .build());
      runtime.catalogChanged();
      ready(runtime, id);
      assertThat(count(runtime, id)).isEqualTo(0);
    }
  }

  @Test
  public void update_sourceReadPin_doesNotDelayPublicationOrRetirePhysicalFiles() throws Exception {
    try (var runtime = new StandaloneRuntime(config())) {
      String id = create(runtime, "pinned-source");
      write(runtime, id);
      var generation =
          runtime
              .liveIndexes()
              .getIndexById(new ObjectId(id))
              .orElseThrow()
              .getDefinitionGeneration();
      try (var pin = runtime.ingestion().acquireQueryLease(generation.getGenerationId())) {
        runtime.updateIndex(update(id, 1));
        runtime.catalogChanged();
        await(() -> runtime.describeIndex(id).getActiveRevision() == 2);

        assertThat(runtime.describeIndex(id).getLifecycleState()).isEqualTo("READY");
        assertThat(count(runtime, id)).isEqualTo(1);
        assertThat(Files.isDirectory(IndexSeed.path(config().dataPath(), generation))).isTrue();
        assertThat(pin.retired()).isFalse();
        assertThat(runtime.describeIndex(id).getGenerationId())
            .isEqualTo(generation.getGenerationId().uniqueString());
      }
      assertThat(Files.exists(IndexSeed.path(config().dataPath(), generation))).isTrue();
    }
  }

  @Test
  public void update_dropDuringPreflight_doesNotReserveStaleSource() throws Exception {
    supersedePreflight(true);
  }

  @Test
  public void update_rebuildDuringPreflight_doesNotOverwriteNewDesiredState() throws Exception {
    supersedePreflight(false);
  }

  @Test
  public void recover_invalidSourceWithoutSeed_failsOnlyIndexAndKeepsRetryFenced()
      throws Exception {
    IndexMigration migration;
    try (var runtime = new StandaloneRuntime(config())) {
      String id = create(runtime, "failed-copy");
      write(runtime, id);
      interruptAt(runtime, StandaloneIndexUpdates.Checkpoint.COMMITTED);
      runtime.updatesForTesting().startLegacyCopyForTesting(update(id, 1));
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("FAILED"));
      migration = runtime.catalog().migration(id).orElseThrow();
    }
    removeCommit(IndexSeed.path(config().dataPath(), migration.source()));

    try (var runtime = new StandaloneRuntime(config())) {
      String id = migration.indexId();
      String healthy = create(runtime, "healthy");
      var error =
          assertThrows(StatusRuntimeException.class, () -> runtime.updateIndex(update(id, 2)));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(runtime.managementReady()).isTrue();
      assertThat(runtime.describeIndex(healthy).getLifecycleState()).isEqualTo("READY");
      assertThat(runtime.describeIndex(id).getDesiredRevision()).isEqualTo(2);
      assertThat(runtime.catalog().migration(id).orElseThrow().state())
          .isEqualTo(IndexMigration.State.FAILED);
      assertThat(runtime.describeIndex(id).getGenerationId())
          .isEqualTo(migration.source().getGenerationId().uniqueString());
      runtime.updateIndex(
          update(id, 2).toBuilder()
              .setUpdateMode(IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
              .build());
      runtime.catalogChanged();
      ready(runtime, id);
      assertThat(count(runtime, id)).isEqualTo(0);
    }
  }

  @Test
  public void recover_validInstalledSeedWithInvalidSource_publishesPreservedContent()
      throws Exception {
    IndexMigration migration;
    try (var runtime = new StandaloneRuntime(config())) {
      String id = create(runtime, "installed-seed");
      write(runtime, id);
      interruptAt(runtime, StandaloneIndexUpdates.Checkpoint.INSTALLED);
      runtime.updatesForTesting().startLegacyCopyForTesting(update(id, 1));
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("FAILED"));
      migration = runtime.catalog().migration(id).orElseThrow();
    }
    removeCommit(IndexSeed.path(config().dataPath(), migration.source()));

    try (var runtime = new StandaloneRuntime(config())) {
      ready(runtime, migration.indexId());

      assertThat(runtime.managementReady()).isTrue();
      assertThat(runtime.describeIndex(migration.indexId()).getGenerationId())
          .isEqualTo(migration.target().getGenerationId().uniqueString());
      assertThat(runtime.describeIndex(migration.indexId()).getActiveRevision()).isEqualTo(2);
      assertThat(count(runtime, migration.indexId())).isEqualTo(1);
    }
  }

  @Test
  public void recover_invalidPublishedTarget_keepsPublishedIdentityAndHealthyIndexes()
      throws Exception {
    IndexMigration migration;
    try (var runtime = new StandaloneRuntime(config())) {
      String id = create(runtime, "published-target");
      write(runtime, id);
      runtime.updatesForTesting().startLegacyCopyForTesting(update(id, 1));
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getActiveRevision() == 2);
      migration = runtime.catalog().migration(id).orElseThrow();
    }
    removeCommit(IndexSeed.path(config().dataPath(), migration.target()));

    try (var runtime = new StandaloneRuntime(config())) {
      String healthy = create(runtime, "healthy");
      await(() -> runtime.describeIndex(migration.indexId()).getLifecycleState().equals("FAILED"));

      assertThat(runtime.managementReady()).isTrue();
      assertThat(runtime.describeIndex(healthy).getLifecycleState()).isEqualTo("READY");
      assertThat(runtime.describeIndex(migration.indexId()).getGenerationId())
          .isEqualTo(migration.target().getGenerationId().uniqueString());
      assertThat(runtime.catalog().migration(migration.indexId()).orElseThrow().state())
          .isEqualTo(IndexMigration.State.COMPLETE);
    }
  }

  private void supersedePreflight(boolean drop) throws Exception {
    var validated = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var runtime = new StandaloneRuntime(config())) {
      String id = create(runtime, "preflight-race");
      runtime
          .updatesForTesting()
          .atCheckpoint(
              checkpoint -> {
                if (checkpoint == StandaloneIndexUpdates.Checkpoint.SOURCE_VALIDATED) {
                  validated.countDown();
                  try {
                    if (!release.await(15, TimeUnit.SECONDS)) {
                      throw new AssertionError(
                          "Concurrent catalog mutation was blocked by" + " preflight");
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                  }
                }
              });
      var pending =
          CompletableFuture.supplyAsync(
              () ->
                  assertThrows(
                      StatusRuntimeException.class,
                      () -> runtime.updatesForTesting().startLegacyCopyForTesting(update(id, 1))));
      try {
        assertThat(validated.await(10, TimeUnit.SECONDS)).isTrue();
        if (drop) {
          runtime
              .ingestion()
              .coordinate(
                  () ->
                      runtime
                          .catalog()
                          .dropIndex(
                              DropIndexRequest.newBuilder()
                                  .setIndexId(id)
                                  .setExpectedRevision(1)
                                  .build()));
        } else {
          runtime.updateIndex(
              update(id, 1).toBuilder()
                  .setUpdateMode(IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
                  .build());
        }
        runtime.catalogChanged();
        release.countDown();
        var error = pending.get(10, TimeUnit.SECONDS);
        assertThat(error.getStatus().getCode())
            .isEqualTo(drop ? Status.Code.NOT_FOUND : Status.Code.ABORTED);
        assertThat(runtime.catalog().migration(id)).isEmpty();
        if (!drop) {
          ready(runtime, id);
          assertThat(runtime.describeIndex(id).getDesiredRevision()).isEqualTo(2);
        }
      } finally {
        release.countDown();
        pending.get(20, TimeUnit.SECONDS);
      }
    }
  }

  private static void interruptAt(
      StandaloneRuntime runtime, StandaloneIndexUpdates.Checkpoint point) {
    runtime
        .updatesForTesting()
        .atCheckpoint(
            checkpoint -> {
              if (checkpoint == point) {
                throw new IllegalStateException("Injected failure at " + point);
              }
            });
  }

  private StandaloneConfig config() {
    return StandaloneConfig.development(this.folder.getRoot().toPath(), 0);
  }

  private static void removeCommit(Path directory) throws Exception {
    try (var files = Files.list(directory)) {
      Files.delete(
          files
              .filter(path -> path.getFileName().toString().startsWith("segments_"))
              .findFirst()
              .orElseThrow());
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
    ready(runtime, id);
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

  private static long count(StandaloneRuntime runtime, String id) throws Exception {
    return runtime
        .statistics()
        .summary(GetIndexStatsRequest.newBuilder().setIndexId(id).setRefresh(true).build())
        .get(10, TimeUnit.SECONDS)
        .getDocumentCount();
  }

  private static UpdateIndexRequest update(String id, long revision) {
    return UpdateIndexRequest.newBuilder()
        .setIndexId(id)
        .setExpectedRevision(revision)
        .setDefinitionBson(bson(UPDATED))
        .build();
  }

  private static void ready(StandaloneRuntime runtime, String id) throws Exception {
    await(() -> runtime.describeIndex(id).getLifecycleState().equals("READY"));
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Index did not converge");
      }
      Thread.sleep(20);
    }
  }

  private static ByteString bson(String json) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json)));
  }
}
