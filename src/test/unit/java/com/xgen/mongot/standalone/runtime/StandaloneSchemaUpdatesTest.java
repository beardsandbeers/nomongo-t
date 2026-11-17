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
import com.xgen.mongot.standalone.grpc.IndexUpdateMode;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StandaloneSchemaUpdatesTest {
  private static final String ORIGINAL =
      "{numPartitions:2,mappings:{fields:{title:{type:'string'}}}}";
  private static final String UPDATED =
      "{numPartitions:2,mappings:{fields:{title:{type:'string'},tag:{type:'token'}}}}";
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void isGenerationReady_unpublishedCompatibleSchema_preservesLeaseUntilRebuild()
      throws Exception {
    try (var runtime = new StandaloneRuntime(config())) {
      String id = create(runtime, "pending-schema");
      try (var lease = runtime.reads().acquire(id, "")) {
        var generation = lease.index().getGenerationId();
        runtime
            .updatesForTesting()
            .atCheckpoint(
                point -> {
                  if (point == StandaloneIndexUpdates.Checkpoint.ACCEPTED) {
                    throw new IllegalStateException("Publication has not completed");
                  }
                });

        assertThrows(
            IllegalStateException.class, () -> runtime.updateIndex(update(id, 1, UPDATED)));

        assertThat(runtime.catalog().getIndex(id).getDesiredRevision()).isEqualTo(2);
        assertThat(lease.retired()).isFalse();
        assertThat(runtime.reads().isGenerationReady(generation)).isTrue();
        assertThat(
                assertThrows(StatusRuntimeException.class, () -> runtime.reads().acquire(id, ""))
                    .getStatus()
                    .getCode())
            .isEqualTo(Status.Code.UNAVAILABLE);

        runtime.updateIndex(
            update(id, 2, UPDATED).toBuilder()
                .setUpdateMode(IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
                .build());

        assertThat(runtime.reads().isGenerationReady(generation)).isFalse();
      }
    }
  }

  @Test
  public void update_concurrentSchemasAndWrites_keepsPhysicalIdentityAndAllAcceptedWrites()
      throws Exception {
    try (var runtime = new StandaloneRuntime(config());
        var workers = Executors.newFixedThreadPool(2)) {
      String id = create(runtime, "concurrent");
      String physical = runtime.describeIndex(id).getGenerationId();
      var prepared = new CountDownLatch(2);
      var release = new CountDownLatch(1);
      runtime
          .updatesForTesting()
          .atCheckpoint(
              point -> {
                if (point == StandaloneIndexUpdates.Checkpoint.SCHEMA_PREPARED) {
                  prepared.countDown();
                  waitOn(release);
                }
              });
      var first = workers.submit(() -> tryUpdate(runtime, update(id, 1, UPDATED)));
      var second =
          workers.submit(
              () -> tryUpdate(runtime, update(id, 1, UPDATED.replace("tag:", "other:"))));
      try {
        assertThat(prepared.await(10, TimeUnit.SECONDS)).isTrue();
        write(runtime, id, "during-preparation");
        release.countDown();
        writeMany(runtime, id);

        assertThat(List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)))
            .containsExactly(true, false);
        assertThat(runtime.describeIndex(id).getGenerationId()).isEqualTo(physical);
        assertThat(runtime.describeIndex(id).getActiveRevision()).isEqualTo(2);
        assertThat(count(runtime, id)).isEqualTo(51);
        assertThat(runtime.catalog().schemas()).hasSize(1);
        assertThat(runtime.catalog().migrations()).isEmpty();
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void
      update_durableSchemaBeforePublication_concurrentRetriesCompleteWithoutAnotherRevision()
          throws Exception {
    var prepared = new CountDownLatch(2);
    var release = new CountDownLatch(1);
    var interrupted = new java.util.concurrent.atomic.AtomicBoolean();
    try (var runtime = new StandaloneRuntime(config());
        var caller = Executors.newSingleThreadExecutor()) {
      String id = create(runtime, "retry");
      write(runtime, id, "kept");
      String physical = runtime.describeIndex(id).getGenerationId();
      runtime
          .updatesForTesting()
          .atCheckpoint(
              point -> {
                if (point == StandaloneIndexUpdates.Checkpoint.SCHEMA_PREPARED
                    && interrupted.get()) {
                  prepared.countDown();
                  waitOn(release);
                }
                if (point == StandaloneIndexUpdates.Checkpoint.ACCEPTED
                    && interrupted.compareAndSet(false, true)) {
                  throw new IllegalStateException("Interrupted after durable schema acceptance");
                }
              });
      try {
        assertThrows(
            IllegalStateException.class, () -> runtime.updateIndex(update(id, 1, UPDATED)));
        assertThat(runtime.catalog().getIndex(id).getDesiredRevision()).isEqualTo(2);
        assertThat(runtime.describeIndex(id).getGenerationId()).isEqualTo(physical);
        var retry = caller.submit(() -> runtime.updateIndex(update(id, 2, UPDATED)));
        assertThat(prepared.await(10, TimeUnit.SECONDS)).isTrue();
        release.countDown();
        assertThat(retry.get(10, TimeUnit.SECONDS).getDesiredRevision()).isEqualTo(2);
        ready(runtime, id, 2);

        assertThat(runtime.describeIndex(id).getGenerationId()).isEqualTo(physical);
        assertThat(runtime.catalog().getIndex(id).getDesiredRevision()).isEqualTo(2);
        assertThat(count(runtime, id)).isEqualTo(1);
        write(runtime, id, "new");
        assertThat(count(runtime, id)).isEqualTo(2);
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void update_dropDuringPreparation_doesNotPublishSchemaOrBlockOtherIndexes()
      throws Exception {
    try (var runtime = new StandaloneRuntime(config());
        var worker = Executors.newSingleThreadExecutor()) {
      String id = create(runtime, "drop");
      var prepared = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      runtime
          .updatesForTesting()
          .atCheckpoint(
              point -> {
                if (point == StandaloneIndexUpdates.Checkpoint.SCHEMA_PREPARED) {
                  prepared.countDown();
                  waitOn(release);
                }
              });
      var updating =
          worker.submit(
              () ->
                  assertThrows(
                      StatusRuntimeException.class,
                      () -> runtime.updateIndex(update(id, 1, UPDATED))));
      try {
        assertThat(prepared.await(10, TimeUnit.SECONDS)).isTrue();
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
        runtime.catalogChanged();
        String healthy = create(runtime, "healthy");
        release.countDown();

        assertThat(updating.get(10, TimeUnit.SECONDS).getStatus().getCode())
            .isAnyOf(Status.Code.NOT_FOUND, Status.Code.UNAVAILABLE, Status.Code.ABORTED);
        assertThat(runtime.catalog().schemas()).isEmpty();
        assertThat(runtime.describeIndex(healthy).getLifecycleState()).isEqualTo("READY");
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void update_explicitResetAfterSchemaPublication_allocatesNewStorageOnlyForTheReset()
      throws Exception {
    String id;
    String reset;
    try (var runtime = new StandaloneRuntime(config())) {
      id = create(runtime, "reset");
      write(runtime, id, "kept");
      String physical = runtime.describeIndex(id).getGenerationId();
      runtime.updateIndex(update(id, 1, UPDATED));
      assertThat(runtime.describeIndex(id).getGenerationId()).isEqualTo(physical);
      runtime.updateIndex(
          update(id, 2, UPDATED).toBuilder()
              .setUpdateMode(IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
              .build());
      runtime.catalogChanged();
      ready(runtime, id, 3);
      reset = runtime.describeIndex(id).getGenerationId();

      assertThat(reset).isNotEqualTo(physical);
      assertThat(runtime.catalog().schemas()).isEmpty();
      assertThat(count(runtime, id)).isEqualTo(0);
      write(runtime, id, "new");
    }
    try (var runtime = new StandaloneRuntime(config())) {
      ready(runtime, id, 3);
      assertThat(runtime.describeIndex(id).getGenerationId()).isEqualTo(reset);
      assertThat(count(runtime, id)).isEqualTo(1);
    }
  }

  @Test
  public void recover_missingCommitAfterSchemaUpdate_failsTheSamePhysicalIndexWithoutEmptyRecovery()
      throws Exception {
    String id;
    com.xgen.mongot.index.definition.IndexDefinitionGeneration physical;
    try (var runtime = new StandaloneRuntime(config())) {
      id = create(runtime, "damaged");
      write(runtime, id, "kept");
      runtime.updateIndex(update(id, 1, UPDATED));
      write(runtime, id, "new");
      physical =
          runtime
              .liveIndexes()
              .getIndexById(new ObjectId(id))
              .orElseThrow()
              .getDefinitionGeneration();
    }
    try (var files = Files.list(IndexSeed.path(config().dataPath(), physical).resolve("0"))) {
      Files.delete(
          files
              .filter(path -> path.getFileName().toString().startsWith("segments_"))
              .findFirst()
              .orElseThrow());
    }
    try (var runtime = new StandaloneRuntime(config())) {
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("FAILED"));
      String healthy = create(runtime, "healthy");

      assertThat(runtime.describeIndex(id).getGenerationId())
          .isEqualTo(physical.getGenerationId().uniqueString());
      assertThat(runtime.catalog().getIndex(id).getDesiredRevision()).isEqualTo(2);
      assertThat(runtime.describeIndex(healthy).getLifecycleState()).isEqualTo("READY");
      assertThrows(
          java.io.IOException.class, () -> IndexSeed.verifyExisting(config().dataPath(), physical));
    }
  }

  private static boolean tryUpdate(StandaloneRuntime runtime, UpdateIndexRequest request) {
    try {
      runtime.updateIndex(request);
      return true;
    } catch (StatusRuntimeException error) {
      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
      return false;
    }
  }

  private static void writeMany(StandaloneRuntime runtime, String id) {
    for (int i = 0; i < 50; i++) {
      write(runtime, id, "write-" + i);
    }
  }

  private static void write(StandaloneRuntime runtime, String id, String key) {
    runtime
        .ingestion()
        .write(
            id,
            () -> runtime.catalog().getIndex(id).getDesiredRevision(),
            "",
            List.of(bson("{_id:'" + key + "',title:'coffee',tag:'new',other:'new'}")),
            false,
            DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);
  }

  private StandaloneConfig config() {
    return StandaloneConfig.development(this.folder.getRoot().toPath(), 0);
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
    ready(runtime, id, 1);
    return id;
  }

  private static UpdateIndexRequest update(String id, long revision, String definition) {
    return UpdateIndexRequest.newBuilder()
        .setIndexId(id)
        .setExpectedRevision(revision)
        .setDefinitionBson(bson(definition))
        .build();
  }

  private static long count(StandaloneRuntime runtime, String id) throws Exception {
    return runtime
        .statistics()
        .summary(GetIndexStatsRequest.newBuilder().setIndexId(id).setRefresh(true).build())
        .get(10, TimeUnit.SECONDS)
        .getDocumentCount();
  }

  private static void ready(StandaloneRuntime runtime, String id, long revision) throws Exception {
    await(
        () ->
            runtime.describeIndex(id).getLifecycleState().equals("READY")
                && runtime.describeIndex(id).getActiveRevision() == revision);
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Schema update did not converge");
      }
      Thread.sleep(20);
    }
  }

  private static void waitOn(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new AssertionError("Publication was blocked by preparation");
      }
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(error);
    }
  }

  private static ByteString bson(String json) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json)));
  }
}
