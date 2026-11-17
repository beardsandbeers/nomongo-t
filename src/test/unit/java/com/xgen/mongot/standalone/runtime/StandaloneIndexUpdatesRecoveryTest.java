package com.xgen.mongot.standalone.runtime;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.standalone.catalog.IndexMigration;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.GetIndexStatsRequest;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import io.grpc.StatusRuntimeException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.bson.BsonDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Legacy copy-recovery fixtures plus regressions for the public in-place update path. */
@RunWith(Parameterized.class)
public class StandaloneIndexUpdatesRecoveryTest {
  private static final String ORIGINAL =
      "{mappings:{dynamic:false,fields:{title:{type:'string'}}}}";
  private static final String UPDATED =
      "{mappings:{dynamic:false,fields:{title:{type:'string'},category:{type:'token'}}}}";

  enum Damage {
    MISSING_MANIFEST,
    MALFORMED_MANIFEST,
    MISSING_COMMIT,
    MISSING_DIRECTORY
  }

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final boolean staged;
  private final Damage damage;

  public StandaloneIndexUpdatesRecoveryTest(boolean staged, Damage damage) {
    this.staged = staged;
    this.damage = damage;
  }

  @Parameterized.Parameters(name = "staged_{0}_damage_{1}")
  public static Object[][] cases() {
    return new Object[][] {
      {false, Damage.MISSING_MANIFEST},
      {false, Damage.MALFORMED_MANIFEST},
      {false, Damage.MISSING_COMMIT},
      {false, Damage.MISSING_DIRECTORY},
      {true, Damage.MISSING_MANIFEST},
      {true, Damage.MALFORMED_MANIFEST},
      {true, Damage.MISSING_COMMIT},
      {true, Damage.MISSING_DIRECTORY}
    };
  }

  @Test
  public void recover_damagedUnpublishedTarget_keepsServicesAvailableAndRetriesPreservedContent()
      throws Exception {
    var config = StandaloneConfig.development(this.folder.getRoot().toPath(), 0);
    IndexMigration migration;
    String healthy;
    try (var runtime = new StandaloneRuntime(config)) {
      String id = create(runtime, "damaged");
      healthy = create(runtime, "healthy");
      write(runtime, id);
      write(runtime, healthy);
      runtime.updatesForTesting().atCheckpoint(StandaloneIndexUpdatesRecoveryTest::interruptSeed);
      runtime.updatesForTesting().startLegacyCopyForTesting(update(id, 1));
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("FAILED"));
      migration = runtime.catalog().migration(id).orElseThrow();
    }
    damageSeed(config.dataPath(), migration);
    recordPreparedState(config.dataPath(), migration);

    try (var runtime = new StandaloneRuntime(config)) {
      // Active metadata can precede initialization; retry only once the source is usable.
      await(
          () ->
              runtime.describeIndex(migration.indexId()).getActiveRevision() == 1
                  && runtime
                      .liveIndexes()
                      .getIndexById(new org.bson.types.ObjectId(migration.indexId()))
                      .filter(
                          index -> runtime.ingestion().isGenerationReady(index.getGenerationId()))
                      .isPresent());
      await(() -> runtime.describeIndex(migration.indexId()).getLifecycleState().equals("FAILED"));
      await(() -> runtime.describeIndex(healthy).getLifecycleState().equals("READY"));

      assertThat(runtime.managementReady()).isTrue();
      assertThat(runtime.describeIndex(migration.indexId()).getDesiredRevision()).isEqualTo(2);
      assertThat(runtime.describeIndex(migration.indexId()).getFailureMessage()).isNotEmpty();
      assertThat(count(runtime, healthy)).isEqualTo(1);
      assertThrows(StatusRuntimeException.class, () -> write(runtime, migration.indexId()));
      assertThat(Files.isDirectory(IndexSeed.path(config.dataPath(), migration.source()))).isTrue();
    }

    // A second restart must retain the failure instead of silently publishing a valid-looking
    // Lucene directory whose manifest was damaged, or creating an empty replacement.
    try (var runtime = new StandaloneRuntime(config)) {
      // Active metadata can precede initialization; retry only once the source is usable.
      await(
          () ->
              runtime.describeIndex(migration.indexId()).getActiveRevision() == 1
                  && runtime
                      .liveIndexes()
                      .getIndexById(new org.bson.types.ObjectId(migration.indexId()))
                      .filter(
                          index -> runtime.ingestion().isGenerationReady(index.getGenerationId()))
                      .isPresent());
      await(() -> runtime.describeIndex(migration.indexId()).getLifecycleState().equals("FAILED"));
      await(() -> retry(runtime, migration.indexId()));
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(migration.indexId()).getLifecycleState().equals("READY"));

      assertThat(runtime.describeIndex(migration.indexId()).getActiveRevision()).isEqualTo(2);
      assertThat(count(runtime, migration.indexId())).isEqualTo(1);
      write(runtime, migration.indexId());
      assertThat(count(runtime, migration.indexId())).isEqualTo(1);
    }
  }

  private static void interruptSeed(StandaloneIndexUpdates.Checkpoint checkpoint) {
    if (checkpoint == StandaloneIndexUpdates.Checkpoint.INSTALLED) {
      throw new IllegalStateException("Interrupted before recording prepared state");
    }
  }

  private static boolean retry(StandaloneRuntime runtime, String id) {
    try {
      runtime.updateIndex(update(id, 2));
      return true;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != io.grpc.Status.Code.UNAVAILABLE) {
        throw e;
      }
      return false;
    }
  }

  private void damageSeed(Path root, IndexMigration migration) throws Exception {
    Path directory = IndexSeed.path(root, migration.target());
    switch (this.damage) {
      case MISSING_MANIFEST -> Files.delete(directory.resolve(".standalone-seed.json"));
      case MALFORMED_MANIFEST ->
          Files.writeString(directory.resolve(".standalone-seed.json"), "{broken");
      case MISSING_COMMIT -> {
        try (var files = Files.list(directory)) {
          Files.delete(
              files
                  .filter(path -> path.getFileName().toString().startsWith("segments_"))
                  .findFirst()
                  .orElseThrow());
        }
      }
      case MISSING_DIRECTORY -> IndexSeed.remove(directory);
    }
  }

  private void recordPreparedState(Path root, IndexMigration migration) throws Exception {
    try (var catalog = new com.xgen.mongot.standalone.catalog.LocalIndexCatalog(root)) {
      catalog.setMigration(migration.withState(IndexMigration.State.PREPARED, ""));
    }
    if (this.staged) {
      Path file = root.resolve("configJournal.json");
      var journal = BsonDocument.parse(Files.readString(file));
      journal.getArray("stagedIndexes").add(migration.target().toBson());
      Files.writeString(file, journal.toJson());
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

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out waiting for index recovery");
      }
      Thread.sleep(20);
    }
  }

  private static ByteString bson(String json) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json)));
  }
}
