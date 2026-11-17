package com.xgen.mongot.standalone.runtime;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import com.google.protobuf.ByteString;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.GetIndexStatsRequest;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StandaloneIndexUpdatesCrashTest {
  private static final String ORIGINAL =
      "{numPartitions:2,mappings:{dynamic:false,fields:{title:{type:'string'}}}}";
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final StandaloneIndexUpdates.Checkpoint checkpoint;
  private final boolean empty;

  public StandaloneIndexUpdatesCrashTest(
      StandaloneIndexUpdates.Checkpoint checkpoint, boolean empty) {
    this.checkpoint = checkpoint;
    this.empty = empty;
  }

  @Parameterized.Parameters(name = "crashAfter_{0}_empty_{1}")
  public static Object[][] checkpoints() {
    return new Object[][] {
      {StandaloneIndexUpdates.Checkpoint.ACCEPTED, false},
      {StandaloneIndexUpdates.Checkpoint.SCHEMA_PREPARED, false},
      {StandaloneIndexUpdates.Checkpoint.SCHEMA_JOURNALED, false},
      {StandaloneIndexUpdates.Checkpoint.PUBLISHED, false},
      {StandaloneIndexUpdates.Checkpoint.ACCEPTED, true}
    };
  }

  @Test
  public void recover_terminatedMigration_preservesCommittedDocumentsAndDeletes() throws Exception {
    Path directory = this.folder.getRoot().toPath();
    prepareInterruptedBootstrap(directory, this.empty);
    Path log = directory.resolve("child.log");
    var child =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-preview",
                "--add-modules=jdk.incubator.vector",
                "--enable-native-access=ALL-UNNAMED",
                "-cp",
                System.getProperty("java.class.path"),
                StandaloneIndexUpdatesCrashTest.class.getName(),
                directory.toString(),
                this.checkpoint.name(),
                Boolean.toString(this.empty))
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    try {
      assertThat(child.waitFor(60, TimeUnit.SECONDS)).isTrue();
      assertWithMessage(Files.readString(log)).that(child.exitValue()).isEqualTo(77);
    } finally {
      child.destroyForcibly();
      child.waitFor(10, TimeUnit.SECONDS);
    }
    String id = Files.readString(directory.resolve("id"));

    try (var runtime = new StandaloneRuntime(StandaloneConfig.development(directory, 0))) {
      resume(runtime, id);
      assertThat(runtime.describeIndex(id).getGenerationId())
          .isEqualTo(Files.readString(directory.resolve("physical")));
      var stats =
          runtime
              .statistics()
              .summary(GetIndexStatsRequest.newBuilder().setIndexId(id).setRefresh(true).build())
              .get(10, TimeUnit.SECONDS);

      assertThat(stats.getDocumentCount()).isEqualTo(this.empty ? 0 : 1);
      assertThat(runtime.catalog().getIndex(id).getDesiredRevision()).isEqualTo(2);
      runtime
          .ingestion()
          .write(
              id,
              () -> 2L,
              "",
              List.of(bson("{_id:'c',title:'coffee',category:'books'}")),
              false,
              DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);
      assertThat(
              runtime
                  .statistics()
                  .summary(
                      GetIndexStatsRequest.newBuilder().setIndexId(id).setRefresh(true).build())
                  .get(10, TimeUnit.SECONDS)
                  .getDocumentCount())
          .isEqualTo(this.empty ? 1 : 2);
    }
  }

  /** Runs in a separate JVM and deliberately bypasses close/shutdown hooks. */
  public static void main(String[] arguments) throws Exception {
    Path directory = Path.of(arguments[0]);
    var crashAt = StandaloneIndexUpdates.Checkpoint.valueOf(arguments[1]);
    try (var runtime = new StandaloneRuntime(StandaloneConfig.development(directory, 0))) {
      String id =
          runtime
              .catalog()
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("crash")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(bson(ORIGINAL))
                      .build())
              .getIndexId();
      runtime.catalogChanged();
      ready(runtime, id, 1);
      if (!Boolean.parseBoolean(arguments[2])) {
        runtime
            .ingestion()
            .write(
                id,
                () -> 1L,
                "",
                List.of(bson("{_id:'a',title:'coffee'}"), bson("{_id:'b',title:'coffee'}")),
                false,
                DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);
        runtime
            .ingestion()
            .write(
                id,
                () -> 1L,
                "",
                List.of(bson("{_id:'b'}")),
                true,
                DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);
      }
      Files.writeString(directory.resolve("id"), id);
      Files.writeString(directory.resolve("physical"), runtime.describeIndex(id).getGenerationId());
      runtime
          .updatesForTesting()
          .atCheckpoint(
              checkpoint -> {
                if (checkpoint == crashAt) {
                  Runtime.getRuntime().halt(77);
                }
              });
      runtime.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setIndexId(id)
              .setExpectedRevision(1)
              .setDefinitionBson(
                  bson(
                      "{numPartitions:2,mappings:{dynamic:false,fields:"
                          + "{title:{type:'string'},category:{type:'token'}}}}"))
              .build());
      runtime.catalogChanged();
      ready(runtime, id, 2);
      throw new AssertionError("Migration did not reach crash checkpoint " + crashAt);
    }
  }

  private static void resume(StandaloneRuntime runtime, String id) throws Exception {
    if (runtime.catalog().getIndex(id).getDesiredRevision() == 1) {
      ready(runtime, id, 1);
      runtime.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setIndexId(id)
              .setExpectedRevision(1)
              .setDefinitionBson(
                  bson(
                      "{numPartitions:2,mappings:{dynamic:false,fields:{title:{type:'string'},"
                          + "category:{type:'token'}}}}"))
              .build());
      runtime.catalogChanged();
    }
    ready(runtime, id, 2);
  }

  private static void ready(StandaloneRuntime runtime, String id, long revision) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      var description = runtime.describeIndex(id);
      if (description.getActiveRevision() == revision
          && description.getLifecycleState().equals("READY")) {
        // Publication observation runs in the following reconciliation, after the shared swap.
        runtime.updatesForTesting().reconcile();
        return;
      }
      Thread.sleep(20);
    }
    throw new AssertionError("Index not ready: " + runtime.describeIndex(id));
  }

  private static void prepareInterruptedBootstrap(Path directory, boolean empty) throws Exception {
    if (!empty) {
      return;
    }
    com.xgen.mongot.index.definition.IndexDefinitionGeneration generation;
    try (var runtime = new StandaloneRuntime(StandaloneConfig.development(directory, 0))) {
      String id =
          runtime
              .catalog()
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("crash")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(bson(ORIGINAL))
                      .build())
              .getIndexId();
      runtime.catalogChanged();
      ready(runtime, id, 1);
      generation =
          runtime
              .liveIndexes()
              .getIndexById(new org.bson.types.ObjectId(id))
              .orElseThrow()
              .getDefinitionGeneration();
    }
    // Replay the durable state of a crash between the first commits, before write admission.
    Path file = directory.resolve("indexes.json");
    var catalog = BsonDocument.parse(Files.readString(file));
    catalog
        .getArray("initializations")
        .getFirst()
        .asDocument()
        .put("state", new org.bson.BsonString("INITIALIZING"));
    Files.writeString(file, catalog.toJson());
    try (var files = Files.list(IndexSeed.path(directory, generation).resolve("0"))) {
      Files.delete(
          files
              .filter(path -> path.getFileName().toString().startsWith("segments_"))
              .findFirst()
              .orElseThrow());
    }
  }

  private static ByteString bson(String json) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json)));
  }
}
