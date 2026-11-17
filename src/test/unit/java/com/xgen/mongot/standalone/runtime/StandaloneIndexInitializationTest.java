package com.xgen.mongot.standalone.runtime;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.standalone.catalog.IndexInitialization;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.GetIndexStatsRequest;
import com.xgen.mongot.standalone.grpc.IndexType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.types.ObjectId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StandaloneIndexInitializationTest {
  enum Boundary {
    BEFORE_JOURNAL,
    BEFORE_COMMITS,
    BETWEEN_COMMITS,
    AFTER_COMMITS,
    READY_RECORDED
  }

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final IndexType type;
  private final Boundary boundary;

  public StandaloneIndexInitializationTest(IndexType type, Boundary boundary) {
    this.type = type;
    this.boundary = boundary;
  }

  @Parameterized.Parameters(name = "recover_{0}_at_{1}")
  public static Object[][] cases() {
    return new Object[][] {
      {IndexType.INDEX_TYPE_SEARCH, Boundary.BEFORE_JOURNAL},
      {IndexType.INDEX_TYPE_SEARCH, Boundary.BEFORE_COMMITS},
      {IndexType.INDEX_TYPE_SEARCH, Boundary.BETWEEN_COMMITS},
      {IndexType.INDEX_TYPE_SEARCH, Boundary.AFTER_COMMITS},
      {IndexType.INDEX_TYPE_SEARCH, Boundary.READY_RECORDED},
      {IndexType.INDEX_TYPE_VECTOR_SEARCH, Boundary.BEFORE_JOURNAL},
      {IndexType.INDEX_TYPE_VECTOR_SEARCH, Boundary.BEFORE_COMMITS},
      {IndexType.INDEX_TYPE_VECTOR_SEARCH, Boundary.BETWEEN_COMMITS},
      {IndexType.INDEX_TYPE_VECTOR_SEARCH, Boundary.AFTER_COMMITS},
      {IndexType.INDEX_TYPE_VECTOR_SEARCH, Boundary.READY_RECORDED}
    };
  }

  @Test
  public void recover_interruptedBootstrap_commitsEveryPartitionAndNeverRecreatesReadyData()
      throws Exception {
    var root = this.folder.getRoot().toPath();
    var config = StandaloneConfig.development(root, 0);
    IndexDefinitionGeneration generation;
    String id;
    try (var runtime = new StandaloneRuntime(config)) {
      id = create(runtime, "interrupted");
      generation =
          runtime
              .liveIndexes()
              .getIndexById(new ObjectId(id))
              .orElseThrow()
              .getDefinitionGeneration();
    }
    // Reconstruct only durable states reachable before any API write was admitted. Retaining
    // real empty mongot commits keeps the fixture's codec and internal metadata accurate.
    interruptAtBoundary(root, generation);

    try (var runtime = new StandaloneRuntime(config)) {
      ready(runtime, id);
      assertThat(runtime.describeIndex(id).getGenerationId())
          .isEqualTo(generation.getGenerationId().uniqueString());
      IndexSeed.verifyExisting(root, generation);
      assertThat(
              runtime.catalog().initialization(generation.getGenerationId()).orElseThrow().state())
          .isEqualTo(IndexInitialization.State.READY);
      assertThat(runtime.catalog().allowsEmptyInitialization(generation.getGenerationId()))
          .isFalse();
      runtime
          .ingestion()
          .write(
              id,
              () -> 1L,
              "",
              List.of(
                  bson("{_id:'a',title:'coffee',v:[1.0,0.0]}"),
                  bson("{_id:'b',title:'coffee',v:[0.0,1.0]}")),
              false,
              DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);
      assertThat(
              runtime
                  .statistics()
                  .summary(
                      GetIndexStatsRequest.newBuilder().setIndexId(id).setRefresh(true).build())
                  .get(10, TimeUnit.SECONDS)
                  .getDocumentCount())
          .isEqualTo(2);
    }

    removeCommit(IndexSeed.path(root, generation).resolve("0"));
    try (var runtime = new StandaloneRuntime(config)) {
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("FAILED"));
      String healthy = create(runtime, "healthy");

      assertThat(runtime.managementReady()).isTrue();
      assertThat(runtime.describeIndex(healthy).getLifecycleState()).isEqualTo("READY");
      assertThat(runtime.describeIndex(id).getGenerationId())
          .isEqualTo(generation.getGenerationId().uniqueString());
      assertThat(runtime.describeIndex(id).getLifecycleState()).isEqualTo("FAILED");
      assertThrows(java.io.IOException.class, () -> IndexSeed.verifyExisting(root, generation));
    }
  }

  private void interruptAtBoundary(Path root, IndexDefinitionGeneration generation)
      throws Exception {
    if (this.boundary == Boundary.READY_RECORDED) {
      return;
    }
    Path catalog = root.resolve("indexes.json");
    var document = BsonDocument.parse(Files.readString(catalog));
    document
        .getArray("initializations")
        .getFirst()
        .asDocument()
        .put("state", new BsonString("INITIALIZING"));
    Files.writeString(catalog, document.toJson());
    Path directory = IndexSeed.path(root, generation);
    switch (this.boundary) {
      case BEFORE_JOURNAL -> {
        IndexSeed.remove(directory);
        Path path = root.resolve("configJournal.json");
        var journal = BsonDocument.parse(Files.readString(path));
        journal.put(
            this.type == IndexType.INDEX_TYPE_SEARCH ? "indexes" : "vectorIndexes",
            new BsonArray());
        Files.writeString(path, journal.toJson());
      }
      case BEFORE_COMMITS -> {
        removeCommit(directory.resolve("0"));
        removeCommit(directory.resolve("1"));
      }
      case BETWEEN_COMMITS -> removeCommit(directory.resolve("0"));
      case AFTER_COMMITS, READY_RECORDED -> {}
    }
  }

  private static void removeCommit(Path partition) throws Exception {
    try (var files = Files.list(partition)) {
      Files.delete(
          files
              .filter(path -> path.getFileName().toString().startsWith("segments_"))
              .findFirst()
              .orElseThrow());
    }
  }

  private String create(StandaloneRuntime runtime, String name) throws Exception {
    String definition =
        this.type == IndexType.INDEX_TYPE_SEARCH
            ? "{numPartitions:2,mappings:{fields:{title:{type:'string'}}}}"
            : "{numPartitions:2,fields:[{type:'vector',path:'v',numDimensions:2,"
                + "similarity:'cosine'}]}";
    String id =
        runtime
            .catalog()
            .createIndex(
                CreateIndexRequest.newBuilder()
                    .setName(name)
                    .setType(this.type)
                    .setDefinitionBson(bson(definition))
                    .build())
            .getIndexId();
    runtime.catalogChanged();
    ready(runtime, id);
    return id;
  }

  private static void ready(StandaloneRuntime runtime, String id) throws Exception {
    await(() -> runtime.describeIndex(id).getLifecycleState().equals("READY"));
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Index initialization did not converge");
      }
      Thread.sleep(20);
    }
  }

  private static ByteString bson(String json) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json)));
  }
}
