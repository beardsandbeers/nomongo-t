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
import com.xgen.mongot.standalone.grpc.IndexUpdateMode;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StandaloneInitializationFailuresTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final IndexType type;
  private final String definition;

  public StandaloneInitializationFailuresTest(IndexType type, String definition) {
    this.type = type;
    this.definition = definition;
  }

  @Parameterized.Parameters(name = "failedBootstrap_{0}")
  public static Object[][] cases() {
    return new Object[][] {
      {IndexType.INDEX_TYPE_SEARCH, "{numPartitions:2,mappings:{fields:{title:{type:'string'}}}}"},
      {
        IndexType.INDEX_TYPE_VECTOR_SEARCH,
        "{numPartitions:2,fields:[{type:'vector',path:'v',numDimensions:2,similarity:'cosine'}]}"
      }
    };
  }

  @Test
  public void update_failedEmptyRebuild_reportsFailureAndAllowsExplicitFreshRebuild()
      throws Exception {
    try (var runtime = new StandaloneRuntime(config())) {
      String id = create(runtime);
      write(runtime, id);
      var source =
          runtime
              .liveIndexes()
              .getIndexById(new ObjectId(id))
              .orElseThrow()
              .getDefinitionGeneration();
      Path obstruction = obstructNextGeneration(source);
      runtime.updateIndex(update(id, 1, IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY));
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("FAILED"));

      var failed = runtime.describeIndex(id);
      var authorization =
          runtime
              .catalog()
              .reservedInitialization(runtime.catalog().definitions().getFirst())
              .orElseThrow();
      assertThat(failed.getActiveRevision()).isEqualTo(1);
      assertThat(failed.getDesiredRevision()).isEqualTo(2);
      assertThat(failed.getFailureMessage()).contains("Unexpected data in empty initialization");
      var retry =
          assertThrows(
              StatusRuntimeException.class,
              () -> runtime.updateIndex(update(id, 2, IndexUpdateMode.INDEX_UPDATE_MODE_AUTO)));
      assertThat(retry.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(retry.getStatus().getDescription()).contains("Restart to resume");
      assertThat(retry.getStatus().getDescription()).contains("INDEX_UPDATE_MODE_REBUILD_EMPTY");
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          runtime.updateIndex(
                              update(id, 2, IndexUpdateMode.INDEX_UPDATE_MODE_REQUIRE_PRESERVE)))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(
              runtime.catalog().reservedInitialization(runtime.catalog().definitions().getFirst()))
          .hasValue(authorization);
      assertThat(runtime.catalog().getIndex(id).getDesiredRevision()).isEqualTo(2);
      assertThrows(StatusRuntimeException.class, () -> write(runtime, id));
      assertThat(Files.readString(obstruction)).isEqualTo("retain until an explicit rebuild/drop");
      IndexSeed.verifyExisting(config().dataPath(), source);

      runtime.updateIndex(update(id, 2, IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY));
      runtime.catalogChanged();
      ready(runtime, id, 3);
      assertThat(count(runtime, id)).isEqualTo(0);
      assertThat(runtime.describeIndex(id).getFailureMessage()).isEmpty();
      assertThat(runtime.describeIndex(id).getGenerationId())
          .isNotEqualTo(authorization.getGenerationId().uniqueString());
      write(runtime, id);
      assertThat(count(runtime, id)).isEqualTo(1);
    }
  }

  @Test
  public void recover_failedEmptyRebuild_resumesSameBoundGenerationAfterCauseIsRepaired()
      throws Exception {
    String id;
    Path obstruction;
    IndexDefinitionGeneration target;
    try (var runtime = new StandaloneRuntime(config())) {
      id = create(runtime);
      write(runtime, id);
      var source =
          runtime
              .liveIndexes()
              .getIndexById(new ObjectId(id))
              .orElseThrow()
              .getDefinitionGeneration();
      obstruction = obstructNextGeneration(source);
      runtime.updateIndex(update(id, 1, IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY));
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("FAILED"));
      target =
          runtime
              .catalog()
              .reservedInitialization(runtime.catalog().definitions().getFirst())
              .orElseThrow();
    }
    Files.delete(obstruction);

    try (var runtime = new StandaloneRuntime(config())) {
      ready(runtime, id, 2);

      assertThat(runtime.describeIndex(id).getGenerationId())
          .isEqualTo(target.getGenerationId().uniqueString());
      assertThat(runtime.describeIndex(id).getFailureMessage()).isEmpty();
      assertThat(count(runtime, id)).isEqualTo(0);
      assertThat(runtime.catalog().initialization(target.getGenerationId()).orElseThrow().state())
          .isEqualTo(IndexInitialization.State.READY);
      assertThat(runtime.catalog().allowsEmptyInitialization(target.getGenerationId())).isFalse();
      IndexSeed.verifyExisting(config().dataPath(), target);
      write(runtime, id);
      assertThat(count(runtime, id)).isEqualTo(1);
    }
  }

  private Path obstructNextGeneration(IndexDefinitionGeneration source) throws Exception {
    IndexDefinitionGeneration next =
        this.type == IndexType.INDEX_TYPE_SEARCH
            ? source.asSearch().incrementUser(source.asSearch().definition())
            : source.asVector().incrementUser(source.asVector().definition());
    Path partition = IndexSeed.path(config().dataPath(), next).resolve("0");
    Files.createDirectories(partition);
    Path obstruction = partition.resolve("unexpected-data");
    Files.writeString(obstruction, "retain until an explicit rebuild/drop");
    return obstruction;
  }

  private StandaloneConfig config() {
    return StandaloneConfig.development(this.folder.getRoot().toPath(), 0);
  }

  private String create(StandaloneRuntime runtime) throws Exception {
    String id =
        runtime
            .catalog()
            .createIndex(
                CreateIndexRequest.newBuilder()
                    .setName("index")
                    .setType(this.type)
                    .setDefinitionBson(bson(this.definition))
                    .build())
            .getIndexId();
    runtime.catalogChanged();
    ready(runtime, id, 1);
    return id;
  }

  private UpdateIndexRequest update(String id, long revision, IndexUpdateMode mode) {
    return UpdateIndexRequest.newBuilder()
        .setIndexId(id)
        .setExpectedRevision(revision)
        .setUpdateMode(mode)
        .setDefinitionBson(bson(this.definition))
        .build();
  }

  private static void write(StandaloneRuntime runtime, String id) {
    runtime
        .ingestion()
        .write(
            id,
            () -> runtime.catalog().getIndex(id).getDesiredRevision(),
            "",
            List.of(bson("{_id:'a',title:'coffee',v:[1.0,0.0]}")),
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
        throw new AssertionError("Initialization did not converge");
      }
      Thread.sleep(20);
    }
  }

  private static ByteString bson(String json) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json)));
  }
}
