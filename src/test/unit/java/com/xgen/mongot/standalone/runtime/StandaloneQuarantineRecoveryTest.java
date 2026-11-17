package com.xgen.mongot.standalone.runtime;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.IndexUpdateMode;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StandaloneQuarantineRecoveryTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final IndexType type;
  private final boolean staged;

  public StandaloneQuarantineRecoveryTest(IndexType type, boolean staged) {
    this.type = type;
    this.staged = staged;
  }

  @Parameterized.Parameters(name = "quarantined_{0}_staged_{1}")
  public static Object[][] cases() {
    return new Object[][] {
      {IndexType.INDEX_TYPE_SEARCH, false},
      {IndexType.INDEX_TYPE_SEARCH, true},
      {IndexType.INDEX_TYPE_VECTOR_SEARCH, false},
      {IndexType.INDEX_TYPE_VECTOR_SEARCH, true}
    };
  }

  @Test
  public void recover_unreadableLocalEntry_refusesStartupBeforeDeletingOrRewritingData()
      throws Exception {
    Path root = this.folder.getRoot().toPath();
    IndexDefinitionGeneration source = seed(root);
    Path journalFile = root.resolve("configJournal.json");
    String original = Files.readString(journalFile);
    var journal = BsonDocument.parse(original);
    journal
        .getArray(journalArray())
        .getFirst()
        .asDocument()
        .put("futureProperty", BsonBoolean.TRUE);
    Files.writeString(journalFile, journal.toJson());
    var before = hashes(root);

    var failure = assertThrows(IOException.class, () -> open(root));

    assertThat(failure).hasMessageThat().contains("Unreadable local index journal entry");
    assertThat(failure).hasMessageThat().contains("Local data was retained");
    assertThat(hashes(root)).containsExactlyEntriesIn(before);
    IndexSeed.verifyExisting(root, source);

    Files.writeString(journalFile, original);
    try (var runtime = open(root)) {
      await(() -> runtime.ingestion().isGenerationReady(source.getGenerationId()));
      try (var lease = runtime.ingestion().acquireQueryLease(source.getGenerationId())) {
        assertThat(lease.index().getWriter().getNumDocs()).isEqualTo(1);
      }
      assertThat(runtime.managementReady()).isTrue();
    }
  }

  @Test
  public void recover_unreadableRemoteEntry_leavesNativeQuarantinePolicyInControl()
      throws Exception {
    Path root = this.folder.getRoot().toPath();
    seed(root);
    var journal = BsonDocument.parse(Files.readString(root.resolve("configJournal.json")));
    journal
        .getArray(journalArray())
        .getFirst()
        .asDocument()
        .put("futureProperty", BsonBoolean.TRUE);
    Path nativeRoot = this.folder.newFolder("native-owner").toPath();
    Files.writeString(nativeRoot.resolve("configJournal.json"), journal.toJson());

    try (var local = new LocalIndexCatalog(nativeRoot)) {
      StandaloneIndexUpdates.recover(nativeRoot, local);

      assertThat(local.definitions()).isEmpty();
      assertThat(Files.readString(nativeRoot.resolve("configJournal.json")))
          .isEqualTo(journal.toJson());
    }
  }

  private IndexDefinitionGeneration seed(Path root) throws Exception {
    try (var runtime = open(root)) {
      String id =
          runtime
              .catalog()
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("index")
                      .setType(this.type)
                      .setDefinitionBson(bson(definition()))
                      .build())
              .getIndexId();
      runtime.catalogChanged();
      await(() -> runtime.describeIndex(id).getLifecycleState().equals("READY"));
      runtime
          .ingestion()
          .write(
              id,
              () -> 1L,
              "",
              List.of(bson("{_id:'a',title:'coffee',v:[1.0,0.0]}")),
              false,
              DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);
      var source =
          runtime
              .liveIndexes()
              .getIndexById(new ObjectId(id))
              .orElseThrow()
              .getDefinitionGeneration();
      if (this.staged) {
        IndexDefinitionGeneration next =
            this.type == IndexType.INDEX_TYPE_SEARCH
                ? source.asSearch().incrementUser(source.asSearch().definition())
                : source.asVector().incrementUser(source.asVector().definition());
        Path partition = IndexSeed.path(root, next).resolve("0");
        Files.createDirectories(partition);
        Files.writeString(partition.resolve("unexpected-data"), "do not delete this evidence");
        runtime.updateIndex(
            UpdateIndexRequest.newBuilder()
                .setIndexId(id)
                .setExpectedRevision(1)
                .setUpdateMode(IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
                .setDefinitionBson(bson(definition()))
                .build());
        runtime.catalogChanged();
        await(() -> runtime.describeIndex(id).getLifecycleState().equals("FAILED"));
      }
      return source;
    }
  }

  private String definition() {
    return this.type == IndexType.INDEX_TYPE_SEARCH
        ? "{numPartitions:2,mappings:{fields:{title:{type:'string'}}}}"
        : "{numPartitions:2,fields:[{type:'vector',path:'v',numDimensions:2,similarity:'cosine'}]}";
  }

  private String journalArray() {
    return this.type == IndexType.INDEX_TYPE_SEARCH
        ? (this.staged ? "stagedIndexes" : "indexes")
        : (this.staged ? "stagedVectorIndexes" : "vectorIndexes");
  }

  private static StandaloneRuntime open(Path root) throws Exception {
    return new StandaloneRuntime(
        StandaloneConfig.development(root, 0),
        FeatureFlags.withDefaults().enable(Feature.QUARANTINE_UNPARSEABLE_INDEX).build(),
        DynamicFeatureFlagRegistry.communityDefault());
  }

  private static Map<String, String> hashes(Path root) throws Exception {
    var result = new TreeMap<String, String>();
    try (var files = Files.walk(root)) {
      for (Path file : files.filter(Files::isRegularFile).toList()) {
        result.put(
            root.relativize(file).toString(),
            HexFormat.of()
                .formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file))));
      }
    }
    return result;
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
