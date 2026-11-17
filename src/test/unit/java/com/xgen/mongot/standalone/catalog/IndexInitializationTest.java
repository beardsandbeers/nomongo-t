package com.xgen.mongot.standalone.catalog;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.JournalEntry;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DropIndexRequest;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.IndexUpdateMode;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class IndexInitializationTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final IndexType type;
  private final String definition;

  public IndexInitializationTest(IndexType type, String definition) {
    this.type = type;
    this.definition = definition;
  }

  @Parameterized.Parameters(name = "initialization_{0}")
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
  public void reopen_boundReservation_retainsIdentityAndRevokesPermissionBeforeReadiness()
      throws Exception {
    var root = this.folder.getRoot().toPath();
    IndexDefinitionGeneration generation;
    try (var catalog = new LocalIndexCatalog(root)) {
      catalog.createIndex(create());
      var authorization = catalog.initializations().values().iterator().next();
      generation = generation(authorization.definition());
      catalog.bindInitialization(generation);
      assertThat(catalog.allowsEmptyInitialization(generation.getGenerationId())).isTrue();
    }

    try (var catalog = new LocalIndexCatalog(root)) {
      assertThat(catalog.reservedInitialization(generation.getIndexDefinition()))
          .hasValue(generation);
      var initialization = catalog.initialization(generation.getGenerationId()).orElseThrow();
      catalog.completeInitialization(initialization);
      catalog.createIndex(create());

      assertThat(catalog.allowsEmptyInitialization(generation.getGenerationId())).isFalse();
      assertThat(catalog.requiresExistingData(generation.getGenerationId())).isTrue();
      assertThat(catalog.initializations()).hasSize(1);
    }
    assertThat(
            BsonDocument.parse(Files.readString(root.resolve("indexes.json")))
                .getInt32("catalogVersion")
                .getValue())
        .isEqualTo(4);
    try (var catalog = new LocalIndexCatalog(root)) {
      assertThat(catalog.initialization(generation.getGenerationId()).orElseThrow().state())
          .isEqualTo(IndexInitialization.State.READY);
    }
  }

  @Test
  public void completeInitialization_supersededByRebuild_doesNotReauthorizeOldGeneration()
      throws Exception {
    try (var catalog = new LocalIndexCatalog(this.folder.getRoot().toPath())) {
      var description = catalog.createIndex(create());
      var generation = generation(catalog.definitions().getFirst());
      catalog.bindInitialization(generation);
      var old = catalog.initialization(generation.getGenerationId()).orElseThrow();

      catalog.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setIndexId(description.getIndexId())
              .setExpectedRevision(1)
              .setUpdateMode(IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
              .setDefinitionBson(create().getDefinitionBson())
              .build());

      assertThat(
              assertThrows(StatusRuntimeException.class, () -> catalog.completeInitialization(old))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.ABORTED);
      assertThat(catalog.allowsEmptyInitialization(generation.getGenerationId())).isFalse();
      assertThat(
              catalog.initializations().values().stream()
                  .filter(IndexInitialization::active)
                  .count())
          .isEqualTo(1);
      assertThat(catalog.getIndex(description.getIndexId()).getDesiredRevision()).isEqualTo(2);
    }
  }

  @Test
  public void reopen_versionTwoCatalog_doesNotInventEmptyAuthorization() throws Exception {
    var root = this.folder.getRoot().toPath();
    IndexDefinitionGeneration generation;
    try (var catalog = new LocalIndexCatalog(root)) {
      catalog.createIndex(create());
      generation = generation(catalog.definitions().getFirst());
    }
    var stored = BsonDocument.parse(Files.readString(root.resolve("indexes.json")));
    stored.put("catalogVersion", new org.bson.BsonInt32(2));
    stored.remove("initializations");
    Files.writeString(root.resolve("indexes.json"), stored.toJson());

    try (var catalog = new LocalIndexCatalog(root)) {
      assertThat(catalog.initializations()).isEmpty();
      assertThat(catalog.requiresExistingData(generation.getGenerationId())).isTrue();
      catalog.createIndex(create());
      assertThat(catalog.initializations()).isEmpty();
    }
  }

  @Test
  public void reopen_missingCatalog_checksStagedAndDeletedOwnership() throws Exception {
    var root = this.folder.getRoot().toPath();
    IndexDefinitionGeneration index;
    try (var catalog = new LocalIndexCatalog(root)) {
      catalog.createIndex(create());
      index = generation(catalog.definitions().getFirst());
    }
    Files.move(root.resolve("indexes.json"), root.resolve("saved-indexes.json"));
    var journal = root.resolve("configJournal.json");
    var entry = JournalEntry.parsed(index);
    var staged = new ConfigJournalV1(List.of(entry), List.of(), List.of()).toBson();
    var entries =
        this.type == IndexType.INDEX_TYPE_SEARCH ? "stagedIndexes" : "stagedVectorIndexes";
    staged
        .getArray(entries)
        .getFirst()
        .asDocument()
        .put("futureProperty", org.bson.BsonBoolean.TRUE);
    Files.writeString(journal, staged.toJson());

    assertThat(assertThrows(IOException.class, () -> new LocalIndexCatalog(root)))
        .hasMessageThat()
        .contains("Missing indexes.json");
    assertThat(Files.readString(journal)).isEqualTo(staged.toJson());

    var deleted = new ConfigJournalV1(List.of(), List.of(), List.of(entry)).toBson();
    Files.writeString(journal, deleted.toJson());
    try (var reopened = new LocalIndexCatalog(root)) {
      assertThat(reopened.definitions()).isEmpty();
      assertThat(reopened.owns(index.getIndexId())).isTrue();
    }
    Files.delete(journal);
    try (var reserved = new LocalIndexCatalog(root)) {
      assertThat(reserved.definitions()).isEmpty();
      assertThat(reserved.owns(index.getIndexId())).isTrue();
    }
  }

  @Test
  public void reopen_recordedDropOrOnlyReplicatedLiveEntries_allowsStartup() throws Exception {
    var root = this.folder.getRoot().toPath();
    var journal = root.resolve("configJournal.json");
    JournalEntry<IndexDefinitionGeneration> local;
    try (var catalog = new LocalIndexCatalog(root)) {
      var index = catalog.createIndex(create());
      local = JournalEntry.parsed(generation(catalog.definitions().getFirst()));
      catalog.dropIndex(
          DropIndexRequest.newBuilder()
              .setIndexId(index.getIndexId())
              .setExpectedRevision(index.getDesiredRevision())
              .build());
    }
    Files.writeString(
        journal, new ConfigJournalV1(List.of(), List.of(local), List.of()).toBson().toJson());
    try (var recordedDrop = new LocalIndexCatalog(root)) {
      assertThat(recordedDrop.definitions()).isEmpty();
      assertThat(recordedDrop.owns(local.getGenerationId().indexId)).isTrue();
    }

    var remote =
        StandaloneDefinitionValidator.parse(
                new ObjectId().toHexString(),
                "remote",
                this.type,
                1,
                create().getDefinitionBson().toByteArray())
            .internal();
    var mixed =
        new ConfigJournalV1(
            List.of(), List.of(JournalEntry.parsed(generation(remote))), List.of(local));
    Files.writeString(journal, mixed.toBson().toJson());
    Files.writeString(root.resolve("coexistence.json"), "{\"version\":1}");
    Files.move(root.resolve("indexes.json"), root.resolve("saved-indexes.json"));
    try (var replicatedOnly = new LocalIndexCatalog(root)) {
      assertThat(replicatedOnly.definitions()).isEmpty();
      assertThat(replicatedOnly.owns(remote.getIndexId())).isFalse();
      assertThat(Files.readString(journal)).isEqualTo(mixed.toBson().toJson());
    }
  }

  private CreateIndexRequest create() {
    return CreateIndexRequest.newBuilder()
        .setName("index")
        .setType(this.type)
        .setDefinitionBson(
            ByteString.copyFrom(
                StandaloneDefinitionValidator.encode(BsonDocument.parse(this.definition))))
        .build();
  }

  private static IndexDefinitionGeneration generation(IndexDefinition definition) {
    var generation = definition.newGeneration(new UserIndexVersion(7));
    return definition.getType() == IndexDefinition.Type.SEARCH
        ? new SearchIndexDefinitionGeneration(
            AnalyzerBoundSearchIndexDefinition.create(definition.asSearchDefinition(), List.of()),
            generation)
        : new VectorIndexDefinitionGeneration(definition.asVectorDefinition(), generation);
  }
}
