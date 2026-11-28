package com.xgen.mongot.standalone.catalog;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.index.definition.SearchIndexDefinition.MappingVersion;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DropIndexRequest;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.IntStream;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalIndexCatalogTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void createIndex_catalogVersion_matchesRequiredMappingFeatures() throws Exception {
    var dir = this.folder.getRoot().toPath();
    try (var catalog = new LocalIndexCatalog(dir)) {
      catalog.createIndex(
          createRequest(
              "plain",
              "{mappings:{fields:{title:{type:'string'},'unambiguous.path':{type:'string'}}}}"));
      var plain = BsonDocument.parse(Files.readString(dir.resolve("indexes.json")));
      assertThat(plain.getInt32("catalogVersion").getValue()).isEqualTo(4);
      assertThat(plain.toJson()).doesNotContain("typeSetSimilarity");
      assertThat(plain.toJson()).doesNotContain("mappingVersion");

      catalog.createIndex(
          createRequest(
              "vector",
              """
              {mappings:{dynamic:{typeSet:'v'}},
               typeSets:[{name:'v',types:[{type:'vector',similarity:'euclidean'}]}]}
              """));
      var upgraded = BsonDocument.parse(Files.readString(dir.resolve("indexes.json")));
      assertThat(upgraded.getInt32("catalogVersion").getValue()).isEqualTo(5);
    }
    try (var reopened = new LocalIndexCatalog(dir)) {
      assertThat(reopened.definitions()).hasSize(2);
    }
  }

  @Test
  public void createIndex_staticBsonVectorOnly_usesVersionFiveCatalog() throws Exception {
    var dir = this.folder.getRoot().toPath();
    try (var catalog = new LocalIndexCatalog(dir)) {
      catalog.createIndex(
          createRequest(
              "bson-only",
              """
              {mappings:{dynamic:false,fields:{
                v:{type:'vector',numDimensions:2,similarity:'euclidean',bsonVectorOnly:true}}}}
              """));
      var disk = BsonDocument.parse(Files.readString(dir.resolve("indexes.json")));
      assertThat(disk.getInt32("catalogVersion").getValue()).isEqualTo(5);
    }
    try (var catalog = new LocalIndexCatalog(dir)) {
      assertThat(catalog.definitions()).hasSize(1);
    }
  }

  private static CreateIndexRequest createRequest(String name, String json) {
    return CreateIndexRequest.newBuilder()
        .setType(IndexType.INDEX_TYPE_SEARCH)
        .setName(name)
        .setDefinitionBson(
            ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json))))
        .build();
  }

  @Test
  public void planUpdate_olderScoringGeneration_preservedUntilRebuild() throws Exception {
    var dir = this.folder.getRoot().toPath();
    String schema =
        """
        {mappings:{dynamic:false,fields:{title:{type:'document',dynamic:{typeSet:'s'}}%s}},
         typeSets:[{name:'s',types:[{type:'string',similarity:{type:'boolean'}}]}]}
        """;
    String id;
    try (var catalog = new LocalIndexCatalog(dir)) {
      id = catalog.createIndex(createRequest("older", schema.formatted(""))).getIndexId();
    }
    var catalogFile = dir.resolve("indexes.json");
    // Older catalogs predate schema and initialization records and the scoring generation flag.
    var entries = BsonDocument.parse(Files.readString(catalogFile)).getArray("entries");
    entries.get(0).asDocument().remove("mappingVersion");
    Files.writeString(catalogFile, new BsonDocument("indexes", entries).toJson());
    var update =
        UpdateIndexRequest.newBuilder()
            .setIndexId(id)
            .setExpectedRevision(1)
            .setDefinitionBson(
                createRequest("older", schema.formatted(",extra:{type:'number'}"))
                    .getDefinitionBson());
    try (var catalog = new LocalIndexCatalog(dir)) {
      var preserve = catalog.planUpdate(update.build());
      var rebuild =
          catalog.planUpdate(
              update
                  .setUpdateMode(
                      com.xgen.mongot.standalone.grpc.IndexUpdateMode
                          .INDEX_UPDATE_MODE_REBUILD_EMPTY)
                  .build());
      assertThat(preserve.compatibility().kind())
          .isEqualTo(IndexUpdateCompatibility.Kind.PRESERVE_CONTENT);
      assertThat(preserve.candidate().asSearchDefinition().getMappingVersion())
          .isEqualTo(MappingVersion.LEGACY);
      assertThat(rebuild.candidate().asSearchDefinition().getMappingVersion())
          .isEqualTo(MappingVersion.SCOPED);
    }
  }

  @Test
  public void createIndex_moreThan64Indexes_persistsAcrossRestart() throws Exception {
    var directory = this.folder.getRoot().toPath();
    var definition =
        ByteString.copyFrom(
            StandaloneDefinitionValidator.encode(BsonDocument.parse("{mappings:{dynamic:true}}")));
    var request =
        CreateIndexRequest.newBuilder()
            .setName("index-65")
            .setType(IndexType.INDEX_TYPE_SEARCH)
            .setDefinitionBson(definition)
            .build();
    String id;
    try (var catalog = new LocalIndexCatalog(directory)) {
      IntStream.rangeClosed(1, 64)
          .forEach(i -> catalog.createIndex(request.toBuilder().setName("index-" + i).build()));

      id = catalog.createIndex(request).getIndexId();
    }

    try (var reopened = new LocalIndexCatalog(directory)) {
      assertThat(reopened.definitions()).hasSize(65);
      assertThat(reopened.getIndex(id).getName()).isEqualTo("index-65");
    }
  }

  @Test
  public void reopen_preservesDefinitionsAndRevisionsWithoutContent() throws Exception {
    String id;
    var definition =
        ByteString.copyFrom(
            StandaloneDefinitionValidator.encode(BsonDocument.parse("{mappings:{dynamic:true}}")));
    var request =
        CreateIndexRequest.newBuilder()
            .setName("text")
            .setType(IndexType.INDEX_TYPE_SEARCH)
            .setDefinitionBson(definition)
            .build();
    try (var catalog = new LocalIndexCatalog(this.folder.getRoot().toPath())) {
      id = catalog.createIndex(request).getIndexId();
      assertThat(catalog.createIndex(request).getIndexId()).isEqualTo(id);
      catalog.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setUpdateMode(
                  com.xgen.mongot.standalone.grpc.IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
              .setIndexId(id)
              .setExpectedRevision(1)
              .setDefinitionBson(definition)
              .build());
    }
    try (var catalog = new LocalIndexCatalog(this.folder.getRoot().toPath())) {
      assertThat(catalog.getIndex(id).getDesiredRevision()).isEqualTo(2);
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          catalog.dropIndex(
                              DropIndexRequest.newBuilder()
                                  .setIndexId(id)
                                  .setExpectedRevision(1)
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.ABORTED);
      catalog.dropIndex(
          DropIndexRequest.newBuilder().setIndexId(id).setExpectedRevision(2).build());
    }
    try (var catalog = new LocalIndexCatalog(this.folder.getRoot().toPath())) {
      assertThat(catalog.definitions()).isEmpty();
      assertThat(catalog.createIndex(request).getIndexId()).isNotEqualTo(id);
    }
  }

  @Test
  public void legacyDirectory_failsWithoutChangingDataOrLeakingLock() throws Exception {
    var directory = this.folder.getRoot().toPath();
    var legacy = directory.resolve("standalone.sqlite");
    Files.writeString(legacy, "legacy data");
    assertThrows(IOException.class, () -> new LocalIndexCatalog(directory));
    assertThat(Files.readString(legacy)).isEqualTo("legacy data");
    assertThrows(IOException.class, () -> new LocalIndexCatalog(directory));
  }

  @Test
  public void reopen_droppedIndex_retainsSourceOwnership() throws Exception {
    var directory = this.folder.getRoot().toPath();
    String id;
    try (var catalog = new LocalIndexCatalog(directory)) {
      id =
          catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("owned")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(
                          ByteString.copyFrom(
                              StandaloneDefinitionValidator.encode(
                                  BsonDocument.parse("{mappings:{dynamic:true}}"))))
                      .build())
              .getIndexId();
      catalog.dropIndex(
          DropIndexRequest.newBuilder().setIndexId(id).setExpectedRevision(1).build());
    }

    try (var reopened = new LocalIndexCatalog(directory)) {
      assertThat(reopened.definitions()).isEmpty();
      assertThat(reopened.owns(new ObjectId(id))).isTrue();
    }
  }

  @Test
  public void reopen_missingOwnershipAfterCoexistence_refusesToReclassifyIndexes()
      throws Exception {
    var directory = this.folder.getRoot().toPath();
    Files.writeString(directory.resolve("coexistence.json"), "{\"version\":1}");

    assertThrows(IOException.class, () -> new LocalIndexCatalog(directory));

    assertThat(Files.exists(directory.resolve("standalone-owned-ids.json"))).isFalse();
    assertThrows(IOException.class, () -> new LocalIndexCatalog(directory));
  }

  @Test
  public void reopen_conflictingOwnershipAfterCoexistence_preservesMetadataForRepair()
      throws Exception {
    var directory = this.folder.getRoot().toPath();
    try (var catalog = new LocalIndexCatalog(directory)) {
      catalog.createIndex(
          CreateIndexRequest.newBuilder()
              .setName("owned")
              .setType(IndexType.INDEX_TYPE_SEARCH)
              .setDefinitionBson(
                  ByteString.copyFrom(
                      StandaloneDefinitionValidator.encode(
                          BsonDocument.parse("{mappings:{dynamic:true}}"))))
              .build());
    }
    Files.writeString(directory.resolve("coexistence.json"), "{\"version\":1}");
    Files.writeString(directory.resolve("standalone-owned-ids.json"), "{\"ids\":[]}");

    assertThrows(IOException.class, () -> new LocalIndexCatalog(directory));

    assertThat(Files.readString(directory.resolve("standalone-owned-ids.json")))
        .isEqualTo("{\"ids\":[]}");
  }
}
