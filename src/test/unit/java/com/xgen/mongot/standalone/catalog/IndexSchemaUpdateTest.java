package com.xgen.mongot.standalone.catalog;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.IndexUpdateMode;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import java.nio.file.Files;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IndexSchemaUpdateTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private static final String BEFORE = "{mappings:{fields:{title:{type:'string'}}}}";
  private static final String AFTER =
      "{mappings:{fields:{title:{type:'string'},tag:{type:'token'}}}}";

  @Test
  public void reopen_acceptedSchema_retainsPhysicalIdentityAndNeverAuthorizesEmptyCreation()
      throws Exception {
    var root = this.folder.getRoot().toPath();
    IndexSchemaUpdate schema;
    try (var catalog = new LocalIndexCatalog(root)) {
      var created = catalog.createIndex(create());
      var source = generation(catalog.definitions().getFirst().asSearchDefinition());
      catalog.bindInitialization(source);
      catalog.completeInitialization(
          catalog.initialization(source.getGenerationId()).orElseThrow());
      var target =
          catalog.planUpdate(update(created.getIndexId(), 1)).candidate().asSearchDefinition();
      schema = new IndexSchemaUpdate(source, generation(target));
      catalog.acceptSchemaUpdate(schema);
    }

    try (var catalog = new LocalIndexCatalog(root)) {
      assertThat(catalog.schemas()).containsExactly(schema.indexId(), schema);
      assertThat(schema.target().getGenerationId()).isEqualTo(schema.source().getGenerationId());
      assertThat(catalog.requiresExistingData(schema.target().getGenerationId())).isTrue();
      assertThat(catalog.allowsEmptyInitialization(schema.target().getGenerationId())).isFalse();
      assertThat(catalog.updateIndex(update(schema.indexId(), 2)).getDesiredRevision())
          .isEqualTo(2);
      assertThat(catalog.schemas()).containsExactly(schema.indexId(), schema);
      catalog.updateIndex(
          update(schema.indexId(), 2).toBuilder()
              .setUpdateMode(IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
              .build());
      assertThat(catalog.schemas()).isEmpty();
      assertThat(
              catalog.initializations().values().stream()
                  .filter(IndexInitialization::active)
                  .count())
          .isEqualTo(1);
    }
    assertThat(
            BsonDocument.parse(Files.readString(root.resolve("indexes.json")))
                .getInt32("catalogVersion")
                .getValue())
        .isEqualTo(4);
  }

  @Test
  public void fromBson_changedOwner_rejectsCompatibleMappingWithDifferentIdentity()
      throws Exception {
    var source = generation(parse("original", 1, BEFORE));
    var target = generation(parse("renamed", 2, AFTER));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            IndexSchemaUpdate.fromBson(
                new BsonDocument("source", source.toBson()).append("target", target.toBson())));
  }

  @Test
  public void fromBson_changedDatabase_rejectsTamperedBinding() throws Exception {
    var source = generation(parse("index", 1, BEFORE));
    var target = generation(parse("index", 2, AFTER)).toBson();
    target.getDocument("index").put("database", new BsonString("different"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            IndexSchemaUpdate.fromBson(
                new BsonDocument("source", source.toBson()).append("target", target)));
  }

  @Test
  public void constructor_changedPhysicalGeneration_rejectsBinding() {
    var source = generation(parse("index", 1, BEFORE));
    var target = generation(parse("index", 2, AFTER)).incrementAttempt().asSearch();

    assertThrows(IllegalArgumentException.class, () -> new IndexSchemaUpdate(source, target));
  }

  @Test
  public void acceptSchemaUpdate_unfinishedInitialization_rejectsAndRetainsDesiredDefinition()
      throws Exception {
    try (var catalog = new LocalIndexCatalog(this.folder.getRoot().toPath())) {
      var created = catalog.createIndex(create());
      var source = generation(catalog.definitions().getFirst().asSearchDefinition());
      catalog.bindInitialization(source);
      var target =
          catalog.planUpdate(update(created.getIndexId(), 1)).candidate().asSearchDefinition();

      assertThrows(
          io.grpc.StatusRuntimeException.class,
          () -> catalog.acceptSchemaUpdate(new IndexSchemaUpdate(source, generation(target))));

      assertThat(catalog.getIndex(created.getIndexId()).getDesiredRevision()).isEqualTo(1);
      assertThat(catalog.schemas()).isEmpty();
    }
  }

  private static SearchIndexDefinition parse(String name, long revision, String definition) {
    return StandaloneDefinitionValidator.parse(
            "0123456789abcdef01234567",
            name,
            IndexType.INDEX_TYPE_SEARCH,
            revision,
            bson(definition).toByteArray())
        .internal()
        .asSearchDefinition();
  }

  private static SearchIndexDefinitionGeneration generation(SearchIndexDefinition definition) {
    return new SearchIndexDefinitionGeneration(
        AnalyzerBoundSearchIndexDefinition.create(definition, List.of()),
        definition.newGeneration(new UserIndexVersion(7)));
  }

  private static CreateIndexRequest create() {
    return CreateIndexRequest.newBuilder()
        .setName("index")
        .setType(IndexType.INDEX_TYPE_SEARCH)
        .setDefinitionBson(bson(BEFORE))
        .build();
  }

  private static UpdateIndexRequest update(String id, long revision) {
    return UpdateIndexRequest.newBuilder()
        .setIndexId(id)
        .setExpectedRevision(revision)
        .setDefinitionBson(bson(AFTER))
        .build();
  }

  private static ByteString bson(String json) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json)));
  }
}
