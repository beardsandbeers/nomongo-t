package com.xgen.mongot.standalone.catalog;

import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonDocument;
import org.bson.BsonString;

/** Durable intent for one content-preserving update, including its reserved physical identity. */
public record IndexMigration(
    SearchIndexDefinitionGeneration source,
    SearchIndexDefinitionGeneration target,
    State state,
    String failure) {
  public enum State {
    ACCEPTED,
    PREPARED,
    STAGED,
    COMPLETE,
    FAILED,
    CANCELLED
  }

  public String indexId() {
    return this.source.getIndexId().toHexString();
  }

  public IndexMigration withState(State next, String error) {
    return new IndexMigration(this.source, this.target, next, error);
  }

  public BsonDocument toBson() {
    return new BsonDocument("source", this.source.toBson())
        .append("target", this.target.toBson())
        .append("state", new BsonString(this.state.name()))
        .append("failure", new BsonString(this.failure));
  }

  public static IndexMigration fromBson(BsonDocument document) throws BsonParseException {
    try (var source = BsonDocumentParser.fromRoot(document.getDocument("source")).build();
        var target = BsonDocumentParser.fromRoot(document.getDocument("target")).build()) {
      var result =
          new IndexMigration(
              SearchIndexDefinitionGeneration.fromBson(source),
              SearchIndexDefinitionGeneration.fromBson(target),
              State.valueOf(document.getString("state").getValue()),
              document.getString("failure").getValue());
      if (!result.source.getIndexId().equals(result.target.getIndexId())
          || !result
              .source
              .generation()
              .formatVersion
              .equals(result.target.generation().formatVersion)
          || result.source.generation().userIndexVersion.versionNumber
              >= result.target.generation().userIndexVersion.versionNumber
          || result.source.getIndexDefinition().getDefinitionVersion().orElseThrow() + 1
              != result.target.getIndexDefinition().getDefinitionVersion().orElseThrow()
          || IndexUpdateCompatibility.classify(
                      result.source.getIndexDefinition(), result.target.getIndexDefinition())
                  .kind()
              != IndexUpdateCompatibility.Kind.PRESERVE_CONTENT) {
        throw new IllegalArgumentException("Invalid preserved index migration");
      }
      return result;
    }
  }
}
