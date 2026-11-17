package com.xgen.mongot.standalone.catalog;

import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonDocument;

/** Durable schema publication on an unchanged physical index. Never authorizes empty creation. */
public record IndexSchemaUpdate(
    SearchIndexDefinitionGeneration source, SearchIndexDefinitionGeneration target) {
  /** Validates a single compatible revision advance on the same physical index. */
  public IndexSchemaUpdate {
    if (!source.getGenerationId().equals(target.getGenerationId())
        || !source.getIndexDefinition().getName().equals(target.getIndexDefinition().getName())
        || !source
            .getIndexDefinition()
            .getDatabase()
            .equals(target.getIndexDefinition().getDatabase())
        || !source
            .getIndexDefinition()
            .getCollectionUuid()
            .equals(target.getIndexDefinition().getCollectionUuid())
        || !source
            .getIndexDefinition()
            .getLastObservedCollectionName()
            .equals(target.getIndexDefinition().getLastObservedCollectionName())
        || !source.getIndexDefinition().getView().equals(target.getIndexDefinition().getView())
        || !source
            .definition()
            .analyzerDefinitions()
            .equals(target.definition().analyzerDefinitions())
        || source.getIndexDefinition().getDefinitionVersion().orElseThrow() + 1
            != target.getIndexDefinition().getDefinitionVersion().orElseThrow()
        || IndexUpdateCompatibility.classify(
                    source.getIndexDefinition(), target.getIndexDefinition())
                .kind()
            != IndexUpdateCompatibility.Kind.PRESERVE_CONTENT) {
      throw new IllegalArgumentException("Invalid schema update on an existing physical index");
    }
  }

  public String indexId() {
    return this.source.getIndexId().toHexString();
  }

  public BsonDocument toBson() {
    return new BsonDocument("source", this.source.toBson()).append("target", this.target.toBson());
  }

  public static IndexSchemaUpdate fromBson(BsonDocument document) throws BsonParseException {
    try (var source = BsonDocumentParser.fromRoot(document.getDocument("source")).build();
        var target = BsonDocumentParser.fromRoot(document.getDocument("target")).build()) {
      return new IndexSchemaUpdate(
          SearchIndexDefinitionGeneration.fromBson(source),
          SearchIndexDefinitionGeneration.fromBson(target));
    }
  }
}
