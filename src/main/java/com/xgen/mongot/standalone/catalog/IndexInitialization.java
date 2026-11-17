package com.xgen.mongot.standalone.catalog;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonString;

/** Durable permission to initialize an empty generation before it has ever admitted writes. */
public record IndexInitialization(
    IndexDefinition definition,
    Purpose purpose,
    Optional<IndexDefinitionGeneration> generation,
    State state) {
  public enum Purpose {
    CREATE_EMPTY,
    REBUILD_EMPTY
  }

  public enum State {
    AUTHORIZED,
    INITIALIZING,
    READY,
    CANCELLED
  }

  public IndexInitialization {
    if ((state == State.AUTHORIZED && generation.isPresent())
        || ((state == State.INITIALIZING || state == State.READY) && generation.isEmpty())
        || generation.filter(value -> !value.getIndexDefinition().equals(definition)).isPresent()) {
      throw new IllegalArgumentException("Invalid empty initialization binding");
    }
  }

  public static IndexInitialization authorize(IndexDefinition definition, Purpose purpose) {
    return new IndexInitialization(definition, purpose, Optional.empty(), State.AUTHORIZED);
  }

  public String indexId() {
    return this.definition.getIndexId().toHexString();
  }

  public String key() {
    return indexId() + ":" + this.definition.getDefinitionVersion().orElseThrow();
  }

  public boolean active() {
    return this.state == State.AUTHORIZED || this.state == State.INITIALIZING;
  }

  public IndexInitialization bind(IndexDefinitionGeneration generation) {
    if (this.state != State.AUTHORIZED) {
      throw new IllegalStateException("Initialization is already bound or revoked");
    }
    return new IndexInitialization(
        this.definition, this.purpose, Optional.of(generation), State.INITIALIZING);
  }

  public IndexInitialization withState(State state) {
    return new IndexInitialization(this.definition, this.purpose, this.generation, state);
  }

  public BsonDocument toBson() {
    var result =
        new BsonDocument("type", new BsonString(this.definition.getType().name()))
            .append("definition", this.definition.toBson())
            .append("purpose", new BsonString(this.purpose.name()))
            .append("state", new BsonString(this.state.name()));
    this.generation.ifPresent(value -> result.append("generation", value.toBson()));
    return result;
  }

  public static IndexInitialization fromBson(BsonDocument document) throws BsonParseException {
    var definition =
        StandaloneDefinitionValidator.restore(
            StandaloneDefinitionValidator.encode(document.getDocument("definition")));
    if (!document.getString("type").getValue().equals(definition.getType().name())) {
      throw new IllegalArgumentException("Initialization type does not match definition");
    }
    @Var Optional<IndexDefinitionGeneration> generation = Optional.empty();
    if (document.containsKey("generation")) {
      try (var parser = BsonDocumentParser.fromRoot(document.getDocument("generation")).build()) {
        generation =
            Optional.of(
                definition.getType() == IndexDefinition.Type.SEARCH
                    ? SearchIndexDefinitionGeneration.fromBson(parser)
                    : VectorIndexDefinitionGeneration.fromBson(parser));
      }
    }
    return new IndexInitialization(
        definition,
        Purpose.valueOf(document.getString("purpose").getValue()),
        generation,
        State.valueOf(document.getString("state").getValue()));
  }
}
