package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;

/**
 * Represents a 'vector' field definition within a search index.
 *
 * <p>This class basically wraps a {@link VectorFieldSpecification} to provide feature parity with
 * vector index 'vector' data field type.
 */
public record SearchIndexVectorFieldDefinition(
    VectorFieldSpecification specification, boolean bsonVectorOnly) implements FieldTypeDefinition {

  private static final Field.WithDefault<Boolean> BSON_VECTOR_ONLY =
      Field.builder("bsonVectorOnly").booleanField().optional().withDefault(false);

  public SearchIndexVectorFieldDefinition(VectorFieldSpecification specification) {
    this(specification, false);
  }

  @Override
  public Type getType() {
    return Type.VECTOR;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    var mapping = this.specification.toBson();
    if (this.bsonVectorOnly) {
      mapping.put("bsonVectorOnly", BsonBoolean.TRUE);
    }
    return mapping;
  }

  static SearchIndexVectorFieldDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new SearchIndexVectorFieldDefinition(
        VectorFieldSpecification.fromBson(parser), parser.getField(BSON_VECTOR_ONLY).unwrap());
  }
}
