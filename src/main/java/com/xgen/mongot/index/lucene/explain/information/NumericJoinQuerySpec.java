package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldPathField;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.collections4.Equator;
import org.bson.BsonDocument;

public record NumericJoinQuerySpec(FieldPath path, int keyCount)
    implements LuceneQuerySpecification {

  private static class Fields {
    static final Field.Required<FieldPath> PATH =
        Field.builder("path").classField(FieldPathField::parse, FieldPathField::encode).required();
    static final Field.Required<Integer> KEY_COUNT =
        Field.builder("keyCount").intField().mustBeNonNegative().required();
  }

  static NumericJoinQuerySpec fromBson(DocumentParser parser) throws BsonParseException {
    return new NumericJoinQuerySpec(
        parser.getField(Fields.PATH).unwrap(), parser.getField(Fields.KEY_COUNT).unwrap());
  }

  @Override
  public Type getType() {
    return Type.NUMERIC_JOIN_QUERY;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PATH, this.path)
        .field(Fields.KEY_COUNT, this.keyCount)
        .build();
  }

  @Override
  public boolean equals(
      LuceneQuerySpecification other,
      Equator<QueryExplainInformation> equator,
      Comparator<QueryExplainInformation> comparator) {
    if (other.getType() != Type.NUMERIC_JOIN_QUERY) {
      return false;
    }
    var query = (NumericJoinQuerySpec) other;

    return Objects.equals(this.path, query.path) && Objects.equals(this.keyCount, query.keyCount);
  }
}
