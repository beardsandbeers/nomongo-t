package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonValue;

/** An existence join whose foreign predicate is resolved by shared query planning. */
public record JoinOperator(
    Optional<JoinSource> from, FieldPath localField, FieldPath foreignField, Operator operator)
    implements Operator {
  static class Fields {
    static final Field.Optional<JoinSource> FROM =
        Field.builder("from").classField(JoinSource::fromBson).optional().noDefault();
    static final Field.Required<String> LOCAL_FIELD =
        Field.builder("localField").stringField().mustNotBeEmpty().required();
    static final Field.Required<String> FOREIGN_FIELD =
        Field.builder("foreignField").stringField().mustNotBeEmpty().required();
    static final Field.Required<Operator> OPERATOR =
        Field.builder("operator")
            .classField(Operator::exactlyOneFromBson)
            .disallowUnknownFields()
            .required();
  }

  static JoinOperator fromBson(DocumentParser parser) throws BsonParseException {
    return new JoinOperator(
        parser.getField(Fields.FROM).unwrap(),
        FieldPath.parse(parser.getField(Fields.LOCAL_FIELD).unwrap()),
        FieldPath.parse(parser.getField(Fields.FOREIGN_FIELD).unwrap()),
        parser.getField(Fields.OPERATOR).unwrap());
  }

  @Override
  public Type getType() {
    return Type.JOIN;
  }

  @Override
  public Score score() {
    return Score.defaultScore();
  }

  @Override
  public BsonValue operatorToBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.FROM, this.from)
        .field(Fields.LOCAL_FIELD, this.localField.toString())
        .field(Fields.FOREIGN_FIELD, this.foreignField.toString())
        .field(Fields.OPERATOR, this.operator)
        .build();
  }
}
