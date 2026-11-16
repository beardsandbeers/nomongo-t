package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;

/** Join preparation for the whole query and execution for the current local partition. */
public record JoinExplainInformation(
    Optional<JoinPreparationInformation> preparation,
    Optional<List<JoinExecutionInformation>> execution)
    implements DocumentEncodable {
  private static class Fields {
    static final Field.Optional<JoinPreparationInformation> PREPARATION =
        Field.builder("preparation")
            .classField(JoinPreparationInformation::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
    static final Field.Optional<List<JoinExecutionInformation>> EXECUTION =
        Field.builder("execution")
            .classField(JoinExecutionInformation::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();
  }

  public static JoinExplainInformation fromBson(DocumentParser parser) throws BsonParseException {
    return new JoinExplainInformation(
        parser.getField(Fields.PREPARATION).unwrap(), parser.getField(Fields.EXECUTION).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.PREPARATION, this.preparation)
        .field(Fields.EXECUTION, this.execution)
        .build();
  }
}
