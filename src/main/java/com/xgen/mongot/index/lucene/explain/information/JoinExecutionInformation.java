package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;

/** Local strategy metadata and cumulative execution for a prepared join domain. */
public record JoinExecutionInformation(
    int id,
    String field,
    String keyType,
    int keyCount,
    String strategy,
    String selectionReason,
    boolean exhaustive,
    int countThreshold,
    Optional<QueryExecutionArea> weightSetup,
    Optional<List<JoinSegmentInformation>> segments)
    implements DocumentEncodable {
  private static class Fields {
    static final Field.Required<Integer> ID = Field.builder("id").intField().required();
    static final Field.Required<String> FIELD = Field.builder("field").stringField().required();
    static final Field.Required<String> KEY_TYPE =
        Field.builder("keyType").stringField().required();
    static final Field.Required<Integer> KEY_COUNT =
        Field.builder("keyCount").intField().required();
    static final Field.Required<String> STRATEGY =
        Field.builder("strategy").stringField().required();
    static final Field.Required<String> SELECTION_REASON =
        Field.builder("selectionReason").stringField().required();
    static final Field.Required<Boolean> EXHAUSTIVE =
        Field.builder("exhaustive").booleanField().required();
    static final Field.Required<Integer> COUNT_THRESHOLD =
        Field.builder("countThreshold").intField().required();
    static final Field.Optional<QueryExecutionArea> WEIGHT_SETUP =
        Field.builder("weightSetup")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
    static final Field.Optional<List<JoinSegmentInformation>> SEGMENTS =
        Field.builder("segments")
            .classField(JoinSegmentInformation::fromBson)
            .disallowUnknownFields()
            .asList()
            .optional()
            .noDefault();
  }

  public static JoinExecutionInformation fromBson(DocumentParser parser) throws BsonParseException {
    return new JoinExecutionInformation(
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.FIELD).unwrap(),
        parser.getField(Fields.KEY_TYPE).unwrap(),
        parser.getField(Fields.KEY_COUNT).unwrap(),
        parser.getField(Fields.STRATEGY).unwrap(),
        parser.getField(Fields.SELECTION_REASON).unwrap(),
        parser.getField(Fields.EXHAUSTIVE).unwrap(),
        parser.getField(Fields.COUNT_THRESHOLD).unwrap(),
        parser.getField(Fields.WEIGHT_SETUP).unwrap(),
        parser.getField(Fields.SEGMENTS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ID, this.id)
        .field(Fields.FIELD, this.field)
        .field(Fields.KEY_TYPE, this.keyType)
        .field(Fields.KEY_COUNT, this.keyCount)
        .field(Fields.STRATEGY, this.strategy)
        .field(Fields.SELECTION_REASON, this.selectionReason)
        .field(Fields.EXHAUSTIVE, this.exhaustive)
        .field(Fields.COUNT_THRESHOLD, this.countThreshold)
        .field(Fields.WEIGHT_SETUP, this.weightSetup)
        .field(Fields.SEGMENTS, this.segments)
        .build();
  }
}
