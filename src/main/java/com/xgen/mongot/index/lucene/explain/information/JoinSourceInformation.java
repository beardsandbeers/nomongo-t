package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** One prepared join equality domain. Work units include checks, segments, documents and values. */
public record JoinSourceInformation(
    int id,
    Optional<BsonValue> from,
    String foreignIndexId,
    String foreignGeneration,
    boolean selfJoin,
    String localField,
    String foreignField,
    String keyType,
    String normalizer,
    int foreignPartitions,
    int keyCount,
    long keyBytes,
    long reservedBytes,
    Optional<Long> foreignWorkUnits,
    Optional<QueryExecutionArea> collection)
    implements DocumentEncodable {
  private static class Fields {
    static final Field.Required<Integer> ID = Field.builder("id").intField().required();
    static final Field.Optional<BsonValue> FROM =
        Field.builder("from").unparsedValueField().optional().noDefault();
    static final Field.Required<String> FOREIGN_INDEX_ID =
        Field.builder("foreignIndexId").stringField().required();
    static final Field.Required<String> FOREIGN_GENERATION =
        Field.builder("foreignGeneration").stringField().required();
    static final Field.Required<Boolean> SELF_JOIN =
        Field.builder("selfJoin").booleanField().required();
    static final Field.Required<String> LOCAL_FIELD =
        Field.builder("localField").stringField().required();
    static final Field.Required<String> FOREIGN_FIELD =
        Field.builder("foreignField").stringField().required();
    static final Field.Required<String> KEY_TYPE =
        Field.builder("keyType").stringField().required();
    static final Field.Required<String> NORMALIZER =
        Field.builder("normalizer").stringField().required();
    static final Field.Required<Integer> FOREIGN_PARTITIONS =
        Field.builder("foreignPartitions").intField().required();
    static final Field.Required<Integer> KEY_COUNT =
        Field.builder("keyCount").intField().required();
    static final Field.Required<Long> KEY_BYTES = Field.builder("keyBytes").longField().required();
    static final Field.Required<Long> RESERVED_BYTES =
        Field.builder("reservedBytes").longField().required();
    static final Field.Optional<Long> FOREIGN_WORK_UNITS =
        Field.builder("foreignWorkUnits").longField().optional().noDefault();
    static final Field.Optional<QueryExecutionArea> COLLECTION =
        Field.builder("collection")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  public static JoinSourceInformation fromBson(DocumentParser parser) throws BsonParseException {
    return new JoinSourceInformation(
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.FROM).unwrap(),
        parser.getField(Fields.FOREIGN_INDEX_ID).unwrap(),
        parser.getField(Fields.FOREIGN_GENERATION).unwrap(),
        parser.getField(Fields.SELF_JOIN).unwrap(),
        parser.getField(Fields.LOCAL_FIELD).unwrap(),
        parser.getField(Fields.FOREIGN_FIELD).unwrap(),
        parser.getField(Fields.KEY_TYPE).unwrap(),
        parser.getField(Fields.NORMALIZER).unwrap(),
        parser.getField(Fields.FOREIGN_PARTITIONS).unwrap(),
        parser.getField(Fields.KEY_COUNT).unwrap(),
        parser.getField(Fields.KEY_BYTES).unwrap(),
        parser.getField(Fields.RESERVED_BYTES).unwrap(),
        parser.getField(Fields.FOREIGN_WORK_UNITS).unwrap(),
        parser.getField(Fields.COLLECTION).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ID, this.id)
        .field(Fields.FROM, this.from)
        .field(Fields.FOREIGN_INDEX_ID, this.foreignIndexId)
        .field(Fields.FOREIGN_GENERATION, this.foreignGeneration)
        .field(Fields.SELF_JOIN, this.selfJoin)
        .field(Fields.LOCAL_FIELD, this.localField)
        .field(Fields.FOREIGN_FIELD, this.foreignField)
        .field(Fields.KEY_TYPE, this.keyType)
        .field(Fields.NORMALIZER, this.normalizer)
        .field(Fields.FOREIGN_PARTITIONS, this.foreignPartitions)
        .field(Fields.KEY_COUNT, this.keyCount)
        .field(Fields.KEY_BYTES, this.keyBytes)
        .field(Fields.RESERVED_BYTES, this.reservedBytes)
        .field(Fields.FOREIGN_WORK_UNITS, this.foreignWorkUnits)
        .field(Fields.COLLECTION, this.collection)
        .build();
  }
}
