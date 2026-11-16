package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Foreign membership preparation. Reserved bytes are conservative admission estimates, not measured
 * heap.
 */
public record JoinPreparationInformation(
    int localPartitions,
    int keyCount,
    long keyBytes,
    long reservedBytes,
    Map<String, Long> limits,
    Optional<Long> foreignWorkUnits,
    Optional<QueryExecutionArea> timing,
    Optional<QueryExecutionArea> dependencySetup,
    List<JoinSourceInformation> sources)
    implements DocumentEncodable {
  private static class Fields {
    static final Field.Required<Integer> LOCAL_PARTITIONS =
        Field.builder("localPartitions").intField().required();
    static final Field.Required<Integer> KEY_COUNT =
        Field.builder("keyCount").intField().required();
    static final Field.Required<Long> KEY_BYTES = Field.builder("keyBytes").longField().required();
    static final Field.Required<Long> RESERVED_BYTES =
        Field.builder("reservedBytes").longField().required();
    static final Field.Required<Map<String, Long>> LIMITS =
        Field.builder("limits").mapOf(Value.builder().longValue().required()).required();
    static final Field.Optional<Long> FOREIGN_WORK_UNITS =
        Field.builder("foreignWorkUnits").longField().optional().noDefault();
    static final Field.Optional<QueryExecutionArea> TIMING =
        Field.builder("timing")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
    static final Field.Optional<QueryExecutionArea> DEPENDENCY_SETUP =
        Field.builder("dependencySetup")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
    static final Field.Required<List<JoinSourceInformation>> SOURCES =
        Field.builder("sources")
            .classField(JoinSourceInformation::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  public static JoinPreparationInformation fromBson(DocumentParser parser)
      throws BsonParseException {
    return new JoinPreparationInformation(
        parser.getField(Fields.LOCAL_PARTITIONS).unwrap(),
        parser.getField(Fields.KEY_COUNT).unwrap(),
        parser.getField(Fields.KEY_BYTES).unwrap(),
        parser.getField(Fields.RESERVED_BYTES).unwrap(),
        parser.getField(Fields.LIMITS).unwrap(),
        parser.getField(Fields.FOREIGN_WORK_UNITS).unwrap(),
        parser.getField(Fields.TIMING).unwrap(),
        parser.getField(Fields.DEPENDENCY_SETUP).unwrap(),
        parser.getField(Fields.SOURCES).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.LOCAL_PARTITIONS, this.localPartitions)
        .field(Fields.KEY_COUNT, this.keyCount)
        .field(Fields.KEY_BYTES, this.keyBytes)
        .field(Fields.RESERVED_BYTES, this.reservedBytes)
        .field(Fields.LIMITS, this.limits)
        .field(Fields.FOREIGN_WORK_UNITS, this.foreignWorkUnits)
        .field(Fields.TIMING, this.timing)
        .field(Fields.DEPENDENCY_SETUP, this.dependencySetup)
        .field(Fields.SOURCES, this.sources)
        .build();
  }
}
