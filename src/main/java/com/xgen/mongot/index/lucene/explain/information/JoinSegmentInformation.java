package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Cumulative work on one pinned local segment. Summed phase timings can overlap across concurrent
 * segments.
 */
public record JoinSegmentInformation(
    int ordinal,
    int maxDoc,
    Map<String, Double> estimates,
    Optional<Long> probeBudget,
    Optional<Long> minLeadCost,
    Optional<Long> maxLeadCost,
    Map<String, Long> strategyCounts,
    Map<String, Long> counters,
    Optional<String> fallbackReason,
    QueryExecutionArea costEstimation,
    QueryExecutionArea indexSetup)
    implements DocumentEncodable {
  private static class Fields {
    static final Field.Required<Integer> ORDINAL = Field.builder("ordinal").intField().required();
    static final Field.Required<Integer> MAX_DOC = Field.builder("maxDoc").intField().required();
    static final Field.Required<Map<String, Double>> ESTIMATES =
        Field.builder("estimates").mapOf(Value.builder().doubleValue().required()).required();
    static final Field.Optional<Long> PROBE_BUDGET =
        Field.builder("probeBudget").longField().optional().noDefault();
    static final Field.Optional<Long> MIN_LEAD_COST =
        Field.builder("minLeadCost").longField().optional().noDefault();
    static final Field.Optional<Long> MAX_LEAD_COST =
        Field.builder("maxLeadCost").longField().optional().noDefault();
    static final Field.Required<Map<String, Long>> STRATEGY_COUNTS =
        Field.builder("strategyCounts").mapOf(Value.builder().longValue().required()).required();
    static final Field.Required<Map<String, Long>> COUNTERS =
        Field.builder("counters").mapOf(Value.builder().longValue().required()).required();
    static final Field.Optional<String> FALLBACK_REASON =
        Field.builder("fallbackReason").stringField().optional().noDefault();
    static final Field.Required<QueryExecutionArea> COST_ESTIMATION =
        Field.builder("costEstimation")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .required();
    static final Field.Required<QueryExecutionArea> INDEX_SETUP =
        Field.builder("indexSetup")
            .classField(QueryExecutionArea::fromBson)
            .disallowUnknownFields()
            .required();
  }

  public static JoinSegmentInformation fromBson(DocumentParser parser) throws BsonParseException {
    return new JoinSegmentInformation(
        parser.getField(Fields.ORDINAL).unwrap(),
        parser.getField(Fields.MAX_DOC).unwrap(),
        parser.getField(Fields.ESTIMATES).unwrap(),
        parser.getField(Fields.PROBE_BUDGET).unwrap(),
        parser.getField(Fields.MIN_LEAD_COST).unwrap(),
        parser.getField(Fields.MAX_LEAD_COST).unwrap(),
        parser.getField(Fields.STRATEGY_COUNTS).unwrap(),
        parser.getField(Fields.COUNTERS).unwrap(),
        parser.getField(Fields.FALLBACK_REASON).unwrap(),
        parser.getField(Fields.COST_ESTIMATION).unwrap(),
        parser.getField(Fields.INDEX_SETUP).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ORDINAL, this.ordinal)
        .field(Fields.MAX_DOC, this.maxDoc)
        .field(Fields.ESTIMATES, this.estimates)
        .field(Fields.PROBE_BUDGET, this.probeBudget)
        .field(Fields.MIN_LEAD_COST, this.minLeadCost)
        .field(Fields.MAX_LEAD_COST, this.maxLeadCost)
        .field(Fields.STRATEGY_COUNTS, this.strategyCounts)
        .field(Fields.COUNTERS, this.counters)
        .field(Fields.FALLBACK_REASON, this.fallbackReason)
        .field(Fields.COST_ESTIMATION, this.costEstimation)
        .field(Fields.INDEX_SETUP, this.indexSetup)
        .build();
  }
}
