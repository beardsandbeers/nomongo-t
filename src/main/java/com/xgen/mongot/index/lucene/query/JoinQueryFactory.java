package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.explain.joins.JoinFeatureExplainer;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.custom.NumericJoinQuery;
import com.xgen.mongot.index.query.operators.JoinField;
import com.xgen.mongot.index.query.operators.JoinKeySet;
import com.xgen.mongot.index.query.operators.JoinMembership;
import com.xgen.mongot.index.query.operators.MaterializedJoinOperator;
import com.xgen.mongot.index.query.operators.OrdinalJoinMembership;
import java.util.Optional;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

/** Builds exact membership from owned native keys using the existing indexed columns. */
final class JoinQueryFactory {
  static Query fromJoin(MaterializedJoinOperator operator) {
    FieldName.TypeField typeField =
        switch (operator.field().type()) {
          case TOKEN -> FieldName.TypeField.TOKEN;
          case OBJECT_ID -> FieldName.TypeField.OBJECT_ID;
          case INT64 -> FieldName.TypeField.NUMBER_INT64_V2;
        };
    String field = typeField.getLuceneFieldName(operator.join().localField(), Optional.empty());
    var explain =
        JoinFeatureExplainer.forJoin(
            operator.membership(),
            field,
            operator.field().type().name(),
            operator.exhaustive(),
            operator.countThreshold());
    return switch (operator.membership()) {
      case JoinMembership.EmptyLocal ignored -> {
        explain.ifPresent(trace -> trace.strategy("empty", "noLocalKeys"));
        yield new MatchNoDocsQuery("No local join keys");
      }
      case JoinMembership keys when keys.size() == 0 -> {
        explain.ifPresent(trace -> trace.strategy("empty", "noForeignKeys"));
        yield new MatchNoDocsQuery("No foreign join keys");
      }
      case OrdinalJoinMembership ordinals -> {
        explain.ifPresent(trace -> trace.strategy("ordinalBitmap", ordinals.selectionReason()));
        yield new OrdinalJoinQuery(field, ordinals, explain);
      }
      case JoinKeySet keys ->
          operator.field().type() == JoinField.Type.INT64
              ? new NumericJoinQuery(field, keys, explain)
              : new AdaptiveJoinQuery(
                  field, keys, operator.exhaustive(), operator.countThreshold(), explain);
    };
  }
}
