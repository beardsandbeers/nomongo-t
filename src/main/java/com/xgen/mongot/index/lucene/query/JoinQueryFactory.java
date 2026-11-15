package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.query.custom.NumericJoinQuery;
import com.xgen.mongot.index.query.operators.JoinField;
import com.xgen.mongot.index.query.operators.MaterializedJoinOperator;
import java.util.Optional;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

/** Builds exact membership from owned native keys using the existing indexed columns. */
final class JoinQueryFactory {
  static Query fromJoin(MaterializedJoinOperator operator) {
    if (operator.keys().size() == 0) {
      return new MatchNoDocsQuery("No foreign join keys");
    }
    FieldName.TypeField typeField =
        switch (operator.field().type()) {
          case TOKEN -> FieldName.TypeField.TOKEN;
          case OBJECT_ID -> FieldName.TypeField.OBJECT_ID;
          case INT64 -> FieldName.TypeField.NUMBER_INT64_V2;
        };
    String field = typeField.getLuceneFieldName(operator.join().localField(), Optional.empty());
    if (operator.field().type() == JoinField.Type.INT64) {
      return new NumericJoinQuery(field, operator.keys());
    }
    return new AdaptiveJoinQuery(
        field, operator.keys(), operator.exhaustive(), operator.countThreshold());
  }
}
