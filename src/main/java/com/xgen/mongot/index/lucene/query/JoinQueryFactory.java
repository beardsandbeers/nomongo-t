package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.operators.JoinField;
import com.xgen.mongot.index.query.operators.MaterializedJoinOperator;
import java.util.Optional;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;

/** Builds adaptive exact membership from owned native keys. */
final class JoinQueryFactory {
  static Query fromJoin(MaterializedJoinOperator operator) {
    if (operator.keys().size() == 0) {
      return new MatchNoDocsQuery("No foreign join keys");
    }
    String field =
        (operator.field().type() == JoinField.Type.TOKEN
                ? FieldName.TypeField.TOKEN
                : FieldName.TypeField.OBJECT_ID)
            .getLuceneFieldName(operator.join().localField(), Optional.empty());
    return new AdaptiveJoinQuery(
        field, operator.keys(), operator.exhaustive(), operator.countThreshold());
  }
}
