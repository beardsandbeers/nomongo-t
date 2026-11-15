package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;

/** The indexed equality domain of a join key. Numeric joins are deliberately unsupported. */
public record JoinField(Type type, StockNormalizerName normalizer) {
  public enum Type {
    TOKEN,
    OBJECT_ID
  }
}
