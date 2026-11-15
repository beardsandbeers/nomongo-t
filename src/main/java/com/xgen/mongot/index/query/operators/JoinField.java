package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;

/** The indexed equality domain of a join key. */
public record JoinField(Type type, StockNormalizerName normalizer) {
  public enum Type {
    TOKEN,
    OBJECT_ID,
    INT64
  }
}
