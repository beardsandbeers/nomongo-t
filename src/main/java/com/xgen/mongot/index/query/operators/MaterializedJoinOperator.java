package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.index.query.scores.Score;
import org.bson.BsonValue;

/** Internal binary membership and execution hints. Neither is serialized into a request. */
public record MaterializedJoinOperator(
    JoinOperator join, JoinField field, JoinKeySet keys, boolean exhaustive, int countThreshold)
    implements Operator {

  @Override
  public Type getType() {
    return Type.JOIN;
  }

  @Override
  public Score score() {
    return Score.defaultScore();
  }

  @Override
  public BsonValue operatorToBson() {
    return this.join.operatorToBson();
  }
}
