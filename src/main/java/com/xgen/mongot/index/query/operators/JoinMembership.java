package com.xgen.mongot.index.query.operators;

/** Exact prepared membership for one join equality domain. */
public sealed interface JoinMembership permits JoinKeySet, OrdinalJoinMembership {
  int size();
}
