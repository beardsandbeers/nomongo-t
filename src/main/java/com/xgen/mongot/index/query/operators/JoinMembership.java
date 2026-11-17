package com.xgen.mongot.index.query.operators;

/** Exact prepared membership for one join equality domain. */
public sealed interface JoinMembership
    permits JoinKeySet, OrdinalJoinMembership, JoinMembership.EmptyLocal {
  int size();

  /** The pinned local dictionaries contain no keys in this equality domain. */
  record EmptyLocal() implements JoinMembership {
    @Override
    public int size() {
      return 0;
    }
  }
}
