package com.xgen.mongot.index.query.operators;

import java.util.Map;
import java.util.Optional;
import java.util.function.LongPredicate;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.util.LongBitSet;

/** Local ordinal membership whose bitmaps are owned by this prepared domain. */
public final class OrdinalJoinMembership implements JoinMembership {
  private final Map<CacheKey, LongBitSet> leaves;
  private final int size;
  private final String selectionReason;

  /** Transfers ownership of the bitmaps. They must not be mutated after construction. */
  public OrdinalJoinMembership(Map<CacheKey, LongBitSet> leaves, int size, String selectionReason) {
    this.leaves = Map.copyOf(leaves);
    this.size = size;
    this.selectionReason = selectionReason;
  }

  @Override
  public int size() {
    return this.size;
  }

  public String selectionReason() {
    return this.selectionReason;
  }

  public Optional<LongPredicate> matcher(CacheKey key) {
    var ordinals = this.leaves.get(key);
    if (ordinals == null) {
      throw new IllegalStateException("Ordinal join requires its pinned local reader");
    }
    return ordinals.scanIsEmpty() ? Optional.empty() : Optional.of(ordinals::get);
  }
}
