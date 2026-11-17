package com.xgen.mongot.index.query.operators;

import java.util.Map;
import java.util.Optional;
import java.util.function.LongPredicate;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.util.LongBitSet;

/** Local ordinal membership whose bitmaps are owned by this prepared domain. */
public final class OrdinalJoinMembership implements JoinMembership {
  private final Map<CacheKey, Leaf> leaves;
  private final int size;
  private final String selectionReason;

  /** Transfers ownership of the bitmaps. They must not be mutated after construction. */
  public OrdinalJoinMembership(Map<CacheKey, Leaf> leaves, int size, String selectionReason) {
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
    var leaf = this.leaves.get(key);
    if (leaf == null) {
      throw new IllegalStateException("Ordinal join requires its pinned local reader");
    }
    return leaf.hasMatches ? Optional.of(leaf.ordinals::get) : Optional.empty();
  }

  /** Local dictionary ordinals selected during preparation. */
  public static final class Leaf {
    private final LongBitSet ordinals;
    private boolean hasMatches;

    public Leaf(long dictionarySize) {
      this.ordinals = new LongBitSet(dictionarySize);
    }

    public void add(long ordinal) {
      this.ordinals.set(ordinal);
      this.hasMatches = true;
    }

    public long bitmapBytes() {
      return this.ordinals.ramBytesUsed();
    }
  }
}
