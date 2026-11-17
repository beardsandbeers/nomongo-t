package com.xgen.mongot.index.query.operators;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/** Owned binary membership, frozen before publication to concurrent local searchers. */
public final class JoinKeySet implements JoinMembership {

  private final BytesRefHash values;

  private JoinKeySet(BytesRefHash values) {
    this.values = values;
  }

  @Override
  public int size() {
    return this.values.size();
  }

  public boolean contains(BytesRef key) {
    return this.values.find(key) >= 0;
  }

  /** Returns a borrowed, read-only view. Callers must not modify the referenced bytes. */
  public BytesRef get(int index, BytesRef scratch) {
    return this.values.get(index, scratch);
  }

  /** Views used only when building the postings query, whose constructor copies the bytes. */
  public List<BytesRef> terms() {
    List<BytesRef> terms = new ArrayList<>(size());
    for (int i = 0; i < size(); i++) {
      terms.add(this.values.get(i, new BytesRef()));
    }
    return terms;
  }

  public static final class Builder {
    private final BytesRefHash values = new BytesRefHash();
    private final IntConsumer reserveKey;
    private boolean built;

    public Builder(IntConsumer reserveKey) {
      this.reserveKey = reserveKey;
    }

    public void add(BytesRef value) {
      Preconditions.checkState(!this.built, "Join membership is already frozen");
      if (this.values.find(value) < 0) {
        // Reserve before copying borrowed DocValues bytes or growing the hash table.
        this.reserveKey.accept(value.length);
        this.values.add(value);
      }
    }

    public JoinKeySet build() {
      Preconditions.checkState(!this.built, "Join membership is already frozen");
      this.built = true;
      return new JoinKeySet(this.values);
    }
  }
}
