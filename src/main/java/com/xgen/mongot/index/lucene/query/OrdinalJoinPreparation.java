package com.xgen.mongot.index.lucene.query;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.SearchIndexReader.QuerySnapshot.OrdinalJoinPartition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.JoinField;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.OrdinalJoinMembership;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.IntConsumer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;

/** Estimates and builds ordinal join membership on pinned partition readers. */
public final class OrdinalJoinPreparation {
  private OrdinalJoinPreparation() {}

  public record DictionaryStats(
      long entries, long largestDictionary, int leaves, long bitmapBytes, long dictionaryBytes) {}

  public record Preparation(OrdinalJoinMembership membership, long peakBitmapBytes) {}

  /** Budget for one decoded term block plus the term buffers in the current DocValues codec. */
  public static long dictionaryBytes(JoinField.Type type, long entries) {
    int maxKeyBytes = type == JoinField.Type.OBJECT_ID ? 12 : IndexWriter.MAX_TERM_LENGTH;
    return 8192 + (Math.min(entries, 64) + 3) * (maxKeyBytes + 5);
  }

  public static String field(JoinField.Type type, FieldPath path) {
    var typeField =
        switch (type) {
          case TOKEN -> FieldName.TypeField.TOKEN;
          case OBJECT_ID -> FieldName.TypeField.OBJECT_ID;
          case INT64 ->
              throw new IllegalArgumentException("Numeric joins have no ordinal dictionary");
        };
    return typeField.getLuceneFieldName(path, Optional.empty());
  }

  /** Reads dictionary cardinality without enumerating terms or evaluating the foreign predicate. */
  public static Optional<DictionaryStats> inspect(
      List<OrdinalJoinPartition> partitions,
      String field,
      JoinField.Type type,
      boolean local,
      Runnable check)
      throws IOException {
    @Var long entries = 0;
    @Var long largestDictionary = 0;
    @Var int leaves = 0;
    @Var long bitmapBytes = 0;
    @Var long dictionaryBytes = 0;
    for (var partition : partitions) {
      for (var leaf : partition.reader().leaves()) {
        check.run();
        long count = DocValues.getSortedSet(leaf.reader(), field).getValueCount();
        if (count > Integer.MAX_VALUE || (local && leaf.reader().getReaderCacheHelper() == null)) {
          return Optional.empty();
        }
        leaves++;
        entries += count;
        largestDictionary = Math.max(largestDictionary, count);
        bitmapBytes += 64 + 8 * ((count + 63) >>> 6);
        dictionaryBytes += dictionaryBytes(type, count);
      }
    }
    return Optional.of(
        new DictionaryStats(entries, largestDictionary, leaves, bitmapBytes, dictionaryBytes));
  }

  /**
   * Translates foreign ordinals into local membership without retaining foreign readers or keys.
   */
  public static Preparation prepare(
      List<OrdinalJoinPartition> foreign,
      Operator predicate,
      String foreignField,
      List<OrdinalJoinPartition> local,
      String localField,
      String selectionReason,
      Runnable check,
      IntConsumer checkKey,
      int sequentialSteps)
      throws IOException, InvalidQueryException {
    var selected = new ArrayList<ForeignCursor>();
    for (var partition : foreign) {
      partition
          .search()
          .collect(
              predicate,
              new SimpleCollector() {
                private SortedSetDocValues values = DocValues.emptySortedSet();
                private LongBitSet ordinals = new LongBitSet(0);

                @Override
                protected void doSetNextReader(LeafReaderContext context) throws IOException {
                  check.run();
                  this.values = DocValues.getSortedSet(context.reader(), foreignField);
                  this.ordinals = new LongBitSet(this.values.getValueCount());
                  selected.add(new ForeignCursor(this.values, this.ordinals));
                }

                @Override
                public void collect(int doc) throws IOException {
                  check.run();
                  if (this.values.advanceExact(doc)) {
                    for (int i = 0; i < this.values.docValueCount(); i++) {
                      check.run();
                      this.ordinals.set(this.values.nextOrd());
                    }
                  }
                }

                @Override
                public ScoreMode scoreMode() {
                  return ScoreMode.COMPLETE_NO_SCORES;
                }
              },
              check);
    }

    long foreignBytes = selected.stream().mapToLong(cursor -> cursor.ordinals.ramBytesUsed()).sum();
    var foreignQueue =
        new PriorityQueue<ForeignCursor>(Comparator.comparing(cursor -> cursor.term));
    for (var cursor : selected) {
      check.run();
      if (cursor.next()) {
        foreignQueue.add(cursor);
      }
    }

    Map<CacheKey, LongBitSet> membership = new IdentityHashMap<>();
    var localQueue = new PriorityQueue<LocalCursor>(Comparator.comparing(cursor -> cursor.term));
    @Var long localBytes = 0;
    for (var partition : local) {
      for (var context : partition.reader().leaves()) {
        check.run();
        var helper = context.reader().getReaderCacheHelper();
        if (helper == null) {
          throw new IllegalStateException("Ordinal join requires a local reader cache identity");
        }
        var values = DocValues.getSortedSet(context.reader(), localField);
        var ordinals = new LongBitSet(values.getValueCount());
        if (membership.putIfAbsent(helper.getKey(), ordinals) != null) {
          continue;
        }
        localBytes += ordinals.ramBytesUsed();
        var cursor = new LocalCursor(values.termsEnum(), ordinals, sequentialSteps);
        if (cursor.next()) {
          localQueue.add(cursor);
        }
      }
    }

    @Var int distinct = 0;
    while (!foreignQueue.isEmpty()) {
      check.run();
      var current = foreignQueue.remove();
      // Keep this cursor positioned until all equal terms have been processed. lookupOrd may
      // reuse its BytesRef, so advancing it sooner would mutate the comparison key.
      BytesRef key = current.term;
      checkKey.accept(key.length);
      distinct++;
      while (!localQueue.isEmpty() && localQueue.peek().term.compareTo(key) <= 0) {
        var target = localQueue.remove();
        if (target.term.equals(key)) {
          target.ordinals.set(target.terms.ord());
          if (target.next()) {
            localQueue.add(target);
          }
        } else if (target.seek(key, check)) {
          localQueue.add(target);
        }
        check.run();
      }
      while (!foreignQueue.isEmpty() && foreignQueue.peek().term.equals(key)) {
        var duplicate = foreignQueue.remove();
        if (duplicate.next()) {
          foreignQueue.add(duplicate);
        }
        check.run();
      }
      if (current.next()) {
        foreignQueue.add(current);
      }
    }
    return new Preparation(
        new OrdinalJoinMembership(membership, distinct, selectionReason),
        foreignBytes + localBytes);
  }

  private static final class ForeignCursor {
    private final SortedSetDocValues values;
    private final LongBitSet ordinals;
    private long ordinal = -1;
    private BytesRef term = new BytesRef();

    private ForeignCursor(SortedSetDocValues values, LongBitSet ordinals) {
      this.values = values;
      this.ordinals = ordinals;
    }

    private boolean next() throws IOException {
      long next = this.ordinal + 1;
      this.ordinal = next < this.ordinals.length() ? this.ordinals.nextSetBit(next) : -1;
      if (this.ordinal < 0) {
        return false;
      }
      this.term = this.values.lookupOrd(this.ordinal);
      return true;
    }
  }

  private static final class LocalCursor {
    private final TermsEnum terms;
    private final LongBitSet ordinals;
    private final int sequentialSteps;
    private BytesRef term = new BytesRef();

    private LocalCursor(TermsEnum terms, LongBitSet ordinals, int sequentialSteps) {
      this.terms = terms;
      this.ordinals = ordinals;
      this.sequentialSteps = sequentialSteps;
    }

    private boolean next() throws IOException {
      var next = this.terms.next();
      if (next == null) {
        return false;
      }
      this.term = next;
      return true;
    }

    private boolean seek(BytesRef key, Runnable check) throws IOException {
      for (int i = 0; i < this.sequentialSteps; i++) {
        check.run();
        if (!next()) {
          return false;
        }
        if (this.term.compareTo(key) >= 0) {
          return true;
        }
      }
      if (this.terms.seekCeil(key) == TermsEnum.SeekStatus.END) {
        return false;
      }
      this.term = this.terms.term();
      return true;
    }
  }
}
