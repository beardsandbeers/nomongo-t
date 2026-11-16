package com.xgen.mongot.index.lucene.query.custom;

import com.xgen.mongot.index.lucene.explain.joins.JoinFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.joins.JoinFeatureExplainer.Segment;
import com.xgen.mongot.index.lucene.explain.joins.JoinFeatureExplainer.Strategy;
import com.xgen.mongot.index.query.operators.JoinKeySet;
import com.xgen.mongot.util.timers.InvocationCountingTimer;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.NumericUtils;
import org.jetbrains.annotations.Nullable;

/** Exact membership over existing numeric columns with cancellation inside local scans. */
public final class NumericJoinQuery extends Query {
  private final String field;
  private final JoinKeySet keys;
  private final Optional<JoinFeatureExplainer.Execution> explain;

  public NumericJoinQuery(String field, JoinKeySet keys) {
    this(field, keys, Optional.empty());
  }

  public NumericJoinQuery(
      String field, JoinKeySet keys, Optional<JoinFeatureExplainer.Execution> explain) {
    this.field = field;
    this.keys = keys;
    this.explain = explain;
    explain.ifPresent(
        trace ->
            trace.strategy(
                keys.size() == 0 ? "empty" : "numeric",
                keys.size() == 0 ? "noForeignKeys" : "pointCostVsLeadCost"));
  }

  public String getField() {
    return this.field;
  }

  public int getKeyCount() {
    return this.keys.size();
  }

  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    return this.keys.size() == 0
        ? new MatchNoDocsQuery("No foreign join keys")
        : super.rewrite(searcher);
  }

  private static void checkCancellation(IndexSearcher searcher) throws IOException {
    if (Thread.currentThread().isInterrupted()
        || (searcher.getTimeout() != null && searcher.getTimeout().shouldExit())) {
      throw new InterruptedIOException("Join query cancelled or time limit exceeded");
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    Weight pointWeight;
    try (var ignored =
        new InvocationCountingTimer.AutocloseableOptional<>(
            this.explain.map(JoinFeatureExplainer.Execution::timeWeightSetup))) {
      checkCancellation(searcher);
      long[] numbers = new long[this.keys.size()];
      var scratch = new BytesRef();
      for (int i = 0; i < numbers.length; i++) {
        if ((i & 1023) == 0) {
          checkCancellation(searcher);
        }
        var key = this.keys.get(i, scratch);
        numbers[i] = NumericUtils.sortableBytesToLong(key.bytes, key.offset);
      }
      Query points = LongPoint.newSetQuery(this.field, numbers);
      checkCancellation(searcher);
      pointWeight = searcher.createWeight(searcher.rewrite(points), scoreMode, boost);
    }
    return new ConstantScoreWeight(this, boost) {
      @Override
      @Nullable
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        checkCancellation(searcher);
        var segment =
            NumericJoinQuery.this.explain.flatMap(
                trace -> trace.segment(context.ord, context.reader().maxDoc()));
        ScorerSupplier pointSupplier;
        try (var ignored =
            new InvocationCountingTimer.AutocloseableOptional<>(
                segment.map(Segment::timeCostEstimation))) {
          // Borrow a cancellation-aware view; the pinned snapshot retains ownership of the reader.
          // Lucene's point traversal happens while constructing the scorer, before its bulk-scoring
          // timeout wrapper is installed, so the reader must check cancellation during traversal.
          var checkedReader =
              new ExitableDirectoryReader.ExitableFilterAtomicReader(
                  context.reader(),
                  () ->
                      Thread.currentThread().isInterrupted()
                          || (searcher.getTimeout() != null
                              && searcher.getTimeout().shouldExit())) {
                @Override
                @Nullable
                public PointValues getPointValues(String field) throws IOException {
                  var values = super.getPointValues(field);
                  return values == null ? null : checkBulkPointVisits(values, searcher);
                }
              };
          pointSupplier = pointWeight.scorerSupplier(checkedReader.getContext());
          segment.ifPresent(
              trace ->
                  trace.estimates(
                      Map.of(
                          "pointCost", pointSupplier == null ? 0d : (double) pointSupplier.cost()),
                      Optional.empty()));
        }
        if (pointSupplier == null) {
          return null;
        }
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            checkCancellation(searcher);
            // Follow Lucene's numeric point/DocValues cost choice. The DocValues branch uses
            // owned byte-hash membership rather than a collision-prone Long.hashCode table.
            if ((pointSupplier.cost() >>> 3) <= leadCost) {
              segment.ifPresent(trace -> trace.scorer(Strategy.POINTS, leadCost));
              try (var ignored =
                  new InvocationCountingTimer.AutocloseableOptional<>(
                      segment.map(Segment::timeIndexSetup))) {
                return pointSupplier.get(leadCost);
              }
            }
            segment.ifPresent(trace -> trace.scorer(Strategy.DOC_VALUES, leadCost));
            var values = DocValues.getSortedNumeric(context.reader(), NumericJoinQuery.this.field);
            var confirmation =
                new TwoPhaseIterator(values) {
                  private final BytesRef key = new BytesRef(new byte[Long.BYTES]);

                  @Override
                  public boolean matches() throws IOException {
                    segment.ifPresent(Segment::probe);
                    for (int i = 0; i < values.docValueCount(); i++) {
                      if ((i & 1023) == 0) {
                        checkCancellation(searcher);
                      }
                      NumericUtils.longToSortableBytes(values.nextValue(), this.key.bytes, 0);
                      segment.ifPresent(Segment::valueExamined);
                      if (NumericJoinQuery.this.keys.contains(this.key)) {
                        segment.ifPresent(Segment::probeMatch);
                        return true;
                      }
                    }
                    return false;
                  }

                  @Override
                  public float matchCost() {
                    return 64;
                  }
                };
            return new ConstantScoreScorer(score(), scoreMode, confirmation);
          }

          @Override
          public long cost() {
            return pointSupplier.cost();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext context) {
        return false;
      }
    };
  }

  /** Lucene's exitable reader checks before visitDocIDs, but not within its accepted subtree. */
  private static PointValues checkBulkPointVisits(PointValues values, IndexSearcher searcher) {
    return new PointValues() {
      @Override
      public PointTree getPointTree() throws IOException {
        return new CancellablePointTree(values.getPointTree(), searcher);
      }

      @Override
      public byte[] getMinPackedValue() throws IOException {
        return values.getMinPackedValue();
      }

      @Override
      public byte[] getMaxPackedValue() throws IOException {
        return values.getMaxPackedValue();
      }

      @Override
      public int getNumDimensions() throws IOException {
        return values.getNumDimensions();
      }

      @Override
      public int getNumIndexDimensions() throws IOException {
        return values.getNumIndexDimensions();
      }

      @Override
      public int getBytesPerDimension() throws IOException {
        return values.getBytesPerDimension();
      }

      @Override
      public long size() {
        return values.size();
      }

      @Override
      public int getDocCount() {
        return values.getDocCount();
      }
    };
  }

  private record CancellablePointTree(PointValues.PointTree delegate, IndexSearcher searcher)
      implements PointValues.PointTree {
    @Override
    public PointValues.PointTree clone() {
      return new CancellablePointTree(this.delegate.clone(), this.searcher);
    }

    @Override
    public boolean moveToChild() throws IOException {
      return this.delegate.moveToChild();
    }

    @Override
    public boolean moveToSibling() throws IOException {
      return this.delegate.moveToSibling();
    }

    @Override
    public boolean moveToParent() throws IOException {
      return this.delegate.moveToParent();
    }

    @Override
    public byte[] getMinPackedValue() {
      return this.delegate.getMinPackedValue();
    }

    @Override
    public byte[] getMaxPackedValue() {
      return this.delegate.getMaxPackedValue();
    }

    @Override
    public long size() {
      return this.delegate.size();
    }

    @Override
    public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
      this.delegate.visitDocValues(visitor);
    }

    @Override
    public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
      this.delegate.visitDocIDs(
          new PointValues.IntersectVisitor() {
            private int visits;

            private void check() throws IOException {
              if ((this.visits++ & 1023) == 0) {
                checkCancellation(CancellablePointTree.this.searcher);
              }
            }

            @Override
            public void visit(int docID) throws IOException {
              check();
              visitor.visit(docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
              check();
              visitor.visit(docID, packedValue);
            }

            @Override
            public void visit(DocIdSetIterator iterator) throws IOException {
              visitor.visit(
                  new FilterDocIdSetIterator(iterator) {
                    @Override
                    public int nextDoc() throws IOException {
                      check();
                      return this.in.nextDoc();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                      check();
                      return this.in.advance(target);
                    }
                  });
            }

            @Override
            public void visit(IntsRef docs) throws IOException {
              // Retain Lucene's bulk API, but bound the work between cancellation checks.
              int end = docs.offset + docs.length;
              for (int offset = docs.offset; offset < end; offset += Math.min(1024, end - offset)) {
                checkCancellation(CancellablePointTree.this.searcher);
                visitor.visit(new IntsRef(docs.ints, offset, Math.min(1024, end - offset)));
              }
            }

            @Override
            public PointValues.Relation compare(byte[] min, byte[] max) {
              return visitor.compare(min, max);
            }

            @Override
            public void grow(int count) {
              visitor.grow(count);
            }
          });
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public String toString(String defaultField) {
    return "NumericJoin(" + this.field + ", keys=" + this.keys.size() + ")";
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other)
        && this.field.equals(((NumericJoinQuery) other).field)
        && this.keys == ((NumericJoinQuery) other).keys;
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), this.field, this.keys);
  }
}
