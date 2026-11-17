package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.explain.joins.JoinFeatureExplainer.Execution;
import com.xgen.mongot.index.lucene.explain.joins.JoinFeatureExplainer.Segment;
import com.xgen.mongot.index.lucene.explain.joins.JoinFeatureExplainer.Strategy;
import com.xgen.mongot.index.query.operators.OrdinalJoinMembership;
import java.io.IOException;
import java.util.Optional;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.jetbrains.annotations.Nullable;

/** Exact constant-score semijoin against the prepared local ordinal bitmaps. */
public final class OrdinalJoinQuery extends Query {
  private final String field;
  private final OrdinalJoinMembership membership;
  private final Optional<Execution> explain;

  public OrdinalJoinQuery(
      String field, OrdinalJoinMembership membership, Optional<Execution> explain) {
    this.field = field;
    this.membership = membership;
    this.explain = explain;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new ConstantScoreWeight(this, boost) {
      @Override
      @Nullable
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        var helper = context.reader().getReaderCacheHelper();
        if (helper == null) {
          throw new IllegalStateException("Ordinal join requires its pinned local reader");
        }
        var matcher = OrdinalJoinQuery.this.membership.matcher(helper.getKey());
        if (matcher.isEmpty()) {
          return null;
        }
        var matchesOrdinal = matcher.get();
        var values = DocValues.getSortedSet(context.reader(), OrdinalJoinQuery.this.field);
        var segment =
            OrdinalJoinQuery.this.explain.flatMap(
                trace -> trace.segment(context.ord, context.reader().maxDoc()));
        var confirmation =
            new TwoPhaseIterator(values) {
              @Override
              public boolean matches() throws IOException {
                segment.ifPresent(Segment::probe);
                for (int i = 0; i < values.docValueCount(); i++) {
                  if ((i & 1023) == 0
                      && searcher.getTimeout() != null
                      && searcher.getTimeout().shouldExit()) {
                    throw new java.io.InterruptedIOException("Ordinal join cancelled");
                  }
                  segment.ifPresent(Segment::valueExamined);
                  if (matchesOrdinal.test(values.nextOrd())) {
                    segment.ifPresent(Segment::probeMatch);
                    return true;
                  }
                }
                return false;
              }

              @Override
              public float matchCost() {
                return 4;
              }
            };
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) {
            segment.ifPresent(trace -> trace.scorer(Strategy.DOC_VALUES, leadCost));
            return new ConstantScoreScorer(score(), scoreMode, confirmation);
          }

          @Override
          public long cost() {
            return values.cost();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext context) {
        return false;
      }
    };
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public String toString(String defaultField) {
    return "OrdinalJoin(" + this.field + ")";
  }

  @Override
  public boolean equals(Object other) {
    return this == other;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }
}
