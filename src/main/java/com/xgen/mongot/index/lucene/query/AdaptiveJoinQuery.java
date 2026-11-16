package com.xgen.mongot.index.lucene.query;

import com.google.errorprone.annotations.Var;
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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;

/**
 * Exact binary semijoin with bounded term-statistics sampling and lazy postings construction.
 *
 * <p>Statistics choose an execution strategy, never which documents match. Correlation with sibling
 * filters, deletions, and score competitiveness are unknown. A speculative top-k probe therefore
 * switches to postings if it consumes its estimated break-even budget, without dropping matches.
 */
public final class AdaptiveJoinQuery extends Query {
  private static final int MAX_TERM_SAMPLES = 16;
  // Relative work units, deliberately conservative about random dictionary lookups during probes.
  private static final int TERM_SEEK_COST = 64;
  private static final int PROBE_COST = 64;
  private static final int ORDINAL_CACHE_SIZE = 256;

  private final String field;
  private final JoinKeySet keys;
  private final boolean exhaustive;
  private final int countThreshold;
  private final Optional<JoinFeatureExplainer.Execution> explain;

  public AdaptiveJoinQuery(String field, JoinKeySet keys, boolean exhaustive, int countThreshold) {
    this(field, keys, exhaustive, countThreshold, Optional.empty());
  }

  public AdaptiveJoinQuery(
      String field,
      JoinKeySet keys,
      boolean exhaustive,
      int countThreshold,
      Optional<JoinFeatureExplainer.Execution> explain) {
    this.field = field;
    this.keys = keys;
    this.exhaustive = exhaustive;
    this.countThreshold = countThreshold;
    this.explain = explain;
    explain.ifPresent(
        trace -> {
          if (keys.size() == 0) {
            trace.strategy("empty", "noForeignKeys");
          } else if (keys.size() <= MAX_TERM_SAMPLES) {
            trace.strategy("termInSet", "smallKeySet");
          } else {
            trace.strategy("adaptive", "sampledCost");
          }
        });
  }

  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    if (this.keys.size() == 0) {
      return new MatchNoDocsQuery("No foreign join keys");
    }
    if (this.keys.size() <= MAX_TERM_SAMPLES) {
      // Lucene's small-term disjunction already has cheap setup and optimized bulk scoring.
      // Sampling every term and wrapping its scorer costs more than it saves for these joins.
      return new TermInSetQuery(this.field, this.keys.terms());
    }
    return super.rewrite(searcher);
  }

  private record Statistics(long documents, double postings, double valuesPerDocument) {
    double hitRatio() {
      return Math.min(1d, Math.max(1d, this.postings) / this.documents);
    }
  }

  private Statistics statistics(Terms terms, int maxDoc, IndexSearcher searcher)
      throws IOException {
    @Var long documents = terms.getDocCount();
    if (documents < 0) {
      documents = maxDoc;
    }
    int samples = Math.min(MAX_TERM_SAMPLES, this.keys.size());
    var iterator = terms.iterator();
    var scratch = new BytesRef();
    @Var long sum = 0;
    for (int i = 0; i < samples; i++) {
      checkCancellation(searcher);
      int key = (int) ((long) i * this.keys.size() / samples);
      if (iterator.seekExact(this.keys.get(key, scratch))) {
        sum += iterator.docFreq();
      }
    }
    // Zero is conclusive only if every key was checked. Sampling misses are not an empty join.
    @Var double postings = (double) sum * this.keys.size() / samples;
    if (samples < this.keys.size()) {
      postings = Math.max(1d, postings);
    }
    long sumDocFreq = terms.getSumDocFreq();
    double valuesPerDocument = Math.max(1d, (double) sumDocFreq / Math.max(1, documents));
    return new Statistics(documents, postings, valuesPerDocument);
  }

  private static void checkCancellation(IndexSearcher searcher) throws IOException {
    var timeout = searcher.getTimeout();
    if (timeout != null && timeout.shouldExit()) {
      throw new InterruptedIOException("Join query time limit exceeded");
    }
  }

  private double indexWork(Statistics statistics) {
    return (double) TERM_SEEK_COST * this.keys.size() + statistics.postings();
  }

  private long probeBudget(Statistics statistics) {
    return Math.max(1L, (long) (indexWork(statistics) / probeCost(statistics)));
  }

  private static double probeCost(Statistics statistics) {
    return PROBE_COST * statistics.valuesPerDocument();
  }

  private boolean useProbe(Statistics statistics, long leadCost) {
    @Var double candidates = Math.min(leadCost, statistics.documents());
    if (!this.exhaustive) {
      // This is a planning hint, not a collection limit. Ranking, pagination, or correlation may
      // require more work, which the iterator handles by switching to the exact postings path.
      double competitiveCandidates =
          Math.max(64L, (long) this.countThreshold + 1) / statistics.hitRatio();
      candidates = Math.min(candidates, competitiveCandidates);
    }
    return candidates * probeCost(statistics) < indexWork(statistics);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new ConstantScoreWeight(this, boost) {
      @Nullable private Weight postingsWeight;

      private synchronized Weight postingsWeight() throws IOException {
        if (this.postingsWeight == null) {
          checkCancellation(searcher);
          Query postings =
              new TermInSetQuery(AdaptiveJoinQuery.this.field, AdaptiveJoinQuery.this.keys.terms());
          this.postingsWeight = searcher.createWeight(searcher.rewrite(postings), scoreMode, boost);
        }
        return this.postingsWeight;
      }

      @Override
      @Nullable
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        var segment =
            AdaptiveJoinQuery.this.explain.flatMap(
                trace -> trace.segment(context.ord, context.reader().maxDoc()));
        Statistics statistics;
        try (var ignored =
            new InvocationCountingTimer.AutocloseableOptional<>(
                segment.map(Segment::timeCostEstimation))) {
          Terms terms = context.reader().terms(AdaptiveJoinQuery.this.field);
          if (AdaptiveJoinQuery.this.keys.size() == 0 || terms == null) {
            segment.ifPresent(
                trace -> trace.estimates(Map.of("documentsWithField", 0d), Optional.empty()));
            return null;
          }
          statistics = statistics(terms, context.reader().maxDoc(), searcher);
          segment.ifPresent(
              trace ->
                  trace.estimates(
                      Map.of(
                          "documentsWithField",
                          (double) statistics.documents(),
                          "postings",
                          statistics.postings(),
                          "valuesPerDocument",
                          statistics.valuesPerDocument(),
                          "sampledKeys",
                          (double) Math.min(MAX_TERM_SAMPLES, AdaptiveJoinQuery.this.keys.size())),
                      Optional.of(probeBudget(statistics))));
        }
        if (statistics.documents() == 0 || statistics.postings() == 0) {
          return null;
        }
        return new ScorerSupplier() {
          private Scorer postingsScorer() throws IOException {
            try (var ignored =
                new InvocationCountingTimer.AutocloseableOptional<>(
                    segment.map(Segment::timeIndexSetup))) {
              var supplier = postingsWeight().scorerSupplier(context);
              return supplier == null
                  ? new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.empty())
                  : supplier.get(Long.MAX_VALUE);
            }
          }

          @Override
          public Scorer get(long leadCost) throws IOException {
            if (!useProbe(statistics, leadCost)) {
              segment.ifPresent(trace -> trace.scorer(Strategy.POSTINGS, leadCost));
              return postingsScorer();
            }
            segment.ifPresent(trace -> trace.scorer(Strategy.DOC_VALUES, leadCost));
            SortedSetDocValues values =
                DocValues.getSortedSet(context.reader(), AdaptiveJoinQuery.this.field);
            long budget = probeBudget(statistics);
            var probeIterator =
                new DocIdSetIterator() {
                  private DocIdSetIterator iterator = values;
                  private long probes;
                  private boolean usingPostings;

                  @Override
                  public int docID() {
                    return this.iterator.docID();
                  }

                  @Override
                  public int nextDoc() throws IOException {
                    return docID() == NO_MORE_DOCS ? NO_MORE_DOCS : advance(docID() + 1);
                  }

                  @Override
                  public int advance(int target) throws IOException {
                    if (!this.usingPostings && this.probes >= budget) {
                      Scorer scorer = postingsScorer();
                      this.iterator =
                          scorer.twoPhaseIterator() == null
                              ? scorer.iterator()
                              : TwoPhaseIterator.asDocIdSetIterator(scorer.twoPhaseIterator());
                      this.usingPostings = true;
                      segment.ifPresent(Segment::fallback);
                    }
                    return this.iterator.advance(target);
                  }

                  @Override
                  public long cost() {
                    return values.cost();
                  }
                };
            var confirmation =
                new TwoPhaseIterator(probeIterator) {
                  // Fixed-size, execution-local memoization. Never allocate by field cardinality.
                  private final long[] ordinals = new long[ORDINAL_CACHE_SIZE];
                  private final byte[] membership = new byte[ORDINAL_CACHE_SIZE];

                  @Override
                  public boolean matches() throws IOException {
                    if (probeIterator.usingPostings) {
                      return true;
                    }
                    segment.ifPresent(Segment::probe);
                    if ((probeIterator.probes++ & 255) == 0) {
                      checkCancellation(searcher);
                    }
                    for (int i = 0; i < values.docValueCount(); i++) {
                      if (i > 0 && (i & 1023) == 0) {
                        checkCancellation(searcher);
                      }
                      long ordinal = values.nextOrd();
                      segment.ifPresent(Segment::valueExamined);
                      int slot = (int) ordinal & (ORDINAL_CACHE_SIZE - 1);
                      if (this.membership[slot] == 0 || this.ordinals[slot] != ordinal) {
                        segment.ifPresent(Segment::dictionaryLookup);
                        this.ordinals[slot] = ordinal;
                        this.membership[slot] =
                            (byte)
                                (AdaptiveJoinQuery.this.keys.contains(values.lookupOrd(ordinal))
                                    ? 1
                                    : 2);
                      }
                      if (this.membership[slot] == 1) {
                        segment.ifPresent(Segment::probeMatch);
                        return true;
                      }
                    }
                    return false;
                  }

                  @Override
                  public float matchCost() {
                    return (float) probeCost(statistics);
                  }
                };
            return new ConstantScoreScorer(score(), scoreMode, confirmation);
          }

          @Override
          public long cost() {
            // The probe's approximation visits documents with values, not only join matches.
            // Advertising the sampled match count here would make Lucene mistake this clause for
            // a cheap lead iterator and hide the actual selectivity of sibling predicates.
            return statistics.documents();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext context) {
        // Membership is owned by the cursor's reservation, never by the cross-request query cache.
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
    return "AdaptiveJoin("
        + this.field
        + ", keys="
        + this.keys.size()
        + ", exhaustive="
        + this.exhaustive
        + ")";
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other)
        && this.field.equals(((AdaptiveJoinQuery) other).field)
        && this.keys == ((AdaptiveJoinQuery) other).keys
        && this.exhaustive == ((AdaptiveJoinQuery) other).exhaustive
        && this.countThreshold == ((AdaptiveJoinQuery) other).countThreshold;
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), this.field, this.keys, this.exhaustive, this.countThreshold);
  }
}
