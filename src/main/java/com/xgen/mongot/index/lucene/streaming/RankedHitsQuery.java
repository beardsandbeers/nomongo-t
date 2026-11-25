package com.xgen.mongot.index.lucene.streaming;

import java.util.Arrays;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.jetbrains.annotations.Nullable;

/**
 * Reader-bound finite hits in document order. Lucene's equivalent DocAndScoreQuery is private to
 * its search package.
 */
final class RankedHitsQuery extends Query {
  private final IndexReader reader;
  private final int[] docs;
  private final float[] scores;

  RankedHitsQuery(IndexReader reader, List<RankedVectorCursor.Hit> sorted) {
    this.reader = reader;
    this.docs = new int[sorted.size()];
    this.scores = new float[sorted.size()];
    for (int i = 0; i < sorted.size(); i++) {
      var hit = sorted.get(i);
      this.docs[i] = hit.doc();
      this.scores[i] = hit.score();
    }
  }

  private int lowerBound(int doc) {
    int pos = Arrays.binarySearch(this.docs, doc);
    return pos >= 0 ? pos : -pos - 1;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode mode, float boost) {
    if (searcher.getIndexReader() != this.reader) {
      throw new IllegalStateException("Reader changed");
    }
    return new Weight(this) {
      @Override
      public Explanation explain(LeafReaderContext ctx, int doc) {
        int pos = Arrays.binarySearch(RankedHitsQuery.this.docs, ctx.docBase + doc);
        return pos >= 0
            ? Explanation.match(
                boost * RankedHitsQuery.this.scores[pos], "Streaming vector candidate")
            : Explanation.noMatch("Outside streaming candidate pool");
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }

      @Override
      public @Nullable ScorerSupplier scorerSupplier(LeafReaderContext ctx) {
        int start = lowerBound(ctx.docBase);
        int end = lowerBound(ctx.docBase + ctx.reader().maxDoc());
        if (start == end) {
          return null;
        }
        var iterator = new HitIterator(start, end, ctx.docBase);
        return new DefaultScorerSupplier(
            new Scorer() {
              @Override
              public float getMaxScore(int upTo) {
                return Float.POSITIVE_INFINITY;
              }

              @Override
              public int docID() {
                return iterator.docID();
              }

              @Override
              public float score() {
                return boost * RankedHitsQuery.this.scores[iterator.pos];
              }

              @Override
              public DocIdSetIterator iterator() {
                return iterator;
              }
            });
      }
    };
  }

  private final class HitIterator extends DocIdSetIterator {
    final int start;
    final int end;
    final int base;
    int pos;

    HitIterator(int start, int end, int base) {
      this.start = start;
      this.end = end;
      this.base = base;
      this.pos = start - 1;
    }

    @Override
    public long cost() {
      return this.end - this.start;
    }

    @Override
    public int docID() {
      if (this.pos < this.start) {
        return -1;
      }
      return this.pos >= this.end ? NO_MORE_DOCS : RankedHitsQuery.this.docs[this.pos] - this.base;
    }

    @Override
    public int advance(int target) {
      this.pos = target >= NO_MORE_DOCS - this.base ? this.end : lowerBound(this.base + target);
      return docID();
    }

    @Override
    public int nextDoc() {
      this.pos++;
      return docID();
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public String toString(String field) {
    return "StreamingVectorHits(" + this.docs.length + ")";
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
