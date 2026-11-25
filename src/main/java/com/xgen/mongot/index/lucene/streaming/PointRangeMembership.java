package com.xgen.mongot.index.lucene.streaming;

import com.xgen.mongot.index.query.DeadlineExceededException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.PointRangeQuery;
import org.jetbrains.annotations.Nullable;

/**
 * Legacy numeric fields can have only a BKD index. Intersect matching points with one batch of IDs,
 * retaining only batch membership. A broad range can be expensive to revisit on each refill.
 */
final class PointRangeMembership {
  static CandidateFilter compile(
      LeafReader leaf, PointRangeQuery range, @Nullable QueryTimeout timeout) throws IOException {
    var points = leaf.getPointValues(range.getField());
    if (points == null) {
      return CandidateFilter.NONE;
    }
    if (range.getNumDims() != 1
        || points.getNumIndexDimensions() != 1
        || range.getBytesPerDim() != points.getBytesPerDimension()) {
      throw new IllegalArgumentException(
          "Unsupported streaming point range on " + range.getField());
    }
    byte[] lower = range.getLowerPoint();
    byte[] upper = range.getUpperPoint();
    Relation bounds = compare(lower, upper, points.getMinPackedValue(), points.getMaxPackedValue());
    if (bounds == Relation.CELL_OUTSIDE_QUERY) {
      return CandidateFilter.NONE;
    }
    if (bounds == Relation.CELL_INSIDE_QUERY && points.getDocCount() == leaf.maxDoc()) {
      return CandidateFilter.ALL;
    }
    return (docs, matches) -> {
      Arrays.fill(matches, false);
      if (docs.length == 0) {
        return;
      }
      var visitor =
          new PointValues.IntersectVisitor() {
            int remaining = docs.length;

            @Override
            public void visit(int doc) {
              if (doc < docs[0] || doc > docs[docs.length - 1]) {
                return;
              }
              int pos = Arrays.binarySearch(docs, doc);
              if (pos >= 0 && !matches[pos]) {
                matches[pos] = true;
                if (--this.remaining == 0) {
                  throw new CollectionTerminatedException();
                }
              }
            }

            @Override
            public void visit(int doc, byte[] value) {
              if (Arrays.compareUnsigned(value, lower) >= 0
                  && Arrays.compareUnsigned(value, upper) <= 0) {
                visit(doc);
              }
            }

            @Override
            public Relation compare(byte[] min, byte[] max) {
              if (timeout != null && timeout.shouldExit()) {
                throw new DeadlineExceededException();
              }
              return PointRangeMembership.compare(lower, upper, min, max);
            }
          };
      try {
        points.intersect(visitor);
      } catch (CollectionTerminatedException allMatched) {
        // Remaining cells cannot change any result in this batch.
      }
    };
  }

  private static Relation compare(byte[] lower, byte[] upper, byte[] min, byte[] max) {
    if (Arrays.compareUnsigned(min, upper) > 0 || Arrays.compareUnsigned(max, lower) < 0) {
      return Relation.CELL_OUTSIDE_QUERY;
    }
    if (Arrays.compareUnsigned(min, lower) >= 0 && Arrays.compareUnsigned(max, upper) <= 0) {
      return Relation.CELL_INSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  private PointRangeMembership() {}
}
