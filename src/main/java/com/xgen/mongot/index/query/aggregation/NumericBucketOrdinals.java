package com.xgen.mongot.index.query.aggregation;

import com.google.common.primitives.ImmutableLongArray;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.lucene.query.pushdown.MqlComparator;
import com.xgen.mongot.index.lucene.util.LuceneDoubleConversionUtils;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.lucene.facet.MultiLongValues;
import org.apache.lucene.facet.MultiLongValuesSource;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonNumber;

/**
 * Sorted bucket ordinals for the two numeric columns backing the same dotted path. Each column
 * compares values in its own encoding, so int64 comparisons retain all 64 bits. Merging ordinals
 * preserves Lucene's sorted multi-value contract without buffering a document's values.
 */
public final class NumericBucketOrdinals extends MultiLongValuesSource {
  /** Whether encoding these boundaries directly in the selected column would lose precision. */
  public static boolean hasInexactBoundary(TypeField type, List<BsonNumber> boundaries) {
    return boundaries.stream()
        .anyMatch(
            boundary ->
                type == TypeField.NUMBER_INT64_V2
                    ? boundary.isDouble()
                        && MqlComparator.compare(new BsonInt64(boundary.longValue()), boundary) != 0
                    : boundary.isInt64()
                        && MqlComparator.compare(new BsonDouble(boundary.doubleValue()), boundary)
                            != 0);
  }

  private record Column(String name, ImmutableLongArray edges) {
    static Column create(
        FieldPath path, Optional<FieldPath> root, TypeField type, List<BsonNumber> boundaries) {
      var edges = ImmutableLongArray.builder(boundaries.size());
      for (var boundary : boundaries) {
        if (type == TypeField.NUMBER_DOUBLE_V2) {
          // Encode the first representable double at or above the BSON boundary. A cast
          // alone can round large int64 boundaries down into the preceding bucket.
          double rounded = boundary.doubleValue();
          double ceiling =
              !boundary.isDouble() && MqlComparator.compare(new BsonDouble(rounded), boundary) < 0
                  ? Math.nextUp(rounded)
                  : rounded;
          edges.add(LuceneDoubleConversionUtils.toMqlSortableLong(ceiling));
        } else if (!boundary.isDouble()) {
          edges.add(boundary.longValue());
        } else {
          double bound = boundary.doubleValue();
          // All longs are above NaN. Bounds at 2^63 or beyond are above every long,
          // so an int64 value can never cross those trailing edges.
          if (Double.isNaN(bound)) {
            edges.add(Long.MIN_VALUE);
          } else if (bound < 0x1.0p63) {
            edges.add((long) Math.ceil(bound));
          }
        }
      }
      return new Column(type.getLuceneFieldName(path, root), edges.build());
    }

    long bucket(long value) {
      // Upper bound also handles adjacent boundaries that coincide in just one representation.
      @Var int low = 0;
      @Var int high = this.edges.length();
      while (low < high) {
        int mid = (low + high) >>> 1;
        if (this.edges.get(mid) <= value) {
          low = mid + 1;
        } else {
          high = mid;
        }
      }
      // Values before the first boundary map to -1, values past the last to its index.
      return low - 1;
    }
  }

  private final Column integers;
  private final Column doubles;

  public NumericBucketOrdinals(
      FieldPath path, Optional<FieldPath> root, List<BsonNumber> boundaries) {
    this.integers = Column.create(path, root, TypeField.NUMBER_INT64_V2, boundaries);
    this.doubles = Column.create(path, root, TypeField.NUMBER_DOUBLE_V2, boundaries);
  }

  private static final class Cursor {
    private final Column column;
    private final SortedNumericDocValues values;
    private int remaining;
    private long next;

    Cursor(Column column, LeafReaderContext leaf) throws IOException {
      this.column = column;
      this.values = DocValues.getSortedNumeric(leaf.reader(), column.name());
    }

    int position(int doc) throws IOException {
      this.remaining = this.values.advanceExact(doc) ? this.values.docValueCount() : 0;
      readNext();
      return this.remaining;
    }

    long take() throws IOException {
      long result = this.next;
      this.remaining--;
      readNext();
      return result;
    }

    private void readNext() throws IOException {
      this.next =
          this.remaining == 0 ? Long.MAX_VALUE : this.column.bucket(this.values.nextValue());
    }
  }

  @Override
  public MultiLongValues getValues(LeafReaderContext leaf) throws IOException {
    var ints = new Cursor(this.integers, leaf);
    var doubles = new Cursor(this.doubles, leaf);
    return new MultiLongValues() {
      private long count;

      @Override
      public boolean advanceExact(int doc) throws IOException {
        this.count = (long) ints.position(doc) + doubles.position(doc);
        return this.count > 0;
      }

      @Override
      public long getValueCount() {
        return this.count;
      }

      @Override
      public long nextValue() throws IOException {
        return ints.next <= doubles.next ? ints.take() : doubles.take();
      }
    };
  }

  @Override
  public boolean isCacheable(LeafReaderContext leaf) {
    return DocValues.isCacheable(leaf, this.integers.name(), this.doubles.name());
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof NumericBucketOrdinals that
        && this.integers.equals(that.integers)
        && this.doubles.equals(that.doubles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.integers, this.doubles);
  }

  @Override
  public String toString() {
    return "numeric buckets(" + this.integers.name() + ", " + this.doubles.name() + ")";
  }
}
