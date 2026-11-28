package com.xgen.mongot.index.query.aggregation;

import com.xgen.mongot.index.definition.NumericFieldOptions;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DoubleValues;

/** Lucene doc-values field and representation for a resolved numeric aggregation metric path. */
public record ResolvedNumericMetricField(
    String luceneFieldName,
    NumericFieldOptions.Representation representation,
    Optional<String> alternateFieldName) {
  public ResolvedNumericMetricField(
      String luceneFieldName, NumericFieldOptions.Representation representation) {
    this(luceneFieldName, representation, Optional.empty());
  }

  /**
   * Sum and average consume a document only when exactly one numeric value exists across its
   * columns.
   */
  DoubleValues singleValues(LeafReader reader) throws IOException {
    var primary = DocValues.getSortedNumeric(reader, this.luceneFieldName);
    var other =
        this.alternateFieldName.isPresent()
            ? DocValues.getSortedNumeric(reader, this.alternateFieldName.get())
            : null;
    var otherRepresentation =
        this.representation == NumericFieldOptions.Representation.DOUBLE
            ? NumericFieldOptions.Representation.INT64
            : NumericFieldOptions.Representation.DOUBLE;
    return new DoubleValues() {
      private double value;

      @Override
      public boolean advanceExact(int doc) throws IOException {
        boolean hasPrimary = primary.advanceExact(doc);
        boolean hasOther = other != null && other.advanceExact(doc);
        if (hasPrimary == hasOther) {
          return false;
        }
        var values = hasPrimary ? primary : Objects.requireNonNull(other);
        if (values.docValueCount() != 1) {
          return false;
        }
        this.value =
            NumericMetricValues.decode(
                values.nextValue(),
                hasPrimary ? ResolvedNumericMetricField.this.representation : otherRepresentation);
        return true;
      }

      @Override
      public double doubleValue() {
        return this.value;
      }
    };
  }
}
