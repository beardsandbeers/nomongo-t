package com.xgen.mongot.index.lucene;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.Nullable;

/** Tests whether a typed field actually occurs in any surviving pre-observation document. */
final class LegacyMappingFields {
  private LegacyMappingFields() {}

  static boolean contains(LeafReader leaf, FieldInfo field, FixedBitSet legacyDocs)
      throws IOException {
    String name = field.name;
    if (field.getVectorDimension() > 0) {
      var values =
          field.getVectorEncoding() == VectorEncoding.BYTE
              ? leaf.getByteVectorValues(name)
              : leaf.getFloatVectorValues(name);
      return values != null && intersects(values.iterator(), legacyDocs);
    }
    if (intersects(FieldExistsQuery.getDocValuesDocIdSetIterator(name, leaf), legacyDocs)
        || (field.hasNorms() && intersects(leaf.getNormValues(name), legacyDocs))) {
      return true;
    }
    var terms = leaf.terms(name);
    if (terms != null) {
      var iterator = terms.iterator();
      @Var PostingsEnum postings = null;
      while (iterator.next() != null) {
        postings = iterator.postings(postings, PostingsEnum.NONE);
        if (intersects(postings, legacyDocs)) {
          return true;
        }
      }
    }
    var points = leaf.getPointValues(name);
    if (points == null) {
      // An analysis chain can produce zero terms for a stored string with norms disabled.
      return field.getIndexOptions() != IndexOptions.NONE
          && hasStoredValue(leaf, field, legacyDocs);
    }
    var visitor =
        new PointValues.IntersectVisitor() {
          boolean found;

          @Override
          public void visit(int doc) {
            this.found |= legacyDocs.get(doc);
          }

          @Override
          public void visit(int doc, byte[] value) {
            visit(doc);
          }

          @Override
          public PointValues.Relation compare(byte[] min, byte[] max) {
            return this.found
                ? PointValues.Relation.CELL_OUTSIDE_QUERY
                : PointValues.Relation.CELL_INSIDE_QUERY;
          }
        };
    points.intersect(visitor);
    return visitor.found;
  }

  private static boolean hasStoredValue(LeafReader leaf, FieldInfo field, FixedBitSet docs)
      throws IOException {
    var visitor =
        new StoredFieldVisitor() {
          boolean found;

          @Override
          public Status needsField(FieldInfo candidate) {
            if (candidate.number == field.number) {
              this.found = true;
              return Status.STOP;
            }
            return Status.NO;
          }
        };
    var stored = leaf.storedFields();
    for (int doc = docs.nextSetBit(0);
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = docs.nextSetBit(doc + 1)) {
      stored.document(doc, visitor);
      if (visitor.found) {
        return true;
      }
      if (doc + 1 == docs.length()) {
        break;
      }
    }
    return false;
  }

  private static boolean intersects(@Nullable DocIdSetIterator iterator, FixedBitSet docs)
      throws IOException {
    if (iterator == null) {
      return false;
    }
    for (int doc = iterator.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = iterator.nextDoc()) {
      if (docs.get(doc)) {
        return true;
      }
    }
    return false;
  }
}
