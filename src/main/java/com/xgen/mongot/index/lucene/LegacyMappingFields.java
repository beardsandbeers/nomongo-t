package com.xgen.mongot.index.lucene;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.field.FieldName;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.Nullable;

/** Tests whether a typed field actually occurs in any surviving pre-observation document. */
final class LegacyMappingFields {
  private final FixedBitSet allLegacyDocs;
  private @Nullable Map<String, DocIdSet> storedFields;
  private @Nullable IndexReader.CacheKey snapshot;
  private ImmutableList<String> liveFields = ImmutableList.of();

  LegacyMappingFields(FixedBitSet allLegacyDocs) {
    this.allLegacyDocs = allLegacyDocs;
  }

  // Refreshes that change deletions keep the stored evidence but need a fresh liveness check.
  // Keep only the most recently queried snapshot of this core. Never retain a reader.
  synchronized ImmutableList<String> liveFields(LeafReader leaf) throws IOException {
    var helper = leaf.getReaderCacheHelper();
    if (helper != null && helper.getKey() == this.snapshot) {
      return this.liveFields;
    }
    var docs = this.allLegacyDocs.clone();
    if (leaf.getLiveDocs() != null) {
      leaf.getLiveDocs().applyMask(docs, 0);
    }
    var names = ImmutableList.<String>builder();
    if (!docs.scanIsEmpty()) {
      for (var field : leaf.getFieldInfos()) {
        if (FieldName.TypeField.getTypeOf(field.name)
                .flatMap(LuceneIndexMappingExporter::publicType)
                .isPresent()
            && contains(leaf, field, docs)) {
          names.add(field.name);
        }
      }
    }
    this.liveFields = names.build();
    this.snapshot = helper == null ? null : helper.getKey();
    return this.liveFields;
  }

  private boolean contains(LeafReader leaf, FieldInfo field, FixedBitSet legacyDocs)
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

  private boolean hasStoredValue(LeafReader leaf, FieldInfo field, FixedBitSet liveLegacyDocs)
      throws IOException {
    var docs = storedFields(leaf).get(field.name);
    return docs != null && intersects(docs.iterator(), liveLegacyDocs);
  }

  // Stored-only evidence is needed when analysis emitted no terms and norms were omitted.
  // Read the field headers once per immutable core and apply each snapshot's deletes at lookup.
  private Map<String, DocIdSet> storedFields(LeafReader leaf) throws IOException {
    if (this.storedFields != null) {
      return this.storedFields;
    }
    var builders = new HashMap<String, DocIdSetBuilder>();
    var names = new HashSet<String>();
    var visitor =
        new StoredFieldVisitor() {
          @Override
          public Status needsField(FieldInfo field) {
            if (field.getIndexOptions() != IndexOptions.NONE) {
              names.add(field.name);
            }
            return Status.NO;
          }
        };
    var reader = leaf.storedFields();
    var docs = new BitSetIterator(this.allLegacyDocs, this.allLegacyDocs.cardinality());
    for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
      reader.document(doc, visitor);
      for (String name : names) {
        builders.computeIfAbsent(name, key -> new DocIdSetBuilder(leaf.maxDoc())).grow(1).add(doc);
      }
      names.clear();
    }
    var postings = new HashMap<String, DocIdSet>();
    builders.forEach((name, builder) -> postings.put(name, builder.build()));
    this.storedFields = postings;
    return postings;
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
