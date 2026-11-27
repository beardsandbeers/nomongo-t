package com.xgen.mongot.index.lucene;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.xgen.mongot.index.lucene.field.MappingObservations;
import com.xgen.mongot.index.lucene.field.MappingObservations.Entry;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.jetbrains.annotations.Nullable;

/** Builds postings once for stored V1 observations and keeps pre-observation documents separate. */
record LegacyMappingObservations(Map<Entry, DocIdSet> stored, FixedBitSet unrecorded) {
  private static final Cache<IndexReader.CacheKey, LegacyMappingObservations> CACHE =
      CacheBuilder.newBuilder().build();

  static LegacyMappingObservations forSegment(LeafReader leaf) throws IOException {
    var helper = leaf.getCoreCacheHelper();
    if (helper == null) {
      return read(leaf);
    }
    try {
      return CACHE.get(
          helper.getKey(),
          () -> {
            var observations = read(leaf);
            helper.addClosedListener(CACHE::invalidate);
            return observations;
          });
    } catch (ExecutionException e) {
      throw new IOException("Could not read legacy mapping observations", e.getCause());
    }
  }

  private static LegacyMappingObservations read(LeafReader leaf) throws IOException {
    // Include deleted documents. Readers sharing this core can have different live-doc masks.
    var unrecorded = new FixedBitSet(leaf.maxDoc());
    var indexed =
        leaf.postings(new Term(MappingObservations.NAME, new BytesRef()), PostingsEnum.NONE);
    if (indexed != null && indexed.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      indexed.intoBitSet(leaf.maxDoc(), unrecorded, 0);
    }
    unrecorded.flip(0, leaf.maxDoc());
    if (leaf.getFieldInfos().fieldInfo(MappingObservations.STORED_V1) == null) {
      return new LegacyMappingObservations(Map.of(), unrecorded);
    }

    var stored = leaf.storedFields();
    var selected = Set.of(MappingObservations.STORED_V1);
    var docs = new BitSetIterator(unrecorded, unrecorded.cardinality());
    var builders = new HashMap<Entry, DocIdSetBuilder>();
    var entries = new HashSet<Entry>();
    for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
      var value = stored.document(doc, selected).getBinaryValue(MappingObservations.STORED_V1);
      if (value != null) {
        unrecorded.clear(doc);
        entries.clear();
        MappingObservations.read(value, entries);
        for (var entry : entries) {
          builders
              .computeIfAbsent(entry, key -> new DocIdSetBuilder(leaf.maxDoc()))
              .grow(1)
              .add(doc);
        }
      }
    }
    var postings = new HashMap<Entry, DocIdSet>();
    builders.forEach((entry, builder) -> postings.put(entry, builder.build()));
    return new LegacyMappingObservations(postings, unrecorded);
  }

  FixedBitSet liveUnrecorded(@Nullable Bits live) {
    if (live == null || this.unrecorded.scanIsEmpty()) {
      return this.unrecorded;
    }
    var result = this.unrecorded.clone();
    live.applyMask(result, 0);
    return result;
  }
}
