package com.xgen.mongot.index.lucene.query.custom;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.TopDocs;
import org.jetbrains.annotations.Nullable;

/** Optional candidate collection before Lucene materializes the segment filter. */
@FunctionalInterface
public interface LeafKnnSearch {
  /** Return global document IDs in descending score order, or null to run classic search. */
  @Nullable
  TopDocs search(LeafReaderContext leaf, @Nullable QueryTimeout timeout) throws IOException;
}
