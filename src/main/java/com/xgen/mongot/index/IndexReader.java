package com.xgen.mongot.index;

import com.xgen.mongot.index.statistics.LuceneStatisticsSnapshot;
import java.io.IOException;

/** An IndexReader is capable of reading an index and producing results for queries. */
public interface IndexReader {

  /**
   * Retains the current Lucene readers for bounded statistics work. Caller must close the lease.
   */
  default LuceneStatisticsSnapshot acquireStatisticsSnapshot()
      throws IOException, ReaderClosedException {
    throw new UnsupportedOperationException("This reader does not expose Lucene statistics");
  }

  /**
   * Refreshes the IndexReader such that future calls to query() should see all updates to the Index
   * that occurred prior to the refresh() call.
   */
  void refresh() throws IOException, ReaderClosedException;

  /**
   * Refreshes and exhaustively selects root IDs from this reader's partitions. Selection may
   * overlap writes. The callback receives copied internal BSON ID bytes; it may abort collection
   * with a runtime exception. The check must throw on cancellation or budget exhaustion.
   */
  default void collectDocumentIds(
      org.bson.BsonDocument predicate,
      boolean vector,
      boolean all,
      java.util.function.Consumer<byte[]> ids,
      Runnable check)
      throws IOException, ReaderClosedException, com.xgen.mongot.index.query.InvalidQueryException {
    throw new UnsupportedOperationException("ID collection unavailable");
  }

  void open();

  void close();
}
