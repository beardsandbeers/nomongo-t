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

  /** One collector's ID sink. Finish flushes a successful leaf; it is not called after failure. */
  interface DocumentIdConsumer extends java.util.function.Consumer<byte[]> {
    default void finish() {}
  }

  /**
   * Exhaustively collects IDs, with an independent sink for each search slice. Implementations may
   * collect concurrently and must drain all running collectors before returning, including on
   * failure. Finish each leaf's sink during execution so reduction retains only counts.
   */
  default long collectDocumentIds(
      org.bson.BsonDocument predicate,
      boolean vector,
      boolean all,
      java.util.function.Supplier<DocumentIdConsumer> consumers,
      Runnable check)
      throws IOException, ReaderClosedException, com.xgen.mongot.index.query.InvalidQueryException {
    var consumer = consumers.get();
    long[] count = {0};
    collectDocumentIds(
        predicate,
        vector,
        all,
        (byte[] id) -> {
          consumer.accept(id);
          count[0]++;
        },
        check);
    consumer.finish();
    return count[0];
  }

  /**
   * Refreshes and exhaustively selects live root IDs from this reader's partitions. Each partition
   * uses a pinned reader and invokes the callback sequentially, once per live root. The callback
   * receives copied internal BSON ID bytes and may synchronously apply writes or abort collection
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
