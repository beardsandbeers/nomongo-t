package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.IndexReader.DocumentIdConsumer;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.mongot.index.lucene.util.LuceneDocumentIdEncoder;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonInt32;
import org.junit.Test;

public class LuceneDocumentIdCollectorTest {
  @Test
  public void collect_parallelSlices_countsEveryRootAndFinishesEachLeaf() throws Exception {
    try (var f = new Fixture()) {
      var entered = new CountDownLatch(2);
      Set<Long> threads = ConcurrentHashMap.newKeySet();
      Set<BytesRef> ids = ConcurrentHashMap.newKeySet();
      var finishes = new AtomicInteger();

      long count =
          f.collect(
              () ->
                  new DocumentIdConsumer() {
                    @Override
                    public void accept(byte[] id) {
                      threads.add(Thread.currentThread().threadId());
                      entered.countDown();
                      await(entered);
                      assertThat(ids.add(new BytesRef(id))).isTrue();
                    }

                    @Override
                    public void finish() {
                      finishes.incrementAndGet();
                    }
                  },
              true);

      assertThat(count).isEqualTo(12);
      assertThat(ids).hasSize(12);
      assertThat(threads.size()).isAtLeast(2);
      assertThat(finishes.get()).isEqualTo(12);
    }
  }

  @Test
  public void collect_missingExecutor_exhaustsSerially() throws Exception {
    try (var f = new Fixture()) {
      Set<Long> threads = ConcurrentHashMap.newKeySet();

      long count = f.collect(() -> id -> threads.add(Thread.currentThread().threadId()), false);

      assertThat(count).isEqualTo(12);
      assertThat(threads).containsExactly(Thread.currentThread().threadId());
    }
  }

  @Test
  public void collect_concurrentInsert_exhaustsPinnedReaderWithoutChasingNewDocuments()
      throws Exception {
    try (var f = new Fixture()) {
      var inserted = new AtomicBoolean();

      long count =
          f.collect(
              () ->
                  id -> {
                    if (inserted.compareAndSet(false, true)) {
                      try {
                        f.add(99);
                      } catch (IOException e) {
                        throw new UncheckedIOException(e);
                      }
                    }
                  },
              true);

      assertThat(count).isEqualTo(12);
      try (var refreshed = DirectoryReader.open(f.writer)) {
        assertThat(refreshed.numDocs()).isEqualTo(13);
      }
      assertThat(f.reader.numDocs()).isEqualTo(12);
    }
  }

  @Test
  public void collect_workerFailure_waitsForOtherRunningCollectors() throws Exception {
    try (var f = new Fixture();
        var caller = java.util.concurrent.Executors.newSingleThreadExecutor()) {
      var blocked = new CountDownLatch(1);
      var failed = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      var first = new AtomicBoolean(true);
      var expected = new IllegalStateException("injected collection failure");
      var result =
          caller.submit(
              () ->
                  f.collect(
                      () ->
                          id -> {
                            if (first.compareAndSet(true, false)) {
                              blocked.countDown();
                              await(release);
                            } else {
                              await(blocked);
                              failed.countDown();
                              throw expected;
                            }
                          },
                      true));
      try {
        assertThat(failed.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(result.isDone()).isFalse();
      } finally {
        release.countDown();
      }

      var error = assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));
      assertThat(error).hasCauseThat().isSameInstanceAs(expected);
      assertThat(f.collect(() -> id -> {}, true)).isEqualTo(12);
    }
  }

  @Test
  public void collect_scorerCreationFailure_stopsPeerAndPreservesOriginalError() throws Exception {
    assertScorerFailureStopsPeer(false);
  }

  @Test
  public void collect_scoringFailure_stopsPeerAndPreservesOriginalError() throws Exception {
    assertScorerFailureStopsPeer(true);
  }

  private static void assertScorerFailureStopsPeer(boolean duringScoring) throws Exception {
    try (var f = new Fixture(1);
        var caller = java.util.concurrent.Executors.newSingleThreadExecutor()) {
      var blocked = new CountDownLatch(1);
      var failedTaskExited = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      var expected = new IOException("injected scorer failure");
      var delivered = new AtomicInteger();
      var finishes = new AtomicInteger();
      var query =
          new FailingScorerQuery(
              f.executor, blocked, release, failedTaskExited, expected, duringScoring);
      var result =
          caller.submit(
              () ->
                  LuceneDocumentIdCollector.collect(
                      f.searcher,
                      query,
                      () ->
                          new DocumentIdConsumer() {
                            @Override
                            public void accept(byte[] id) {
                              delivered.incrementAndGet();
                            }

                            @Override
                            public void finish() {
                              finishes.incrementAndGet();
                            }
                          },
                      () -> {},
                      Optional.of(f.executor)));
      try {
        assertThat(failedTaskExited.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(result.isDone()).isFalse();
      } finally {
        release.countDown();
      }

      var error = assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));
      assertThat(error).hasCauseThat().isSameInstanceAs(expected);
      assertThat(delivered.get()).isEqualTo(0);
      assertThat(finishes.get()).isEqualTo(0);
      assertThat(f.collect(() -> id -> {}, true)).isEqualTo(12);
    }
  }

  /** Fails on the sole pool thread while the caller's slice is blocked in scorer construction. */
  private static final class FailingScorerQuery extends Query {
    private final NamedExecutorService executor;
    private final CountDownLatch blocked;
    private final CountDownLatch release;
    private final CountDownLatch failedTaskExited;
    private final IOException failure;
    private final boolean duringScoring;

    private FailingScorerQuery(
        NamedExecutorService executor,
        CountDownLatch blocked,
        CountDownLatch release,
        CountDownLatch failedTaskExited,
        IOException failure,
        boolean duringScoring) {
      this.executor = executor;
      this.blocked = blocked;
      this.release = release;
      this.failedTaskExited = failedTaskExited;
      this.failure = failure;
      this.duringScoring = duringScoring;
    }

    private IOException fail() {
      await(this.blocked);
      // With one pool thread this runs only after the failing slice has fully unwound.
      this.executor.execute(this.failedTaskExited::countDown);
      return this.failure;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode mode, float boost)
        throws IOException {
      var caller = Thread.currentThread();
      var delegate = new MatchAllDocsQuery().createWeight(searcher, mode, boost);
      return new FilterWeight(delegate) {
        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          if (Thread.currentThread() == caller) {
            FailingScorerQuery.this.blocked.countDown();
            await(FailingScorerQuery.this.release);
            return delegate.scorerSupplier(context);
          }
          if (!FailingScorerQuery.this.duringScoring) {
            throw FailingScorerQuery.this.fail();
          }
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) {
              throw new AssertionError("Expected bulk scoring");
            }

            @Override
            public BulkScorer bulkScorer() {
              return new BulkScorer() {
                @Override
                public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
                    throws IOException {
                  throw FailingScorerQuery.this.fail();
                }

                @Override
                public long cost() {
                  return 1;
                }
              };
            }

            @Override
            public long cost() {
              return 1;
            }
          };
        }
      };
    }

    @Override
    public String toString(String field) {
      return "FailingScorerQuery";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
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

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("Collector did not make progress");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final ByteBuffersDirectory directory = new ByteBuffersDirectory();
    private final KeywordAnalyzer analyzer = new KeywordAnalyzer();
    private final IndexWriter writer;
    private final DirectoryReader reader;
    private final LuceneIndexSearcher searcher;
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final NamedExecutorService executor;

    private Fixture() throws IOException {
      this(2);
    }

    private Fixture(int threads) throws IOException {
      this.executor =
          Executors.fixedSizeThreadPool(
              "delete-collection-test",
              threads,
              32,
              new ThreadPoolExecutor.AbortPolicy(),
              this.metrics);
      this.writer =
          new IndexWriter(
              this.directory,
              new IndexWriterConfig(this.analyzer).setMergePolicy(NoMergePolicy.INSTANCE));
      for (int id = 0; id < 12; id++) {
        add(id);
      }
      this.reader = DirectoryReader.open(this.writer);
      this.searcher =
          LuceneIndexSearcher.create(
              this.reader,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.empty(),
              Optional.empty(),
              false,
              false,
              Optional.empty());
    }

    private void add(int id) throws IOException {
      var document = new Document();
      document.add(
          new StringField(
              FieldName.MetaField.ID.getLuceneFieldName(),
              new BytesRef(LuceneDocumentIdEncoder.encodeDocumentId(new BsonInt32(id))),
              Field.Store.YES));
      this.writer.addDocument(document);
      this.writer.commit();
    }

    private long collect(
        java.util.function.Supplier<DocumentIdConsumer> consumers, boolean concurrent)
        throws IOException {
      return LuceneDocumentIdCollector.collect(
          this.searcher,
          new MatchAllDocsQuery(),
          consumers,
          () -> {},
          concurrent ? Optional.of(this.executor) : Optional.empty());
    }

    @Override
    public void close() throws Exception {
      this.executor.shutdown();
      assertThat(this.executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      this.reader.close();
      this.writer.close();
      this.analyzer.close();
      this.directory.close();
      this.metrics.close();
    }
  }
}
