package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherFactory;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.opentelemetry.context.Context;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TotalHitCountCollectorManager;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class LuceneIndexSearcherReferenceTest {
  @Parameterized.Parameters(name = "phantomCleanup={0}, explain={1}")
  public static Collection<Object[]> parameters() {
    return List.of(
        new Object[] {false, false},
        new Object[] {false, true},
        new Object[] {true, false},
        new Object[] {true, true});
  }

  private final boolean phantomCleanup;
  private final boolean explain;

  public LuceneIndexSearcherReferenceTest(boolean phantomCleanup, boolean explain) {
    this.phantomCleanup = phantomCleanup;
    this.explain = explain;
  }

  @Test
  public void close_interruptedInnerSearch_keepsReaderOpenUntilSegmentFinishes() throws Exception {
    var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
    var caller = java.util.concurrent.Executors.newSingleThreadExecutor();
    var control = new InterruptedSearch();
    Executor orderedExecutor = startWorkerBeforeCaller(pool, control.started);
    try (var fixture = new Fixture();
        var explainScope =
            Explain.setup(
                this.explain ? Optional.of(Explain.Verbosity.EXECUTION_STATS) : Optional.empty(),
                Optional.of(1))) {
      var reference = fixture.reference(orderedExecutor, this.phantomCleanup);
      var searcher = reference.getIndexSearcher();
      var reader = searcher.getIndexReader();
      fixture.manager.close();
      fixture.writer.close();
      assertThat(searcher.getSlices().length).isAtLeast(2);
      var parentContext = Context.current();
      var original = new AtomicReference<Throwable>();
      var operationThread = new AtomicReference<Thread>();
      var interruptRestored = new AtomicBoolean();
      var failed = new CountDownLatch(1);
      var operation =
          caller.submit(
              () -> {
                operationThread.set(Thread.currentThread());
                try (var context = parentContext.makeCurrent();
                    reference) {
                  try {
                    searcher.search(new MatchAllDocsQuery(), control);
                  } catch (Exception | Error e) {
                    original.set(e);
                    failed.countDown();
                    throw e;
                  }
                  return null;
                } finally {
                  interruptRestored.set(Thread.interrupted());
                }
              });
      try {
        await(failed);

        assertThrows(TimeoutException.class, () -> operation.get(100, TimeUnit.MILLISECONDS));
        operationThread.get().interrupt();
        assertThrows(TimeoutException.class, () -> operation.get(100, TimeUnit.MILLISECONDS));
        operationThread.get().interrupt();
        assertThat(reader.getRefCount()).isEqualTo(1);
        control.release.countDown();
        var thrown =
            assertThrows(ExecutionException.class, () -> operation.get(5, TimeUnit.SECONDS));

        assertThat(thrown.getCause()).isSameInstanceAs(original.get());
        assertThat(control.readAfterRelease.get()).isTrue();
        assertThat(reader.getRefCount()).isEqualTo(0);
        assertThat(interruptRestored.get()).isTrue();
      } finally {
        control.release.countDown();
      }
    } finally {
      control.release.countDown();
      caller.shutdownNow();
      pool.shutdownNow();
    }
  }

  @Test
  public void close_successfulRepeatedSearch_releasesReferenceAndKeepsPoolUsable()
      throws Exception {
    try (var fixture = new Fixture();
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        var explainScope =
            Explain.setup(
                this.explain ? Optional.of(Explain.Verbosity.EXECUTION_STATS) : Optional.empty(),
                Optional.of(1))) {
      var reference = fixture.reference(pool, this.phantomCleanup);
      var searcher = reference.getIndexSearcher();
      var reader = searcher.getIndexReader();
      int references = reader.getRefCount();
      try (reference) {
        assertThat(
                searcher.search(
                    new MatchAllDocsQuery(),
                    new TotalHitCountCollectorManager(searcher.getSlices())))
            .isEqualTo(12);
        assertThat(
                searcher.search(
                    new MatchAllDocsQuery(),
                    new TotalHitCountCollectorManager(searcher.getSlices())))
            .isEqualTo(12);
      }

      assertThat(reader.getRefCount()).isEqualTo(references - 1);
      assertThat(pool.submit(() -> 1).get(5, TimeUnit.SECONDS)).isEqualTo(1);
    }
  }

  @Test
  public void close_attachedCursor_closesBeforeReaderRelease() throws Exception {
    try (var fixture = new Fixture();
        var pool = java.util.concurrent.Executors.newSingleThreadExecutor();
        var ref = fixture.reference(pool, this.phantomCleanup)) {
      var reader = ref.getIndexSearcher().getIndexReader();
      int refs = reader.getRefCount();
      var closed = new AtomicBoolean();
      ref.attachCursor(
          () -> {
            assertThat(reader.getRefCount()).isEqualTo(refs);
            assertThat(closed.getAndSet(true)).isFalse();
          });

      ref.close();

      assertThat(closed.get()).isTrue();
      assertThat(reader.getRefCount()).isEqualTo(refs - 1);
    }
  }

  private static final class InterruptedSearch implements CollectorManager<SimpleCollector, Void> {
    private final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicBoolean firstSegment = new AtomicBoolean(true);
    private final AtomicBoolean readAfterRelease = new AtomicBoolean();

    @Override
    public SimpleCollector newCollector() {
      return new SimpleCollector() {
        private LeafReader reader;

        @Override
        protected void doSetNextReader(LeafReaderContext context) {
          this.reader = context.reader();
        }

        @Override
        public ScoreMode scoreMode() {
          return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        public void collect(int doc) throws IOException {
          if (InterruptedSearch.this.firstSegment.compareAndSet(true, false)) {
            InterruptedSearch.this.started.countDown();
            await(InterruptedSearch.this.release);
            assertThat(this.reader.storedFields().document(doc).get("value")).isEqualTo("present");
            InterruptedSearch.this.readAfterRelease.set(true);
          } else {
            Thread.currentThread().interrupt();
          }
        }
      };
    }

    @Override
    public Void reduce(Collection<SimpleCollector> collectors) {
      return null;
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final ByteBuffersDirectory directory = new ByteBuffersDirectory();
    private final IndexWriter writer;
    private final LuceneSearcherManager manager;

    private Fixture() throws IOException {
      this.writer =
          new IndexWriter(
              this.directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
      for (int i = 0; i < 12; i++) {
        var doc = new Document();
        doc.add(new StoredField("value", "present"));
        this.writer.addDocument(doc);
        this.writer.commit();
      }
      this.manager =
          LuceneSearcherManager.create(
              this.writer,
              new LuceneSearcherFactory(
                  SearchIndex.MOCK_INDEX_DEFINITION,
                  false,
                  new QueryCacheProvider.DefaultQueryCacheProvider(),
                  Optional.empty(),
                  SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH)),
              SearchIndex.mockMetricsFactory(),
              () -> false);
    }

    private LuceneIndexSearcherReference reference(Executor executor, boolean phantomCleanup)
        throws IOException {
      var flags =
          phantomCleanup
              ? FeatureFlags.withDefaults().enable(Feature.PHANTOM_REFERENCE_CLEANUP).build()
              : FeatureFlags.withDefaults().disable(Feature.PHANTOM_REFERENCE_CLEANUP).build();
      return LuceneIndexSearcherReference.create(
          this.manager,
          executor,
          SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH),
          flags);
    }

    @Override
    public void close() throws IOException {
      this.manager.close();
      this.writer.close();
      this.directory.close();
    }
  }

  private static Executor startWorkerBeforeCaller(Executor executor, CountDownLatch started) {
    var firstSubmission = new AtomicBoolean(true);
    return command -> {
      executor.execute(command);
      if (firstSubmission.compareAndSet(true, false)) {
        // Ensure the first future is running on a worker before the caller claims another slice.
        await(started);
      }
    };
  }

  private static void await(CountDownLatch latch) {
    try {
      assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
