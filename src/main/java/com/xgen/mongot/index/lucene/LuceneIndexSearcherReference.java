package com.xgen.mongot.index.lucene;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.lucene.explain.ProfilingIndexSearcher;
import com.xgen.mongot.index.lucene.explain.explainers.CollectorTimingFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.explainers.MetadataFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.profiler.QueryProfilerFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.searcher.ConcurrentIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.LuceneSearcherManager;
import com.xgen.mongot.util.CloseablePhantomCleaner;
import com.xgen.mongot.util.concurrent.DrainingExecutor;
import io.opentelemetry.context.Context;
import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.IOUtils;

/**
 * LuceneIndexSearcherReference couples together an IndexSearcher with the SearcherManager or other
 * manager that created it/is responsible for its lifecycle. LuceneIndexSearcherReference
 * automatically releases the IndexSearcher when closing the LuceneIndexSearcherReference.
 *
 * <p>This allows for IDEs to warn that the IndexSearcher wasn't released, and also allows for code
 * like the following to be written which will automatically release the searcher: <code>
 * try(final LuceneIndexSearcherReference searcherReference = getReference()) {
 * useSearcher(searcherReference.getIndexSearcher());
 * }
 * </code>
 */
public class LuceneIndexSearcherReference implements Closeable {

  /**
   * Cleanup handler for releasing IndexSearcher references when the parent
   * LuceneIndexSearcherReference is garbage collected.
   *
   * <p><b>Uses strong references</b> to prevent resource leaks: When the SearcherManager refreshes
   * (rotates to a new searcher), the old searcher would become weakly-reachable and could be GC'd
   * before this cleanup runs. Strong references ensure the searcher remains alive until {@code
   * manager.release(searcher)} is called, preventing leaks.
   */
  private record Cleaning(
      WeakReference<ReferenceManager<LuceneIndexSearcher>> searcherManager,
      LuceneIndexSearcher indexSearcher,
      Optional<DrainingExecutor> tasks)
      implements CloseablePhantomCleaner.NoRefCloseable {

    Cleaning(
        ReferenceManager<LuceneIndexSearcher> searcherManager,
        LuceneIndexSearcher indexSearcher,
        Optional<DrainingExecutor> tasks) {
      this(new WeakReference<>(searcherManager), indexSearcher, tasks);
    }

    private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

    @Override
    public void close() throws IOException {
      // Lucene may return on interruption while an inner segment task is still running.
      // Both explicit and phantom cleanup must drain before releasing the reader.
      this.tasks.ifPresent(DrainingExecutor::close);
      Optional<ReferenceManager<LuceneIndexSearcher>> maybeManager =
          Optional.ofNullable(this.searcherManager.get());
      if (maybeManager.isPresent()) {
        LuceneIndexSearcherReference.releaseSearcher(maybeManager.get(), this.indexSearcher);
      } else {
        FLOGGER.atWarning().atMostEvery(10, TimeUnit.SECONDS).log(
            "Skip releasing searcher because manager or searcher become phantom");
      }
    }
  }

  private static final Closeable CLOSED = () -> {};
  private final AtomicReference<Closeable> cursor = new AtomicReference<>();
  private final LuceneIndexSearcher indexSearcher;
  private final Closeable cleaning;

  // The phantom action may hold the cursor, but must never retain its owning reference.
  private record CloseWithCursor(AtomicReference<Closeable> cursor, Closeable release)
      implements CloseablePhantomCleaner.NoRefCloseable {
    @Override
    public void close() throws IOException {
      var owned = this.cursor.getAndSet(CLOSED);
      if (owned != CLOSED) {
        // Join active read-ahead before releasing the searcher, including on phantom cleanup.
        IOUtils.close(owned, this.release);
      }
    }
  }

  void attachCursor(Closeable owned) throws IOException {
    if (!this.cursor.compareAndSet(null, owned)) {
      owned.close();
      throw new IllegalStateException("Searcher already closed or owns a cursor");
    }
  }

  private static void releaseSearcher(
      ReferenceManager<LuceneIndexSearcher> searcherManager, LuceneIndexSearcher indexSearcher)
      throws IOException {
    searcherManager.release(indexSearcher);
  }

  private LuceneIndexSearcherReference(
      ReferenceManager<LuceneIndexSearcher> searcherManager,
      LuceneIndexSearcher indexSearcher,
      FeatureFlags featureFlags,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater,
      Optional<DrainingExecutor> tasks) {

    this.indexSearcher = indexSearcher;

    this.cleaning =
        featureFlags.isEnabled(Feature.PHANTOM_REFERENCE_CLEANUP)
            ? CloseablePhantomCleaner.register(
                this,
                new CloseablePhantomCleaner.CleanerThreadAction(
                    metricsUpdater.getPhantomSearcherCleanupCounter(),
                    new CloseWithCursor(
                        this.cursor, new Cleaning(searcherManager, indexSearcher, tasks))))
            : new CloseWithCursor(
                this.cursor,
                () -> {
                  tasks.ifPresent(DrainingExecutor::close);
                  releaseSearcher(searcherManager, indexSearcher);
                });
  }

  public static LuceneIndexSearcherReference create(
      LuceneSearcherManager searcherManager,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater,
      FeatureFlags featureFlags)
      throws IOException {
    var initialSearcher = searcherManager.acquire();
    var searcher =
        Explain.getQueryInfo()
            .map(
                info ->
                    (LuceneIndexSearcher)
                        new ProfilingIndexSearcher(
                            initialSearcher,
                            info.getFeatureExplainer(
                                    QueryProfilerFeatureExplainer.class,
                                    () ->
                                        new QueryProfilerFeatureExplainer(
                                            metricsUpdater, info.getVerbosity()))
                                .getQueryProfiler(),
                            info.getFeatureExplainer(
                                CollectorTimingFeatureExplainer.class,
                                CollectorTimingFeatureExplainer::new),
                            info.getFeatureExplainer(
                                MetadataFeatureExplainer.class, MetadataFeatureExplainer::new)))
            .orElseGet(() -> LuceneIndexSearcher.create(initialSearcher));
    return new LuceneIndexSearcherReference(
        searcherManager, searcher, featureFlags, metricsUpdater, Optional.empty());
  }

  public static LuceneIndexSearcherReference create(
      LuceneSearcherManager searcherManager,
      Executor executor,
      IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater,
      FeatureFlags featureFlags)
      throws IOException {
    var initialSearcher = searcherManager.acquire();
    var tasks = new DrainingExecutor(executor);
    var searcher =
        Explain.getQueryInfo()
            .map(
                info ->
                    (LuceneIndexSearcher)
                        new ProfilingIndexSearcher(
                            initialSearcher,
                            info.getFeatureExplainer(
                                    QueryProfilerFeatureExplainer.class,
                                    () ->
                                        new QueryProfilerFeatureExplainer(
                                            metricsUpdater, info.getVerbosity()))
                                .getQueryProfiler(),
                            info.getFeatureExplainer(
                                CollectorTimingFeatureExplainer.class,
                                CollectorTimingFeatureExplainer::new),
                            info.getFeatureExplainer(
                                MetadataFeatureExplainer.class, MetadataFeatureExplainer::new),
                            Context.current().wrap(Explain.maybeWrap(tasks))))
            .orElseGet(
                () -> new ConcurrentIndexSearcher(initialSearcher, Context.current().wrap(tasks)));
    return new LuceneIndexSearcherReference(
        searcherManager, searcher, featureFlags, metricsUpdater, Optional.of(tasks));
  }

  public LuceneIndexSearcher getIndexSearcher() {
    return this.indexSearcher;
  }

  @Override
  public void close() throws IOException {
    this.cleaning.close();
  }
}
