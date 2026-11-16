package com.xgen.mongot.index.lucene;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.IndexReader.DocumentIdConsumer;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.searcher.ConcurrentIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/** Exhaustive root-ID collection with independent sinks and complete draining on failure. */
final class LuceneDocumentIdCollector {
  private LuceneDocumentIdCollector() {}

  static long collect(
      LuceneIndexSearcher source,
      Query query,
      Supplier<DocumentIdConsumer> consumers,
      Runnable check,
      Optional<NamedExecutorService> executor)
      throws IOException {
    var failure = new AtomicReference<Throwable>();
    Runnable guardedCheck =
        () -> {
          if (failure.get() != null) {
            throw new IllegalStateException("Document ID collection stopped", failure.get());
          }
          try {
            check.run();
          } catch (RuntimeException | Error error) {
            failure.compareAndSet(null, error);
            throw error;
          }
        };
    class IdCollector implements Collector {
      private final DocumentIdConsumer consumer = consumers.get();
      private long count;

      @Override
      public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        guardedCheck.run();
        org.apache.lucene.index.StoredFields fields;
        try {
          fields = context.reader().storedFields();
        } catch (IOException | RuntimeException | Error error) {
          failure.compareAndSet(null, error);
          throw error;
        }
        return new LeafCollector() {
          @Override
          public void setScorer(Scorable scorer) {}

          @Override
          public void collect(int doc) throws IOException {
            try {
              guardedCheck.run();
              var value =
                  fields
                      .document(doc, Set.of(FieldName.MetaField.ID.getLuceneFieldName()))
                      .getBinaryValue(FieldName.MetaField.ID.getLuceneFieldName());
              if (value == null) {
                throw new IOException("Missing document ID");
              }
              IdCollector.this.consumer.accept(
                  Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length));
              IdCollector.this.count++;
            } catch (IOException | RuntimeException | Error error) {
              failure.compareAndSet(null, error);
              throw error;
            }
          }

          @Override
          public void finish() {
            try {
              guardedCheck.run();
              IdCollector.this.consumer.finish();
            } catch (RuntimeException | Error error) {
              failure.compareAndSet(null, error);
              throw error;
            }
          }
        };
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
      }
    }

    try (var tasks = new TaskGroup(executor.<Executor>map(value -> value).orElse(Runnable::run))) {
      var searcher =
          executor.isPresent()
              ? new ConcurrentIndexSearcher(source, tasks) {
                @Override
                protected void search(
                    LeafReaderContextPartition[] partitions, Weight weight, Collector collector)
                    throws IOException {
                  try {
                    guardedCheck.run();
                    super.search(partitions, weight, collector);
                  } catch (IOException | RuntimeException | Error error) {
                    // Scorer construction and iteration can fail outside
                    // collector callbacks.
                    // Publish before Lucene waits for other slices so those
                    // slices stop too.
                    failure.compareAndSet(null, error);
                    throw error;
                  }
                }
              }
              : source;
      searcher.setQueryCache(null);
      searcher.setTimeout(
          () -> {
            guardedCheck.run();
            return false;
          });
      try {
        long count =
            searcher.search(
                query,
                new CollectorManager<IdCollector, Long>() {
                  @Override
                  public IdCollector newCollector() {
                    guardedCheck.run();
                    return new IdCollector();
                  }

                  @Override
                  public Long reduce(Collection<IdCollector> collectors) {
                    guardedCheck.run();
                    return collectors.stream().mapToLong(collector -> collector.count).sum();
                  }
                });
        guardedCheck.run();
        return count;
      } catch (IOException | RuntimeException | Error error) {
        failure.compareAndSet(null, error);
        throw error;
      }
    } catch (IOException | RuntimeException | Error error) {
      // Preserve the original failure when other workers stopped in response to it.
      var original = failure.get();
      if (original instanceof IOException io) {
        throw io;
      }
      if (original instanceof RuntimeException runtime) {
        throw runtime;
      }
      if (original instanceof Error fatal) {
        throw fatal;
      }
      throw error;
    }
  }

  /**
   * Uses the existing pool. Queued work becomes a no-op after close; running work drains before the
   * caller may release its reader. This also protects against interruption during Future.get().
   */
  private static final class TaskGroup implements Executor, AutoCloseable {
    private final Executor executor;
    private boolean closing;
    private int running;

    private TaskGroup(Executor executor) {
      this.executor = executor;
    }

    @Override
    public void execute(Runnable command) {
      this.executor.execute(
          () -> {
            synchronized (this) {
              if (this.closing) {
                return;
              }
              this.running++;
            }
            try {
              command.run();
            } finally {
              synchronized (this) {
                this.running--;
                notifyAll();
              }
            }
          });
    }

    @Override
    public void close() {
      @Var boolean interrupted = false;
      synchronized (this) {
        this.closing = true;
        while (this.running != 0) {
          try {
            wait();
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
