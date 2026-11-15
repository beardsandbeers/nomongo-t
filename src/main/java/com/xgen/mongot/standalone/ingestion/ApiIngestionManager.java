package com.xgen.mongot.standalone.ingestion;

import com.google.protobuf.ByteString;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.standalone.StandaloneBson;
import com.xgen.mongot.standalone.config.StandaloneConfig.Limits;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.grpc.Context;
import io.grpc.Status;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.Nullable;

/**
 * Pushes BSON directly through mongot's ordinary mapping pipeline into the selected Lucene index.
 */
public final class ApiIngestionManager implements ReplicationManager {
  private final IndexCatalog liveIndexes;
  private final InitializedIndexCatalog initializedIndexes;
  private final Limits limits;
  private final IndexingWorkScheduler scheduler;
  private final Map<String, CompletableFuture<Void>> pending = new HashMap<>();
  private final Set<String> retiring = new HashSet<>();
  private final Map<String, InitializedIndex> indexes = new HashMap<>();
  private final Map<String, StatisticsLeases> statisticsLeases = new HashMap<>();
  private final Set<String> dirty = new HashSet<>();
  private final ScheduledThreadPoolExecutor commits;
  private boolean stopping;
  private @Nullable CompletableFuture<Void> shutdownFuture;

  private static final class StatisticsLeases {
    private int count;
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
  }

  /** Prevents lifecycle close/drop while a bounded statistics collection owns reader resources. */
  public final class StatisticsLease implements AutoCloseable {
    private final GenerationId generation;
    private final InitializedIndex index;
    private final AtomicBoolean closed = new AtomicBoolean();

    private StatisticsLease(GenerationId generation, InitializedIndex index) {
      this.generation = generation;
      this.index = index;
    }

    public InitializedIndex index() {
      return this.index;
    }

    public boolean retired() {
      return !isGenerationReady(this.generation);
    }

    @Override
    public void close() {
      if (!this.closed.compareAndSet(false, true)) {
        return;
      }
      CompletableFuture<Void> drained;
      synchronized (ApiIngestionManager.this) {
        String id = this.generation.uniqueString();
        var leases = ApiIngestionManager.this.statisticsLeases.get(id);
        leases.count--;
        if (leases.count != 0) {
          return;
        }
        ApiIngestionManager.this.statisticsLeases.remove(id);
        drained = leases.drained;
      }
      // Completing this future may close and delete Lucene resources. Never run those lifecycle
      // continuations while holding the ingestion monitor.
      drained.complete(null);
    }
  }

  public synchronized StatisticsLease acquireStatisticsLease(GenerationId generation) {
    if (!isGenerationReady(generation)) {
      throw Status.UNAVAILABLE.withDescription("Index is not ready").asRuntimeException();
    }
    String id = generation.uniqueString();
    this.statisticsLeases.computeIfAbsent(id, ignored -> new StatisticsLeases()).count++;
    return new StatisticsLease(generation, this.indexes.get(id));
  }

  public ApiIngestionManager(
      IndexCatalog liveIndexes,
      InitializedIndexCatalog initializedIndexes,
      Limits limits,
      Duration commitInterval,
      IndexingWorkScheduler scheduler) {
    this.liveIndexes = liveIndexes;
    this.initializedIndexes = initializedIndexes;
    this.limits = limits;
    this.scheduler = scheduler;
    this.commits =
        new ScheduledThreadPoolExecutor(1, Thread.ofPlatform().name("push-commit").factory());
    this.commits.setRemoveOnCancelPolicy(true);
    this.commits.scheduleWithFixedDelay(
        this::commitDirty,
        commitInterval.toMillis(),
        commitInterval.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  // Serializes mapping changes, retirement and writes. No content is queued or persisted here.
  public synchronized <T> T coordinate(Supplier<T> action) {
    if (this.stopping) {
      throw Status.UNAVAILABLE
          .withDescription("Push ingestion is shutting down")
          .asRuntimeException();
    }
    return action.get();
  }

  @Override
  public synchronized void add(IndexGeneration generation) {
    if (this.stopping) {
      return;
    }
    var index = this.initializedIndexes.getIndex(generation.getGenerationId()).orElseThrow();
    if (this.indexes.containsKey(generation.getGenerationId().uniqueString())) {
      return;
    }
    this.indexes.put(generation.getGenerationId().uniqueString(), index);
    index.setStatus(IndexStatus.steady());
  }

  public synchronized boolean isGenerationReady(GenerationId id) {
    var index = this.indexes.get(id.uniqueString());
    return !this.stopping
        && !this.retiring.contains(id.uniqueString())
        && index != null
        && index.getStatus().getStatusCode() == IndexStatus.StatusCode.STEADY;
  }

  /** Returns after the requested Lucene write concern. Failure may leave a prefix applied. */
  public String write(
      String indexId,
      long revision,
      String requestedGeneration,
      List<ByteString> documents,
      boolean delete,
      DocumentWriteConcern concern) {
    return writeAsync(indexId, revision, requestedGeneration, documents, delete, concern).join();
  }

  /** Admission and generation fencing are short; all writer work uses the shared scheduler. */
  public synchronized CompletableFuture<String> writeAsync(
      String indexId,
      long revision,
      String requestedGeneration,
      List<ByteString> documents,
      boolean delete,
      DocumentWriteConcern concern) {
    if (this.stopping) {
      throw Status.UNAVAILABLE
          .withDescription("Push ingestion is shutting down")
          .asRuntimeException();
    }
    if (concern == DocumentWriteConcern.UNRECOGNIZED) {
      throw new IllegalArgumentException("Unknown write concern");
    }
    if (documents.isEmpty()
        || documents.size() > this.limits.maxBatchDocuments()
        || documents.stream().mapToLong(ByteString::size).sum() > this.limits.maxBatchBytes()) {
      throw new IllegalArgumentException("Batch size is outside the configured limits");
    }
    var generation =
        this.liveIndexes
            .getIndexById(new ObjectId(indexId))
            .orElseThrow(
                () ->
                    Status.UNAVAILABLE
                        .withDescription("Index is initializing")
                        .asRuntimeException());
    String id = generation.getGenerationId().uniqueString();
    if (!requestedGeneration.isEmpty() && !requestedGeneration.equals(id)) {
      throw Status.FAILED_PRECONDITION
          .withDescription("Requested generation is no longer live")
          .asRuntimeException();
    }
    if (!isGenerationReady(generation.getGenerationId())
        || generation.getDefinition().getDefinitionVersion().orElse(1L) != revision) {
      throw Status.UNAVAILABLE
          .withDescription("Index schema is changing or index is unavailable")
          .asRuntimeException();
    }
    // Validate the whole input before mutating Lucene; field mapping uses the current index schema.
    var events =
        documents.stream()
            .map(
                bytes -> {
                  var validated =
                      StandaloneBson.mutation(
                          bytes.toByteArray(),
                          delete,
                          this.limits.maxDocumentBytes(),
                          this.limits.maxDepth());
                  return delete
                      ? DocumentEvent.createDelete(validated.document().get("_id"))
                      : DocumentEvent.createUpdate(
                          DocumentMetadata.fromOriginalDocument(Optional.of(validated.document())),
                          validated.document());
                })
            .toList();
    var index = this.indexes.get(id);
    var context = Context.current();
    var indexer = new ApiDocumentIndexer(index);
    var future =
        this.scheduler.scheduleOperation(
            events,
            indexer,
            generation.getGenerationId(),
            index.getMetricsUpdater().getIndexingMetricsUpdater(),
            () -> {
              try {
                if (context.isCancelled()) {
                  throw Status.CANCELLED.asRuntimeException();
                }
                synchronized (this) {
                  if (this.retiring.contains(id)) {
                    throw Status.UNAVAILABLE
                        .withDescription("Index is retiring")
                        .asRuntimeException();
                  }
                }
                if (index.getStatus().getStatusCode() != IndexStatus.StatusCode.STEADY) {
                  throw Status.UNAVAILABLE
                      .withDescription("Index is unavailable")
                      .asRuntimeException();
                }
                for (var event : indexer.prepareBatch(events)) {
                  if (context.isCancelled()) {
                    throw Status.CANCELLED.asRuntimeException();
                  }
                  synchronized (this) {
                    this.dirty.add(id);
                  }
                  indexer.indexDocumentEvent(event);
                  var exceeded = indexer.exceededLimits();
                  if (exceeded.isPresent()) {
                    throw exceeded.get();
                  }
                }
                if (concern == DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED) {
                  commit(id, index);
                } else if (concern == DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE) {
                  index.getReader().refresh();
                }
                return null;
              } catch (ExceededLimitsException e) {
                throw Status.RESOURCE_EXHAUSTED
                    .withDescription("Index limits exceeded; a prefix may have been applied")
                    .withCause(e)
                    .asRuntimeException();
              } catch (IOException | UncheckedIOException | ReaderClosedException e) {
                index.setStatus(
                    IndexStatus.failed("Lucene write failed; retry or recreate the index"));
                throw Status.UNAVAILABLE
                    .withDescription("Lucene write failed; a prefix may have been applied")
                    .withCause(e)
                    .asRuntimeException();
              }
            });
    // This internal future is never exposed to the RPC cancellation machinery.
    CompletableFuture<Void> drained = future.handle((ignored, error) -> null);
    this.pending.put(id, drained);
    return drained.thenCompose(ignored -> future.thenApply(completed -> id));
  }

  private void commit(String id, InitializedIndex index) throws IOException {
    index.getWriter().commit(EncodedUserData.EMPTY);
    synchronized (this) {
      this.dirty.remove(id);
    }
  }

  private final Set<String> commitQueued = new HashSet<>();

  private synchronized void commitDirty() {
    if (this.stopping) {
      return;
    }
    for (String id : List.copyOf(this.dirty)) {
      var index = this.indexes.get(id);
      if (index == null || this.retiring.contains(id) || !this.commitQueued.add(id)) {
        continue;
      }
      var future =
          this.scheduler.scheduleOperation(
              List.of(),
              new ApiDocumentIndexer(index),
              index.getGenerationId(),
              index.getMetricsUpdater().getIndexingMetricsUpdater(),
              () -> {
                try {
                  commit(id, index);
                } catch (IOException e) {
                  index.setStatus(IndexStatus.failed("Lucene commit failed"));
                  throw e;
                }
                return null;
              });
      this.pending.put(id, future.handle((ignored, error) -> null));
      future.whenComplete(
          (ignored, error) -> {
            synchronized (this) {
              this.commitQueued.remove(id);
            }
          });
    }
  }

  @Override
  public synchronized CompletableFuture<Void> dropIndex(GenerationId generation) {
    String id = generation.uniqueString();
    this.retiring.add(id);
    this.dirty.remove(id);
    var leases = this.statisticsLeases.get(id);
    var statistics =
        leases == null ? CompletableFuture.<Void>completedFuture(null) : leases.drained;
    return CompletableFuture.allOf(
            statistics, this.pending.getOrDefault(id, CompletableFuture.completedFuture(null)))
        .thenRun(
            () -> {
              synchronized (this) {
                this.indexes.remove(id);
                this.pending.remove(id);
                this.retiring.remove(id);
                this.dirty.remove(id);
              }
            });
  }

  @Override
  public Optional<SyncSourceConfig> getSyncSourceConfig() {
    return Optional.empty();
  }

  @Override
  public synchronized boolean isInitialized() {
    return !this.stopping;
  }

  @Override
  public boolean isReplicationSupported() {
    return true;
  }

  @Override
  public synchronized CompletableFuture<Void> shutdown() {
    if (this.shutdownFuture != null) {
      return this.shutdownFuture;
    }
    this.stopping = true;
    this.commits.shutdown();
    var finalCommits = new ArrayList<CompletableFuture<Void>>();
    this.indexes.forEach(
        (id, index) -> {
          if (this.retiring.contains(id)) {
            return;
          }
          finalCommits.add(
              this.scheduler.scheduleOperation(
                  List.of(),
                  new ApiDocumentIndexer(index),
                  index.getGenerationId(),
                  index.getMetricsUpdater().getIndexingMetricsUpdater(),
                  () -> {
                    commit(id, index);
                    return null;
                  }));
        });
    this.shutdownFuture =
        CompletableFuture.allOf(finalCommits.toArray(CompletableFuture<?>[]::new))
            .thenCompose(
                ignored -> {
                  synchronized (this) {
                    return CompletableFuture.allOf(
                        this.statisticsLeases.values().stream()
                            .map(leases -> leases.drained)
                            .toArray(CompletableFuture<?>[]::new));
                  }
                });
    return this.shutdownFuture;
  }
}
