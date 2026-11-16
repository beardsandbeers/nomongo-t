package com.xgen.mongot.standalone.ingestion;

import com.google.protobuf.ByteString;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.DocumentMetadata;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.standalone.StandaloneBson;
import com.xgen.mongot.standalone.config.StandaloneConfig.Limits;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import com.xgen.mongot.util.regex.RegexBudget;
import io.grpc.Context;
import io.grpc.Status;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.bson.types.ObjectId;

/**
 * Pushes BSON directly through mongot's ordinary mapping pipeline into the selected Lucene index.
 */
public final class ApiIngestionManager implements ReplicationManager {
  private final IndexCatalog liveIndexes;
  private final InitializedIndexCatalog initializedIndexes;
  private final Limits limits;
  private final Optional<IndexingWorkScheduler> scheduler;
  private final Map<String, IndexState> indexes = new HashMap<>();
  private final Map<String, Leases> leases = new HashMap<>();
  private final ScheduledThreadPoolExecutor commits;
  private boolean stopping;
  private final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();

  private static final class IndexState {
    private final InitializedIndex index;
    // Hot scan checks must not take the ingestion monitor for every matching ID.
    private volatile boolean retired;
    // Serializes explicit/background commits, including multipart writers, without blocking writes.
    private final Object commitLock = new Object();
    // Accessed under the ingestion monitor. A completed batch advances the version even on failure.
    private long writeVersion;
    private long committedVersion;

    private IndexState(InitializedIndex index) {
      this.index = index;
    }
  }

  private static final class Leases {
    private int count;
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
  }

  /** Pins a generation while writes, queries or statistics use its reader/writer resources. */
  public final class IndexLease implements AutoCloseable {
    private final GenerationId generation;
    private final IndexState state;
    private final java.util.concurrent.atomic.AtomicBoolean closed =
        new java.util.concurrent.atomic.AtomicBoolean();

    private IndexLease(GenerationId generation, IndexState state) {
      this.generation = generation;
      this.state = state;
    }

    public InitializedIndex index() {
      return this.state.index;
    }

    public boolean retired() {
      return this.state.retired
          || this.state.index.getStatus().getStatusCode() != IndexStatus.StatusCode.STEADY;
    }

    @Override
    public void close() {
      if (!this.closed.compareAndSet(false, true)) {
        return;
      }
      CompletableFuture<Void> drained;
      synchronized (ApiIngestionManager.this) {
        String id = this.generation.uniqueString();
        var leases = ApiIngestionManager.this.leases.get(id);
        leases.count--;
        if (leases.count != 0) {
          return;
        }
        ApiIngestionManager.this.leases.remove(id);
        drained = leases.drained;
      }
      // Completing this future may close and delete Lucene resources. Never run those lifecycle
      // continuations while holding the ingestion monitor.
      drained.complete(null);
    }
  }

  public synchronized IndexLease acquireStatisticsLease(GenerationId generation) {
    return acquireQueryLease(generation);
  }

  /** Prevents generation resources from retiring until query readers have drained. */
  public synchronized IndexLease acquireQueryLease(GenerationId generation) {
    if (!isGenerationReady(generation)) {
      throw Status.UNAVAILABLE.withDescription("Index is not ready").asRuntimeException();
    }
    return acquireLease(generation);
  }

  private IndexLease acquireLease(GenerationId generation) {
    String id = generation.uniqueString();
    this.leases.computeIfAbsent(id, ignored -> new Leases()).count++;
    return new IndexLease(generation, this.indexes.get(id));
  }

  public ApiIngestionManager(
      IndexCatalog liveIndexes,
      InitializedIndexCatalog initializedIndexes,
      Limits limits,
      Duration commitInterval) {
    this(liveIndexes, initializedIndexes, limits, commitInterval, Optional.empty());
  }

  public ApiIngestionManager(
      IndexCatalog liveIndexes,
      InitializedIndexCatalog initializedIndexes,
      Limits limits,
      Duration commitInterval,
      IndexingWorkScheduler scheduler) {
    this(liveIndexes, initializedIndexes, limits, commitInterval, Optional.of(scheduler));
  }

  private ApiIngestionManager(
      IndexCatalog liveIndexes,
      InitializedIndexCatalog initializedIndexes,
      Limits limits,
      Duration commitInterval,
      Optional<IndexingWorkScheduler> scheduler) {
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

  // Serializes catalog changes and generation acquisition. Indexing runs outside this monitor.
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
    this.indexes.put(generation.getGenerationId().uniqueString(), new IndexState(index));
    index.setStatus(IndexStatus.steady());
  }

  public synchronized boolean isGenerationReady(GenerationId id) {
    var index = this.indexes.get(id.uniqueString());
    return !this.stopping
        && index != null
        && index.index.getStatus().getStatusCode() == IndexStatus.StatusCode.STEADY;
  }

  /**
   * Applies an ordered batch outside the catalog monitor. Concurrent batches may interleave. The
   * desired revision is read under the monitor to fence catalog changes before leasing a
   * generation. Failure may leave a prefix applied.
   */
  public String write(
      String indexId,
      LongSupplier revision,
      String requestedGeneration,
      List<ByteString> documents,
      boolean delete,
      DocumentWriteConcern concern) {
    try {
      return writeAsync(indexId, revision, requestedGeneration, documents, delete, concern).join();
    } catch (java.util.concurrent.CompletionException error) {
      if (error.getCause() instanceof RuntimeException cause) {
        throw cause;
      }
      throw error;
    }
  }

  /** Leases before queueing and retains the lease until indexing and its barrier have drained. */
  public CompletableFuture<String> writeAsync(
      String indexId,
      LongSupplier revision,
      String requestedGeneration,
      List<ByteString> documents,
      boolean delete,
      DocumentWriteConcern concern) {
    if (concern == DocumentWriteConcern.UNRECOGNIZED) {
      throw new IllegalArgumentException("Unknown write concern");
    }
    if (documents.isEmpty()
        || documents.size() > this.limits.maxBatchDocuments()
        || documents.stream().mapToLong(ByteString::size).sum() > this.limits.maxBatchBytes()) {
      throw new IllegalArgumentException("Batch size is outside the configured limits");
    }
    return executeWrite(
        indexId,
        revision,
        requestedGeneration,
        lease -> applyBatch(lease, documents, delete, concern));
  }

  /** Applies trusted reader IDs without decoding them back into ordinary document events. */
  public String deleteDocuments(
      String indexId,
      LongSupplier revision,
      String requestedGeneration,
      List<byte[]> encodedIds,
      Runnable check) {
    if (encodedIds.isEmpty()
        || encodedIds.size() > this.limits.maxBatchDocuments()
        || encodedIds.stream().mapToLong(id -> id.length).sum() > this.limits.maxBatchBytes()) {
      throw new IllegalArgumentException("Batch size is outside the configured limits");
    }
    try {
      return executeWrite(
              indexId,
              revision,
              requestedGeneration,
              lease ->
                  applyWrite(
                      lease,
                      () -> {
                        check.run();
                        lease.index().getWriter().deleteDocuments(encodedIds, check, ignored -> {});
                        var exceeded = lease.index().getWriter().exceededLimits();
                        if (exceeded.isPresent()) {
                          throw exceeded.get();
                        }
                      },
                      DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_INDEXED))
          .join();
    } catch (java.util.concurrent.CompletionException error) {
      if (error.getCause() instanceof RuntimeException cause) {
        throw cause;
      }
      throw error;
    }
  }

  private CompletableFuture<String> executeWrite(
      String indexId,
      LongSupplier revision,
      String requestedGeneration,
      java.util.function.Function<IndexLease, String> operation) {
    var lease = acquireWriteLease(indexId, revision, requestedGeneration);
    var context = Context.current();
    Supplier<String> work =
        () -> {
          var previous = context.attach();
          try {
            if (context.isCancelled()) {
              throw Status.CANCELLED.asRuntimeException();
            }
            if (lease.retired()) {
              throw Status.UNAVAILABLE.withDescription("Index is retiring").asRuntimeException();
            }
            return operation.apply(lease);
          } finally {
            context.detach(previous);
          }
        };
    try {
      var result =
          this.scheduler
              .map(value -> value.executeConcurrentOperation(work))
              .orElseGet(() -> CompletableFuture.completedFuture(work.get()));
      // The caller may cancel its returned future; cleanup is attached to the private work future.
      var drained = result.whenComplete((unused, error) -> lease.close());
      return drained.thenApply(value -> value);
    } catch (RuntimeException | Error error) {
      lease.close();
      throw error;
    }
  }

  private synchronized IndexLease acquireWriteLease(
      String indexId, LongSupplier revision, String requestedGeneration) {
    long desiredRevision = revision.getAsLong();
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
        || generation.getDefinition().getDefinitionVersion().orElse(1L) != desiredRevision) {
      throw Status.UNAVAILABLE
          .withDescription("Index schema is changing or index is unavailable")
          .asRuntimeException();
    }
    return acquireLease(generation.getGenerationId());
  }

  private String applyBatch(
      IndexLease lease, List<ByteString> documents, boolean delete, DocumentWriteConcern concern) {
    var index = lease.index();
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
    return applyWrite(
        lease,
        () -> {
          for (var event : index.getWriter().prepareBatch(events)) {
            if (Context.current().isCancelled()) {
              throw Status.CANCELLED.asRuntimeException();
            }
            try (var ignored = RegexBudget.openScope()) {
              index.getWriter().updateIndex(event);
            }
            var exceeded = index.getWriter().exceededLimits();
            if (exceeded.isPresent()) {
              throw exceeded.get();
            }
          }
        },
        concern);
  }

  @FunctionalInterface
  private interface BatchOperation {
    void run() throws java.io.IOException, com.xgen.mongot.index.ExceededLimitsException;
  }

  private String applyWrite(
      IndexLease lease, BatchOperation operation, DocumentWriteConcern concern) {
    String id = lease.generation.uniqueString();
    var index = lease.index();
    try {
      try {
        operation.run();
      } finally {
        // Mark after applying the batch: a commit racing an in-flight write must not clear it.
        synchronized (this) {
          lease.state.writeVersion++;
        }
      }
      if (concern == DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED) {
        commit(lease.state);
      } else if (concern == DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE) {
        index.getReader().refresh();
      }
      return id;
    } catch (com.xgen.mongot.index.ExceededLimitsException e) {
      throw Status.RESOURCE_EXHAUSTED
          .withDescription("Index limits exceeded; a prefix may have been applied")
          .withCause(e)
          .asRuntimeException();
    } catch (java.io.IOException | com.xgen.mongot.index.ReaderClosedException e) {
      index.setStatus(IndexStatus.failed("Lucene write failed; retry or recreate the index"));
      throw Status.UNAVAILABLE
          .withDescription("Lucene write failed; a prefix may have been applied")
          .withCause(e)
          .asRuntimeException();
    }
  }

  /** Completes batched writes using a generation lease without blocking concurrent ingestion. */
  public void completeWrite(
      String indexId,
      LongSupplier revision,
      String requestedGeneration,
      DocumentWriteConcern concern) {
    if (concern == DocumentWriteConcern.UNRECOGNIZED) {
      throw new IllegalArgumentException("Unknown write concern");
    }
    try (var lease = acquireWriteLease(indexId, revision, requestedGeneration)) {
      if (Context.current().isCancelled()) {
        throw Status.CANCELLED.asRuntimeException();
      }
      try {
        if (concern == DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED) {
          commit(lease.state);
        } else if (concern == DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE) {
          lease.index().getReader().refresh();
        }
      } catch (java.io.IOException | com.xgen.mongot.index.ReaderClosedException e) {
        lease.index().setStatus(IndexStatus.failed("Lucene completion failed"));
        throw Status.UNAVAILABLE.withCause(e).asRuntimeException();
      }
    }
  }

  private void commit(IndexState state) throws java.io.IOException {
    synchronized (state.commitLock) {
      long version;
      synchronized (this) {
        version = state.writeVersion;
        if (version == state.committedVersion) {
          return;
        }
      }
      state.index.getWriter().commit(EncodedUserData.EMPTY);
      synchronized (this) {
        state.committedVersion = version;
      }
    }
  }

  void commitDirty() {
    List<GenerationId> generations;
    synchronized (this) {
      if (this.stopping) {
        return;
      }
      generations =
          this.indexes.values().stream()
              .filter(state -> state.writeVersion != state.committedVersion)
              .map(state -> state.index.getGenerationId())
              .toList();
    }
    for (var generation : generations) {
      IndexLease lease;
      synchronized (this) {
        if (this.stopping || !this.indexes.containsKey(generation.uniqueString())) {
          continue;
        }
        lease = acquireLease(generation);
      }
      try (lease) {
        try {
          commit(lease.state);
        } catch (java.io.IOException e) {
          lease.index().setStatus(IndexStatus.failed("Lucene commit failed"));
        }
      }
    }
  }

  @Override
  public synchronized CompletableFuture<Void> dropIndex(GenerationId generation) {
    String id = generation.uniqueString();
    var state = this.indexes.remove(id);
    if (state != null) {
      state.retired = true;
    }
    var leases = this.leases.get(id);
    var drained = leases == null ? CompletableFuture.<Void>completedFuture(null) : leases.drained;
    return this.stopping ? CompletableFuture.allOf(drained, this.shutdownFuture) : drained;
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
  public CompletableFuture<Void> shutdown() {
    List<IndexState> states;
    CompletableFuture<Void> drained;
    CompletableFuture<Void> result;
    synchronized (this) {
      if (this.stopping) {
        return this.shutdownFuture;
      }
      this.stopping = true;
      this.commits.shutdown();
      result = this.shutdownFuture;
      states = List.copyOf(this.indexes.values());
      states.forEach(state -> state.retired = true);
      drained =
          CompletableFuture.allOf(
              this.leases.values().stream()
                  .map(leases -> leases.drained)
                  .toArray(CompletableFuture<?>[]::new));
    }
    // Retirement waits for shutdownFuture, so these generations remain open for the final commit.
    // Complete futures outside the monitor: continuations can close/delete Lucene resources.
    drained.whenComplete(
        (unused, error) -> {
          if (error != null) {
            result.completeExceptionally(error);
            return;
          }
          try {
            for (var state : states) {
              commit(state);
            }
            result.complete(null);
          } catch (java.io.IOException | RuntimeException e) {
            result.completeExceptionally(e);
          }
        });
    return result;
  }
}
