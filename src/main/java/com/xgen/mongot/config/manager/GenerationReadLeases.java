package com.xgen.mongot.config.manager;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/** Direct reader lifetimes, independent of the producer that owns a generation. */
public final class GenerationReadLeases {
  private final Map<GenerationId, State> states = new HashMap<>();
  private final ExecutorService notifications =
      Executors.newSingleThreadExecutor(
          Thread.ofPlatform().name("direct-read-retirement").factory());
  private boolean stopped;
  private long invalidations;
  private long drains;
  private long drainNanos;

  private static final class State {
    private volatile boolean invalidated;
    private long invalidatedAt;
    private int readers;
    private final List<Runnable> callbacks = new ArrayList<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
  }

  public final class Lease implements AutoCloseable {
    private final InitializedIndex index;
    private final IndexGeneration generation;
    private final State state;
    private final List<Runnable> callbacks = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private Lease(IndexGeneration generation, InitializedIndex index, State state) {
      this.generation = generation;
      this.index = index;
      this.state = state;
    }

    public InitializedIndex index() {
      return this.index;
    }

    public IndexDefinition definition() {
      return this.generation.getDefinition();
    }

    public boolean retired() {
      return this.closed.get()
          || this.state.invalidated
          || !this.index.getStatus().canServiceQueries()
          || !this.generation.getIndex().getStatus().canServiceQueries();
    }

    /** Callbacks never run under a catalog, registry, or caller lock. */
    public void onInvalidated(Runnable callback) {
      synchronized (GenerationReadLeases.this) {
        if (this.closed.get()) {
          return;
        }
        if (this.state.invalidated) {
          GenerationReadLeases.this.notifications.execute(callback);
        } else {
          this.state.callbacks.add(callback);
          this.callbacks.add(callback);
        }
      }
    }

    @Override
    public void close() {
      if (!this.closed.compareAndSet(false, true)) {
        return;
      }
      boolean drained;
      synchronized (GenerationReadLeases.this) {
        this.state.callbacks.removeAll(this.callbacks);
        this.callbacks.clear();
        drained = --this.state.readers == 0;
        if (drained) {
          if (this.state.invalidated) {
            GenerationReadLeases.this.drains++;
            GenerationReadLeases.this.drainNanos += System.nanoTime() - this.state.invalidatedAt;
          }
          GenerationReadLeases.this.states.remove(this.index.getGenerationId(), this.state);
          this.state.callbacks.clear();
        }
      }
      if (drained) {
        this.state.drained.complete(null);
      }
    }
  }

  /** Caller holds the config-manager monitor and has validated the current live generation. */
  public synchronized Lease acquire(IndexGeneration generation, InitializedIndex index) {
    if (this.stopped) {
      throw new IllegalStateException("Direct reads are stopping");
    }
    var state = this.states.computeIfAbsent(index.getGenerationId(), ignored -> new State());
    if (state.invalidated) {
      throw new IllegalStateException("Direct read generation is retiring");
    }
    state.readers++;
    return new Lease(generation, index, state);
  }

  /** Close admission at the catalog transition; callbacks release idle cursors asynchronously. */
  public synchronized CompletableFuture<Void> invalidate(GenerationId id) {
    var state = this.states.get(id);
    if (state == null) {
      return CompletableFuture.completedFuture(null);
    }
    if (!state.invalidated) {
      state.invalidatedAt = System.nanoTime();
      state.invalidated = true;
      this.invalidations++;
      state.callbacks.forEach(this.notifications::execute);
      state.callbacks.clear();
    }
    return state.drained;
  }

  public synchronized long activeCount() {
    return this.states.values().stream().mapToLong(state -> state.readers).sum();
  }

  public synchronized long invalidationCount() {
    return this.invalidations;
  }

  public synchronized long drainCount() {
    return this.drains;
  }

  public synchronized long drainNanos() {
    return this.drainNanos;
  }

  public synchronized CompletableFuture<Void> stop() {
    this.stopped = true;
    var drained =
        CompletableFuture.allOf(
            List.copyOf(this.states.keySet()).stream()
                .map(this::invalidate)
                .toArray(CompletableFuture<?>[]::new));
    drained.whenComplete((unused, error) -> this.notifications.shutdown());
    return drained;
  }
}
