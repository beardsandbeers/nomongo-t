package com.xgen.mongot.util.concurrent;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.QueryCancellation;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Borrows an executor for nested parallel work without starving its worker threads.
 *
 * <p>Use {@link #openScope()} around each operation. Untimed {@code get()} on futures returned by
 * {@code submit()} (including those used by {@code invokeAll()}) runs the task if no worker has
 * claimed it yet. This lets a partition worker count segments on the same pool while available
 * workers still help. Timed waits do not run tasks, so they retain their timeout semantics.
 *
 * <p>Closing a scope skips pending tasks and waits for running tasks, even after interruption or
 * failure. A running task cannot be cancelled through its future: the scope must not release
 * resources while that task still uses them. Query cancellation is propagated to worker threads.
 * Scopes may be nested and the adapter may be reused concurrently by different callers.
 *
 * <p>The underlying executor remains owned by the caller; lifecycle operations are unsupported.
 */
public final class WorkSharingExecutor extends AbstractExecutorService {
  private final ExecutorService delegate;
  private final ThreadLocal<Scope> currentScope = new ThreadLocal<>();

  public WorkSharingExecutor(ExecutorService delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  public Scope openScope() {
    Scope scope = new Scope(this.currentScope.get());
    this.currentScope.set(scope);
    return scope;
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    Scope scope = Objects.requireNonNull(this.currentScope.get(), "An open scope is required");
    synchronized (scope) {
      if (scope.closing) {
        throw new RejectedExecutionException("Task scope is closed");
      }
      Task<T> task =
          new Task<>(
              () -> {
                QueryCancellation.check();
                return callable.call();
              },
              scope);
      scope.tasks.add(task);
      return task;
    }
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
    return newTaskFor(java.util.concurrent.Executors.callable(runnable, value));
  }

  @Override
  public void execute(Runnable command) {
    Runnable task =
        command instanceof WorkSharingExecutor.Task<?>
            ? command
            : newTaskFor(command, Boolean.TRUE);
    try {
      this.delegate.execute(task);
    } catch (RejectedExecutionException e) {
      task.run();
    }
  }

  private final class Task<T> extends FutureTask<T> {
    private final Scope scope;
    private final Runnable cancellationCheck = QueryCancellation.capture();
    // Guarded by scope; cancellation may only claim work that has not started.
    private boolean claimed;

    private Task(Callable<T> callable, Scope scope) {
      super(callable);
      this.scope = scope;
    }

    @Override
    public void run() {
      synchronized (this.scope) {
        if (this.claimed || this.scope.closing) {
          return;
        }
        this.claimed = true;
        this.scope.running++;
      }
      Scope previous = WorkSharingExecutor.this.currentScope.get();
      WorkSharingExecutor.this.currentScope.set(this.scope);
      try (var unused = QueryCancellation.openScope(this.cancellationCheck)) {
        super.run();
      } finally {
        restoreScope(previous);
        synchronized (this.scope) {
          this.scope.running--;
          this.scope.notifyAll();
        }
      }
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      run();
      return super.get();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      synchronized (this.scope) {
        if (this.claimed) {
          return false;
        }
        this.claimed = true;
        return super.cancel(false);
      }
    }
  }

  public final class Scope implements AutoCloseable {
    private final Scope previous;
    private final List<Task<?>> tasks = new ArrayList<>();
    private boolean closing;
    private int running;

    private Scope(Scope previous) {
      this.previous = previous;
    }

    @Override
    public void close() {
      @Var boolean interrupted = false;
      try {
        synchronized (this) {
          this.closing = true;
          this.tasks.forEach(task -> task.cancel(false));
          while (this.running != 0) {
            try {
              wait();
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
          this.tasks.clear();
        }
      } finally {
        restoreScope(this.previous);
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private void restoreScope(Scope previous) {
    if (previous == null) {
      this.currentScope.remove();
    } else {
      this.currentScope.set(previous);
    }
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException("The caller owns the underlying executor");
  }

  @Override
  public List<Runnable> shutdownNow() {
    throw new UnsupportedOperationException("The caller owns the underlying executor");
  }

  @Override
  public boolean isShutdown() {
    return this.delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return this.delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException("The caller owns the underlying executor");
  }
}
