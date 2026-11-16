package com.xgen.mongot.util.concurrent;

import com.google.errorprone.annotations.Var;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Tracks work submitted to a borrowed executor so its resources can be released safely.
 *
 * <p>Close this guard after the submitting operation has stopped, before releasing resources used
 * by its tasks. Closing skips queued work and waits for running work, even if interrupted, then
 * restores the interrupt flag. It does not shut down the underlying executor.
 *
 * <p>This guard is intended for Lucene task groups whose caller also executes pending tasks.
 * Skipped commands are not run or completed; it is not a general-purpose executor replacement. The
 * owner must not close the guard from one of its running tasks.
 */
public final class DrainingExecutor implements Executor, AutoCloseable {
  private final Executor executor;
  private boolean closing;
  private int running;

  public DrainingExecutor(Executor executor) {
    this.executor = Objects.requireNonNull(executor);
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
