package com.xgen.mongot.util.concurrent;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class DrainingExecutorTest {
  @Test
  public void close_repeatedInterruptions_drainsRunningWorkAndSkipsQueuedWork() throws Exception {
    var pool = java.util.concurrent.Executors.newSingleThreadExecutor();
    var caller = java.util.concurrent.Executors.newSingleThreadExecutor();
    var tasks = new DrainingExecutor(pool);
    var running = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var closing = new CountDownLatch(1);
    var closingThread = new AtomicReference<Thread>();
    var finished = new AtomicBoolean();
    var queuedRan = new AtomicBoolean();
    try {
      tasks.execute(
          () -> {
            running.countDown();
            await(release);
            finished.set(true);
          });
      await(running);
      tasks.execute(() -> queuedRan.set(true));
      var closed =
          caller.submit(
              () -> {
                closingThread.set(Thread.currentThread());
                Thread.currentThread().interrupt();
                closing.countDown();
                tasks.close();
                return Thread.interrupted();
              });
      await(closing);

      assertThrows(TimeoutException.class, () -> closed.get(100, TimeUnit.MILLISECONDS));
      closingThread.get().interrupt();
      assertThrows(TimeoutException.class, () -> closed.get(100, TimeUnit.MILLISECONDS));
      release.countDown();

      assertThat(closed.get(5, TimeUnit.SECONDS)).isTrue();
      assertThat(finished.get()).isTrue();
      pool.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertThat(queuedRan.get()).isFalse();
      assertThat(pool.isShutdown()).isFalse();
    } finally {
      release.countDown();
      caller.shutdownNow();
      pool.shutdownNow();
    }
  }

  @Test
  public void close_inlineWorkFails_preservesFailureAndInterruptStatus() {
    var expected = new IllegalStateException("task failed");
    var tasks = new DrainingExecutor(Runnable::run);
    try {
      var actual =
          assertThrows(
              IllegalStateException.class,
              () -> {
                try (tasks) {
                  tasks.execute(
                      () -> {
                        Thread.currentThread().interrupt();
                        throw expected;
                      });
                }
              });

      assertThat(actual).isSameInstanceAs(expected);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
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
