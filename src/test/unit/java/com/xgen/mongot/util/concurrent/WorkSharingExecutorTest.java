package com.xgen.mongot.util.concurrent;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.util.QueryCancellation;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class WorkSharingExecutorTest {
  @Test
  public void invokeAll_saturatedSingleWorker_completesNestedWork() throws Exception {
    ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(1);
    var executor = new WorkSharingExecutor(pool);
    try {
      Future<Integer> result =
          pool.submit(
              () -> {
                try (var unused = executor.openScope()) {
                  List<Future<Integer>> tasks = executor.invokeAll(List.of(() -> 10, () -> 20));
                  return tasks.get(0).get() + tasks.get(1).get();
                }
              });

      assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo(30);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void invokeAll_twoPartitions_usesMoreThanTwoThreadsForSegments() throws Exception {
    ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(4);
    var executor = new WorkSharingExecutor(pool);
    var partitionsStarted = new CountDownLatch(2);
    var segmentsStarted = new CountDownLatch(3);
    var counted = new AtomicInteger();
    Callable<Void> partition =
        () -> {
          partitionsStarted.countDown();
          await(partitionsStarted);
          try (var unused = executor.openScope()) {
            Callable<Void> segment =
                () -> {
                  segmentsStarted.countDown();
                  // Two partition threads alone cannot pass this barrier.
                  await(segmentsStarted);
                  counted.incrementAndGet();
                  return null;
                };
            executor.invokeAll(List.of(segment, segment, segment));
          }
          return null;
        };
    try {
      Future<Void> first = pool.submit(partition);
      Future<Void> second = pool.submit(partition);

      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);

      assertThat(segmentsStarted.getCount()).isEqualTo(0);
      assertThat(counted.get()).isEqualTo(6);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void close_taskFails_drainsRunningWorkAndSkipsQueuedWork() throws Exception {
    ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(1);
    ExecutorService caller = java.util.concurrent.Executors.newSingleThreadExecutor();
    var executor = new WorkSharingExecutor(pool);
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var closing = new CountDownLatch(1);
    var completed = new AtomicBoolean();
    var queuedRan = new AtomicBoolean();
    var failure = new IllegalStateException("segment failed");
    try {
      Future<Void> operation =
          caller.submit(
              () -> {
                try (var unused = executor.openScope()) {
                  executor.submit(
                      () -> {
                        started.countDown();
                        await(release);
                        completed.set(true);
                        return null;
                      });
                  await(started);
                  Future<Void> failed =
                      executor.submit(
                          () -> {
                            throw failure;
                          });
                  executor.submit(() -> queuedRan.set(true));
                  try {
                    failed.get();
                  } finally {
                    closing.countDown();
                  }
                }
                return null;
              });
      await(closing);

      assertThrows(TimeoutException.class, () -> operation.get(100, TimeUnit.MILLISECONDS));
      release.countDown();
      var thrown = assertThrows(ExecutionException.class, () -> operation.get(5, TimeUnit.SECONDS));

      assertThat(thrown).hasCauseThat().hasCauseThat().isSameInstanceAs(failure);
      assertThat(completed.get()).isTrue();
      // A barrier also lets the delegate dequeue any skipped work before checking this flag.
      pool.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertThat(queuedRan.get()).isFalse();
    } finally {
      release.countDown();
      caller.shutdownNow();
      pool.shutdownNow();
    }
  }

  @Test
  public void close_interruptedCaller_drainsWorkAndRestoresInterrupt() throws Exception {
    ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(1);
    ExecutorService caller = java.util.concurrent.Executors.newSingleThreadExecutor();
    var executor = new WorkSharingExecutor(pool);
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var closing = new CountDownLatch(1);
    var completed = new AtomicBoolean();
    try {
      Future<Boolean> operation =
          caller.submit(
              () -> {
                try (var unused = executor.openScope()) {
                  Future<Void> running =
                      executor.submit(
                          () -> {
                            started.countDown();
                            await(release);
                            completed.set(true);
                            return null;
                          });
                  await(started);
                  assertThat(running.cancel(true)).isFalse();
                  Thread.currentThread().interrupt();
                  closing.countDown();
                }
                return Thread.interrupted();
              });
      await(closing);

      assertThrows(TimeoutException.class, () -> operation.get(100, TimeUnit.MILLISECONDS));
      release.countDown();

      assertThat(operation.get(5, TimeUnit.SECONDS)).isTrue();
      assertThat(completed.get()).isTrue();
    } finally {
      release.countDown();
      caller.shutdownNow();
      pool.shutdownNow();
    }
  }

  @Test
  public void submit_workerThread_propagatesAndRestoresCancellation() throws Exception {
    ExecutorService pool = java.util.concurrent.Executors.newSingleThreadExecutor();
    var executor = new WorkSharingExecutor(pool);
    var checks = new AtomicInteger();
    try {
      try (var cancellation = QueryCancellation.openScope(checks::incrementAndGet);
          var unused = executor.openScope()) {
        executor.submit(QueryCancellation::check).get(5, TimeUnit.SECONDS);
      }
      pool.submit(QueryCancellation::check).get(5, TimeUnit.SECONDS);

      assertThat(checks.get()).isEqualTo(2);
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void get_timedWait_doesNotRunTaskOnCaller() throws Exception {
    ExecutorService pool = java.util.concurrent.Executors.newSingleThreadExecutor();
    var executor = new WorkSharingExecutor(pool);
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var ran = new AtomicBoolean();
    pool.submit(
        () -> {
          started.countDown();
          await(release);
          return null;
        });
    try {
      await(started);
      Future<?> task;
      try (var unused = executor.openScope()) {
        task = executor.submit(() -> ran.set(true));

        assertThrows(TimeoutException.class, () -> task.get(1, TimeUnit.MILLISECONDS));
      }

      assertThat(task.isCancelled()).isTrue();
      assertThat(ran.get()).isFalse();
    } finally {
      release.countDown();
      pool.shutdownNow();
    }
  }

  @Test
  public void submit_rejectedByDelegate_runsOnCallerAndCanReuseScope() throws Exception {
    ExecutorService pool = java.util.concurrent.Executors.newSingleThreadExecutor();
    pool.shutdown();
    var executor = new WorkSharingExecutor(pool);
    try (var unused = executor.openScope()) {
      assertThat(executor.submit(() -> 1).get()).isEqualTo(1);
    }
    try (var unused = executor.openScope()) {
      assertThat(executor.submit(() -> 2).get()).isEqualTo(2);
    }
  }

  private static void await(CountDownLatch latch) throws InterruptedException {
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
  }
}
