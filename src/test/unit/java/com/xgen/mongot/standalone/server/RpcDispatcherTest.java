package com.xgen.mongot.standalone.server;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.metrics.StandaloneMetrics;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class RpcDispatcherTest {
  @Test
  public void unary_expiredContext_skipsWorkBeforeDispatch() throws Exception {
    var config = StandaloneConfig.development(Path.of("unused"), 0);
    var called = new AtomicBoolean();
    var response = new Response<String>();
    try (var dispatcher = new RpcDispatcher(config, StandaloneMetrics.disabled());
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        var expired = Context.current().withDeadlineAfter(0, TimeUnit.NANOSECONDS, scheduler)) {
      expired.run(
          () ->
              dispatcher.unary(
                  1,
                  response,
                  () -> {
                    called.set(true);
                    return "unexpected";
                  }));
      var error =
          assertThrows(ExecutionException.class, () -> response.result.get(5, TimeUnit.SECONDS));
      assertThat(Status.fromThrowable(error.getCause()).getCode())
          .isEqualTo(Status.Code.DEADLINE_EXCEEDED);
      assertThat(called.get()).isFalse();
    }
  }

  @Test
  public void async_cancelledClient_holdsAdmissionUntilWriterDrains() throws Exception {
    var base = StandaloneConfig.development(Path.of("unused"), 0);
    var config =
        new StandaloneConfig(
            base.dataPath(),
            base.address(),
            base.documentLimits(),
            1,
            base.maxInFlightBytes(),
            base.minFreeDiskBytes(),
            base.commitInterval(),
            base.refreshInterval(),
            base.maxRpcDuration(),
            base.tls(),
            base.principals());
    var pending = new CompletableFuture<String>();
    var started = new CountDownLatch(1);
    var first = new Response<String>();
    var rejected = new Response<String>();
    try (var dispatcher = new RpcDispatcher(config, StandaloneMetrics.disabled());
        var context = Context.current().withCancellation()) {
      context.run(
          () ->
              dispatcher.async(
                  1,
                  first,
                  () -> {
                    started.countDown();
                    return pending;
                  }));
      assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

      context.cancel(null);
      dispatcher.async(1, rejected, () -> CompletableFuture.completedFuture("too soon"));

      assertThat(pending.isCancelled()).isFalse();
      var capacity =
          assertThrows(ExecutionException.class, () -> rejected.result.get(5, TimeUnit.SECONDS));
      assertThat(((StatusRuntimeException) capacity.getCause()).getStatus().getCode())
          .isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      pending.complete("drained");
      var cancelled =
          assertThrows(ExecutionException.class, () -> first.result.get(5, TimeUnit.SECONDS));
      assertThat(((StatusRuntimeException) cancelled.getCause()).getStatus().getCode())
          .isEqualTo(Status.Code.CANCELLED);
      var next = new Response<String>();
      dispatcher.async(1, next, () -> CompletableFuture.completedFuture("accepted"));
      assertThat(next.result.get(5, TimeUnit.SECONDS)).isEqualTo("accepted");
    } finally {
      pending.complete("cleanup");
    }
  }

  private static final class Response<T> implements StreamObserver<T> {
    private final CompletableFuture<T> result = new CompletableFuture<>();

    @Override
    public void onNext(T value) {
      this.result.complete(value);
    }

    @Override
    public void onError(Throwable error) {
      this.result.completeExceptionally(error);
    }

    @Override
    public void onCompleted() {}
  }
}
