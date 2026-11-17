package com.xgen.mongot.standalone.server;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.server.grpc.GrpcStreamingClient.checkHealth;
import static io.grpc.health.v1.HealthCheckResponse.ServingStatus.NOT_SERVING;
import static io.grpc.health.v1.HealthCheckResponse.ServingStatus.SERVING;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.CatalogServiceGrpc;
import com.xgen.mongot.standalone.grpc.DocumentServiceGrpc;
import com.xgen.mongot.standalone.grpc.GetStatusRequest;
import com.xgen.mongot.standalone.grpc.OperationsServiceGrpc;
import com.xgen.mongot.standalone.grpc.QueryServiceGrpc;
import com.xgen.mongot.standalone.grpc.ServiceStatus;
import com.xgen.mongot.standalone.grpc.UpsertRequest;
import com.xgen.mongot.standalone.metrics.StandaloneMetrics;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockMakers;

@RunWith(Parameterized.class)
public class StandaloneListenerTest {
  @Parameterized.Parameters(name = "shared={0}")
  public static Object[] modes() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public boolean shared;

  @Test
  public void publishHealth_reconciliationChanges_updatesAllExtensionServices() throws Exception {
    try (var f = new Listener(this.shared)) {
      f.awaitHealth(SERVING);
      f.ready.set(false);
      f.awaitHealth(NOT_SERVING);

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  DocumentServiceGrpc.newBlockingStub(f.channel)
                      .upsert(UpsertRequest.getDefaultInstance()));
      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);

      f.ready.set(true);
      f.awaitHealth(SERVING);
    }
  }

  @Test
  public void getStatus_serverDeadline_closesTransportWhileWorkerFinishes() throws Exception {
    var entered = new CountDownLatch(1);
    var finish = new CountDownLatch(1);
    var workerContext = new AtomicReference<Context>();
    try (var f = new Listener(this.shared)) {
      f.awaitHealth(SERVING);
      when(f.runtime.serviceStatus())
          .thenAnswer(
              unused -> {
                assertThat(Context.current().getDeadline()).isNotNull();
                workerContext.set(Context.current());
                entered.countDown();
                assertThat(finish.await(10, TimeUnit.SECONDS)).isTrue();
                return ServiceStatus.getDefaultInstance();
              })
          .thenReturn(ServiceStatus.getDefaultInstance());
      var stub = OperationsServiceGrpc.newFutureStub(f.channel);
      var pending = stub.getStatus(GetStatusRequest.getDefaultInstance());
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        var error = assertThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS));
        // A Netty deadline reset can surface as CANCELLED to a client with no local deadline.
        assertThat(Status.fromThrowable(error.getCause()).getCode())
            .isAnyOf(Status.Code.CANCELLED, Status.Code.DEADLINE_EXCEEDED);
        assertThat(Contexts.statusFromCancelled(workerContext.get()).getCode())
            .isEqualTo(Status.Code.DEADLINE_EXCEEDED);
      } finally {
        finish.countDown();
      }
      assertThat(stub.getStatus(GetStatusRequest.getDefaultInstance()).get(5, TimeUnit.SECONDS))
          .isEqualTo(ServiceStatus.getDefaultInstance());
    }
  }

  private static final class Listener implements AutoCloseable {
    private final AtomicBoolean ready = new AtomicBoolean(true);
    private final StandaloneRuntime runtime =
        mock(StandaloneRuntime.class, withSettings().mockMaker(MockMakers.INLINE));
    private final StandaloneGrpcServer extension;
    private final Optional<Server> nativeServer;
    private final ManagedChannel channel;

    private Listener(boolean shared) throws Exception {
      var base = StandaloneConfig.development(Path.of("unused"), 0);
      var config =
          new StandaloneConfig(
              base.dataPath(),
              base.address(),
              base.documentLimits(),
              base.maxInFlightRequests(),
              base.maxInFlightBytes(),
              base.minFreeDiskBytes(),
              base.commitInterval(),
              base.refreshInterval(),
              Duration.ofSeconds(1),
              base.tls(),
              base.principals(),
              shared);
      when(this.runtime.config()).thenReturn(config);
      when(this.runtime.metrics()).thenReturn(StandaloneMetrics.disabled());
      when(this.runtime.managementReady()).thenAnswer(unused -> this.ready.get());
      doCallRealMethod().when(this.runtime).checkWriteAdmission(anyLong());
      this.extension = new StandaloneGrpcServer(this.runtime, shared);
      int port;
      if (shared) {
        var health = new HealthStatusManager();
        health.setStatus("", SERVING);
        this.extension.publishHealth(
            (name, ready) -> health.setStatus(name, ready ? SERVING : NOT_SERVING));
        var builder =
            NettyServerBuilder.forAddress(config.address())
                .addService(health.getHealthService())
                .addStreamTracerFactory(this.extension.deadlineTracer());
        this.extension.services().forEach(builder::addService);
        var server = builder.build().start();
        this.nativeServer = Optional.of(server);
        port = server.getPort();
      } else {
        this.nativeServer = Optional.empty();
        port = this.extension.start().port();
      }
      this.channel = NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
    }

    private void awaitHealth(ServingStatus expected) throws InterruptedException {
      var names =
          List.of(
              CatalogServiceGrpc.SERVICE_NAME,
              DocumentServiceGrpc.SERVICE_NAME,
              QueryServiceGrpc.SERVICE_NAME,
              OperationsServiceGrpc.SERVICE_NAME);
      long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (checkHealth(this.channel, OperationsServiceGrpc.SERVICE_NAME) != expected
          && System.nanoTime() < end) {
        Thread.sleep(20);
      }
      names.forEach(name -> assertThat(checkHealth(this.channel, name)).isEqualTo(expected));
      assertThat(checkHealth(this.channel, ""))
          .isEqualTo(this.nativeServer.isPresent() ? SERVING : expected);
    }

    @Override
    public void close() throws Exception {
      this.channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      if (this.nativeServer.isPresent()) {
        this.nativeServer.get().shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      }
      this.extension.close();
    }
  }
}
