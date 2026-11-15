package com.xgen.mongot.standalone.server;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.GetStatusRequest;
import com.xgen.mongot.standalone.grpc.OperationsServiceGrpc;
import com.xgen.mongot.standalone.grpc.ServiceStatus;
import com.xgen.mongot.standalone.metrics.StandaloneMetrics;
import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLSession;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class StandaloneAuthInterceptorTest {
  @Test
  public void interceptCall_sharedRemotePlaintext_deniesApiWithoutChangingListener() {
    assertThat(authenticate("192.0.2.10", false, false)).isEqualTo(Status.Code.UNAUTHENTICATED);
  }

  @Test
  public void interceptCall_sharedRemoteTlsWithoutPrincipals_deniesApi() {
    assertThat(authenticate("192.0.2.10", true, false)).isEqualTo(Status.Code.UNAUTHENTICATED);
  }

  @Test
  public void interceptCall_sharedRemotePlaintextWithBearer_deniesApi() {
    assertThat(authenticate("192.0.2.10", false, true)).isEqualTo(Status.Code.UNAUTHENTICATED);
  }

  @Test
  public void interceptCall_sharedRemoteTlsWithBearer_allowsApi() {
    assertThat(authenticate("192.0.2.10", true, true)).isEqualTo(Status.Code.OK);
  }

  @Test
  public void interceptCall_sharedLoopbackWithoutPrincipals_preservesLocalAccess() {
    assertThat(authenticate("127.0.0.1", false, false)).isEqualTo(Status.Code.OK);
  }

  @SuppressWarnings("unchecked")
  private static Status.Code authenticate(String peer, boolean tls, boolean authenticated) {
    var deadlines = Executors.newSingleThreadScheduledExecutor();
    var meters = new SimpleMeterRegistry();
    try {
      String token = "test-token-" + "x".repeat(32);
      var principal =
          new StandaloneConfig.Principal(
              "test",
              LocalIndexCatalog.digest(token.getBytes(StandardCharsets.US_ASCII)),
              Set.of(StandaloneConfig.Role.CATALOG),
              Set.of("*"));
      var config =
          new StandaloneConfig(
              Path.of("unused"),
              new InetSocketAddress("0.0.0.0", 27028),
              StandaloneConfig.Limits.defaults(),
              4,
              67108864,
              0,
              Duration.ofSeconds(1),
              Duration.ofSeconds(1),
              Duration.ofSeconds(30),
              Optional.empty(),
              authenticated ? List.of(principal) : List.of(),
              true);
      var attributes =
          Attributes.newBuilder()
              .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress(peer, 12345));
      if (tls) {
        attributes.set(Grpc.TRANSPORT_ATTR_SSL_SESSION, mock(SSLSession.class));
      }
      ServerCall<GetStatusRequest, ServiceStatus> call = mock(ServerCall.class);
      when(call.getAttributes()).thenReturn(attributes.build());
      when(call.getMethodDescriptor()).thenReturn(OperationsServiceGrpc.getGetStatusMethod());
      ServerCallHandler<GetStatusRequest, ServiceStatus> next = mock(ServerCallHandler.class);
      when(next.startCall(any(), any())).thenReturn(new ServerCall.Listener<>() {});
      var metadata = new Metadata();
      if (authenticated) {
        metadata.put(
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
      }
      var interceptor =
          new StandaloneAuthInterceptor(config, deadlines, new StandaloneMetrics(meters));
      var listener = interceptor.interceptCall(call, metadata, next);
      if (tls && authenticated || peer.equals("127.0.0.1")) {
        verify(next).startCall(any(), any());
        listener.onComplete();
        return Status.Code.OK;
      }
      verify(next, never()).startCall(any(), any());
      var status = ArgumentCaptor.forClass(Status.class);
      verify(call).close(status.capture(), any());
      return status.getValue().getCode();
    } finally {
      deadlines.shutdownNow();
      meters.close();
    }
  }
}
