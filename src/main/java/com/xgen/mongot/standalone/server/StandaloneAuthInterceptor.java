package com.xgen.mongot.standalone.server;

import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.config.StandaloneConfig.Principal;
import com.xgen.mongot.standalone.config.StandaloneConfig.Role;
import com.xgen.mongot.standalone.metrics.StandaloneMetrics;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Authentication, role checks, transport-call admission and server-enforced RPC deadlines. */
final class StandaloneAuthInterceptor implements ServerInterceptor {
  private static final Context.Key<Principal> PRINCIPAL = Context.key("standalone-principal");
  private static final Context.Key<String> OPERATION = Context.key("standalone-operation");
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> READ_CONTRACT =
      Metadata.Key.of("mongot-direct-read-contract", Metadata.ASCII_STRING_MARSHALLER);
  private static final String READ_CONTRACT_VERSION = "all-index-reads-v1";
  private static final Principal LOCAL =
      new Principal("local", new byte[0], Set.of(Role.values()), Set.of("*"));
  private final StandaloneConfig config;
  private final ScheduledExecutorService deadlines;
  private final Semaphore transportCalls;
  private final StandaloneMetrics metrics;

  StandaloneAuthInterceptor(
      StandaloneConfig config, ScheduledExecutorService deadlines, StandaloneMetrics metrics) {
    this.config = config;
    this.deadlines = deadlines;
    this.transportCalls = new Semaphore(config.maxInFlightRequests() + 16);
    this.metrics = metrics;
  }

  static Principal principal() {
    return Optional.ofNullable(PRINCIPAL.get())
        .orElseThrow(() -> Status.UNAUTHENTICATED.asRuntimeException());
  }

  static String operation() {
    return Optional.ofNullable(OPERATION.get()).orElse("unknown");
  }

  static void requireRole(Role role) {
    if (!principal().roles().contains(role)) {
      throw Status.PERMISSION_DENIED
          .withDescription("Principal requires the " + role.name() + " role")
          .asRuntimeException();
    }
  }

  static void requireIndex(String indexId) {
    if (!principal().allows(indexId)) {
      throw Status.PERMISSION_DENIED
          .withDescription("Principal cannot access this index")
          .asRuntimeException();
    }
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    Principal principal;
    try {
      principal = authenticate(call, headers);
      String contract = headers.get(READ_CONTRACT);
      if (contract != null && !contract.equals(READ_CONTRACT_VERSION)) {
        throw Status.FAILED_PRECONDITION
            .withDescription("Unsupported direct read contract")
            .asRuntimeException();
      }
      String service = call.getMethodDescriptor().getServiceName();
      Role role =
          service != null
                  && (service.endsWith("CatalogService") || service.endsWith("OperationsService"))
              ? Role.CATALOG
              : service != null && service.endsWith("DocumentService") ? Role.WRITE : Role.QUERY;
      if (!"grpc.health.v1.Health".equals(service) && !principal.roles().contains(role)) {
        throw Status.PERMISSION_DENIED
            .withDescription("Principal is not authorized for this service")
            .asRuntimeException();
      }
    } catch (io.grpc.StatusRuntimeException e) {
      this.metrics.count("authentication.failures", 1, "status", e.getStatus().getCode().name());
      call.close(e.getStatus(), new Metadata());
      return new ServerCall.Listener<>() {};
    }
    if (!this.transportCalls.tryAcquire()) {
      this.metrics.count("rpc.rejected", 1, "stage", "transport");
      call.close(
          Status.RESOURCE_EXHAUSTED.withDescription("Too many concurrent RPCs"), new Metadata());
      return new ServerCall.Listener<>() {};
    }
    var context =
        Context.current()
            .withValue(PRINCIPAL, principal)
            .withValue(OPERATION, call.getMethodDescriptor().getBareMethodName())
            .withDeadlineAfter(
                this.config.maxRpcDuration().toMillis(), TimeUnit.MILLISECONDS, this.deadlines);
    var released = new java.util.concurrent.atomic.AtomicBoolean();
    Runnable release =
        () -> {
          if (released.compareAndSet(false, true)) {
            this.transportCalls.release();
            context.cancel(null);
          }
        };
    var acknowledged =
        new io.grpc.ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
          @Override
          public void sendHeaders(Metadata responseHeaders) {
            responseHeaders.put(READ_CONTRACT, READ_CONTRACT_VERSION);
            super.sendHeaders(responseHeaders);
          }
        };
    try {
      return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
          Contexts.interceptCall(context, acknowledged, headers, next)) {
        @Override
        public void onComplete() {
          try {
            super.onComplete();
          } finally {
            release.run();
          }
        }

        @Override
        public void onCancel() {
          try {
            super.onCancel();
          } finally {
            release.run();
          }
        }
      };
    } catch (RuntimeException e) {
      release.run();
      throw e;
    }
  }

  private Principal authenticate(ServerCall<?, ?> call, Metadata headers) {
    List<Principal> principals = this.config.principals();
    if (this.config.sharedListener()) {
      var remote = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
      boolean loopback =
          remote instanceof InetSocketAddress address
              && address.getAddress() != null
              && address.getAddress().isLoopbackAddress();
      if (!loopback
          && (principals.isEmpty()
              || call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION) == null)) {
        throw Status.UNAUTHENTICATED
            .withDescription(
                "Remote standalone API access requires TLS and configured bearer principals")
            .asRuntimeException();
      }
    }
    if (principals.isEmpty()) {
      return LOCAL;
    }
    String authorization = headers.get(AUTHORIZATION);
    if (authorization == null
        || !authorization.startsWith("Bearer ")
        || authorization.length() > 4103
        || authorization.substring(7).chars().anyMatch(value -> value < 33 || value > 126)) {
      throw Status.UNAUTHENTICATED
          .withDescription("A bearer token is required")
          .asRuntimeException();
    }
    byte[] digest =
        LocalIndexCatalog.digest(authorization.substring(7).getBytes(StandardCharsets.US_ASCII));
    return principals.stream()
        .filter(principal -> MessageDigest.isEqual(principal.tokenDigest(), digest))
        .findFirst()
        .orElseThrow(
            () ->
                Status.UNAUTHENTICATED
                    .withDescription("Invalid bearer token")
                    .asRuntimeException());
  }
}
