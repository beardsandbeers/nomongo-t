package com.xgen.mongot.standalone.server;

import io.grpc.Metadata;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerStreamTracer;
import io.grpc.internal.GrpcUtil;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Sets the server ceiling before gRPC creates its transport cancellation context. */
final class StandaloneRpcDeadline extends ServerStreamTracer.Factory {
  private static final ServerStreamTracer NOOP = new ServerStreamTracer() {};
  private final Set<String> methods;
  private final long maxNanos;

  StandaloneRpcDeadline(List<ServerServiceDefinition> services, Duration maximum) {
    this.methods =
        services.stream()
            .flatMap(service -> service.getMethods().stream())
            .map(method -> method.getMethodDescriptor().getFullMethodName())
            .collect(Collectors.toUnmodifiableSet());
    this.maxNanos = TimeUnit.NANOSECONDS.convert(maximum);
  }

  @Override
  public ServerStreamTracer newServerStreamTracer(String fullMethodName, Metadata headers) {
    if (this.methods.contains(fullMethodName)) {
      Long requested;
      try {
        requested = headers.get(GrpcUtil.TIMEOUT_KEY);
      } catch (IllegalArgumentException malformed) {
        // Let the transport report malformed client metadata through its normal error path.
        return NOOP;
      }
      if (requested == null || requested > this.maxNanos) {
        headers.discardAll(GrpcUtil.TIMEOUT_KEY);
        headers.put(GrpcUtil.TIMEOUT_KEY, this.maxNanos);
      }
    }
    return NOOP;
  }
}
