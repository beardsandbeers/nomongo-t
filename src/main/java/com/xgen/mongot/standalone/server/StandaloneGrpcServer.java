package com.xgen.mongot.standalone.server;

import com.xgen.mongot.standalone.grpc.CatalogServiceGrpc;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DeleteRequest;
import com.xgen.mongot.standalone.grpc.DeleteResponse;
import com.xgen.mongot.standalone.grpc.DocumentServiceGrpc;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.DropIndexRequest;
import com.xgen.mongot.standalone.grpc.GetIndexFieldStatsRequest;
import com.xgen.mongot.standalone.grpc.GetIndexMappingsRequest;
import com.xgen.mongot.standalone.grpc.GetIndexMappingsResponse;
import com.xgen.mongot.standalone.grpc.GetIndexRequest;
import com.xgen.mongot.standalone.grpc.GetIndexStatsRequest;
import com.xgen.mongot.standalone.grpc.GetStatusRequest;
import com.xgen.mongot.standalone.grpc.IndexDescription;
import com.xgen.mongot.standalone.grpc.IndexFieldStatsResponse;
import com.xgen.mongot.standalone.grpc.IndexStats;
import com.xgen.mongot.standalone.grpc.ListIndexesRequest;
import com.xgen.mongot.standalone.grpc.ListIndexesResponse;
import com.xgen.mongot.standalone.grpc.OperationsServiceGrpc;
import com.xgen.mongot.standalone.grpc.QueryServiceGrpc;
import com.xgen.mongot.standalone.grpc.ServiceStatus;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import com.xgen.mongot.standalone.grpc.UpsertRequest;
import com.xgen.mongot.standalone.grpc.UpsertResponse;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslProvider;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/** Ordinary generated protobuf services on a dedicated standalone listener. */
public final class StandaloneGrpcServer implements AutoCloseable {
  private final StandaloneRuntime runtime;
  private final RpcDispatcher dispatcher;
  private final ScheduledThreadPoolExecutor deadlines;
  private final HealthStatusManager health = new HealthStatusManager();
  private final Optional<Server> server;
  private final List<ServerServiceDefinition> services;
  private final StandaloneQueryService queries;
  private final StandaloneDeleteByQuery deleteByQuery;

  /**
   * Registers bounded standalone services and their transport security without starting listeners.
   */
  public StandaloneGrpcServer(StandaloneRuntime runtime) throws IOException {
    this(runtime, false);
  }

  /** Constructs only service definitions when attached to a native listener. */
  public StandaloneGrpcServer(StandaloneRuntime runtime, boolean shared) throws IOException {
    this.runtime = runtime;
    this.dispatcher = new RpcDispatcher(runtime.config(), runtime.metrics());
    this.queries = new StandaloneQueryService(runtime, this.dispatcher);
    this.deleteByQuery = new StandaloneDeleteByQuery(runtime);
    runtime.onIndexPublication(this::generationRetired);
    this.deadlines =
        new ScheduledThreadPoolExecutor(
            1, Thread.ofPlatform().name("standalone-deadlines").factory());
    this.deadlines.setRemoveOnCancelPolicy(true);
    var interceptor =
        new StandaloneAuthInterceptor(runtime.config(), this.deadlines, runtime.metrics());
    this.services =
        List.of(
            ServerInterceptors.intercept(new Catalog(), interceptor),
            ServerInterceptors.intercept(new Documents(), interceptor),
            ServerInterceptors.intercept(this.queries, interceptor),
            ServerInterceptors.intercept(new Operations(), interceptor));
    var builder =
        NettyServerBuilder.forAddress(runtime.config().address())
            .directExecutor()
            .maxInboundMessageSize(runtime.config().documentLimits().maxBatchBytes() + 65536)
            .maxConcurrentCallsPerConnection(runtime.config().maxInFlightRequests() + 16);
    this.services.forEach(builder::addService);
    builder.addService(ServerInterceptors.intercept(this.health.getHealthService(), interceptor));
    if (!shared && runtime.config().tls().isPresent()) {
      var tls = runtime.config().tls().get();
      builder.sslContext(
          GrpcSslContexts.forServer(tls.certificate().toFile(), tls.privateKey().toFile())
              .sslProvider(SslProvider.JDK)
              .build());
    }
    this.server = shared ? Optional.empty() : Optional.of(builder.build());
    this.health.setStatus("", ServingStatus.SERVING);
    this.health.setStatus(CatalogServiceGrpc.SERVICE_NAME, ServingStatus.SERVING);
    this.health.setStatus(DocumentServiceGrpc.SERVICE_NAME, ServingStatus.SERVING);
    this.health.setStatus(QueryServiceGrpc.SERVICE_NAME, ServingStatus.SERVING);
    this.health.setStatus(OperationsServiceGrpc.SERVICE_NAME, ServingStatus.SERVING);
    this.deadlines.scheduleWithFixedDelay(
        () ->
            this.health.setStatus(
                "", runtime.managementReady() ? ServingStatus.SERVING : ServingStatus.NOT_SERVING),
        1,
        1,
        TimeUnit.SECONDS);
    this.deadlines.scheduleWithFixedDelay(this.queries::expireIdle, 10, 10, TimeUnit.SECONDS);
  }

  public List<ServerServiceDefinition> services() {
    return this.services;
  }

  public void publishHealth(BiConsumer<String, Boolean> update) {
    this.deadlines.scheduleWithFixedDelay(
        () ->
            this.services.forEach(
                service ->
                    update.accept(
                        service.getServiceDescriptor().getName(), this.runtime.managementReady())),
        0,
        1,
        TimeUnit.SECONDS);
  }

  public StandaloneGrpcServer start() throws IOException {
    this.server.orElseThrow().start();
    return this;
  }

  public int port() {
    return this.server.orElseThrow().getPort();
  }

  public void awaitTermination() throws InterruptedException {
    this.server.orElseThrow().awaitTermination();
  }

  @Override
  public synchronized void close() throws InterruptedException {
    this.health.enterTerminalState();
    if (this.server.isPresent()) {
      var owned = this.server.get();
      owned.shutdown();
      if (!owned.awaitTermination(30, TimeUnit.SECONDS)) {
        owned.shutdownNow();
        owned.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
    this.dispatcher.close();
    this.queries.close();
    this.deadlines.shutdownNow();
    this.deadlines.awaitTermination(10, TimeUnit.SECONDS);
  }

  void generationRetired(String generation) {
    if (!generation.isEmpty()) {
      this.queries.generationRetired(generation);
      this.runtime.statistics().invalidateGeneration(generation);
    }
  }

  private final class Catalog extends CatalogServiceGrpc.CatalogServiceImplBase {
    @Override
    public void createIndex(CreateIndexRequest request, StreamObserver<IndexDescription> observer) {
      StandaloneGrpcServer.this.dispatcher.unary(
          request.getSerializedSize(),
          observer,
          () -> {
            StandaloneAuthInterceptor.requireIndex("*");
            var result =
                StandaloneGrpcServer.this
                    .runtime
                    .ingestion()
                    .coordinate(
                        () -> StandaloneGrpcServer.this.runtime.catalog().createIndex(request));
            StandaloneGrpcServer.this.runtime.catalogChanged();
            return result;
          });
    }

    @Override
    public void updateIndex(UpdateIndexRequest request, StreamObserver<IndexDescription> observer) {
      StandaloneGrpcServer.this.dispatcher.unary(
          request.getSerializedSize(),
          observer,
          () -> {
            StandaloneAuthInterceptor.requireIndex(request.getIndexId());
            var result = StandaloneGrpcServer.this.runtime.updateIndex(request);
            if (result.getDesiredRevision() != request.getExpectedRevision()
                && !result.getLifecycleState().equals("READY")) {
              StandaloneGrpcServer.this.generationRetired(result.getGenerationId());
            }
            if (!result.getLifecycleState().equals("READY")) {
              StandaloneGrpcServer.this.runtime.catalogChanged();
            }
            return result;
          });
    }

    @Override
    public void dropIndex(DropIndexRequest request, StreamObserver<IndexDescription> observer) {
      StandaloneGrpcServer.this.dispatcher.unary(
          request.getSerializedSize(),
          observer,
          () -> {
            StandaloneAuthInterceptor.requireIndex(request.getIndexId());
            var result =
                StandaloneGrpcServer.this
                    .runtime
                    .ingestion()
                    .coordinate(
                        () -> StandaloneGrpcServer.this.runtime.catalog().dropIndex(request));
            StandaloneGrpcServer.this.queries.indexDropped(request.getIndexId());
            StandaloneGrpcServer.this.runtime.statistics().invalidate(request.getIndexId());
            StandaloneGrpcServer.this.runtime.catalogChanged();
            return result;
          });
    }

    @Override
    public void getIndex(GetIndexRequest request, StreamObserver<IndexDescription> observer) {
      StandaloneGrpcServer.this.dispatcher.unary(
          request.getSerializedSize(),
          observer,
          () -> {
            StandaloneAuthInterceptor.requireIndex(request.getIndexId());
            return StandaloneGrpcServer.this.runtime.describeIndex(request.getIndexId());
          });
    }

    @Override
    public void getIndexMappings(
        GetIndexMappingsRequest request, StreamObserver<GetIndexMappingsResponse> observer) {
      StandaloneGrpcServer.this.dispatcher.unary(
          request.getSerializedSize(),
          observer,
          () -> {
            StandaloneAuthInterceptor.requireIndex(request.getIndexId());
            return StandaloneGrpcServer.this.runtime.getIndexMappings(request);
          });
    }

    @Override
    public void getIndexStats(GetIndexStatsRequest request, StreamObserver<IndexStats> observer) {
      StandaloneGrpcServer.this.dispatcher.async(
          request.getSerializedSize(),
          observer,
          () -> {
            StandaloneAuthInterceptor.requireIndex(request.getIndexId());
            return StandaloneGrpcServer.this.runtime.statistics().summary(request);
          });
    }

    @Override
    public void getIndexFieldStats(
        GetIndexFieldStatsRequest request, StreamObserver<IndexFieldStatsResponse> observer) {
      StandaloneGrpcServer.this.dispatcher.unary(
          request.getSerializedSize(),
          observer,
          () -> {
            StandaloneAuthInterceptor.requireIndex(request.getIndexId());
            return StandaloneGrpcServer.this
                .runtime
                .statistics()
                .fields(request, StandaloneAuthInterceptor.principal().name());
          });
    }

    @Override
    public void listIndexes(
        ListIndexesRequest request, StreamObserver<ListIndexesResponse> observer) {
      StandaloneGrpcServer.this.dispatcher.unary(
          request.getSerializedSize(),
          observer,
          () -> {
            var result = ListIndexesResponse.newBuilder();
            for (var definition : StandaloneGrpcServer.this.runtime.catalog().definitions()) {
              String id = definition.getIndexId().toHexString();
              if (StandaloneAuthInterceptor.principal().allows(id)) {
                result.addIndexes(StandaloneGrpcServer.this.runtime.describeIndex(id));
              }
            }
            return result.build();
          });
    }
  }

  private final class Documents extends DocumentServiceGrpc.DocumentServiceImplBase {
    @Override
    public void deleteByQuery(
        com.xgen.mongot.standalone.grpc.DeleteByQueryRequest request,
        StreamObserver<com.xgen.mongot.standalone.grpc.DeleteByQueryResponse> observer) {
      StandaloneGrpcServer.this.dispatcher.unary(
          request.getSerializedSize(),
          observer,
          () -> StandaloneGrpcServer.this.deleteByQuery.execute(request));
    }

    @Override
    public void upsert(UpsertRequest request, StreamObserver<UpsertResponse> observer) {
      StandaloneGrpcServer.this.dispatcher.async(
          request.getSerializedSize(),
          observer,
          () -> {
            StandaloneAuthInterceptor.requireIndex(request.getIndexId());
            StandaloneGrpcServer.this.runtime.checkWriteAdmission(request.getSerializedSize());
            return StandaloneGrpcServer.this
                .runtime
                .ingestion()
                .writeAsync(
                    request.getIndexId(),
                    () ->
                        StandaloneGrpcServer.this
                            .runtime
                            .catalog()
                            .getIndex(request.getIndexId())
                            .getDesiredRevision(),
                    request.getGenerationId(),
                    request.getDocumentsBsonList(),
                    false,
                    request.hasWriteConcern()
                        ? request.getWriteConcern()
                        : DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_INDEXED)
                .thenApply(
                    generation ->
                        UpsertResponse.newBuilder()
                            .setIndexId(request.getIndexId())
                            .setGenerationId(generation)
                            .setIndexedCount(request.getDocumentsBsonCount())
                            .build());
          });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> observer) {
      StandaloneGrpcServer.this.dispatcher.async(
          request.getSerializedSize(),
          observer,
          () -> {
            StandaloneAuthInterceptor.requireIndex(request.getIndexId());
            StandaloneGrpcServer.this.runtime.checkWriteAdmission(request.getSerializedSize());
            return StandaloneGrpcServer.this
                .runtime
                .ingestion()
                .writeAsync(
                    request.getIndexId(),
                    () ->
                        StandaloneGrpcServer.this
                            .runtime
                            .catalog()
                            .getIndex(request.getIndexId())
                            .getDesiredRevision(),
                    request.getGenerationId(),
                    request.getIdsBsonList(),
                    true,
                    request.hasWriteConcern()
                        ? request.getWriteConcern()
                        : DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_INDEXED)
                .thenApply(
                    generation ->
                        DeleteResponse.newBuilder()
                            .setIndexId(request.getIndexId())
                            .setGenerationId(generation)
                            .setProcessedCount(request.getIdsBsonCount())
                            .build());
          });
    }
  }

  private final class Operations extends OperationsServiceGrpc.OperationsServiceImplBase {
    @Override
    public void getStatus(GetStatusRequest request, StreamObserver<ServiceStatus> observer) {
      StandaloneGrpcServer.this.dispatcher.unary(
          request.getSerializedSize(),
          observer,
          () -> {
            StandaloneAuthInterceptor.requireIndex("*");
            try {
              return StandaloneGrpcServer.this.runtime.serviceStatus();
            } catch (Exception e) {
              throw new CompletionException(e);
            }
          });
    }
  }
}
