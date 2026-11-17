package com.xgen.mongot.standalone.server;

import com.google.errorprone.annotations.Var;
import com.google.protobuf.ByteString;
import com.xgen.mongot.cursor.CursorQuery;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.lucene.explain.explainers.MetadataFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainTooLargeException;
import com.xgen.mongot.index.query.QueryExecutionContext;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.standalone.StandaloneBson;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.grpc.ExplainVerbosity;
import com.xgen.mongot.standalone.grpc.GetMoreRequest;
import com.xgen.mongot.standalone.grpc.KillCursorRequest;
import com.xgen.mongot.standalone.grpc.KillCursorResponse;
import com.xgen.mongot.standalone.grpc.QueryRequest;
import com.xgen.mongot.standalone.grpc.QueryResponse;
import com.xgen.mongot.standalone.grpc.QueryServiceGrpc;
import com.xgen.mongot.standalone.grpc.SearchMetaRequest;
import com.xgen.mongot.standalone.grpc.SearchMetaResponse;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.QueryCancellation;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;

/** Public queries using ordinary mongot parsers, readers and snapshot-pinning cursors. */
final class StandaloneQueryService extends QueryServiceGrpc.QueryServiceImplBase
    implements AutoCloseable {
  private static final long IDLE_NANOS = Duration.ofMinutes(2).toNanos();
  private final StandaloneRuntime runtime;
  private final RpcDispatcher dispatcher;
  private final StandaloneJoinPlanner joinPlanner;
  private final Semaphore capacity = new Semaphore(1024);
  private final ConcurrentHashMap<Long, Lease> leases = new ConcurrentHashMap<>();

  /** A stable callback for producers that retain it across RPCs, including counting workers. */
  private static final class CursorCancellation {
    private volatile Context context = Context.current();

    private void activate(Context context) {
      this.context = context;
      check();
    }

    private void check() {
      StandaloneJoinPlanner.checkCancellation(this.context);
    }
  }

  private static final class Lease {
    private final String principal;
    private final String index;
    private final String generation;
    private final com.xgen.mongot.config.manager.GenerationReadLeases.Lease readLease;
    private final Optional<StandaloneJoinPlanner.Plan> joinPlan;
    private final CursorCancellation cancellation;
    private volatile long lastAccess = System.nanoTime();

    private Lease(
        String principal,
        String index,
        String generation,
        com.xgen.mongot.config.manager.GenerationReadLeases.Lease readLease,
        Optional<StandaloneJoinPlanner.Plan> joinPlan,
        CursorCancellation cancellation) {
      this.principal = principal;
      this.index = index;
      this.generation = generation;
      this.readLease = readLease;
      this.joinPlan = joinPlan;
      this.cancellation = cancellation;
    }
  }

  StandaloneQueryService(StandaloneRuntime runtime, RpcDispatcher dispatcher) {
    this.runtime = runtime;
    this.dispatcher = dispatcher;
    this.joinPlanner = new StandaloneJoinPlanner(runtime);
  }

  @Override
  public void search(QueryRequest request, StreamObserver<QueryResponse> observer) {
    this.dispatcher.unary(
        request.getSerializedSize(), observer, () -> query(request, false), this::abandoned);
  }

  @Override
  public void searchMeta(SearchMetaRequest request, StreamObserver<SearchMetaResponse> observer) {
    this.dispatcher.unary(
        request.getSerializedSize(),
        observer,
        () -> new StandaloneSearchMeta(this.runtime, this.joinPlanner).execute(request));
  }

  @Override
  public void vectorSearch(QueryRequest request, StreamObserver<QueryResponse> observer) {
    this.dispatcher.unary(
        request.getSerializedSize(), observer, () -> query(request, true), this::abandoned);
  }

  private void abandoned(QueryResponse response) {
    var lease = this.leases.get(response.getCursorId());
    if (lease != null) {
      synchronized (lease) {
        release(response.getCursorId(), lease);
      }
    }
  }

  QueryResponse query(QueryRequest request, boolean vector) {
    StandaloneAuthInterceptor.requireIndex(request.getIndexId());
    var generation = this.runtime.reads().resolve(request.getIndexId(), request.getGenerationId());
    String generationId = generation.getGenerationId().uniqueString();
    if (!request.getGenerationId().isEmpty() && !generationId.equals(request.getGenerationId())) {
      throw Status.FAILED_PRECONDITION
          .withDescription("Requested generation is no longer live")
          .asRuntimeException();
    }
    int size = batchSize(request.getBatchSize());
    var verbosity = explainVerbosity(request.getExplainVerbosity());
    boolean plannerOnly =
        verbosity.filter(value -> value == Explain.Verbosity.QUERY_PLANNER).isPresent();
    if (!this.capacity.tryAcquire()) {
      throw Status.RESOURCE_EXHAUSTED
          .withDescription("Open cursor capacity is full")
          .asRuntimeException();
    }
    @Var long cursorId = 0;
    @Var Lease cursorLease = null;
    @Var StandaloneJoinPlanner.Plan joinPlan = null;
    @Var com.xgen.mongot.config.manager.GenerationReadLeases.Lease readLease = null;
    var cancellation = new CursorCancellation();
    try (var unused =
            Explain.setup(verbosity, Optional.of(generation.getDefinition().getNumPartitions()));
        var scope = QueryCancellation.openScope(cancellation::check)) {
      cancellation.check();
      readLease = this.runtime.reads().acquire(generation.getGenerationId());
      var definition = readLease.definition();
      Explain.getQueryInfo()
          .map(
              info ->
                  info.getFeatureExplainer(
                      MetadataFeatureExplainer.class, MetadataFeatureExplainer::new))
          .ifPresent(metadata -> metadata.setIndexName(definition.getName()));
      var expression = new BsonDocument();
      expression.putAll(
          StandaloneBson.document(request.getQueryBson().toByteArray(), 1024 * 1024, 100));
      if (expression.containsKey("index")
          && !expression.getString("index").getValue().equals(definition.getName())) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Query index does not match index_id")
            .asRuntimeException();
      }
      expression.put("index", new org.bson.BsonString(definition.getName()));
      @Var CursorQuery query;
      if (vector) {
        rejectUnusedBindings(request);
        var parsed = VectorSearchQuery.fromBson(expression);
        var deadline =
            Optional.ofNullable(Context.current().getDeadline())
                .map(
                    value ->
                        System.currentTimeMillis()
                            + Math.max(0, value.timeRemaining(TimeUnit.MILLISECONDS)));
        query =
            new CursorQuery.Vector(
                this.runtime.materializeVectorQuery(parsed, definition),
                QueryExecutionContext.withDeadline(deadline));
      } else {
        @Var
        var parsed =
            SearchQuery.fromBson(
                expression,
                QueryOptimizationFlags.DEFAULT_OPTIONS,
                this.runtime.searchQueryParseOptions());
        this.runtime.validateSearchQuery(parsed);
        parsed = this.runtime.materializeSearchQuery(parsed, definition);
        if (StandaloneJoinPlanner.containsJoin(StandaloneJoinPlanner.operator(parsed))) {
          joinPlan =
              this.joinPlanner.prepare(
                  parsed, generation.getGenerationId(), request.getJoinIndexesList());
          query = new CursorQuery.Search(joinPlan.query(), Optional.of(joinPlan.snapshot()));
        } else {
          rejectUnusedBindings(request);
          query = new CursorQuery.Search(parsed);
        }
      }
      if (readLease.retired()) {
        throw Status.UNAVAILABLE
            .withDescription("Query generation was retired")
            .asRuntimeException();
      }
      com.xgen.mongot.cursor.SearchCursorInfo cursor;
      try (var snapshot =
          query instanceof CursorQuery.Search search && search.snapshot().isEmpty()
              ? this.runtime.acquireQuerySnapshot(
                  readLease, ((SearchQuery) search.query()).concurrent())
              : null) {
        if (snapshot != null) {
          query = new CursorQuery.Search(query.getQuery(), Optional.of(snapshot));
        }
        if (query instanceof CursorQuery.Search search) {
          search.snapshot().orElseThrow().setCancellationCheck(cancellation::check);
        }
        cancellation.check();
        cursor =
            this.runtime
                .cursors()
                .newCursor(
                    generation.getGenerationId(),
                    query,
                    new QueryCursorOptions(Optional.empty(), Optional.of(size), false),
                    QueryOptimizationFlags.DEFAULT_OPTIONS);
        cursorId = cursor.cursorId;
      }
      if (joinPlan != null) {
        joinPlan.verifyDependencies();
      }
      cancellation.check();
      var batch =
          this.runtime
              .cursors()
              .getNextBatch(
                  cursorId,
                  Bytes.ofBytes(4 * 1024 * 1024),
                  new BatchCursorOptions(Optional.empty(), Optional.of(size)));
      cancellation.check();
      var response =
          QueryResponse.newBuilder()
              .setGenerationId(generationId)
              .setBatchBson(
                  encode(
                      new BsonDocument(
                          "results", plannerOnly ? new org.bson.BsonArray() : batch.batch)))
              .setMetadataBson(encode(cursor.metaResults.applyAggregationLimit().toBson()));
      batch.explainResult.ifPresent(explain -> response.setExplainBson(encode(explain.toBson())));
      if (joinPlan != null) {
        joinPlan.verifyDependencies();
      }
      if (readLease.retired()) {
        throw Status.UNAVAILABLE
            .withDescription("Query generation was retired")
            .asRuntimeException();
      }
      if (!plannerOnly && !batch.exhausted) {
        var lease =
            new Lease(
                StandaloneAuthInterceptor.principal().name(),
                request.getIndexId(),
                generationId,
                readLease,
                Optional.ofNullable(joinPlan),
                cancellation);
        synchronized (lease) {
          this.leases.put(cursorId, lease);
          // Transfer cleanup ownership before callbacks can run or registration can fail.
          // The same monitor keeps retirement from closing the plan during registration.
          cursorLease = lease;
          long registeredId = cursorId;
          readLease.onInvalidated(() -> discardCursor(registeredId));
          if (joinPlan != null) {
            joinPlan.onInvalidated(() -> discardCursor(registeredId));
          }
          if (readLease.retired()) {
            throw Status.UNAVAILABLE
                .withDescription("Query generation was retired")
                .asRuntimeException();
          }
          if (joinPlan != null) {
            joinPlan.verifyDependencies();
          }
          response.setCursorId(cursorId);
        }
      }
      return response.build();
    } catch (Exception e) {
      if (cursorLease != null) {
        release(cursorId, cursorLease);
      }
      throw queryError(e);
    } finally {
      if (cursorLease == null) {
        try {
          if (cursorId != 0) {
            this.runtime.cursors().killCursor(cursorId);
          }
        } finally {
          try {
            if (joinPlan != null) {
              closeJoinPlan(joinPlan);
            }
          } finally {
            if (readLease != null) {
              readLease.close();
            }
            this.capacity.release();
          }
        }
      }
    }
  }

  private static void rejectUnusedBindings(QueryRequest request) {
    if (request.getJoinIndexesCount() != 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Join index bindings require a join operator")
          .asRuntimeException();
    }
  }

  @Override
  public void getMore(GetMoreRequest request, StreamObserver<QueryResponse> observer) {
    this.dispatcher.unary(
        request.getSerializedSize(), observer, () -> nextBatch(request), this::abandoned);
  }

  QueryResponse nextBatch(GetMoreRequest request) {
    var lease = requireLease(request.getCursorId());
    synchronized (lease) {
      try (var unused =
              Explain.setup(this.runtime.cursors().getExplainQueryState(request.getCursorId()));
          var scope = QueryCancellation.openScope(lease.cancellation::check)) {
        lease.cancellation.activate(Context.current());
        lease.joinPlan.ifPresent(plan -> plan.activate(lease.cancellation::check));
        var batch =
            this.runtime
                .cursors()
                .getNextBatch(
                    request.getCursorId(),
                    Bytes.ofBytes(4 * 1024 * 1024),
                    new BatchCursorOptions(
                        Optional.empty(), Optional.of(batchSize(request.getBatchSize()))));
        lease.cancellation.check();
        lease.joinPlan.ifPresent(StandaloneJoinPlanner.Plan::verifyDependencies);
        if (lease.readLease.retired()) {
          throw Status.NOT_FOUND
              .withDescription("Cursor generation was retired")
              .asRuntimeException();
        }
        lease.lastAccess = System.nanoTime();
        if (batch.exhausted) {
          release(request.getCursorId(), lease);
        }
        var response =
            QueryResponse.newBuilder()
                .setCursorId(batch.exhausted ? 0 : request.getCursorId())
                .setGenerationId(lease.generation)
                .setBatchBson(encode(new BsonDocument("results", batch.batch)));
        batch.explainResult.ifPresent(explain -> response.setExplainBson(encode(explain.toBson())));
        return response.build();
      } catch (Exception e) {
        release(request.getCursorId(), lease);
        throw queryError(e);
      }
    }
  }

  void discardCursor(long id) {
    var lease = this.leases.get(id);
    if (lease != null) {
      synchronized (lease) {
        release(id, lease);
      }
    }
  }

  @Override
  public void killCursor(KillCursorRequest request, StreamObserver<KillCursorResponse> observer) {
    this.dispatcher.unary(
        request.getSerializedSize(),
        observer,
        () -> {
          var lease = requireLease(request.getCursorId());
          synchronized (lease) {
            release(request.getCursorId(), lease);
          }
          return KillCursorResponse.getDefaultInstance();
        });
  }

  private Lease requireLease(long id) {
    var lease = this.leases.get(id);
    if (lease == null
        || !lease.principal.equals(StandaloneAuthInterceptor.principal().name())
        || !StandaloneAuthInterceptor.principal().allows(lease.index)) {
      throw Status.NOT_FOUND
          .withDescription("Cursor does not exist for this principal")
          .asRuntimeException();
    }
    synchronized (lease) {
      // A search already in progress can register its cursor after drop's invalidation pass.
      // Consult the index catalog so that such a cursor cannot serve another batch.
      try {
        if (lease.readLease.retired()
            || !this.runtime.reads().isGenerationReady(lease.readLease.index().getGenerationId())) {
          throw Status.NOT_FOUND
              .withDescription("Cursor generation was retired")
              .asRuntimeException();
        }
        lease.joinPlan.ifPresent(StandaloneJoinPlanner.Plan::verifyDependencies);
      } catch (io.grpc.StatusRuntimeException e) {
        release(id, lease);
        throw e;
      }
    }
    return lease;
  }

  void indexDropped(String indexId) {
    this.leases.forEach(
        (id, lease) -> {
          if (lease.index.equals(indexId)
              || lease.joinPlan.map(plan -> plan.dependsOn(indexId)).orElse(false)) {
            synchronized (lease) {
              release(id, lease);
            }
          }
        });
  }

  void generationRetired(String generation) {
    this.leases.forEach(
        (id, lease) -> {
          if (lease.generation.equals(generation)
              || lease.joinPlan.map(plan -> plan.dependsOnGeneration(generation)).orElse(false)) {
            synchronized (lease) {
              release(id, lease);
            }
          }
        });
  }

  void expireIdle() {
    long cutoff = System.nanoTime() - IDLE_NANOS;
    this.leases.forEach(
        (id, lease) -> {
          if (lease.lastAccess < cutoff
              || lease.readLease.retired()
              || !this.runtime.cursors().isDirectCursor(id)) {
            synchronized (lease) {
              if (lease.lastAccess < cutoff
                  || lease.readLease.retired()
                  || !this.runtime.cursors().isDirectCursor(id)) {
                release(id, lease);
              }
            }
          }
        });
  }

  private void release(long id, Lease lease) {
    synchronized (lease) {
      if (this.leases.remove(id, lease)) {
        try {
          this.runtime.cursors().killCursor(id);
        } finally {
          try {
            lease.joinPlan.ifPresent(StandaloneQueryService::closeJoinPlan);
          } finally {
            lease.readLease.close();
            this.capacity.release();
          }
        }
      }
    }
  }

  private static void closeJoinPlan(StandaloneJoinPlanner.Plan plan) {
    try {
      plan.close();
    } catch (IOException e) {
      throw Status.UNAVAILABLE
          .withDescription("Could not release join readers")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private ByteString encode(BsonDocument document) {
    byte[] bytes = StandaloneDefinitionValidator.encode(document);
    if (bytes.length > this.runtime.config().documentLimits().maxBatchBytes()) {
      throw Status.RESOURCE_EXHAUSTED
          .withDescription("Query response exceeds the configured byte limit")
          .asRuntimeException();
    }
    return ByteString.copyFrom(bytes);
  }

  private static int batchSize(int requested) {
    if (requested < 0 || requested > 1000) {
      throw Status.INVALID_ARGUMENT
          .withDescription("batch_size must be between 1 and 1000")
          .asRuntimeException();
    }
    return requested == 0 ? 100 : requested;
  }

  static Optional<Explain.Verbosity> explainVerbosity(ExplainVerbosity verbosity) {
    return switch (verbosity) {
      case EXPLAIN_VERBOSITY_UNSPECIFIED -> Optional.empty();
      case EXPLAIN_VERBOSITY_QUERY_PLANNER -> Optional.of(Explain.Verbosity.QUERY_PLANNER);
      case EXPLAIN_VERBOSITY_EXECUTION_STATS -> Optional.of(Explain.Verbosity.EXECUTION_STATS);
      case EXPLAIN_VERBOSITY_ALL_PLANS_EXECUTION ->
          Optional.of(Explain.Verbosity.ALL_PLANS_EXECUTION);
      case UNRECOGNIZED ->
          throw Status.INVALID_ARGUMENT
              .withDescription("Unknown explain_verbosity")
              .asRuntimeException();
    };
  }

  static RuntimeException queryError(Exception e) {
    if (e instanceof io.grpc.StatusRuntimeException status) {
      return status;
    }
    if (e instanceof com.xgen.mongot.cursor.MongotCursorNotFoundException) {
      return Status.NOT_FOUND
          .withDescription("Cursor expired or its index was dropped")
          .asRuntimeException();
    }
    if (e instanceof ExplainTooLargeException) {
      return Status.RESOURCE_EXHAUSTED.withDescription(e.getMessage()).asRuntimeException();
    }
    if (e instanceof com.xgen.mongot.util.bson.parser.BsonParseException
        || e instanceof com.xgen.mongot.index.query.InvalidQueryException
        || e instanceof com.xgen.mongot.util.regex.RegexLimitExceededException
        || e instanceof org.bson.BsonInvalidOperationException
        || e instanceof IllegalArgumentException) {
      return Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
    }
    if (e instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
    return Status.UNAVAILABLE
        .withDescription("Local query could not complete")
        .withCause(e)
        .asRuntimeException();
  }

  @Override
  public void close() {
    this.leases.forEach(this::release);
  }
}
