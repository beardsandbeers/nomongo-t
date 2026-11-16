package com.xgen.mongot.standalone.server;

import com.google.errorprone.annotations.Var;
import com.google.protobuf.ByteString;
import com.xgen.mongot.cursor.CursorQuery;
import com.xgen.mongot.cursor.SearchCursorInfo;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.SearchIndexReader.QuerySnapshot;
import com.xgen.mongot.index.lucene.explain.explainers.MetadataFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.standalone.StandaloneBson;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.grpc.IndexDescription;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.SearchMetaRequest;
import com.xgen.mongot.standalone.grpc.SearchMetaResponse;
import com.xgen.mongot.standalone.ingestion.ApiIngestionManager.IndexLease;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.QueryCancellation;
import com.xgen.mongot.util.regex.RegexBudget;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.jetbrains.annotations.Nullable;

/** Metadata-only transport for the ordinary search parser and cursor execution pipeline. */
final class StandaloneSearchMeta {
  private static final QueryOptimizationFlags FLAGS = new QueryOptimizationFlags(true);
  private final StandaloneRuntime runtime;
  private final StandaloneJoinPlanner joins;

  StandaloneSearchMeta(StandaloneRuntime runtime, StandaloneJoinPlanner joins) {
    this.runtime = runtime;
    this.joins = joins;
  }

  SearchMetaResponse execute(SearchMetaRequest request) {
    var context = Context.current();
    try (var regexScope = RegexBudget.openScope();
        var cancellation = QueryCancellation.openScope(() -> checkCancellation(context))) {
      var expression =
          StandaloneBson.document(request.getQueryBson().toByteArray(), 16 * 1024 * 1024, 100);
      var query = SearchQuery.fromBson(expression, FLAGS, this.runtime.searchQueryParseOptions());
      this.runtime.validateSearchQuery(query);
      var verbosity = StandaloneQueryService.explainVerbosity(request.getExplainVerbosity());
      validateFences(request);
      checkCancellation(context);
      var desired = resolve(query, request);
      if (desired.isEmpty()) {
        return executeEmpty(query, verbosity, context);
      }
      var index = desired.get();
      try (var lease = acquire(index, request);
          var explain =
              Explain.setup(
                  verbosity, Optional.of(lease.index().getDefinition().getNumPartitions()))) {
        addExplainMetadata(query);
        return executePinned(query, request, lease, context);
      }
    } catch (Exception e) {
      checkCancellation(context);
      throw StandaloneQueryService.queryError(e);
    }
  }

  private SearchMetaResponse executeEmpty(
      SearchQuery query, Optional<Explain.Verbosity> verbosity, Context context) throws Exception {
    try (var explain = Explain.setup(verbosity, Optional.empty());
        var resources = new QueryResources()) {
      addExplainMetadata(query);
      var cursor = this.runtime.cursors().newEmptyCursor(query.index());
      resources.cursorId = cursor.cursorId;
      var response =
          collectResponse(
              cursor, verbosity.filter(v -> v == Explain.Verbosity.QUERY_PLANNER).isPresent());
      return checked(response.build(), context);
    }
  }

  private static void addExplainMetadata(SearchQuery query) {
    Explain.getQueryInfo()
        .map(
            info ->
                info.getFeatureExplainer(
                    MetadataFeatureExplainer.class, MetadataFeatureExplainer::new))
        .ifPresent(metadata -> metadata.setIndexName(query.index()));
  }

  private SearchMetaResponse executePinned(
      @Var SearchQuery query, SearchMetaRequest request, IndexLease lease, Context context)
      throws Exception {
    @Var long cursorId = 0;
    @Var StandaloneJoinPlanner.Plan join = null;
    @Var QuerySnapshot snapshot = null;
    // Resource declarations ensure cleanup failures do not replace the original query failure.
    try (var resources = new QueryResources()) {
      if (StandaloneJoinPlanner.containsJoin(StandaloneJoinPlanner.operator(query))) {
        join = this.joins.prepare(query, lease.index().getGenerationId());
        resources.join = join;
        snapshot = join.snapshot();
        query = join.query();
      } else {
        snapshot =
            this.runtime.acquireQuerySnapshot(lease.index().getGenerationId(), query.concurrent());
        resources.snapshot = snapshot;
      }
      snapshot.setCancellationCheck(() -> checkCancellation(context));
      var cursor =
          this.runtime
              .cursors()
              .newCursor(
                  lease.index().getGenerationId(),
                  new CursorQuery.Search(query, Optional.of(snapshot)),
                  QueryCursorOptions.empty(),
                  FLAGS);
      cursorId = cursor.cursorId;
      resources.cursorId = cursorId;
      boolean planner =
          request.getExplainVerbosity()
              == com.xgen.mongot.standalone.grpc.ExplainVerbosity.EXPLAIN_VERBOSITY_QUERY_PLANNER;
      var response =
          collectResponse(cursor, planner)
              .setGenerationId(lease.index().getGenerationId().uniqueString());
      if (join != null) {
        join.verifyDependencies();
      }
      if (lease.retired()) {
        throw Status.FAILED_PRECONDITION
            .withDescription("Query generation was retired")
            .asRuntimeException();
      }
      return checked(response.build(), context);
    }
  }

  private SearchMetaResponse.Builder collectResponse(SearchCursorInfo cursor, boolean planner)
      throws Exception {
    var response = SearchMetaResponse.newBuilder();
    if (!planner) {
      response.setMetadataBson(encode(cursor.metaResults.applyAggregationLimit().toBson()));
    }
    // Finalize explain through the same batch machinery as coupled search. Some operators
    // still produce documents internally; they are discarded and never exposed as a hit cursor.
    var batch =
        this.runtime
            .cursors()
            .getNextBatch(cursor.cursorId, Bytes.ofMebi(16), QueryCursorOptions.empty());
    batch.explainResult.ifPresent(value -> response.setExplainBson(encode(value.toBson())));
    return response;
  }

  private Optional<IndexDescription> resolve(SearchQuery query, SearchMetaRequest request) {
    try {
      var index = this.runtime.catalog().getIndexByName(query.index());
      StandaloneAuthInterceptor.requireIndex(index.getIndexId());
      if (!request.getIndexId().isEmpty() && !request.getIndexId().equals(index.getIndexId())) {
        throw Status.FAILED_PRECONDITION
            .withDescription("Query index does not match index_id")
            .asRuntimeException();
      }
      if (index.getType() != IndexType.INDEX_TYPE_SEARCH) {
        throw Status.INVALID_ARGUMENT
            .withDescription("SearchMeta requires a search index")
            .asRuntimeException();
      }
      return Optional.of(index);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
      if (!request.getIndexId().isEmpty() || !request.getGenerationId().isEmpty()) {
        throw Status.FAILED_PRECONDITION
            .withDescription("Fenced query index does not exist")
            .asRuntimeException();
      }
      return Optional.empty();
    }
  }

  private IndexLease acquire(IndexDescription desired, SearchMetaRequest request) {
    return this.runtime
        .ingestion()
        .coordinate(
            () -> {
              var live =
                  this.runtime
                      .liveIndexes()
                      .getIndexById(new org.bson.types.ObjectId(desired.getIndexId()))
                      .orElseThrow(
                          () ->
                              Status.UNAVAILABLE
                                  .withDescription("Index is not query-ready")
                                  .asRuntimeException());
              if (!request.getGenerationId().isEmpty()
                  && !request.getGenerationId().equals(live.getGenerationId().uniqueString())) {
                throw Status.FAILED_PRECONDITION
                    .withDescription("Requested generation is no longer live")
                    .asRuntimeException();
              }
              return this.runtime.ingestion().acquireQueryLease(live.getGenerationId());
            });
  }

  private static void validateFences(SearchMetaRequest request) {
    if (!request.getIndexId().isEmpty()) {
      LocalIndexCatalog.validateIndexId(request.getIndexId());
      StandaloneAuthInterceptor.requireIndex(request.getIndexId());
    }
    if (!request.getGenerationId().isEmpty()
        && !request
            .getGenerationId()
            .matches("[0-9a-f]{24}-f[1-9][0-9]*-u(?:0|[1-9][0-9]*)-a(?:0|[1-9][0-9]*)")) {
      throw new IllegalArgumentException("generation_id must be a canonical generation identifier");
    }
  }

  private static ByteString encode(org.bson.BsonDocument document) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(document));
  }

  private SearchMetaResponse checked(SearchMetaResponse response, Context context) {
    checkCancellation(context);
    if (response.getSerializedSize() > this.runtime.config().documentLimits().maxBatchBytes()) {
      throw Status.RESOURCE_EXHAUSTED
          .withDescription("Query response exceeds the configured byte limit")
          .asRuntimeException();
    }
    this.runtime
        .metrics()
        .count(
            "query.metadata.bytes", response.getMetadataBson().size(), "operation", "SearchMeta");
    return response;
  }

  private static void checkCancellation(Context context) {
    if (context.isCancelled()) {
      throw Optional.ofNullable(Contexts.statusFromCancelled(context))
          .orElse(Status.CANCELLED)
          .asRuntimeException();
    }
  }

  private final class QueryResources implements AutoCloseable {
    private long cursorId;
    @Nullable private StandaloneJoinPlanner.Plan join;
    @Nullable private QuerySnapshot snapshot;

    @Override
    public void close() throws Exception {
      try {
        if (this.cursorId != 0) {
          StandaloneSearchMeta.this.runtime.cursors().killCursor(this.cursorId);
        }
      } finally {
        try {
          if (this.join != null) {
            this.join.close();
          }
        } finally {
          if (this.snapshot != null) {
            this.snapshot.close();
          }
        }
      }
    }
  }
}
