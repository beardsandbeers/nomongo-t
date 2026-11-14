package com.xgen.mongot.standalone.server;

import com.google.errorprone.annotations.Var;
import com.google.protobuf.ByteString;
import com.xgen.mongot.standalone.StandaloneBson;
import com.xgen.mongot.standalone.grpc.DeleteByQueryRequest;
import com.xgen.mongot.standalone.grpc.DeleteByQueryResponse;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import io.grpc.Context;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.concurrent.Semaphore;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/** Bounded selection followed by ordinary ID deletions; ingestion may interleave. */
final class StandaloneDeleteByQuery {
  private final StandaloneRuntime runtime;
  private final Semaphore capacity = new Semaphore(1);
  private static final java.util.Set<String> OPERATORS =
      java.util.Set.of(
          "compound",
          "equals",
          "in",
          "range",
          "exists",
          "text",
          "phrase",
          "autocomplete",
          "regex",
          "wildcard",
          "geoShape",
          "geoWithin",
          "embeddedDocument");

  StandaloneDeleteByQuery(StandaloneRuntime runtime) {
    this.runtime = runtime;
  }

  private static void validateSearch(BsonDocument document) {
    if (document.size() != 1 || !OPERATORS.contains(document.getFirstKey())) {
      throw new IllegalArgumentException("Unsupported deletion operator");
    }
    String name = document.getFirstKey();
    var body = document.getDocument(name);
    if (body.containsKey("score")) {
      throw new IllegalArgumentException("Scores are unsupported");
    }
    if (name.equals("compound")) {
      for (String key : java.util.List.of("must", "mustNot", "should", "filter")) {
        if (body.containsKey(key)) {
          var clause = body.get(key);
          // CompoundOperator accepts either one operator or an array. Apply the same recursive
          // allowlist to both forms so shorthand cannot bypass unsupported-operator checks.
          if (clause.isDocument()) {
            validateSearch(clause.asDocument());
          } else {
            for (var child : clause.asArray()) {
              validateSearch(child.asDocument());
            }
          }
        }
      }
    } else if (name.equals("embeddedDocument")) {
      validateSearch(body.getDocument("operator"));
    }
  }

  DeleteByQueryResponse execute(DeleteByQueryRequest request) {
    StandaloneAuthInterceptor.requireRole(
        com.xgen.mongot.standalone.config.StandaloneConfig.Role.QUERY);
    StandaloneAuthInterceptor.requireIndex(request.getIndexId());
    StandaloneAuthInterceptor.requireRole(
        com.xgen.mongot.standalone.config.StandaloneConfig.Role.WRITE);
    if (request.getWriteConcern() == DocumentWriteConcern.UNRECOGNIZED
        || request.getMaxMatches() < 0
        || request.getMaxMatches() > 10000) {
      throw new IllegalArgumentException("Invalid deletion limits or write concern");
    }
    boolean all = request.getSelectorCase() == DeleteByQueryRequest.SelectorCase.MATCH_ALL;
    boolean vector =
        request.getSelectorCase() == DeleteByQueryRequest.SelectorCase.VECTOR_FILTER_BSON;
    if (request.getSelectorCase() == DeleteByQueryRequest.SelectorCase.SELECTOR_NOT_SET
        || (all && !request.getMatchAll())) {
      throw new IllegalArgumentException("An explicit selector is required");
    }
    BsonDocument predicate =
        all
            ? new BsonDocument()
            : StandaloneBson.document(
                (vector ? request.getVectorFilterBson() : request.getSearchQueryBson())
                    .toByteArray(),
                1024 * 1024,
                100);
    if (!all && predicate.isEmpty()) {
      throw new IllegalArgumentException("Empty predicate");
    }
    if (!all && !vector) {
      try {
        validateSearch(predicate);
      } catch (org.bson.BsonInvalidOperationException e) {
        throw new IllegalArgumentException("Invalid deletion predicate", e);
      }
    }
    if (!this.capacity.tryAcquire()) {
      throw Status.RESOURCE_EXHAUSTED.asRuntimeException();
    }
    try {
      var ingestion = this.runtime.ingestion();
      var lease =
          ingestion.coordinate(
              () -> {
                if (Context.current().isCancelled()) {
                  throw Status.CANCELLED.asRuntimeException();
                }
                var desired = this.runtime.catalog().getIndex(request.getIndexId());
                var generation =
                    this.runtime
                        .liveIndexes()
                        .getIndexById(new ObjectId(request.getIndexId()))
                        .orElseThrow(() -> Status.UNAVAILABLE.asRuntimeException());
                String id = generation.getGenerationId().uniqueString();
                if (!request.getGenerationId().isEmpty() && !request.getGenerationId().equals(id)) {
                  throw Status.FAILED_PRECONDITION.asRuntimeException();
                }
                if (generation.getDefinition().getDefinitionVersion().orElse(1L)
                    != desired.getDesiredRevision()) {
                  throw Status.UNAVAILABLE.asRuntimeException();
                }
                boolean isVector = desired.getType() == IndexType.INDEX_TYPE_VECTOR_SEARCH;
                if (!all && isVector != vector) {
                  throw Status.FAILED_PRECONDITION.asRuntimeException();
                }
                if (isVector
                    && (generation.getDefinition().asVectorDefinition().getNestedRoot().isPresent()
                        || generation
                            .getDefinition()
                            .asVectorDefinition()
                            .getIndexNamespace()
                            .isPresent())) {
                  throw Status.FAILED_PRECONDITION
                      .withDescription("Nested/namespaced vector indexes unsupported")
                      .asRuntimeException();
                }
                this.runtime.checkWriteAdmission(request.getSerializedSize());
                return ingestion.acquireStatisticsLease(generation.getGenerationId());
              });
      try (lease) {
        var generation = lease.index().getGenerationId();
        String id = generation.uniqueString();
        long revision = lease.index().getDefinition().getDefinitionVersion().orElse(1L);
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        Runnable check =
            () -> {
              if (Context.current().isCancelled()) {
                throw Status.CANCELLED.asRuntimeException();
              }
              if (System.nanoTime() >= deadline) {
                throw Status.DEADLINE_EXCEEDED.asRuntimeException();
              }
              if (lease.retired()) {
                throw Status.FAILED_PRECONDITION.asRuntimeException();
              }
            };
        var ids = new LinkedHashSet<ByteString>();
        long[] bytes = {0};
        long ceiling = request.getMaxMatches() == 0 ? 10000 : request.getMaxMatches();
        lease
            .index()
            .getReader()
            .collectDocumentIds(
                predicate,
                vector,
                all,
                value -> {
                  StandaloneBson.mutation(
                      value,
                      true,
                      this.runtime.config().documentLimits().maxDocumentBytes(),
                      this.runtime.config().documentLimits().maxDepth());
                  var encoded = ByteString.copyFrom(value);
                  if (!ids.contains(encoded)) {
                    if (ids.size() >= ceiling || bytes[0] + value.length > 16 * 1024 * 1024) {
                      throw Status.RESOURCE_EXHAUSTED
                          .withDescription("Selection limit exceeded")
                          .asRuntimeException();
                    }
                    ids.add(encoded);
                    bytes[0] += value.length;
                  }
                },
                check);
        var list = new ArrayList<>(ids);
        int batchSize = Math.min(100, this.runtime.config().documentLimits().maxBatchDocuments());
        if (!request.getDryRun()) {
          for (@Var int start = 0; start < list.size(); ) {
            @Var int end = start;
            @Var long batchBytes = 0;
            while (end < list.size()
                && end - start < batchSize
                && batchBytes + list.get(end).size()
                    <= this.runtime.config().documentLimits().maxBatchBytes()) {
              batchBytes += list.get(end++).size();
            }
            if (end == start) {
              throw Status.RESOURCE_EXHAUSTED.asRuntimeException();
            }
            var batch = list.subList(start, end);
            start = end;
            ingestion.coordinate(
                () -> {
                  var desired = this.runtime.catalog().getIndex(request.getIndexId());
                  if (desired.getDesiredRevision() != revision) {
                    throw Status.FAILED_PRECONDITION.asRuntimeException();
                  }
                  this.runtime.checkWriteAdmission(request.getSerializedSize());
                  return ingestion.write(
                      request.getIndexId(),
                      revision,
                      id,
                      batch,
                      true,
                      DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_INDEXED);
                });
          }
        }
        ingestion.coordinate(
            () -> {
              if (Context.current().isCancelled()) {
                throw Status.CANCELLED.asRuntimeException();
              }
              var desired = this.runtime.catalog().getIndex(request.getIndexId());
              if (desired.getDesiredRevision() != revision || lease.retired()) {
                throw Status.FAILED_PRECONDITION.asRuntimeException();
              }
              if (!request.getDryRun()) {
                ingestion.completeWrite(id, lease.index(), request.getWriteConcern());
              }
              return true;
            });
        return DeleteByQueryResponse.newBuilder()
            .setIndexId(request.getIndexId())
            .setGenerationId(id)
            .setActiveRevision(revision)
            .setMatchedCount(ids.size())
            .setProcessedCount(request.getDryRun() ? 0 : ids.size())
            .setDryRun(request.getDryRun())
            .build();
      } catch (IndexSearcher.TooManyClauses | TooComplexToDeterminizeException e) {
        throw Status.RESOURCE_EXHAUSTED
            .withDescription("Deletion predicate exceeds query complexity limits")
            .withCause(e)
            .asRuntimeException();
      } catch (java.io.IOException | com.xgen.mongot.index.ReaderClosedException e) {
        throw Status.UNAVAILABLE.withCause(e).asRuntimeException();
      } catch (com.xgen.mongot.index.query.InvalidQueryException e) {
        throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
      }
    } finally {
      this.capacity.release();
    }
  }
}
