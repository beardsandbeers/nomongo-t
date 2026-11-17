package com.xgen.mongot.standalone.server;

import com.xgen.mongot.index.IndexReader;
import com.xgen.mongot.standalone.StandaloneBson;
import com.xgen.mongot.standalone.grpc.DeleteByQueryRequest;
import com.xgen.mongot.standalone.grpc.DeleteByQueryResponse;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import io.grpc.Context;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/** Streams root IDs into bounded native delete batches; ingestion may interleave. */
final class StandaloneDeleteByQuery {
  private record ScanSchema(IndexReader reader, long revision) {}

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

  // The captured physical-generation fence still rejects rebuild/drop. Compatible schema changes
  // do not invalidate a scan's already selected IDs or interrupt its remaining delete batches.
  private long currentRevision(String indexId) {
    return this.runtime.catalog().getIndex(indexId).getDesiredRevision();
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
    if (request.getWriteConcern() == DocumentWriteConcern.UNRECOGNIZED) {
      throw new IllegalArgumentException("Invalid write concern");
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
        var scan =
            ingestion.coordinate(
                () ->
                    new ScanSchema(
                        lease.index().getReader(),
                        lease.index().getDefinition().getDefinitionVersion().orElse(1L)));
        var context = Context.current();
        Runnable check =
            () -> {
              if (context.isCancelled()) {
                throw Status.CANCELLED.asRuntimeException();
              }
              if (lease.retired()) {
                throw Status.FAILED_PRECONDITION.asRuntimeException();
              }
            };
        var limits = this.runtime.config().documentLimits();
        int batchSize = Math.min(100, limits.maxBatchDocuments());
        // Each live root appears once in a pinned reader, and ID routing assigns it to one
        // partition. Keeping a global set of every selected ID would defeat bounded streaming.
        class DeleteBatches implements IndexReader.DocumentIdConsumer {
          private final ArrayList<byte[]> batch = new ArrayList<>();
          private long batchBytes;

          @Override
          public void accept(byte[] value) {
            check.run();
            StandaloneBson.validateEncodedId(value, limits.maxDocumentBytes(), limits.maxDepth());
            if (request.getDryRun()) {
              return;
            }
            if (this.batchBytes + value.length > limits.maxBatchBytes()) {
              flush();
            }
            this.batch.add(value);
            this.batchBytes += value.length;
            if (this.batch.size() >= batchSize) {
              flush();
            }
          }

          @Override
          public void finish() {
            flush();
          }

          private void flush() {
            check.run();
            if (this.batch.isEmpty()) {
              return;
            }
            StandaloneDeleteByQuery.this.runtime.checkWriteAdmission(request.getSerializedSize());
            var previous = context.attach();
            try {
              ingestion.deleteDocuments(
                  request.getIndexId(),
                  () -> currentRevision(request.getIndexId()),
                  id,
                  this.batch,
                  check);
            } finally {
              context.detach(previous);
            }
            this.batch.clear();
            this.batchBytes = 0;
            check.run();
          }
        }

        long matched =
            scan.reader().collectDocumentIds(predicate, vector, all, DeleteBatches::new, check);
        ingestion.completeWrite(
            request.getIndexId(),
            () -> currentRevision(request.getIndexId()),
            id,
            request.getDryRun()
                ? DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_INDEXED
                : request.getWriteConcern());
        return DeleteByQueryResponse.newBuilder()
            .setIndexId(request.getIndexId())
            .setGenerationId(id)
            .setActiveRevision(scan.revision())
            .setMatchedCount(matched)
            .setProcessedCount(request.getDryRun() ? 0 : matched)
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
