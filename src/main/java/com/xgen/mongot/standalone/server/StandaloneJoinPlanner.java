package com.xgen.mongot.standalone.server;

import com.xgen.mongot.extensions.join.JoinPlanner;
import com.xgen.mongot.index.SearchIndexReader.QuerySnapshot;
import com.xgen.mongot.index.query.operators.JoinSource;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import io.grpc.Context;
import io.grpc.Status;
import java.io.Closeable;
import java.util.Optional;
import org.bson.types.ObjectId;

/** Standalone name resolution and access checks for the shared join planner. */
final class StandaloneJoinPlanner extends JoinPlanner {
  StandaloneJoinPlanner(StandaloneRuntime runtime) {
    super(
        new Environment() {
          public String resolve(JoinSource source) {
            if (!(source instanceof JoinSource.StandaloneIndex index)) {
              throw invalid("Standalone joins require a string index name");
            }
            return runtime.catalog().getIndexByName(index.name()).getIndexId();
          }

          public GenerationId pin(String id, Optional<GenerationId> expected) {
            StandaloneAuthInterceptor.requireIndex(id);
            if (runtime.catalog().getIndex(id).getType() != IndexType.INDEX_TYPE_SEARCH) {
              throw invalid("Joins require search indexes");
            }
            var generation =
                runtime
                    .liveIndexes()
                    .getIndexById(new ObjectId(id))
                    .filter(index -> runtime.ingestion().isGenerationReady(index.getGenerationId()))
                    .orElseThrow(
                        () ->
                            Status.UNAVAILABLE
                                .withDescription("Join index is not query-ready")
                                .asRuntimeException())
                    .getGenerationId();
            if (expected.isPresent() && !expected.get().equals(generation)) {
              throw Status.FAILED_PRECONDITION
                  .withDescription("Requested generation is no longer live")
                  .asRuntimeException();
            }
            return generation;
          }

          public Closeable lease(GenerationId generation) {
            var lease = runtime.ingestion().acquireQueryLease(generation);
            return lease::close;
          }

          public QuerySnapshot acquire(GenerationId generation, boolean concurrent)
              throws Exception {
            return runtime.acquireQuerySnapshot(generation, concurrent);
          }

          public void verify(String id, GenerationId generation) {
            StandaloneAuthInterceptor.requireIndex(id);
            runtime.catalog().getIndex(id);
            if (runtime
                .liveIndexes()
                .getIndexById(new ObjectId(id))
                .filter(index -> index.getGenerationId().equals(generation))
                .isEmpty()) {
              throw Status.NOT_FOUND
                  .withDescription("Join dependency generation was retired")
                  .asRuntimeException();
            }
          }

          public Runnable cancellation() {
            var context = Context.current();
            return () -> checkCancellation(context);
          }

          public RuntimeException invalid(String message) {
            return Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException();
          }

          public RuntimeException exhausted(String message) {
            return Status.RESOURCE_EXHAUSTED.withDescription(message).asRuntimeException();
          }

          public void elapsed(String name, long start) {
            runtime.metrics().elapsed(name, start);
          }

          public void count(String name, int value) {
            runtime.metrics().count(name, value);
          }
        });
    runtime.metrics().gauge("joins.reservedBytes", runtime, ignored -> JoinPlanner.retainedBytes());
  }

  static void checkCancellation(Context context) {
    if (context.getDeadline() != null && context.getDeadline().isExpired()) {
      throw Status.DEADLINE_EXCEEDED.asRuntimeException();
    }
    if (context.isCancelled() || Thread.currentThread().isInterrupted()) {
      throw Status.CANCELLED.asRuntimeException();
    }
  }
}
