package com.xgen.mongot.standalone.server;

import com.xgen.mongot.config.manager.GenerationReadLeases.Lease;
import com.xgen.mongot.extensions.join.JoinPlanner;
import com.xgen.mongot.index.SearchIndexReader.QuerySnapshot;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.operators.JoinSource;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.grpc.JoinIndexBinding;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import io.grpc.Context;
import io.grpc.Status;
import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Direct index selection and read leases for the shared join planner. */
final class StandaloneJoinPlanner extends JoinPlanner {
  private final StandaloneRuntime runtime;

  StandaloneJoinPlanner(StandaloneRuntime runtime) {
    super(new DirectEnvironment(runtime, Map.of()));
    this.runtime = runtime;
    runtime.metrics().gauge("joins.reservedBytes", runtime, ignored -> JoinPlanner.retainedBytes());
  }

  @Override
  public Plan prepare(SearchQuery query, GenerationId localGeneration) throws Exception {
    return prepare(query, localGeneration, List.of());
  }

  Plan prepare(SearchQuery query, GenerationId localGeneration, List<JoinIndexBinding> bindings)
      throws Exception {
    var bound = new HashMap<String, JoinIndexBinding>();
    for (var binding : bindings) {
      if (binding.getAlias().isBlank() || bound.putIfAbsent(binding.getAlias(), binding) != null) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Join bindings require unique nonempty aliases")
            .asRuntimeException();
      }
      LocalIndexCatalog.validateIndexId(binding.getIndexId());
      StandaloneAuthInterceptor.requireIndex(binding.getIndexId());
    }
    // Resolution state belongs to this plan, including bindings and acquired generation leases.
    return new JoinPlanner(new DirectEnvironment(this.runtime, Map.copyOf(bound)))
        .prepare(query, localGeneration);
  }

  private static final class DirectEnvironment implements Environment {
    private final StandaloneRuntime runtime;
    private final Map<String, JoinIndexBinding> bindings;
    private final Map<GenerationId, Lease> leases = new HashMap<>();

    private DirectEnvironment(StandaloneRuntime runtime, Map<String, JoinIndexBinding> bindings) {
      this.runtime = runtime;
      this.bindings = bindings;
    }

    private String alias(JoinSource source) {
      if (!(source instanceof JoinSource.StandaloneIndex index)) {
        throw invalid("Direct joins require a string index name or bound alias");
      }
      return index.name();
    }

    @Override
    public void validateSources(List<JoinSource> sources) {
      var used = sources.stream().map(this::alias).collect(java.util.stream.Collectors.toSet());
      if (!used.containsAll(this.bindings.keySet())) {
        throw invalid("Join binding alias is unused");
      }
    }

    @Override
    public String resolve(JoinSource source) {
      String name = alias(source);
      var binding = this.bindings.get(name);
      return binding == null
          ? this.runtime.catalog().getIndexByName(name).getIndexId()
          : binding.getIndexId();
    }

    @Override
    public GenerationId pin(String id, Optional<GenerationId> expected) {
      StandaloneAuthInterceptor.requireIndex(id);
      var resolved =
          this.runtime.reads().resolve(id, expected.map(GenerationId::uniqueString).orElse(""));
      if (!(resolved.getDefinition() instanceof SearchIndexDefinition)) {
        throw invalid("Joins require search indexes");
      }
      var generation = resolved.getGenerationId();
      // Several aliases (including a self-join) can select the same ID. Enforce every fence even
      // though the shared planner acquires that dependency only once.
      for (var binding : this.bindings.values()) {
        if (binding.getIndexId().equals(id)
            && !binding.getGenerationId().isEmpty()
            && !binding.getGenerationId().equals(generation.uniqueString())) {
          throw Status.FAILED_PRECONDITION
              .withDescription("Requested generation is no longer live")
              .asRuntimeException();
        }
      }
      return generation;
    }

    @Override
    public Closeable lease(GenerationId generation) {
      var lease = this.runtime.reads().acquire(generation);
      this.leases.put(generation, lease);
      return lease::close;
    }

    @Override
    public QuerySnapshot acquire(GenerationId generation, boolean concurrent) throws Exception {
      return this.runtime.acquireQuerySnapshot(this.leases.get(generation), concurrent);
    }

    @Override
    public void onInvalidated(Runnable callback) {
      this.leases.values().forEach(lease -> lease.onInvalidated(callback));
    }

    @Override
    public void verify(String id, GenerationId generation) {
      StandaloneAuthInterceptor.requireIndex(id);
      if (this.leases.get(generation).retired()
          || !this.runtime.reads().isGenerationReady(generation)) {
        throw Status.NOT_FOUND
            .withDescription("Join dependency generation was retired")
            .asRuntimeException();
      }
    }

    @Override
    public Runnable cancellation() {
      var context = Context.current();
      return () -> checkCancellation(context);
    }

    @Override
    public RuntimeException invalid(String message) {
      return Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException();
    }

    @Override
    public RuntimeException exhausted(String message) {
      return Status.RESOURCE_EXHAUSTED.withDescription(message).asRuntimeException();
    }

    @Override
    public void elapsed(String name, long start) {
      this.runtime.metrics().elapsed(name, start);
    }

    @Override
    public void count(String name, int value) {
      this.runtime.metrics().count(name, value);
    }
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
