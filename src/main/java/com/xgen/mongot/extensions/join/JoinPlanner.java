package com.xgen.mongot.extensions.join;

import com.xgen.mongot.index.SearchIndexReader.QuerySnapshot;
import com.xgen.mongot.index.lucene.explain.information.JoinSourceInformation;
import com.xgen.mongot.index.lucene.explain.joins.JoinFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.CollectorQuery;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.collectors.AggregationCollector;
import com.xgen.mongot.index.query.collectors.FacetCollector;
import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import com.xgen.mongot.index.query.operators.CompoundClause;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.HasAncestorOperator;
import com.xgen.mongot.index.query.operators.HasRootOperator;
import com.xgen.mongot.index.query.operators.JoinField;
import com.xgen.mongot.index.query.operators.JoinKeySet;
import com.xgen.mongot.index.query.operators.JoinOperator;
import com.xgen.mongot.index.query.operators.JoinSource;
import com.xgen.mongot.index.query.operators.KnnBetaOperator;
import com.xgen.mongot.index.query.operators.MaterializedJoinOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.index.version.GenerationId;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.util.IOUtils;
import org.jetbrains.annotations.Nullable;

/** Resolves authorized index dependencies and bounded binary membership before opening a cursor. */
public class JoinPlanner {
  public static final int MAX_JOINS = 8;
  public static final int MAX_KEYS = 100_000;
  public static final long MAX_QUERY_BYTES = 32L * 1024 * 1024;
  public static final long MAX_CONCURRENT_BYTES = 256L * 1024 * 1024;
  public static final long MAX_FOREIGN_WORK = 10_000_000;

  /** Estimated retained-memory limits, shared by direct and coupled query adapters. */
  public record MemoryLimits(long maxQueryBytes, long maxConcurrentBytes) {
    public MemoryLimits {
      if (maxQueryBytes <= 0 || maxConcurrentBytes <= 0) {
        throw new IllegalArgumentException("Join memory limits must be positive byte counts");
      }
    }

    public static MemoryLimits defaults() {
      return new MemoryLimits(MAX_QUERY_BYTES, MAX_CONCURRENT_BYTES);
    }
  }

  public interface Environment {
    String resolve(JoinSource source) throws Exception;

    /**
     * Checks transport-specific source bindings before acquiring readers or executing predicates.
     */
    default void validateSources(List<JoinSource> sources) {}

    GenerationId pin(String id, Optional<GenerationId> expected) throws Exception;

    QuerySnapshot acquire(GenerationId generation, boolean concurrent) throws Exception;

    /** Keeps adapter-owned generation resources alive until the prepared plan is closed. */
    default Closeable lease(GenerationId generation) {
      return () -> {};
    }

    /** Registers direct-reader retirement; native adapters retain their own cursor lifecycle. */
    default void onInvalidated(Runnable callback) {}

    void verify(String id, GenerationId generation);

    Runnable cancellation();

    RuntimeException invalid(String message);

    RuntimeException exhausted(String message);

    default void elapsed(String name, long start) {}

    default void count(String name, int value) {}
  }

  // One conservative admission budget across both architectures in this process.
  private static final AtomicLong RETAINED_BYTES = new AtomicLong();
  private final Environment environment;
  private final MemoryLimits memoryLimits;

  public JoinPlanner(Environment environment) {
    this(environment, MemoryLimits.defaults());
  }

  public JoinPlanner(Environment environment, MemoryLimits memoryLimits) {
    this.environment = environment;
    this.memoryLimits = memoryLimits;
  }

  public static double retainedBytes() {
    return RETAINED_BYTES.doubleValue();
  }

  public static Operator operator(SearchQuery query) {
    return switch (query) {
      case OperatorQuery search -> search.operator();
      case CollectorQuery search ->
          search.collector().getOperator().orElse(AllDocumentsOperator.INSTANCE);
    };
  }

  public static boolean containsJoin(Operator operator) {
    return switch (operator) {
      case JoinOperator ignored -> true;
      case MaterializedJoinOperator ignored -> true;
      case CompoundOperator compound -> compound.getOperators().anyMatch(JoinPlanner::containsJoin);
      case EmbeddedDocumentOperator embedded -> containsJoin(embedded.operator());
      case HasRootOperator root -> containsJoin(root.operator());
      case HasAncestorOperator ancestor -> containsJoin(ancestor.operator());
      case KnnBetaOperator knn -> knn.filter().map(JoinPlanner::containsJoin).orElse(false);
      default -> false;
    };
  }

  /** Pins dependencies and materializes bounded membership; the caller owns the returned plan. */
  public Plan prepare(SearchQuery query, GenerationId localGeneration) throws Exception {
    if (query.returnScope().isPresent()) {
      throw invalid("Joins do not support returnScope");
    }
    List<JoinOperator> joins = new ArrayList<>();
    validate(operator(query), false, false, joins);
    if (joins.size() > MAX_JOINS) {
      throw exhausted("Too many join predicates (maximum " + MAX_JOINS + ")");
    }
    this.environment.validateSources(joins.stream().flatMap(join -> join.from().stream()).toList());
    var plan = new Plan(query);
    var joinExplain = JoinFeatureExplainer.root();
    long startNanos = System.nanoTime();
    try {
      plan.reserveBytes(8192L + 8L * query.toBson().toJson().length());
      long dependencyStart = joinExplain.isPresent() ? System.nanoTime() : 0;
      plan.add(localGeneration.indexId.toHexString(), Optional.of(localGeneration));
      // Authorize and pin every dependency before executing any foreign predicate.
      Map<JoinOperator, String> targets = new HashMap<>();
      for (var join : joins) {
        String id =
            join.from().isPresent()
                ? this.environment.resolve(join.from().get())
                : localGeneration.indexId.toHexString();
        // Foreign indexes can have a different partition count. Their selection work must not
        // capture the local query's profilers (self-joins reuse the already pinned local readers).
        try (var ignored = Explain.suspend()) {
          plan.add(id, Optional.empty());
        }
        targets.put(join, id);
      }
      long dependencyNanos = joinExplain.isPresent() ? System.nanoTime() - dependencyStart : 0;
      var localSnapshot =
          Objects.requireNonNull(plan.snapshots.get(localGeneration.indexId.toHexString()));
      plan.localSnapshot = localSnapshot;
      Map<JoinOperator, Operator> resolved = new HashMap<>();
      for (var join : joins) {
        var foreign = Objects.requireNonNull(plan.snapshots.get(targets.get(join)));
        var localFields = localSnapshot.joinFields(join.localField());
        var foreignFields = foreign.joinFields(join.foreignField());
        List<Operator> domains = new ArrayList<>();
        for (var localField : localFields) {
          var foreignField =
              foreignFields.stream().filter(field -> field.type() == localField.type()).findFirst();
          if (foreignField.isEmpty()) {
            continue;
          }
          if (!localField.equals(foreignField.get())) {
            throw invalid("Join fields must use the same type and token normalizer");
          }
          // Each equality domain owns separate membership and uses the same bounded plan.
          long bytesBefore = plan.bytes;
          long keyBytesBefore = plan.keyBytes;
          long workBefore = plan.work;
          plan.reserveBytes(
              64L * 1024 * (1L + (long) localSnapshot.partitions() * plan.localQueryCopies));
          var keys = new JoinKeySet.Builder(plan::reserveKey);
          long foreignStart = System.nanoTime();
          long foreignNanos;
          try (var ignored = Explain.suspend()) {
            foreign.collectKeys(
                join.operator(),
                join.foreignField(),
                localField.type(),
                keys::add,
                plan::checkWork);
          } finally {
            foreignNanos = joinExplain.isPresent() ? System.nanoTime() - foreignStart : 0;
            this.environment.elapsed("joins.foreign", foreignStart);
          }
          var membership = keys.build();
          if (joinExplain.isPresent()) {
            var explanation = joinExplain.get();
            String foreignId = Objects.requireNonNull(targets.get(join));
            explanation.source(
                new JoinSourceInformation(
                    explanation.register(membership),
                    join.from().map(JoinSource::toBson),
                    foreignId,
                    Objects.requireNonNull(plan.dependencies.get(foreignId)).toString(),
                    foreignId.equals(localGeneration.indexId.toHexString()),
                    join.localField().toString(),
                    join.foreignField().toString(),
                    localField.type().name(),
                    localField.normalizer().getNormalizerName(),
                    foreign.partitions(),
                    membership.size(),
                    plan.keyBytes - keyBytesBefore,
                    plan.bytes - bytesBefore,
                    explanation.executionCount(plan.work - workBefore),
                    explanation.executionTiming(foreignNanos, "collectForeignKeys")));
          }
          domains.add(materializeJoin(join, localField, membership, query));
        }
        if (domains.isEmpty()) {
          throw invalid("Join fields must use the same type and token normalizer");
        }
        resolved.put(
            join,
            domains.size() == 1
                ? domains.getFirst()
                : new CompoundOperator(
                    join.score(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(new CompoundClause(domains)),
                    1,
                    Optional.empty()));
      }
      var materialized = materialize(operator(query), resolved);
      plan.query =
          switch (query) {
            case OperatorQuery local -> local.withOperator(materialized);
            case CollectorQuery collector -> collector.withOperator(materialized);
          };
      // Foreign searchers are unnecessary once membership owns immutable binary keys.
      for (var entry : plan.snapshots.entrySet()) {
        if (entry.getValue() != localSnapshot) {
          entry.getValue().close();
        }
      }
      plan.snapshots.clear();
      plan.snapshots.put(localGeneration.indexId.toHexString(), localSnapshot);
      plan.activate(this.environment.cancellation());
      plan.verifyDependencies();
      this.environment.count("joins.keys", plan.keys);
      joinExplain.ifPresent(
          explanation ->
              explanation.prepared(
                  localSnapshot.partitions(),
                  plan.keys,
                  plan.keyBytes,
                  plan.bytes,
                  Map.of(
                      "keys",
                      (long) MAX_KEYS,
                      "memoryBytes",
                      this.memoryLimits.maxQueryBytes(),
                      "concurrentMemoryBytes",
                      this.memoryLimits.maxConcurrentBytes(),
                      "workUnits",
                      MAX_FOREIGN_WORK),
                  plan.work,
                  System.nanoTime() - startNanos,
                  dependencyNanos));
      return plan;
    } catch (Exception | Error e) {
      try {
        plan.close();
      } catch (IOException cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    } finally {
      this.environment.elapsed("joins.prepare", startNanos);
    }
  }

  public static MaterializedJoinOperator materializeJoin(
      JoinOperator join, JoinField field, JoinKeySet keys, SearchQuery query) {
    return new MaterializedJoinOperator(
        join,
        field,
        keys,
        query instanceof CollectorQuery || query.count().type() == Count.Type.TOTAL,
        query.count().threshold());
  }

  private void validate(
      Operator operator, boolean filter, boolean foreign, List<JoinOperator> joins) {
    if (operator instanceof CompoundOperator compound) {
      compound.filter().operators().forEach(child -> validate(child, true, foreign, joins));
      compound.mustNot().operators().forEach(child -> validate(child, true, foreign, joins));
      compound.must().operators().forEach(child -> validate(child, filter, foreign, joins));
      compound.should().operators().forEach(child -> validate(child, filter, foreign, joins));
    } else if (operator instanceof JoinOperator join) {
      if (foreign || !filter) {
        throw invalid("join is allowed only in filter/exclusion positions and cannot be nested");
      }
      joins.add(join);
      validate(join.operator(), true, true, joins);
    } else if (operator instanceof EmbeddedDocumentOperator
        || operator instanceof HasAncestorOperator
        || operator instanceof HasRootOperator
        || operator instanceof KnnBetaOperator
        || operator instanceof VectorSearchOperator) {
      throw invalid("Joins do not support embedded-document or vector operators");
    }
  }

  private static Operator materialize(Operator operator, Map<JoinOperator, Operator> joins) {
    if (operator instanceof JoinOperator join) {
      return joins.get(join);
    }
    if (operator instanceof CompoundOperator compound) {
      return new CompoundOperator(
          compound.score(),
          materialize(compound.filter(), joins),
          materialize(compound.must(), joins),
          materialize(compound.mustNot(), joins),
          materialize(compound.should(), joins),
          compound.minimumShouldMatch(),
          compound.doesNotAffect());
    }
    return operator;
  }

  private static CompoundClause materialize(
      CompoundClause clause, Map<JoinOperator, Operator> joins) {
    return new CompoundClause(
        clause.operators().stream().map(child -> materialize(child, joins)).toList());
  }

  private RuntimeException invalid(String message) {
    return this.environment.invalid(message);
  }

  private static int localQueryCopies(SearchQuery query) {
    if (query instanceof CollectorQuery collector) {
      return switch (collector.collector()) {
        case FacetCollector facet -> 2 + facet.facetDefinitions().size();
        case AggregationCollector aggregation -> 2 + aggregation.facets().size();
      };
    }
    return 1;
  }

  private RuntimeException exhausted(String message) {
    return this.environment.exhausted(message);
  }

  public final class Plan implements Closeable {
    private final boolean concurrent;
    private final Runnable check = JoinPlanner.this.environment.cancellation();
    private final int localQueryCopies;
    private final Map<String, GenerationId> dependencies = new LinkedHashMap<>();
    private final Map<String, QuerySnapshot> snapshots = new LinkedHashMap<>();
    private final List<Closeable> leases = new ArrayList<>();
    @Nullable private QuerySnapshot localSnapshot;
    private SearchQuery query;
    private long bytes;
    private long work;
    private int keys;
    private long keyBytes;
    private boolean closed;

    private Plan(SearchQuery query) {
      this.concurrent = query.concurrent();
      this.query = query;
      this.localQueryCopies = localQueryCopies(query);
    }

    public SearchQuery query() {
      return this.query;
    }

    public QuerySnapshot snapshot() {
      return Objects.requireNonNull(this.localSnapshot);
    }

    public boolean dependsOn(String id) {
      return this.dependencies.containsKey(id);
    }

    public boolean dependsOnGeneration(String generation) {
      return this.dependencies.values().stream()
          .anyMatch(value -> value.uniqueString().equals(generation));
    }

    public void activate(Runnable check) {
      check.run();
      snapshot().setCancellationCheck(check);
    }

    public void verifyDependencies() {
      for (var dependency : this.dependencies.entrySet()) {
        JoinPlanner.this.environment.verify(dependency.getKey(), dependency.getValue());
      }
    }

    public void onInvalidated(Runnable callback) {
      JoinPlanner.this.environment.onInvalidated(callback);
    }

    private void add(String id, Optional<GenerationId> expected) throws Exception {
      if (this.dependencies.containsKey(id)) {
        return;
      }
      this.check.run();
      var generation = JoinPlanner.this.environment.pin(id, expected);
      this.leases.add(JoinPlanner.this.environment.lease(generation));
      this.dependencies.put(id, generation);
      this.snapshots.put(
          id,
          JoinPlanner.this.environment.acquire(
              generation, expected.isPresent() && this.concurrent));
    }

    private void checkWork() {
      this.check.run();
      if (++this.work > MAX_FOREIGN_WORK) {
        throw exhausted("Join foreign-work limit exceeded");
      }
    }

    private void reserveKey(int keyBytes) {
      if (++this.keys > MAX_KEYS) {
        throw exhausted("Join distinct-key limit exceeded");
      }
      // Includes pooled bytes, foreign ordinal deduplication, per-partition probes,
      // lazy term queries and rewrites, or numeric set-query arrays and hash tables.
      // Keep conservative cursor-lifetime admission.
      long estimate =
          (400L + 16L * keyBytes) * (1L + (long) snapshot().partitions() * this.localQueryCopies);
      reserveBytes(estimate);
      this.keyBytes += keyBytes;
    }

    private void reserveBytes(long estimate) {
      var limits = JoinPlanner.this.memoryLimits;
      if (this.bytes > limits.maxQueryBytes() - estimate) {
        throw memoryExhausted(
            "Join query-memory limit exceeded", limits.maxQueryBytes(), this.bytes, estimate);
      }
      // Check before adding so even limits near Long.MAX_VALUE cannot overflow the counter.
      long retained =
          RETAINED_BYTES.getAndUpdate(
              current ->
                  current > limits.maxConcurrentBytes() - estimate ? current : current + estimate);
      if (retained > limits.maxConcurrentBytes() - estimate) {
        throw memoryExhausted(
            "Concurrent join-memory limit exceeded",
            limits.maxConcurrentBytes(),
            retained,
            estimate);
      }
      this.bytes += estimate;
    }

    private RuntimeException memoryExhausted(
        String message, long limit, long reserved, long requested) {
      return exhausted(
          message
              + " (limit: "
              + limit
              + " bytes, reserved: "
              + reserved
              + " bytes, requested: "
              + requested
              + " bytes)");
    }

    @Override
    public void close() throws IOException {
      if (!this.closed) {
        this.closed = true;
        try {
          IOUtils.close(this.snapshots.values());
        } finally {
          RETAINED_BYTES.addAndGet(-this.bytes);
          this.snapshots.clear();
          try {
            IOUtils.close(this.leases);
          } finally {
            this.leases.clear();
          }
        }
      }
    }
  }
}
