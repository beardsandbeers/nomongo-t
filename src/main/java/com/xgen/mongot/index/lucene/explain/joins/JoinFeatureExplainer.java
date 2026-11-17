package com.xgen.mongot.index.lucene.explain.joins;

import com.google.common.base.Ticker;
import com.xgen.mongot.index.lucene.explain.information.JoinExecutionInformation;
import com.xgen.mongot.index.lucene.explain.information.JoinExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.JoinPreparationInformation;
import com.xgen.mongot.index.lucene.explain.information.JoinSegmentInformation;
import com.xgen.mongot.index.lucene.explain.information.JoinSourceInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.explain.tracing.FeatureExplainer;
import com.xgen.mongot.index.query.operators.JoinMembership;
import com.xgen.mongot.util.timers.InvocationCountingTimer;
import com.xgen.mongot.util.timers.ThreadSafeInvocationCountingTimer;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Query-scoped join preparation and partition-scoped execution, retained across cursor batches. */
public final class JoinFeatureExplainer implements FeatureExplainer {
  private final boolean executionStats;
  private final Map<JoinMembership, Integer> ids = new IdentityHashMap<>();
  private final Map<Integer, JoinSourceInformation> sources = new TreeMap<>();
  private final Map<Integer, Execution> executions = new TreeMap<>();
  private Optional<JoinPreparationInformation> preparation = Optional.empty();

  private JoinFeatureExplainer(Explain.Verbosity verbosity) {
    this.executionStats = verbosity != Explain.Verbosity.QUERY_PLANNER;
  }

  public static Optional<JoinFeatureExplainer> root() {
    return Explain.getExplainQueryState()
        .map(state -> state.getRootQueryInfo())
        .map(
            info ->
                info.getFeatureExplainer(
                    JoinFeatureExplainer.class,
                    () -> new JoinFeatureExplainer(info.getVerbosity())));
  }

  public static Optional<Execution> forJoin(
      JoinMembership keys, String field, String keyType, boolean exhaustive, int countThreshold) {
    return Explain.getQueryInfo()
        .map(
            info -> {
              int id = root().orElseThrow().register(keys);
              return info.getFeatureExplainer(
                      JoinFeatureExplainer.class,
                      () -> new JoinFeatureExplainer(info.getVerbosity()))
                  .execution(id, field, keyType, keys.size(), exhaustive, countThreshold);
            });
  }

  public synchronized int register(JoinMembership keys) {
    return this.ids.computeIfAbsent(keys, ignored -> this.ids.size());
  }

  public synchronized void source(JoinSourceInformation source) {
    this.sources.put(source.id(), source);
  }

  public Optional<Long> executionCount(long count) {
    return this.executionStats ? Optional.of(count) : Optional.empty();
  }

  public Optional<QueryExecutionArea> executionTiming(long nanos, String operation) {
    return this.executionStats
        ? Optional.of(new QueryExecutionArea(nanos, Optional.of(Map.of(operation, 1L))))
        : Optional.empty();
  }

  public synchronized void prepared(
      int localPartitions,
      int keyCount,
      long keyBytes,
      long reservedBytes,
      Map<String, Long> limits,
      long workUnits,
      long elapsedNanos,
      long dependencyNanos) {
    this.preparation =
        Optional.of(
            new JoinPreparationInformation(
                localPartitions,
                keyCount,
                keyBytes,
                reservedBytes,
                Map.copyOf(limits),
                executionCount(workUnits),
                executionTiming(elapsedNanos, "prepare"),
                executionTiming(dependencyNanos, "acquireDependencies"),
                List.copyOf(this.sources.values())));
  }

  private synchronized Execution execution(
      int id, String field, String keyType, int keyCount, boolean exhaustive, int countThreshold) {
    return this.executions.computeIfAbsent(
        id,
        ignored ->
            new Execution(
                id, field, keyType, keyCount, exhaustive, countThreshold, this.executionStats));
  }

  @Override
  public synchronized void emitExplanation(
      Explain.Verbosity verbosity, SearchExplainInformationBuilder builder) {
    if (this.preparation.isPresent() || !this.executions.isEmpty()) {
      builder.joins(
          new JoinExplainInformation(
              this.preparation,
              this.executions.isEmpty()
                  ? Optional.empty()
                  : Optional.of(
                      this.executions.values().stream().map(Execution::snapshot).toList())));
    }
  }

  private static ThreadSafeInvocationCountingTimer timer() {
    return new ThreadSafeInvocationCountingTimer(Ticker.systemTicker());
  }

  private static QueryExecutionArea area(InvocationCountingTimer timer, String operation) {
    return new QueryExecutionArea(
        timer.getElapsedNanos(), Optional.of(Map.of(operation, timer.getInvocationCount())));
  }

  /** One local join domain. Segment ordinals are stable on the cursor's pinned reader. */
  public static final class Execution {
    private static final InvocationCountingTimer.SafeClosable NO_OP = () -> {};
    private final int id;
    private final String field;
    private final String keyType;
    private final int keyCount;
    private final boolean exhaustive;
    private final int countThreshold;
    private final boolean executionStats;
    private final Map<Integer, Segment> segments = new TreeMap<>();
    private final ThreadSafeInvocationCountingTimer weightSetup = timer();
    private volatile String strategy = "adaptive";
    private volatile String selectionReason = "sampledCost";

    private Execution(
        int id,
        String field,
        String keyType,
        int keyCount,
        boolean exhaustive,
        int countThreshold,
        boolean executionStats) {
      this.id = id;
      this.field = field;
      this.keyType = keyType;
      this.keyCount = keyCount;
      this.exhaustive = exhaustive;
      this.countThreshold = countThreshold;
      this.executionStats = executionStats;
    }

    public void strategy(String strategy, String reason) {
      this.strategy = strategy;
      this.selectionReason = reason;
    }

    public InvocationCountingTimer.SafeClosable timeWeightSetup() {
      return this.executionStats ? this.weightSetup.split() : NO_OP;
    }

    public synchronized Optional<Segment> segment(int ordinal, int maxDoc) {
      if (!this.executionStats) {
        return Optional.empty();
      }
      return Optional.of(
          this.segments.computeIfAbsent(ordinal, ignored -> new Segment(ordinal, maxDoc)));
    }

    private synchronized JoinExecutionInformation snapshot() {
      return new JoinExecutionInformation(
          this.id,
          this.field,
          this.keyType,
          this.keyCount,
          this.strategy,
          this.selectionReason,
          this.exhaustive,
          this.countThreshold,
          this.weightSetup.getInvocationCount() == 0
              ? Optional.empty()
              : Optional.of(area(this.weightSetup, "createWeight")),
          this.executionStats
              ? Optional.of(this.segments.values().stream().map(Segment::snapshot).toList())
              : Optional.empty());
    }
  }

  /** Initial scorer choices; switching a probe to postings is counted separately as a fallback. */
  public enum Strategy {
    DOC_VALUES,
    POSTINGS,
    POINTS
  }

  /** Thread-safe cumulative work for one local segment, reused rather than appended per batch. */
  public static final class Segment {
    private final int ordinal;
    private final int maxDoc;
    private volatile Map<String, Double> estimates = Map.of();
    private volatile Optional<Long> probeBudget = Optional.empty();
    private final AtomicLong minLeadCost = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxLeadCost = new AtomicLong();
    private final LongAdder docValuesScorers = new LongAdder();
    private final LongAdder postingsScorers = new LongAdder();
    private final LongAdder pointScorers = new LongAdder();
    private final LongAdder probes = new LongAdder();
    private final LongAdder probeMatches = new LongAdder();
    private final LongAdder valuesExamined = new LongAdder();
    private final LongAdder dictionaryLookups = new LongAdder();
    private final LongAdder fallbacks = new LongAdder();
    private final ThreadSafeInvocationCountingTimer costEstimation = timer();
    private final ThreadSafeInvocationCountingTimer indexSetup = timer();

    private Segment(int ordinal, int maxDoc) {
      this.ordinal = ordinal;
      this.maxDoc = maxDoc;
    }

    public void estimates(Map<String, Double> estimates, Optional<Long> probeBudget) {
      this.estimates = Map.copyOf(estimates);
      this.probeBudget = probeBudget;
    }

    public InvocationCountingTimer.SafeClosable timeCostEstimation() {
      return this.costEstimation.split();
    }

    public InvocationCountingTimer.SafeClosable timeIndexSetup() {
      return this.indexSetup.split();
    }

    public void scorer(Strategy strategy, long leadCost) {
      this.minLeadCost.accumulateAndGet(leadCost, Math::min);
      this.maxLeadCost.accumulateAndGet(leadCost, Math::max);
      switch (strategy) {
        case DOC_VALUES -> this.docValuesScorers.increment();
        case POSTINGS -> this.postingsScorers.increment();
        case POINTS -> this.pointScorers.increment();
      }
    }

    public void probe() {
      this.probes.increment();
    }

    public void probeMatch() {
      this.probeMatches.increment();
    }

    public void valueExamined() {
      this.valuesExamined.increment();
    }

    public void dictionaryLookup() {
      this.dictionaryLookups.increment();
    }

    public void fallback() {
      this.fallbacks.increment();
    }

    private JoinSegmentInformation snapshot() {
      long docValues = this.docValuesScorers.sum();
      long postings = this.postingsScorers.sum();
      long points = this.pointScorers.sum();
      long fallbackCount = this.fallbacks.sum();
      boolean hasScorers = docValues + postings + points > 0;
      return new JoinSegmentInformation(
          this.ordinal,
          this.maxDoc,
          this.estimates,
          this.probeBudget,
          hasScorers ? Optional.of(this.minLeadCost.get()) : Optional.empty(),
          hasScorers ? Optional.of(this.maxLeadCost.get()) : Optional.empty(),
          Map.of("docValuesProbe", docValues, "postings", postings, "points", points),
          Map.of(
              "probes",
              this.probes.sum(),
              "probeMatches",
              this.probeMatches.sum(),
              "valuesExamined",
              this.valuesExamined.sum(),
              "dictionaryLookups",
              this.dictionaryLookups.sum(),
              "fallbacks",
              fallbackCount),
          fallbackCount == 0 ? Optional.empty() : Optional.of("probeBudgetExceeded"),
          area(this.costEstimation, "estimateCost"),
          area(this.indexSetup, "createIndexScorer"));
    }
  }
}
