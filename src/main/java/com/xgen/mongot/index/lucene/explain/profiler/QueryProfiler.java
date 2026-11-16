package com.xgen.mongot.index.lucene.explain.profiler;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.explain.information.LuceneQueryDetails;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.creator.QueryExplainInformationCreator;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContext;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.query.QueryVisitorQueryExecutionContext;
import com.xgen.mongot.index.lucene.explain.query.QueryVisitorQueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.query.RewrittenQueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.query.RewrittenQueryNodeException;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.Optionals;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

public class QueryProfiler {
  private static final int RECENT_LUCENE_QUERY_LIMIT = 10;

  private final List<QueryExecutionContext<QueryVisitorQueryExecutionContextNode>>
      executionContexts;
  @Var private Optional<RewrittenQueryExecutionContextNode> mergedNode;
  private final LinkedHashSet<LuceneQueryDetails> luceneQueries = new LinkedHashSet<>();
  private final int luceneQueryLimit;

  public QueryProfiler() {
    this(Explain.Verbosity.ALL_PLANS_EXECUTION);
  }

  public QueryProfiler(Explain.Verbosity verbosity) {
    this(new ArrayList<>(), Optional.empty(), verbosity);
  }

  @VisibleForTesting
  QueryProfiler(
      List<QueryExecutionContext<QueryVisitorQueryExecutionContextNode>> executionContexts,
      Optional<RewrittenQueryExecutionContextNode> mergedNode) {
    this(executionContexts, mergedNode, Explain.Verbosity.ALL_PLANS_EXECUTION);
  }

  private QueryProfiler(
      List<QueryExecutionContext<QueryVisitorQueryExecutionContextNode>> executionContexts,
      Optional<RewrittenQueryExecutionContextNode> mergedNode,
      Explain.Verbosity verbosity) {
    this.executionContexts = executionContexts;
    this.mergedNode = mergedNode;
    this.luceneQueryLimit =
        verbosity == Explain.Verbosity.ALL_PLANS_EXECUTION
            ? Integer.MAX_VALUE
            : RECENT_LUCENE_QUERY_LIMIT;
  }

  public void createQueryExecutionContext() {
    this.executionContexts.add(new QueryVisitorQueryExecutionContext());
  }

  /** Create context for this query or subquery. */
  public Optional<QueryVisitorQueryExecutionContextNode> getOrCreateNode(Query query) {
    return getCurrentExecutionContext().getOrCreateNode(query);
  }

  /** Adds a child must node to a root vector query node */
  public void addVectorMustNode(Query original, Query rewritten) {
    recordLuceneQuery(original, rewritten);
    if (original == rewritten) {
      return;
    }

    var root = Check.isPresent(getCurrentExecutionContext().getRoot(), "root");
    checkArg(root.getQuery() == original, "original query must be root query");

    getCurrentExecutionContext().addChildNode(original, rewritten, BooleanClause.Occur.MUST);
  }

  /** Adds a child filter node to a root vector query node. */
  public void addVectorFilterNode(Query filter) {
    QueryExecutionContext<QueryVisitorQueryExecutionContextNode> currentExecutionContext =
        getCurrentExecutionContext();
    var root = Check.isPresent(currentExecutionContext.getRoot(), "root");
    currentExecutionContext.addChildNode(root.getQuery(), filter, BooleanClause.Occur.FILTER);
  }

  /** Records root query renderings and replaces the node when rewritten != original. */
  public void replaceNode(Query original, Query rewritten) {
    recordLuceneQuery(original, rewritten);
    if (original == rewritten) {
      return;
    }

    if (getCurrentExecutionContext()
        .getRoot()
        .map((root) -> root.getQuery() == original)
        .orElse(false)) {
      this.executionContexts.removeLast();
      createQueryExecutionContext();
      getOrCreateNode(rewritten);
      return;
    }

    getCurrentExecutionContext().replaceNode(original, rewritten);
  }

  private void recordLuceneQuery(Query original, Query rewritten) {
    if (this.executionContexts.isEmpty()) {
      return;
    }
    var root = getCurrentExecutionContext().getRoot();
    if (root.isEmpty() || root.get().getQuery() == original) {
      // Only root rewrites describe the whole search. Nested rewrites (including lazy join
      // postings and vector filters) must not appear as independent query plans.
      this.luceneQueries.add(new LuceneQueryDetails(original.toString(), rewritten.toString()));
      // Bound retained diagnostic strings, not just the serialized snapshot. Otherwise a
      // long-lived executionStats cursor could keep every discarded rendering indefinitely.
      if (this.luceneQueries.size() > this.luceneQueryLimit) {
        this.luceneQueries.removeFirst();
      }
    }
  }

  /** Retained distinct query renderings across batches, in insertion order. */
  public List<LuceneQueryDetails> luceneQueries(Explain.Verbosity verbosity) {
    int skip =
        verbosity == Explain.Verbosity.ALL_PLANS_EXECUTION
            ? 0
            : Math.max(0, this.luceneQueries.size() - RECENT_LUCENE_QUERY_LIMIT);
    return this.luceneQueries.stream().skip(skip).toList();
  }

  /** Get the complete explain information associated with the root query of this Profiler. */
  public List<QueryExplainInformation> explainInformation(Explain.Verbosity verbosity) {
    this.executionContexts.forEach(
        context -> Check.isPresent(context.getRoot(), "root context node"));

    // Combine all root nodes into a single stream then output explain
    return Streams.concat(
            Stream.of(this.mergedNode),
            this.executionContexts.stream().map(QueryExecutionContext::getRoot))
        .flatMap(Optional::stream)
        .map(root -> QueryExplainInformationCreator.fromNode(root, verbosity))
        .toList();
  }

  /**
   * Aggregates statistics collected across individual search + getMore's and updates the
   * QueryProfiler's internal state. If an error occurs in the process, an exception is thrown which
   * should be handled by the calling class.
   *
   * <p>Note: If aggregate fails during a getMore, we do not attempt to aggregate again in
   * subsequent getMore's.
   */
  public void aggregate() throws RewrittenQueryNodeException {
    /*
     Only under these exact conditions should we attempt to aggregate statistics. In any other
     scenario, do not attempt.
    */
    if ((this.executionContexts.size() == 2 && this.mergedNode.isEmpty())
        || (this.executionContexts.size() == 1 && this.mergedNode.isPresent())) {
      aggregateQueryTrees();
    }
  }

  private void aggregateQueryTrees() throws RewrittenQueryNodeException {
    List<? extends QueryExecutionContextNode> allRootNodes =
        Streams.concat(
                Stream.of(this.mergedNode),
                this.executionContexts.stream().map(QueryExecutionContext::getRoot))
            .flatMap(Optional::stream)
            .toList();
    Check.checkState(
        allRootNodes.size() == 2, "Must be exactly 2 root query context nodes present");

    try {
      RewrittenQueryExecutionContextNode firstRoot =
          Optionals.orElseGetChecked(
              this.mergedNode, () -> this.executionContexts.getFirst().rewritten());
      RewrittenQueryExecutionContextNode secondRoot = this.executionContexts.getLast().rewritten();

      // Clear contexts and update mergedNode reference to be used in next aggregate
      this.mergedNode =
          Optional.of(RewrittenQueryExecutionContextNode.mergeTrees(firstRoot, secondRoot));
      this.executionContexts.clear();
    } catch (RewrittenQueryNodeException e) {
      throw RewrittenQueryNodeException.wrapWithRootsAndRethrow(allRootNodes, e);
    }
  }

  private QueryExecutionContext<QueryVisitorQueryExecutionContextNode>
      getCurrentExecutionContext() {
    Check.checkState(!this.executionContexts.isEmpty(), "no execution context present");
    return this.executionContexts.getLast();
  }

  @VisibleForTesting
  Optional<RewrittenQueryExecutionContextNode> getMergedNode() {
    return this.mergedNode;
  }

  @VisibleForTesting
  List<QueryExecutionContext<QueryVisitorQueryExecutionContextNode>> getAllExecutionContexts() {
    return this.executionContexts;
  }
}
