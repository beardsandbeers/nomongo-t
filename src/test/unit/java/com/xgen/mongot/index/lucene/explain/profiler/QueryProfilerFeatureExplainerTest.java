package com.xgen.mongot.index.lucene.explain.profiler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.truth.Truth;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.mongot.index.lucene.explain.information.TermQuerySpec;
import com.xgen.mongot.index.lucene.explain.query.RewrittenQueryNodeException;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

public class QueryProfilerFeatureExplainerTest {
  @Test
  public void recordLuceneQuery_executionStats_discardsOldRenderingsFromRetainedState() {
    var explainer =
        new QueryProfilerFeatureExplainer(
            SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH),
            Explain.Verbosity.EXECUTION_STATS);
    var profiler = explainer.getQueryProfiler();
    IntStream.range(0, 12)
        .forEach(
            i -> {
              var query = new TermQuery(new Term("title", "term" + i));
              profiler.createQueryExecutionContext();
              profiler.replaceNode(query, query);
            });

    // Inspect all retained entries, rather than the executionStats serialization limit.
    var retained = profiler.luceneQueries(Explain.Verbosity.ALL_PLANS_EXECUTION);

    Truth.assertThat(retained).hasSize(10);
    Truth.assertThat(retained.getFirst().analyzed()).isEqualTo("title:term2");
    Truth.assertThat(retained.getLast().analyzed()).isEqualTo("title:term11");
  }

  @Test
  public void replaceNode_unchangedQueryOutsideSearch_doesNotCreateExplainState() {
    var query = new TermQuery(new Term("title", "run"));
    var profiler = new QueryProfiler();

    profiler.replaceNode(query, query);
    profiler.addVectorMustNode(query, query);

    Truth.assertThat(profiler.luceneQueries(Explain.Verbosity.ALL_PLANS_EXECUTION)).isEmpty();
  }

  @Test
  public void emitExplanation_rewrittenQueryAcrossBatches_preservesDistinctLuceneForms()
      throws Exception {
    var term = new TermQuery(new Term("title", "run"));
    var analyzed = new BooleanQuery.Builder().add(term, BooleanClause.Occur.MUST).build();
    var profiler = new QueryProfiler();
    var explainer =
        new QueryProfilerFeatureExplainer(
            profiler, SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH));
    profiler.createQueryExecutionContext();
    profiler.replaceNode(analyzed, term);
    profiler.createQueryExecutionContext();
    profiler.replaceNode(analyzed, term);

    explainer.aggregate();
    var builder = SearchExplainInformationBuilder.newBuilder();
    explainer.emitExplanation(Explain.Verbosity.QUERY_PLANNER, builder);
    var explanation = builder.build();
    var bson = explanation.toBson();

    Truth.assertThat(bson.getDocument("luceneQuery").getString("analyzed").getValue())
        .isEqualTo("+title:run");
    Truth.assertThat(bson.getDocument("luceneQuery").getString("rewritten").getValue())
        .isEqualTo("title:run");
    Truth.assertThat(explanation.luceneQuery().orElseThrow()).hasSize(1);
    try (var parser = BsonDocumentParser.fromRoot(bson).build()) {
      Truth.assertThat(SearchExplainInformation.fromBson(parser).luceneQuery())
          .isEqualTo(explanation.luceneQuery());
    }
  }

  @Test
  public void emitExplanation_multipleQueries_respectsVerbosityAndRoundTrips() throws Exception {
    var profiler = new QueryProfiler();
    var explainer =
        new QueryProfilerFeatureExplainer(
            profiler, SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH));
    IntStream.range(0, 12)
        .forEach(
            i -> {
              var query = new TermQuery(new Term("title", "term" + i));
              profiler.createQueryExecutionContext();
              profiler.getOrCreateNode(query);
              profiler.replaceNode(query, query);
            });

    var execution = SearchExplainInformationBuilder.newBuilder();
    explainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, execution);
    var all = SearchExplainInformationBuilder.newBuilder();
    explainer.emitExplanation(Explain.Verbosity.ALL_PLANS_EXECUTION, all);
    var allBson = all.build().toBson();

    Truth.assertThat(execution.build().luceneQuery().orElseThrow()).hasSize(10);
    Truth.assertThat(execution.build().luceneQuery().orElseThrow().getFirst().analyzed())
        .isEqualTo("title:term2");
    Truth.assertThat(allBson.getArray("luceneQuery")).hasSize(12);
    try (var parser = BsonDocumentParser.fromRoot(allBson).build()) {
      Truth.assertThat(SearchExplainInformation.fromBson(parser).luceneQuery())
          .isEqualTo(all.build().luceneQuery());
    }
  }

  @Test
  public void emitExplanation_nestedRewrite_keepsOnlyWholeQueryRendering() {
    var term = new TermQuery(new Term("title", "run"));
    var query = new BooleanQuery.Builder().add(term, BooleanClause.Occur.MUST).build();
    var profiler = new QueryProfiler();
    profiler.createQueryExecutionContext();
    profiler.getOrCreateNode(query);

    profiler.replaceNode(query, query);
    profiler.replaceNode(term, new TermQuery(new Term("title", "runner")));

    var details = profiler.luceneQueries(Explain.Verbosity.ALL_PLANS_EXECUTION);
    Truth.assertThat(details).hasSize(1);
    Truth.assertThat(details.getFirst().analyzed()).isEqualTo("+title:run");
    Truth.assertThat(details.getFirst().rewritten()).isEqualTo("+title:run");
  }

  @Test
  public void testFailedAggregate() throws RewrittenQueryNodeException {
    QueryProfiler profiler = spy(new QueryProfiler());
    doThrow(RewrittenQueryNodeException.class).when(profiler).aggregate();
    IndexMetricsUpdater.QueryingMetricsUpdater metricsUpdater =
        SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH);

    QueryProfilerFeatureExplainer profilerExplainer =
        new QueryProfilerFeatureExplainer(profiler, metricsUpdater);
    profilerExplainer.aggregate();

    Truth.assertThat(metricsUpdater.getFailedExplainQueryAggregate().count()).isEqualTo(1);
  }

  @Test
  public void testManyQueryExplainInfosAllPlansExecution() {
    QueryProfiler profiler = spy(new QueryProfiler());
    QueryProfilerFeatureExplainer profilerExplainer =
        new QueryProfilerFeatureExplainer(
            profiler, SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH));

    when(profiler.explainInformation(any()))
        .thenReturn(
            IntStream.range(0, 20)
                .mapToObj(
                    unused ->
                        new QueryExplainInformation(
                            Optional.of(FieldPath.newRoot("foo")),
                            LuceneQuerySpecification.Type.TERM_QUERY,
                            Optional.empty(),
                            new TermQuerySpec(FieldPath.newRoot("foo"), "bar"),
                            Optional.empty()))
                .toList());

    SearchExplainInformationBuilder builder = spy(new SearchExplainInformationBuilder());
    profilerExplainer.emitExplanation(Explain.Verbosity.ALL_PLANS_EXECUTION, builder);
    verify(builder).queryExplainInfos(argThat(query -> query.size() == 20));
  }

  @Test
  public void testManyQueryExplainInfosExecutionStats() {
    QueryProfiler profiler = spy(new QueryProfiler());
    QueryProfilerFeatureExplainer profilerExplainer =
        new QueryProfilerFeatureExplainer(
            profiler, SearchIndex.mockQueryMetricsUpdater(IndexDefinition.Type.SEARCH));

    List<QueryExplainInformation> firstTen =
        IntStream.range(0, 10)
            .mapToObj(
                unused ->
                    new QueryExplainInformation(
                        Optional.of(FieldPath.newRoot("foo")),
                        LuceneQuerySpecification.Type.TERM_QUERY,
                        Optional.empty(),
                        new TermQuerySpec(FieldPath.newRoot("foo"), "bar"),
                        Optional.empty()))
            .toList();

    List<QueryExplainInformation> secondTen =
        IntStream.range(0, 10)
            .mapToObj(
                unused ->
                    new QueryExplainInformation(
                        Optional.of(FieldPath.newRoot("abc")),
                        LuceneQuerySpecification.Type.TERM_QUERY,
                        Optional.empty(),
                        new TermQuerySpec(FieldPath.newRoot("abc"), "def"),
                        Optional.empty()))
            .toList();

    when(profiler.explainInformation(any()))
        .thenReturn(Stream.concat(firstTen.stream(), secondTen.stream()).toList());

    SearchExplainInformationBuilder builder = spy(new SearchExplainInformationBuilder());
    profilerExplainer.emitExplanation(Explain.Verbosity.EXECUTION_STATS, builder);
    verify(builder).queryExplainInfos(argThat(query -> query.size() == 10));
    verify(builder).queryExplainInfos(argThat(queryInfos -> queryInfos.equals(secondTen)));
  }
}
