package com.xgen.mongot.index.lucene.explain.tracing;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.lucene.explain.information.LuceneQuerySpecification;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformation;
import com.xgen.mongot.index.lucene.explain.information.SearchExplainInformationBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.QueryExplainInformationBuilder;
import com.xgen.testing.mongot.index.lucene.explain.information.TermQueryBuilder;
import com.xgen.testing.mongot.index.lucene.explain.tracing.FakeExplain;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

public class ExplainTest {

  @Test
  public void suspend_partitionContext_restoresExplainAndPreservesOtherContext() {
    var marker = ContextKey.<String>named("explain-test-marker");
    try (var context = Context.current().with(marker, "present").makeCurrent();
        var explain = Explain.setup(Explain.Verbosity.EXECUTION_STATS, 2);
        var partition = Explain.maybeEnterIndexPartitionQueryContext(1)) {
      var original = Explain.getQueryInfo().orElseThrow();

      try (var ignored = Explain.suspend()) {
        Assert.assertFalse(Explain.isEnabled());
        Assert.assertTrue(Explain.getQueryInfo().isEmpty());
        Assert.assertEquals("present", Context.current().get(marker));
        try (var independent = Explain.setup(Explain.Verbosity.QUERY_PLANNER, 1)) {
          Assert.assertNotSame(original, Explain.getQueryInfo().orElseThrow());
          Assert.assertEquals(
              Explain.Verbosity.QUERY_PLANNER, Explain.getQueryInfo().orElseThrow().getVerbosity());
        }
        Assert.assertFalse(Explain.isEnabled());
      }

      Assert.assertSame(original, Explain.getQueryInfo().orElseThrow());
    }
    Assert.assertFalse(Explain.isEnabled());
  }

  @Test
  public void testInitializeExplainWithExplainQueryState() {
    SearchExplainInformation explain =
        SearchExplainInformationBuilder.newBuilder()
            .queryExplainInfos(
                List.of(
                    QueryExplainInformationBuilder.builder()
                        .type(LuceneQuerySpecification.Type.TERM_QUERY)
                        .args(TermQueryBuilder.builder().path("a").value("hello").build())
                        .build()))
            .build();

    var explainState =
        new FakeExplain.FakeQueryState(
            Explain.Verbosity.QUERY_PLANNER,
            IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue(),
            explain);

    try (Scope unused = Explain.setup(explainState)) {
      Assert.assertEquals(explain, explainState.collect());
    }
  }

  @Test
  public void testInitializeExplainWithQueryStateContainingPartitions() {
    Optional<ExplainQueryState> queryState;
    int numPartitions = 8;
    try (Scope unused = Explain.setup(Explain.Verbosity.EXECUTION_STATS, numPartitions)) {
      queryState = Explain.getExplainQueryState();
    }

    try (Scope unused = Explain.setup(queryState.orElseThrow())) {
      IntStream.range(0, numPartitions).forEach(Explain::maybeEnterIndexPartitionQueryContext);
    }
  }
}
