package com.xgen.mongot.index.lucene.query;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.explain.information.JoinExplainInformation;
import com.xgen.mongot.index.lucene.explain.joins.JoinFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.operators.JoinKeySet;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilterSortedSetDocValues;
import org.apache.lucene.index.IndexReader.CacheHelper;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class AdaptiveJoinQueryTest {
  @Test
  public void search_smallKeyPlanner_keepsFastPathAndOmitsExecutionCounters() throws Exception {
    try (var fixture = new Fixture(100, 128, false, false, 100);
        var scope = Explain.setup(Optional.of(Explain.Verbosity.QUERY_PLANNER), Optional.of(1))) {
      var keys = keys(2, false);
      var trace = JoinFeatureExplainer.forJoin(keys, "key", "TOKEN", false, 1000);
      var query = new AdaptiveJoinQuery("key", keys, false, 1000, trace);

      var actual = fixture.searcher.count(query);
      var expected = fixture.searcher.count(new TermInSetQuery("key", keys.terms()));
      var execution =
          Explain.collect()
              .orElseThrow()
              .joins()
              .orElseThrow()
              .execution()
              .orElseThrow()
              .getFirst();

      assertThat(actual).isEqualTo(expected);
      assertThat(execution.strategy()).isEqualTo("termInSet");
      assertThat(execution.selectionReason()).isEqualTo("smallKeySet");
      assertThat(execution.segments()).isEmpty();
      assertThat(execution.weightSetup()).isEmpty();
    }
  }

  @Test
  public void search_explainedFallback_preservesResultsAndAccumulatesWithoutGrowingSegments()
      throws Exception {
    try (var fixture = new Fixture(10000, 128, false, true, 10000);
        var scope = Explain.setup(Optional.of(Explain.Verbosity.EXECUTION_STATS), Optional.of(1))) {
      var keys = keys(1000, false);
      var baseline = compound(new TermInSetQuery("key", keys.terms()), false, false);
      var trace = JoinFeatureExplainer.forJoin(keys, "key", "TOKEN", false, 0);
      var query = compound(new AdaptiveJoinQuery("key", keys, false, 0, trace), false, false);

      var expected = fixture.searcher.search(baseline, 10001, Sort.INDEXORDER);
      var actual = fixture.searcher.search(query, 10001, Sort.INDEXORDER);
      var first = Explain.collect().orElseThrow().joins().orElseThrow();
      var nextQuery =
          compound(
              new AdaptiveJoinQuery(
                  "key",
                  keys,
                  false,
                  0,
                  JoinFeatureExplainer.forJoin(keys, "key", "TOKEN", false, 0)),
              false,
              false);
      fixture.searcher.search(nextQuery, 10001, Sort.INDEXORDER);
      var second = Explain.collect().orElseThrow().joins().orElseThrow();

      assertThat(Arrays.stream(actual.scoreDocs).mapToInt(hit -> hit.doc).toArray())
          .isEqualTo(Arrays.stream(expected.scoreDocs).mapToInt(hit -> hit.doc).toArray());
      var firstSegments = first.execution().orElseThrow().getFirst().segments().orElseThrow();
      var secondSegments = second.execution().orElseThrow().getFirst().segments().orElseThrow();
      assertThat(first.execution().orElseThrow()).hasSize(1);
      assertThat(second.execution().orElseThrow()).hasSize(1);
      assertThat(firstSegments).hasSize(1);
      assertThat(secondSegments).hasSize(1);
      var initial = firstSegments.getFirst();
      var cumulative = secondSegments.getFirst();
      assertThat(initial.strategyCounts().get("docValuesProbe")).isEqualTo(1L);
      assertThat(initial.counters().get("fallbacks")).isEqualTo(1L);
      assertThat(initial.fallbackReason()).hasValue("probeBudgetExceeded");
      assertThat(initial.counters().get("probes")).isAtLeast(initial.probeBudget().orElseThrow());
      assertThat(initial.counters().get("dictionaryLookups")).isGreaterThan(0L);
      assertThat(cumulative.counters().get("fallbacks")).isEqualTo(2L);
      assertThat(cumulative.counters().get("probes"))
          .isEqualTo(2 * initial.counters().get("probes"));
      assertThat(cumulative.costEstimation().invocationCounts().orElseThrow().get("estimateCost"))
          .isEqualTo(2L);
      try (var parser = BsonDocumentParser.fromRoot(second.toBson()).build()) {
        assertThat(JoinExplainInformation.fromBson(parser)).isEqualTo(second);
      }
    }
  }

  public record Scenario(boolean binary, boolean multi, boolean exhaustive, boolean exclusion) {}

  @DataPoints public static final boolean[] BINARY_KEYS = {false, true};

  @DataPoints
  public static final Scenario[] SCENARIOS = {
    new Scenario(false, false, false, false),
    new Scenario(true, false, false, false),
    new Scenario(false, true, true, false),
    new Scenario(true, true, true, false),
    new Scenario(false, true, false, true),
    new Scenario(true, true, true, true)
  };

  @Theory
  public void search_arraysMissingValuesDeletionsAndSegments_matchesPostings(Scenario scenario)
      throws Exception {
    try (var fixture = new Fixture(3000, 128, scenario.binary(), scenario.multi(), 1000)) {
      var keys = keys(64, scenario.binary());
      var baseline = compound(new TermInSetQuery("key", keys.terms()), scenario.exclusion(), false);
      var adaptive =
          compound(
              new AdaptiveJoinQuery("key", keys, scenario.exhaustive(), 0),
              scenario.exclusion(),
              false);

      var expected = fixture.searcher.search(baseline, 3001, Sort.INDEXORDER);
      var actual = fixture.searcher.search(adaptive, 3001, Sort.INDEXORDER);

      assertThat(Arrays.stream(actual.scoreDocs).mapToInt(hit -> hit.doc).toArray())
          .isEqualTo(Arrays.stream(expected.scoreDocs).mapToInt(hit -> hit.doc).toArray());
      assertThat(fixture.searcher.count(adaptive)).isEqualTo(fixture.searcher.count(baseline));
    }
  }

  @Test
  public void search_selectiveSibling_probesWithoutOpeningJoinPostings() throws Exception {
    try (var fixture = new Fixture(10000, 10000, true, false, 10000)) {
      var query = compound(new AdaptiveJoinQuery("key", keys(5000, true), true, 1000), false, true);

      var result = fixture.searcher.search(query, 10);

      assertThat(result.totalHits.value()).isEqualTo(5);
      assertThat(fixture.accesses.postings.get()).isEqualTo(0);
    }
  }

  @Test
  public void search_sparseJoinWithBroadSibling_usesPostings() throws Exception {
    try (var fixture = new Fixture(10000, 10000, false, false, 10000)) {
      var query =
          compound(new AdaptiveJoinQuery("key", keys(10, false), false, 1000), false, false);

      var result = fixture.searcher.search(query, 10);

      assertThat(result.totalHits.value()).isEqualTo(10);
      assertThat(fixture.accesses.postings.get()).isGreaterThan(0);
    }
  }

  @Theory
  public void search_optimisticTopKPlanExhausted_switchesWithoutLosingMatches(boolean binary)
      throws Exception {
    try (var fixture = new Fixture(10000, 128, binary, true, 10000)) {
      var keys = keys(1000, binary);
      var baseline = compound(new TermInSetQuery("key", keys.terms()), false, false);
      var expected = fixture.searcher.search(baseline, 10001, Sort.INDEXORDER);
      fixture.accesses.reset();
      var query = compound(new AdaptiveJoinQuery("key", keys, false, 0), false, false);

      var actual = fixture.searcher.search(query, 10001, Sort.INDEXORDER);

      assertThat(Arrays.stream(actual.scoreDocs).mapToInt(hit -> hit.doc).toArray())
          .isEqualTo(Arrays.stream(expected.scoreDocs).mapToInt(hit -> hit.doc).toArray());
      assertThat(actual.totalHits).isEqualTo(expected.totalHits);
      assertThat(fixture.accesses.postings.get()).isGreaterThan(0);
      assertThat(fixture.accesses.lookupsBeforeFirstPosting.get()).isGreaterThan(0);
    }
  }

  @Test
  public void search_sampleMissesOnlyMatchingTerm_stillReturnsMatch() throws Exception {
    try (var directory = new ByteBuffersDirectory()) {
      try (var writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {
        var document = new Document();
        addKey(document, key(99, false));
        writer.addDocument(document);
      }
      try (var reader = DirectoryReader.open(directory)) {
        var query = new AdaptiveJoinQuery("key", keys(100, false), true, 1000);

        int count = new IndexSearcher(reader).count(query);

        assertThat(count).isEqualTo(1);
      }
    }
  }

  @Test
  public void search_competitiveScoresAcrossParallelLeaves_preservesTopHitsAndScores()
      throws Exception {
    try (var fixture = new Fixture(10000, 128, true, false, 1000);
        var executor = Executors.newFixedThreadPool(3)) {
      var keys = keys(64, true);
      var searcher = new IndexSearcher(fixture.reader, executor);
      searcher.setQueryCache(null);
      var baseline = ranked(new TermInSetQuery("key", keys.terms()));
      var adaptive = ranked(new AdaptiveJoinQuery("key", keys, false, 0));

      var expected = searcher.search(baseline, 10);
      var actual = searcher.search(adaptive, 10);

      assertThat(Arrays.stream(actual.scoreDocs).mapToInt(hit -> hit.doc).toArray())
          .isEqualTo(Arrays.stream(expected.scoreDocs).mapToInt(hit -> hit.doc).toArray());
      assertThat(Arrays.stream(actual.scoreDocs).map(hit -> hit.score).toList())
          .isEqualTo(Arrays.stream(expected.scoreDocs).map(hit -> hit.score).toList());
    }
  }

  @Test
  public void search_pagingAcrossProbeFallback_matchesPostingsOrder() throws Exception {
    try (var fixture = new Fixture(3000, 128, false, true, 1000)) {
      var keys = keys(64, false);
      var query = compound(new AdaptiveJoinQuery("key", keys, false, 0), false, false);
      var baseline = compound(new TermInSetQuery("key", keys.terms()), false, false);
      var first = fixture.searcher.search(query, 200, Sort.INDEXORDER);

      var next = fixture.searcher.searchAfter(first.scoreDocs[199], query, 200, Sort.INDEXORDER);
      var expected =
          fixture.searcher.searchAfter(first.scoreDocs[199], baseline, 200, Sort.INDEXORDER);

      assertThat(Arrays.stream(next.scoreDocs).mapToInt(hit -> hit.doc).toArray())
          .isEqualTo(Arrays.stream(expected.scoreDocs).mapToInt(hit -> hit.doc).toArray());
    }
  }

  private static Query compound(Query join, boolean exclusion, boolean selective) {
    return new BooleanQuery.Builder()
        .add(new TermQuery(new Term("title", selective ? "rare" : "common")), Occur.MUST)
        .add(join, exclusion ? Occur.MUST_NOT : Occur.FILTER)
        .build();
  }

  private static Query ranked(Query join) {
    return new BooleanQuery.Builder()
        .add(new TermQuery(new Term("title", "common")), Occur.MUST)
        .add(new TermQuery(new Term("title", "rare")), Occur.SHOULD)
        .add(join, Occur.FILTER)
        .build();
  }

  private static JoinKeySet keys(int size, boolean binary) {
    var builder = new JoinKeySet.Builder(ignored -> {});
    for (int i = 0; i < size; i++) {
      builder.add(key(i, binary));
    }
    return builder.build();
  }

  private static BytesRef key(int value, boolean binary) {
    return binary
        ? new BytesRef(ByteBuffer.allocate(12).putInt(0x8912bc45).putLong(value).array())
        : new BytesRef("key-" + value);
  }

  private static void addKey(Document document, BytesRef key) {
    document.add(new StringField("key", key, Field.Store.NO));
    document.add(new SortedSetDocValuesField("key", key));
  }

  private static final class Fixture implements AutoCloseable {
    final ByteBuffersDirectory directory = new ByteBuffersDirectory();
    final JoinAccesses accesses = new JoinAccesses();
    final DirectoryReader reader;
    final IndexSearcher searcher;

    Fixture(int documents, int cardinality, boolean binary, boolean multi, int segmentSize)
        throws IOException {
      try (var writer =
          new IndexWriter(
              this.directory,
              new IndexWriterConfig(new StandardAnalyzer())
                  .setMaxBufferedDocs(segmentSize)
                  .setMergePolicy(NoMergePolicy.INSTANCE))) {
        for (int i = 0; i < documents; i++) {
          var document = new Document();
          document.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          document.add(new StringField("title", "common", Field.Store.NO));
          if (i % 1000 == 0) {
            document.add(new StringField("title", "rare", Field.Store.NO));
          }
          if (!multi || i % 23 != 0) {
            addKey(document, key(i % cardinality, binary));
          }
          if (multi && i % 23 != 0) {
            addKey(document, key((i + 7) % cardinality, binary));
            addKey(document, key(i % cardinality, binary));
          }
          writer.addDocument(document);
        }
        writer.deleteDocuments(new Term("id", "77"));
      }
      this.reader = new TrackingReader(DirectoryReader.open(this.directory), this.accesses);
      this.searcher = new IndexSearcher(this.reader);
      this.searcher.setQueryCache(null);
    }

    @Override
    public void close() throws IOException {
      this.reader.close();
      this.directory.close();
    }
  }

  private static final class JoinAccesses {
    final AtomicInteger postings = new AtomicInteger();
    final AtomicInteger docValuesLookups = new AtomicInteger();
    final AtomicInteger lookupsBeforeFirstPosting = new AtomicInteger(-1);

    void reset() {
      this.postings.set(0);
      this.docValuesLookups.set(0);
      this.lookupsBeforeFirstPosting.set(-1);
    }
  }

  private static final class TrackingReader extends FilterDirectoryReader {
    TrackingReader(DirectoryReader reader, JoinAccesses accesses) throws IOException {
      super(
          reader,
          new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader leaf) {
              return new FilterLeafReader(leaf) {
                @Override
                public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
                  var values = super.getSortedSetDocValues(field);
                  if (!field.equals("key") || values == null) {
                    return values;
                  }
                  return new FilterSortedSetDocValues(values) {
                    @Override
                    public BytesRef lookupOrd(long ordinal) throws IOException {
                      accesses.docValuesLookups.incrementAndGet();
                      return super.lookupOrd(ordinal);
                    }
                  };
                }

                @Override
                public Terms terms(String field) throws IOException {
                  Terms terms = super.terms(field);
                  if (!field.equals("key") || terms == null) {
                    return terms;
                  }
                  return new FilterTerms(terms) {
                    @Override
                    public TermsEnum iterator() throws IOException {
                      return new FilterTermsEnum(super.iterator()) {
                        @Override
                        public PostingsEnum postings(PostingsEnum reuse, int flags)
                            throws IOException {
                          accesses.lookupsBeforeFirstPosting.compareAndSet(
                              -1, accesses.docValuesLookups.get());
                          accesses.postings.incrementAndGet();
                          return super.postings(reuse, flags);
                        }
                      };
                    }
                  };
                }

                @Override
                public CacheHelper getCoreCacheHelper() {
                  return null;
                }

                @Override
                public CacheHelper getReaderCacheHelper() {
                  return null;
                }
              };
            }
          });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader reader) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }
}
