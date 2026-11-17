package com.xgen.mongot.index.lucene.query;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class OrdinalJoinQueryTest {
  @DataPoints public static final int[] SEQUENTIAL_WINDOWS = {0, 4};

  public record Scenario(boolean binary, boolean exclusion, int seed) {}

  @DataPoints
  public static final Scenario[] SCENARIOS = {
    new Scenario(false, false, 7),
    new Scenario(false, true, 11),
    new Scenario(true, false, 13),
    new Scenario(true, true, 17)
  };

  private static Query predicate() {
    return new BooleanQuery.Builder()
        .add(new TermQuery(new Term("selected", "yes")), Occur.FILTER)
        .add(new TermQuery(new Term("root", "yes")), Occur.FILTER)
        .build();
  }

  private static Query compound(Query membership, boolean exclusion) {
    return new BooleanQuery.Builder()
        .add(new TermQuery(new Term("title", "common")), Occur.MUST)
        .add(membership, exclusion ? Occur.MUST_NOT : Occur.FILTER)
        .build();
  }

  @Theory
  public void prepare_arraysMissingDeletionsAndPartitions_matchesIndependentPostingsOracle(
      Scenario scenario, int sequentialSteps) throws Exception {
    try (var fixture = new Fixture(scenario.binary(), scenario.seed());
        var executor = Executors.newFixedThreadPool(3)) {
      var prepared =
          OrdinalJoinFixture.prepare(
              fixture.foreignSearchers(),
              predicate(),
              "key",
              fixture.localReaders(),
              "key",
              () -> {},
              sequentialSteps);
      var searcher = new IndexSearcher(fixture.local.getFirst().reader, executor);
      searcher.setQueryCache(null);
      var expected = compound(new TermInSetQuery("key", fixture.expected), scenario.exclusion());
      var actual = compound(prepared.query(), scenario.exclusion());

      var expectedAll = searcher.search(expected, 2000, Sort.INDEXORDER);
      var actualAll = searcher.search(actual, 2000, Sort.INDEXORDER);
      var expectedTop = searcher.search(expected, 10);
      var actualTop = searcher.search(actual, 10);
      var expectedPage = searcher.searchAfter(expectedTop.scoreDocs[9], expected, 10);
      var actualPage = searcher.searchAfter(actualTop.scoreDocs[9], actual, 10);

      assertThat(prepared.distinctForeignKeys()).isEqualTo(fixture.expected.size());
      assertThat(Arrays.stream(actualAll.scoreDocs).mapToInt(hit -> hit.doc).toArray())
          .isEqualTo(Arrays.stream(expectedAll.scoreDocs).mapToInt(hit -> hit.doc).toArray());
      assertThat(Arrays.stream(actualTop.scoreDocs).mapToInt(hit -> hit.doc).toArray())
          .isEqualTo(Arrays.stream(expectedTop.scoreDocs).mapToInt(hit -> hit.doc).toArray());
      assertThat(Arrays.stream(actualTop.scoreDocs).mapToDouble(hit -> hit.score).toArray())
          .isEqualTo(Arrays.stream(expectedTop.scoreDocs).mapToDouble(hit -> hit.score).toArray());
      assertThat(Arrays.stream(actualPage.scoreDocs).mapToInt(hit -> hit.doc).toArray())
          .isEqualTo(Arrays.stream(expectedPage.scoreDocs).mapToInt(hit -> hit.doc).toArray());
      assertThat(searcher.count(actual)).isEqualTo(searcher.count(expected));
      assertThat(new IndexSearcher(fixture.local.getLast().reader).count(prepared.query()))
          .isEqualTo(
              new IndexSearcher(fixture.local.getLast().reader)
                  .count(new TermInSetQuery("key", fixture.expected)));
    }
  }

  @Test
  public void prepare_selfJoinAcrossSegments_matchesIndependentDocValuesOracle() throws Exception {
    try (var fixture = new Fixture(true, 19)) {
      var source = fixture.foreignSearchers().getFirst();
      var prepared =
          OrdinalJoinFixture.prepare(
              fixture.foreignSearchers(),
              predicate(),
              "key",
              List.of(source.getIndexReader()),
              "key",
              () -> {});
      var baseline = SortedSetDocValuesField.newSlowSetQuery("key", fixture.expected);

      var expected = source.search(baseline, 1000, Sort.INDEXORDER);
      var actual = source.search(prepared.query(), 1000, Sort.INDEXORDER);

      assertThat(actual.scoreDocs).isNotEmpty();
      assertThat(Arrays.stream(actual.scoreDocs).mapToInt(hit -> hit.doc).toArray())
          .isEqualTo(Arrays.stream(expected.scoreDocs).mapToInt(hit -> hit.doc).toArray());
    }
  }

  @Test
  public void prepare_closedForeignReaders_retainsOnlyLocalMembership() throws Exception {
    try (var fixture = new Fixture(true, 23)) {
      var prepared =
          OrdinalJoinFixture.prepare(
              fixture.foreignSearchers(),
              predicate(),
              "key",
              fixture.localReaders(),
              "key",
              () -> {});
      int expected =
          new IndexSearcher(fixture.local.getFirst().reader)
              .count(new TermInSetQuery("key", fixture.expected));
      IOUtils.close(fixture.foreign);

      int actual = new IndexSearcher(fixture.local.getFirst().reader).count(prepared.query());

      assertThat(actual).isEqualTo(expected);
    }
  }

  @Test
  public void prepare_noForeignMatches_producesEmptyMembership() throws Exception {
    try (var fixture = new Fixture(false, 29)) {
      var prepared =
          OrdinalJoinFixture.prepare(
              fixture.foreignSearchers(),
              new TermQuery(new Term("selected", "absent")),
              "key",
              fixture.localReaders(),
              "key",
              () -> {});

      int actual = new IndexSearcher(fixture.local.getFirst().reader).count(prepared.query());

      assertThat(prepared.distinctForeignKeys()).isEqualTo(0);
      assertThat(actual).isEqualTo(0);
    }
  }

  @Test
  public void search_differentReader_rejectsSnapshotMismatch() throws Exception {
    try (var fixture = new Fixture(false, 31)) {
      var prepared =
          OrdinalJoinFixture.prepare(
              fixture.foreignSearchers(),
              predicate(),
              "key",
              fixture.localReaders(),
              "key",
              () -> {});

      assertThrows(
          IllegalStateException.class,
          () -> fixture.foreignSearchers().getFirst().count(prepared.query()));
    }
  }

  @Test
  public void prepare_cancelled_stopsBeforeCollecting() throws Exception {
    try (var fixture = new Fixture(false, 37)) {
      assertThrows(
          IllegalStateException.class,
          () ->
              OrdinalJoinFixture.prepare(
                  fixture.foreignSearchers(),
                  predicate(),
                  "key",
                  fixture.localReaders(),
                  "key",
                  () -> {
                    throw new IllegalStateException("cancelled");
                  }));
    }
  }

  private static BytesRef key(int number, boolean binary) {
    return binary
        ? new BytesRef(
            ByteBuffer.allocate(12)
                .putInt(0xffa01234)
                .putLong((number + 1L) * 0x9e3779b97f4a7c15L)
                .array())
        : new BytesRef("account-" + number);
  }

  private static final class Partition implements Closeable {
    private final ByteBuffersDirectory directory;
    private final DirectoryReader reader;
    private boolean closed;

    private Partition(ByteBuffersDirectory directory) throws IOException {
      this.directory = directory;
      this.reader = DirectoryReader.open(directory);
    }

    @Override
    public void close() throws IOException {
      if (!this.closed) {
        this.closed = true;
        IOUtils.close(this.reader, this.directory);
      }
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final List<Partition> foreign = new ArrayList<>();
    private final List<Partition> local = new ArrayList<>();
    private final List<BytesRef> expected;

    private Fixture(boolean binary, int seed) throws IOException {
      var expectedNumbers = new java.util.HashSet<Integer>();
      var random = new Random(seed);
      for (int partition = 0; partition < 2; partition++) {
        var directory = new ByteBuffersDirectory();
        try (var writer =
            new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer())
                    .setMergePolicy(NoMergePolicy.INSTANCE))) {
          for (int i = 0; i < 300; i++) {
            var doc = new Document();
            boolean selected = i % 3 != 0;
            boolean root = i % 7 != 0;
            boolean deleted = i % 13 == 0;
            doc.add(new StringField("id", "" + i, Field.Store.NO));
            doc.add(new StringField("selected", selected ? "yes" : "no", Field.Store.NO));
            doc.add(new StringField("root", root ? "yes" : "no", Field.Store.NO));
            for (int j = 0; j < i % 4; j++) {
              int number = random.nextInt(500);
              doc.add(new SortedSetDocValuesField("key", key(number, binary)));
              if (selected && root && !deleted) {
                expectedNumbers.add(number);
              }
            }
            writer.addDocument(doc);
            if (i % 37 == 0) {
              writer.commit();
            }
          }
          for (int i = 0; i < 300; i += 13) {
            writer.deleteDocuments(new Term("id", "" + i));
          }
        }
        this.foreign.add(new Partition(directory));
      }
      this.expected = expectedNumbers.stream().map(number -> key(number, binary)).toList();
      for (int partition = 0; partition < 2; partition++) {
        var directory = new ByteBuffersDirectory();
        try (var writer =
            new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer())
                    .setMergePolicy(NoMergePolicy.INSTANCE))) {
          for (int i = 0; i < 1500; i++) {
            var doc = new Document();
            doc.add(new StringField("id", "" + i, Field.Store.NO));
            doc.add(new TextField("title", "common ".repeat(1 + i % 5), Field.Store.NO));
            for (int j = 0; j < i % 4; j++) {
              BytesRef value = key(random.nextInt(700), binary);
              doc.add(new SortedSetDocValuesField("key", value));
              doc.add(new StringField("key", value, Field.Store.NO));
            }
            writer.addDocument(doc);
            if (i % 137 == 0) {
              writer.commit();
            }
          }
          for (int i = 0; i < 1500; i += 17) {
            writer.deleteDocuments(new Term("id", "" + i));
          }
        }
        this.local.add(new Partition(directory));
      }
    }

    private List<IndexSearcher> foreignSearchers() {
      return this.foreign.stream().map(partition -> new IndexSearcher(partition.reader)).toList();
    }

    private List<DirectoryReader> localReaders() {
      return this.local.stream().map(partition -> partition.reader).toList();
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(this.foreign);
      IOUtils.close(this.local);
    }
  }
}
