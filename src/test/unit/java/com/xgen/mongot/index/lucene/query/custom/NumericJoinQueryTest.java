package com.xgen.mongot.index.lucene.query.custom;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.query.operators.JoinKeySet;
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.junit.Test;

public class NumericJoinQueryTest {
  @Test
  public void search_acceptedPointSubtree_cancelsDuringBulkDocIdVisits() throws Exception {
    try (var directory = new ByteBuffersDirectory()) {
      writeRepeatedPoints(directory);
      try (var reader = DirectoryReader.open(directory)) {
        var visits = new AtomicInteger();
        var measured =
            new FilterLeafReader(reader.leaves().getFirst().reader()) {
              @Override
              public CacheHelper getCoreCacheHelper() {
                return null;
              }

              @Override
              public CacheHelper getReaderCacheHelper() {
                return null;
              }

              @Override
              public PointValues getPointValues(String field) throws IOException {
                var values = super.getPointValues(field);
                return values == null ? null : measurePointVisits(values, visits);
              }
            };
        var searcher = new IndexSearcher(measured);
        searcher.setQueryCache(null);
        var query = new NumericJoinQuery("key", keys(7, 8));
        searcher.setTimeout(
            () -> {
              if (visits.get() >= 100) {
                throw new CancellationException("cancelled inside accepted point subtree");
              }
              return false;
            });

        assertThrows(CancellationException.class, () -> searcher.search(query, 10));

        assertThat(visits.get()).isAtLeast(100);
        assertThat(visits.get()).isAtMost(1124);
        searcher.setTimeout(null);
        assertThat(searcher.count(query)).isEqualTo(100_000);
        assertThat(reader.getRefCount()).isEqualTo(1);
      }
    }
  }

  private static void writeRepeatedPoints(ByteBuffersDirectory directory) throws IOException {
    try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (int i = 0; i < 100_000; i++) {
        var doc = new Document();
        doc.add(new LongField("key", 7, Field.Store.NO));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }
  }

  private static PointValues measurePointVisits(PointValues values, AtomicInteger visits) {
    return new PointValues() {
      @Override
      public PointTree getPointTree() throws IOException {
        return new MeasuredPointTree(values.getPointTree(), visits);
      }

      @Override
      public byte[] getMinPackedValue() throws IOException {
        return values.getMinPackedValue();
      }

      @Override
      public byte[] getMaxPackedValue() throws IOException {
        return values.getMaxPackedValue();
      }

      @Override
      public int getNumDimensions() throws IOException {
        return values.getNumDimensions();
      }

      @Override
      public int getNumIndexDimensions() throws IOException {
        return values.getNumIndexDimensions();
      }

      @Override
      public int getBytesPerDimension() throws IOException {
        return values.getBytesPerDimension();
      }

      @Override
      public long size() {
        return values.size();
      }

      @Override
      public int getDocCount() {
        return values.getDocCount();
      }
    };
  }

  private record MeasuredPointTree(PointValues.PointTree delegate, AtomicInteger visits)
      implements PointValues.PointTree {
    @Override
    public PointValues.PointTree clone() {
      return new MeasuredPointTree(this.delegate.clone(), this.visits);
    }

    @Override
    public boolean moveToChild() throws IOException {
      return this.delegate.moveToChild();
    }

    @Override
    public boolean moveToSibling() throws IOException {
      return this.delegate.moveToSibling();
    }

    @Override
    public boolean moveToParent() throws IOException {
      return this.delegate.moveToParent();
    }

    @Override
    public byte[] getMinPackedValue() {
      return this.delegate.getMinPackedValue();
    }

    @Override
    public byte[] getMaxPackedValue() {
      return this.delegate.getMaxPackedValue();
    }

    @Override
    public long size() {
      return this.delegate.size();
    }

    @Override
    public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
      this.delegate.visitDocValues(visitor);
    }

    @Override
    public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
      this.delegate.visitDocIDs(
          new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) throws IOException {
              MeasuredPointTree.this.visits.incrementAndGet();
              visitor.visit(docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
              MeasuredPointTree.this.visits.incrementAndGet();
              visitor.visit(docID, packedValue);
            }

            @Override
            public void visit(DocIdSetIterator iterator) throws IOException {
              visitor.visit(
                  new FilterDocIdSetIterator(iterator) {
                    @Override
                    public int nextDoc() throws IOException {
                      MeasuredPointTree.this.visits.incrementAndGet();
                      return this.in.nextDoc();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                      MeasuredPointTree.this.visits.incrementAndGet();
                      return this.in.advance(target);
                    }
                  });
            }

            @Override
            public PointValues.Relation compare(byte[] min, byte[] max) {
              return visitor.compare(min, max);
            }

            @Override
            public void grow(int count) {
              visitor.grow(count);
            }
          });
    }
  }

  @Test
  public void search_largeArrayCancellation_stopsInsideDocValuesAndLeavesReaderUsable()
      throws Exception {
    try (var fixture = new Fixture()) {
      var query =
          new BooleanQuery.Builder()
              .add(new TermQuery(new Term("candidate", "yes")), BooleanClause.Occur.MUST)
              .add(new NumericJoinQuery("key", keys(fixture.values)), BooleanClause.Occur.FILTER)
              .build();
      fixture.searcher.setTimeout(
          () -> {
            if (fixture.readValues.get() >= 100) {
              throw new CancellationException("cancelled during local value scan");
            }
            return false;
          });

      assertThrows(CancellationException.class, () -> fixture.searcher.search(query, 10));

      assertThat(fixture.readValues.get()).isAtLeast(100);
      assertThat(fixture.readValues.get()).isAtMost(1024);
      fixture.searcher.setTimeout(null);
      assertThat(fixture.searcher.search(query, 10).totalHits.value()).isEqualTo(0);
    }
  }

  @Test
  public void search_pointTraversalCancellation_stopsBeforeBulkScoring() throws Exception {
    try (var fixture = new Fixture()) {
      var query = new NumericJoinQuery("key", keys(fixture.values));
      var checks = new AtomicInteger();
      fixture.searcher.setTimeout(
          () -> {
            if (checks.incrementAndGet() >= 20) {
              throw new CancellationException("cancelled during numeric point setup");
            }
            return false;
          });

      assertThrows(
          CancellationException.class,
          () ->
              fixture
                  .searcher
                  .createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1)
                  .scorerSupplier(fixture.searcher.getIndexReader().leaves().getFirst())
                  .get(Long.MAX_VALUE));

      assertThat(checks.get()).isEqualTo(20);
      fixture.searcher.setTimeout(null);
      assertThat(fixture.searcher.count(query)).isEqualTo(fixture.values.length);
    }
  }

  @Test
  public void search_bothExecutionPaths_preserveExactIntegersAndMissingValues() throws Exception {
    try (var directory = new ByteBuffersDirectory()) {
      writeBoundaryDocuments(directory);
      try (var reader = DirectoryReader.open(directory)) {
        var searcher = new IndexSearcher(reader);
        var query =
            new NumericJoinQuery(
                "key", keys(Long.MIN_VALUE, Long.MAX_VALUE, 9007199254740993L, 0, -7));

        var points = matchingDocs(searcher, query, Long.MAX_VALUE);
        var values = matchingDocs(searcher, query, 0);

        assertThat(points)
            .isEqualTo(
                java.util.stream.IntStream.concat(
                        java.util.stream.IntStream.range(0, 4),
                        java.util.stream.IntStream.range(7, 39))
                    .toArray());
        assertThat(values).isEqualTo(points);
        assertThat(searcher.count(query)).isEqualTo(36);
        assertThat(searcher.count(new NumericJoinQuery("key", keys()))).isEqualTo(0);
      }
    }
  }

  private static int[] matchingDocs(IndexSearcher searcher, Query query, long leadCost)
      throws IOException {
    var scorer =
        searcher
            .createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1)
            .scorerSupplier(searcher.getIndexReader().leaves().getFirst())
            .get(leadCost);
    var iterator =
        scorer.twoPhaseIterator() == null
            ? scorer.iterator()
            : TwoPhaseIterator.asDocIdSetIterator(scorer.twoPhaseIterator());
    var docs = new java.util.ArrayList<Integer>();
    for (int doc = iterator.nextDoc(); doc != Integer.MAX_VALUE; doc = iterator.nextDoc()) {
      docs.add(doc);
    }
    return docs.stream().mapToInt(Integer::intValue).toArray();
  }

  private static void writeBoundaryDocuments(ByteBuffersDirectory directory) throws IOException {
    try (var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (long[] values :
          new long[][] {
            {Long.MIN_VALUE},
            {Long.MAX_VALUE},
            {9007199254740993L},
            {0, -7, -7},
            {9007199254740992L},
            {},
            {42}
          }) {
        var doc = new Document();
        for (long value : values) {
          doc.add(new LongField("key", value, Field.Store.NO));
        }
        writer.addDocument(doc);
      }
      for (int i = 0; i < 32; i++) {
        var doc = new Document();
        doc.add(new LongField("key", 0, Field.Store.NO));
        writer.addDocument(doc);
      }
    }
  }

  private static JoinKeySet keys(long... values) {
    var keys = new JoinKeySet.Builder(ignored -> {});
    var scratch = new BytesRef(new byte[Long.BYTES]);
    for (long value : values) {
      NumericUtils.longToSortableBytes(value, scratch.bytes, 0);
      keys.add(scratch);
    }
    return keys.build();
  }

  private static final class Fixture implements AutoCloseable {
    private final ByteBuffersDirectory directory = new ByteBuffersDirectory();
    private final DirectoryReader reader;
    private final AtomicInteger readValues = new AtomicInteger();
    private final IndexSearcher searcher;
    private final long[] values =
        java.util.stream.LongStream.range(0, 4096)
            .map(value -> ((value * 2) << 32) | (value * 2))
            .toArray();

    private Fixture() throws IOException {
      try (var writer = new IndexWriter(this.directory, new IndexWriterConfig())) {
        for (long value : this.values) {
          var doc = new Document();
          doc.add(new LongField("key", value, Field.Store.NO));
          writer.addDocument(doc);
        }
        var candidate = new Document();
        candidate.add(new StringField("candidate", "yes", Field.Store.NO));
        for (int i = 0; i < 100_000; i++) {
          candidate.add(new LongField("key", (1L << 32) | 1, Field.Store.NO));
        }
        writer.addDocument(candidate);
        writer.forceMerge(1);
      }
      this.reader = DirectoryReader.open(this.directory);
      var measured =
          new FilterLeafReader(this.reader.leaves().getFirst().reader()) {
            @Override
            public CacheHelper getCoreCacheHelper() {
              return null;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
              return null;
            }

            @Override
            public SortedNumericDocValues getSortedNumericDocValues(String field)
                throws IOException {
              var delegate = super.getSortedNumericDocValues(field);
              if (delegate == null) {
                return null;
              }
              return new SortedNumericDocValues() {
                @Override
                public long nextValue() throws IOException {
                  Fixture.this.readValues.incrementAndGet();
                  return delegate.nextValue();
                }

                @Override
                public int docValueCount() {
                  return delegate.docValueCount();
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                  return delegate.advanceExact(target);
                }

                @Override
                public int docID() {
                  return delegate.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                  return delegate.nextDoc();
                }

                @Override
                public int advance(int target) throws IOException {
                  return delegate.advance(target);
                }

                @Override
                public long cost() {
                  return delegate.cost();
                }
              };
            }
          };
      this.searcher = new IndexSearcher(measured);
      this.searcher.setQueryCache(null);
    }

    @Override
    public void close() throws IOException {
      this.reader.close();
      this.directory.close();
    }
  }
}
