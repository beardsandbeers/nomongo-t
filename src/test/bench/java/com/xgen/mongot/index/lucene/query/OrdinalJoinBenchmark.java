package com.xgen.mongot.index.lucene.query;

import com.sun.management.ThreadMXBean;
import com.xgen.mongot.index.query.operators.JoinKeySet;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.internal.hppc.LongHashSet;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

/** Reproducible single-threaded kernel experiment, deliberately outside production admission. */
public final class OrdinalJoinBenchmark {
  private static final String KEY = "key";
  private static volatile long sink;
  private static final Field BUILDER_VALUES = privateField(JoinKeySet.Builder.class, "values");

  private OrdinalJoinBenchmark() {}

  private static Field privateField(Class<?> owner, String name) {
    try {
      var field = owner.getDeclaredField(name);
      field.setAccessible(true);
      return field;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private record Prepared(Query query, long keys, long peakMembershipBytes) {}

  private record Sample(long totalNanos, long preparationNanos, long allocatedBytes) {}

  private record Config(
      String kind,
      int foreignKeys,
      int selectedKeys,
      int localDocs,
      int localKeys,
      int foreignSegments,
      int localSegments,
      int stringBytes,
      int scanBeforeSeek,
      int warmups,
      int samples,
      long seed,
      List<String> shapes,
      Path output,
      Path indexDirectory) {
    private static Config parse(String[] args) {
      Map<String, String> options = new HashMap<>();
      for (int i = 0; i < args.length; i += 2) {
        options.put(args[i], args[i + 1]);
      }
      return new Config(
          options.getOrDefault("--kind", "objectId"),
          Integer.parseInt(options.getOrDefault("--foreign-keys", "100000")),
          Integer.parseInt(options.getOrDefault("--selected-keys", "100000")),
          Integer.parseInt(options.getOrDefault("--local-docs", "1000000")),
          Integer.parseInt(options.getOrDefault("--local-keys", "100000")),
          Integer.parseInt(options.getOrDefault("--foreign-segments", "4")),
          Integer.parseInt(options.getOrDefault("--local-segments", "8")),
          Integer.parseInt(options.getOrDefault("--string-bytes", "64")),
          Integer.parseInt(options.getOrDefault("--scan-before-seek", "0")),
          Integer.parseInt(options.getOrDefault("--warmups", "15")),
          Integer.parseInt(options.getOrDefault("--samples", "30")),
          Long.parseLong(options.getOrDefault("--seed", "42")),
          List.of(
              options.getOrDefault("--shapes", "selectiveTop10,broadTop10,broadCount").split(",")),
          Path.of(options.getOrDefault("--output", "/tmp/ordinal-join-samples.csv")),
          Path.of(options.get("--index-directory")));
    }
  }

  private static BytesRef key(Config config, int number) {
    long mixed = (number + 1L) * 0x9e3779b97f4a7c15L;
    if (config.kind.equals("objectId")) {
      return new BytesRef(
          ByteBuffer.allocate(12).putInt((int) (mixed >>> 32) ^ 0xff12bc45).putLong(mixed).array());
    }
    String prefix = "account/" + Long.toUnsignedString(mixed, 16);
    return new BytesRef(prefix + "x".repeat(config.stringBytes - prefix.length()));
  }

  private static final class Fixture implements AutoCloseable {
    private final FSDirectory foreignDirectory;
    private final FSDirectory localDirectory;
    private final DirectoryReader foreignReader;
    private final DirectoryReader localReader;
    private final IndexSearcher foreign;
    private final IndexSearcher local;
    private final Config config;
    private long broadMatches;
    private long selectiveMatches;

    private Fixture(Config config) throws IOException {
      this.config = config;
      this.foreignDirectory = FSDirectory.open(config.indexDirectory.resolve("foreign"));
      this.localDirectory = FSDirectory.open(config.indexDirectory.resolve("local"));
      writeForeign();
      writeLocal();
      this.foreignReader = DirectoryReader.open(this.foreignDirectory);
      this.localReader = DirectoryReader.open(this.localDirectory);
      this.foreign = new IndexSearcher(this.foreignReader);
      this.local = new IndexSearcher(this.localReader);
      this.foreign.setQueryCache(null);
      this.local.setQueryCache(null);
    }

    private IndexWriter writer(FSDirectory directory) throws IOException {
      return new IndexWriter(
          directory,
          new IndexWriterConfig(new StandardAnalyzer())
              .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
              .setRAMBufferSizeMB(96)
              .setMergePolicy(NoMergePolicy.INSTANCE));
    }

    private void writeForeign() throws IOException {
      try (var writer = writer(this.foreignDirectory)) {
        int segmentSize =
            (this.config.foreignKeys + this.config.foreignSegments - 1)
                / this.config.foreignSegments;
        for (int i = 0; i < this.config.foreignKeys; i++) {
          var doc = new Document();
          doc.add(new SortedSetDocValuesField(KEY, key(this.config, i)));
          doc.add(
              new StringField(
                  "selected",
                  i < this.config.selectedKeys ? "yes" : "no",
                  org.apache.lucene.document.Field.Store.NO));
          writer.addDocument(doc);
          if ((i + 1) % segmentSize == 0) {
            writer.commit();
          }
        }
      }
    }

    private void writeLocal() throws IOException {
      try (var writer = writer(this.localDirectory)) {
        int segmentSize =
            (this.config.localDocs + this.config.localSegments - 1) / this.config.localSegments;
        for (int i = 0; i < this.config.localDocs; i++) {
          int number = (int) ((long) i * 104729 % this.config.localKeys);
          BytesRef value = key(this.config, number);
          var doc = new Document();
          doc.add(new SortedSetDocValuesField(KEY, value));
          doc.add(new StringField(KEY, value, org.apache.lucene.document.Field.Store.NO));
          boolean rare = i % 1000 == 0;
          doc.add(
              new TextField(
                  "title",
                  "common ".repeat(1 + i % 4) + (rare ? "rare" : ""),
                  org.apache.lucene.document.Field.Store.NO));
          writer.addDocument(doc);
          if (number < this.config.selectedKeys && number < this.config.foreignKeys) {
            this.broadMatches++;
            if (rare) {
              this.selectiveMatches++;
            }
          }
          if ((i + 1) % segmentSize == 0) {
            writer.commit();
          }
        }
      }
    }

    private Prepared prepare(String strategy, boolean exhaustive, boolean measureMemory)
        throws Exception {
      Query predicate = new TermQuery(new Term("selected", "yes"));
      if (strategy.equals("ordinal")) {
        var result =
            OrdinalJoinFixture.prepare(
                List.of(this.foreign),
                predicate,
                KEY,
                List.of(this.localReader),
                KEY,
                () -> {},
                this.config.scanBeforeSeek);
        return new Prepared(result.query(), result.distinctForeignKeys(), result.peakBitmapBytes());
      }
      var builder = new JoinKeySet.Builder(ignored -> {});
      var hash = (BytesRefHash) BUILDER_VALUES.get(builder);
      long[] peak = {0};
      this.foreign.search(
          predicate,
          new SimpleCollector() {
            private SortedSetDocValues values;
            private LongHashSet seen = new LongHashSet();

            private void measure() {
              if (measureMemory) {
                peak[0] = Math.max(peak[0], hash.ramBytesUsed() + this.seen.ramBytesUsed());
              }
            }

            @Override
            protected void doSetNextReader(LeafReaderContext context) throws IOException {
              measure();
              this.values = DocValues.getSortedSet(context.reader(), KEY);
              this.seen = new LongHashSet();
            }

            @Override
            public void collect(int doc) throws IOException {
              if (this.values.advanceExact(doc)) {
                for (int i = 0; i < this.values.docValueCount(); i++) {
                  long ordinal = this.values.nextOrd();
                  if (this.seen.add(ordinal)) {
                    builder.add(this.values.lookupOrd(ordinal));
                  }
                }
              }
              if ((doc & 4095) == 0) {
                measure();
              }
            }

            @Override
            public void finish() {
              measure();
            }

            @Override
            public ScoreMode scoreMode() {
              return ScoreMode.COMPLETE_NO_SCORES;
            }
          });
      var keys = builder.build();
      return new Prepared(new AdaptiveJoinQuery(KEY, keys, exhaustive, 1000), keys.size(), peak[0]);
    }

    private Query localQuery(Query membership, String shape) {
      return new BooleanQuery.Builder()
          .add(
              new TermQuery(new Term("title", shape.startsWith("selective") ? "rare" : "common")),
              Occur.MUST)
          .add(membership, Occur.FILTER)
          .build();
    }

    private Sample sample(String strategy, String shape, ThreadMXBean allocations)
        throws Exception {
      long allocatedBefore = allocations.getThreadAllocatedBytes(Thread.currentThread().threadId());
      long start = System.nanoTime();
      var prepared = prepare(strategy, shape.endsWith("Count"), false);
      long preparedAt = System.nanoTime();
      Query query = localQuery(prepared.query, shape);
      if (shape.endsWith("Count")) {
        sink += this.local.count(query);
      } else {
        sink += this.local.search(query, 10).scoreDocs.length;
      }
      long end = System.nanoTime();
      long allocated = allocations.getThreadAllocatedBytes(Thread.currentThread().threadId());
      return new Sample(end - start, preparedAt - start, allocated - allocatedBefore);
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(
          this.foreignReader, this.localReader, this.foreignDirectory, this.localDirectory);
    }
  }

  public static void main(String[] args) throws Exception {
    Config config = Config.parse(args);
    var allocations = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    allocations.setThreadAllocatedMemoryEnabled(true);
    try (var fixture = new Fixture(config);
        BufferedWriter output = Files.newBufferedWriter(config.output)) {
      System.out.printf(
          Locale.ROOT,
          "Java=%s Lucene=%s type=%s foreignKeys=%d selected=%d localDocs=%d localKeys=%d"
              + " segments=%d/%d scanBeforeSeek=%d warmups=%d samples=%d%n",
          System.getProperty("java.version"),
          Version.LATEST,
          config.kind,
          config.foreignKeys,
          config.selectedKeys,
          config.localDocs,
          config.localKeys,
          fixture.foreignReader.leaves().size(),
          fixture.localReader.leaves().size(),
          config.scanBeforeSeek,
          config.warmups,
          config.samples);
      output.write(
          "shape,strategy,iteration,total_ns,preparation_ns,allocated_bytes,"
              + "retained_query_bytes,peak_membership_bytes,expected_hits\n");
      var random = new Random(config.seed);
      for (String shape : config.shapes) {
        var strategies = new ArrayList<>(List.of("hash", "ordinal"));
        Map<String, Long> retained = new HashMap<>();
        Map<String, Long> peak = new HashMap<>();
        Map<String, List<Sample>> samples = new HashMap<>();
        long expected =
            shape.startsWith("selective") ? fixture.selectiveMatches : fixture.broadMatches;
        for (String strategy : strategies) {
          var prepared = fixture.prepare(strategy, shape.endsWith("Count"), true);
          long actual = fixture.local.count(fixture.localQuery(prepared.query, shape));
          if (actual != expected || prepared.keys != config.selectedKeys) {
            throw new AssertionError(
                strategy + " hits=" + actual + "/" + expected + " keys=" + prepared.keys);
          }
          retained.put(strategy, RamUsageTester.ramUsed(prepared.query));
          peak.put(strategy, prepared.peakMembershipBytes);
          samples.put(strategy, new ArrayList<>());
        }
        for (int iteration = -config.warmups; iteration < config.samples; iteration++) {
          Collections.shuffle(strategies, random);
          for (String strategy : strategies) {
            Sample sample = fixture.sample(strategy, shape, allocations);
            if (iteration >= 0) {
              samples.get(strategy).add(sample);
              output.write(
                  String.format(
                      Locale.ROOT,
                      "%s,%s,%d,%d,%d,%d,%d,%d,%d%n",
                      shape,
                      strategy,
                      iteration,
                      sample.totalNanos,
                      sample.preparationNanos,
                      sample.allocatedBytes,
                      retained.get(strategy),
                      peak.get(strategy),
                      expected));
            }
          }
        }
        output.flush();
        for (String strategy : List.of("hash", "ordinal")) {
          var results = samples.get(strategy);
          long[] total = results.stream().mapToLong(Sample::totalNanos).sorted().toArray();
          long[] preparation =
              results.stream().mapToLong(Sample::preparationNanos).sorted().toArray();
          long[] allocated = results.stream().mapToLong(Sample::allocatedBytes).sorted().toArray();
          System.out.printf(
              Locale.ROOT,
              "%s %s p50=%.3fms p95=%.3fms prepare=%.3fms allocated=%.1fKiB retained=%.1fKiB"
                  + " peakMembership=%.1fKiB%n",
              shape,
              strategy,
              total[total.length / 2] / 1e6,
              total[(int) Math.ceil(total.length * 0.95) - 1] / 1e6,
              preparation[preparation.length / 2] / 1e6,
              allocated[allocated.length / 2] / 1024d,
              retained.get(strategy) / 1024d,
              peak.get(strategy) / 1024d);
        }
      }
    }
  }
}
