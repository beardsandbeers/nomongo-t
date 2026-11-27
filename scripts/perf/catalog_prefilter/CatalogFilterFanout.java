import com.sun.management.OperatingSystemMXBean;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NumericUtils;

/** Isolate filter enumeration across the reported leaf geometry using public product metadata. */
public class CatalogFilterFanout {
  private record Product(String category, Long reviews, Long rating, Long price) {}

  private static Long encode(String value) {
    return value.isEmpty() ? null : NumericUtils.doubleToSortableLong(Double.parseDouble(value));
  }

  private static void number(Document doc, String field, Long value) {
    if (value != null) {
      doc.add(new LongPoint(field, value));
      doc.add(new NumericDocValuesField(field, value));
    }
  }

  private static void build(Path index, Path geometry, Path cohorts, boolean priceCap)
      throws Exception {
    var products = new ArrayList<Product>();
    var categories = new HashMap<String, String>();
    for (int cohort : List.of(0, 1)) {
      try (var rows = Files.newBufferedReader(cohorts.resolve(cohort + "/products.tsv"))) {
        for (String line; (line = rows.readLine()) != null; ) {
          String[] values = line.split("\\t", -1);
          String category = categories.computeIfAbsent(values[4], name -> name);
          products.add(
              new Product(
                  category,
                  encode(values[3]),
                  encode(values[2]),
                  priceCap ? encode(values[1]) : null));
        }
      }
    }
    Files.createDirectory(index);
    var config =
        new IndexWriterConfig()
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(Integer.MAX_VALUE - 1)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    try (var dir = FSDirectory.open(index);
        var writer = new IndexWriter(dir, config)) {
      int productIndex = 0;
      for (String line : Files.readAllLines(geometry)) {
        if (line.startsWith("live,")) continue;
        var counts = line.split(",");
        int live = Integer.parseInt(counts[0]);
        int slots = Integer.parseInt(counts[1]);
        for (int i = 0; i < slots; i++) {
          Product product = products.get(productIndex);
          if (++productIndex == products.size()) productIndex = 0;
          var doc = new Document();
          doc.add(new NumericDocValuesField("vector", 1)); // Dense field-existence proxy.
          if (!product.category.isEmpty()) {
            doc.add(new StringField("category", product.category, Field.Store.NO));
          }
          number(doc, "reviews", product.reviews);
          number(doc, "rating", product.rating);
          number(doc, "price", product.price);
          if ((long) (i + 1) * live / slots == (long) i * live / slots) {
            doc.add(new StringField("obsolete", "yes", Field.Store.NO));
          }
          writer.addDocument(doc);
        }
        writer.flush();
      }
      writer.deleteDocuments(new Term("obsolete", "yes"));
    }
    try (var dir = FSDirectory.open(index);
        var reader = DirectoryReader.open(dir)) {
      var rows = Files.readAllLines(geometry);
      if (reader.leaves().size() != rows.size() - 1) throw new IllegalStateException("Leaf count");
      for (var leaf : reader.leaves()) {
        String actual = leaf.reader().numDocs() + "," + leaf.reader().maxDoc();
        if (!rows.get(leaf.ord + 1).equals(actual)) throw new IllegalStateException(actual);
      }
    }
  }

  private record Result(int leaf, int matches, long start, long end) {}

  private static void measure(
      Path index, int workers, boolean profile, int rounds, boolean priceCap) throws Exception {
    var cpu = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    try (var dir = FSDirectory.open(index);
        var reader = DirectoryReader.open(dir);
        var pool = Executors.newFixedThreadPool(workers)) {
      var base =
          LuceneIndexSearcher.create(
              reader,
              new QueryCacheProvider.DefaultQueryCacheProvider(),
              Optional.empty(),
              Optional.empty(),
              false,
              false,
              Optional.empty());
      base.setQueryCache(null);
      var filter = CatalogFilterProfile.catalogCase(priceCap ? 2 : 6, priceCap);
      var query = base.rewrite(filter.query());
      System.out.println("round,leaf,matches,start_ns,end_ns,request_ns,process_cpu_ns");
      for (int round = -2; round < rounds; round++) {
        var weight = CatalogFilterProfile.filterWeight(base, query, profile);
        var tasks = new ArrayList<Callable<Result>>();
        for (var leaf : reader.leaves()) {
          tasks.add(
              () -> {
                var accepted = CatalogFilterProfile.acceptedDocs(weight, leaf);
                long start = System.nanoTime();
                int count = accepted.cost();
                return new Result(leaf.ord, count, start, System.nanoTime());
              });
        }
        long cpuStart = cpu.getProcessCpuTime();
        long start = System.nanoTime();
        var futures = pool.invokeAll(tasks);
        long elapsed = System.nanoTime() - start;
        long cpuNanos = cpu.getProcessCpuTime() - cpuStart;
        for (var future : futures) {
          var r = future.get();
          if (round >= 0)
            System.out.printf(
                "%d,%d,%d,%d,%d,%d,%d%n",
                round, r.leaf, r.matches, r.start - start, r.end - start, elapsed, cpuNanos);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    switch (args[0]) {
      case "build" ->
          build(
              Path.of(args[1]),
              Path.of(args[2]),
              Path.of(args[3]),
              args.length > 4 && args[4].equals("price"));
      case "measure" ->
          measure(
              Path.of(args[1]),
              Integer.parseInt(args[2]),
              Boolean.parseBoolean(args[3]),
              Integer.parseInt(args[4]),
              args.length > 5 && args[5].equals("price"));
      default -> throw new IllegalArgumentException("Use build or measure");
    }
  }
}
