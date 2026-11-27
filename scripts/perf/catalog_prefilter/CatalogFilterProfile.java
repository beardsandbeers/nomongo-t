import com.xgen.mongot.index.lucene.explain.ProfilingIndexSearcher;
import com.xgen.mongot.index.lucene.explain.explainers.CollectorTimingFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.explainers.MetadataFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.profiler.ProfileWeight;
import com.xgen.mongot.index.lucene.explain.profiler.QueryProfiler;
import com.xgen.mongot.index.lucene.searcher.LuceneIndexSearcher;
import com.xgen.mongot.index.lucene.searcher.QueryCacheProvider;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

/** Pair normal and profiled filter materialization on the same real-catalog leaf. */
public class CatalogFilterProfile {
  private static final List<String> CATEGORIES =
      List.of(
          "Books",
          "Automotive",
          "Tools & Home Improvement",
          "Health & Personal Care",
          "Computers",
          "All Electronics");

  static CatalogPrefilter.Case catalogCase(int categoryCount, boolean priceCap) {
    var filter = new BooleanQuery.Builder();
    if (priceCap) {
      var categories = new BooleanQuery.Builder().setMinimumNumberShouldMatch(1);
      for (String name : CATEGORIES.subList(0, categoryCount)) {
        categories.add(new TermQuery(new Term("category", name)), Occur.SHOULD);
      }
      filter.add(categories.build(), Occur.FILTER);
      filter.add(CatalogPrefilter.range("price", -Double.MAX_VALUE, 9), Occur.FILTER);
    } else {
      var terms = CATEGORIES.subList(0, categoryCount).stream().map(BytesRef::new).toList();
      filter.add(new TermInSetQuery("category", terms), Occur.FILTER);
    }
    filter.add(CatalogPrefilter.range("reviews", 1, Double.MAX_VALUE), Occur.FILTER);
    filter.add(CatalogPrefilter.range("rating", 1, 1), Occur.MUST_NOT);
    filter.add(new FieldExistsQuery("vector"), Occur.FILTER);
    return new CatalogPrefilter.Case(
        priceCap ? "price_capped" : "categories_" + categoryCount, filter.build());
  }

  private record Sample(int accepted, long nanos) {}

  static Weight filterWeight(LuceneIndexSearcher base, Query rewritten, boolean profile)
      throws Exception {
    IndexSearcher searcher = base;
    if (profile) {
      var profiler = new QueryProfiler();
      profiler.createQueryExecutionContext();
      profiler.getOrCreateNode(rewritten);
      searcher =
          new ProfilingIndexSearcher(
              base,
              profiler,
              new CollectorTimingFeatureExplainer(),
              new MetadataFeatureExplainer());
      searcher.setQueryCache(null);
    }
    var weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
    if (profile != (weight instanceof ProfileWeight)) {
      throw new IllegalStateException("Unexpected filter weight for requested profiling mode");
    }
    return weight;
  }

  static AcceptDocs acceptedDocs(Weight weight, LeafReaderContext leaf) {
    return AcceptDocs.fromIteratorSupplier(
        () -> {
          var scorer = weight.scorer(leaf);
          return scorer == null ? DocIdSetIterator.empty() : scorer.iterator();
        },
        leaf.reader().getLiveDocs(),
        leaf.reader().maxDoc());
  }

  private static Sample measure(
      LuceneIndexSearcher base, LeafReaderContext leaf, Query rewritten, boolean profile)
      throws Exception {
    var accepted = acceptedDocs(filterWeight(base, rewritten, profile), leaf);
    long start = System.nanoTime();
    int count = accepted.cost();
    return new Sample(count, System.nanoTime() - start);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) throw new IllegalArgumentException("Expected one catalog leaf directory");
    try (var dir = FSDirectory.open(Path.of(args[0]));
        var reader = DirectoryReader.open(dir)) {
      if (reader.leaves().size() != 1) throw new IllegalArgumentException("Expected one leaf");
      var leaf = reader.leaves().getFirst();
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
      var cases =
          new ArrayList<>(
              List.of(catalogCase(2, true), catalogCase(3, false), catalogCase(6, false)));
      if (Boolean.getBoolean("catalog.routingFilters")) {
        cases.clear();
        cases.addAll(
            List.of(
                catalogCase(2, true),
                catalogCase(2, false),
                catalogCase(4, false),
                catalogCase(6, false)));
        cases.add(
            new CatalogPrefilter.Case(
                "rating_at_least_two",
                new BooleanQuery.Builder()
                    .add(CatalogPrefilter.range("rating", 2, Double.MAX_VALUE), Occur.FILTER)
                    .add(new FieldExistsQuery("vector"), Occur.FILTER)
                    .build()));
      }
      var random = new Random(431);
      System.out.println("round,filter,profiled,accepted,materialization_ns");
      for (int round = -5; round < 20; round++) {
        Collections.shuffle(cases, random);
        for (var filter : cases) {
          var rewritten = base.rewrite(filter.query());
          boolean profileFirst = round % 2 == 0;
          var first = measure(base, leaf, rewritten, profileFirst);
          var second = measure(base, leaf, rewritten, !profileFirst);
          if (first.accepted() != second.accepted()) {
            throw new IllegalStateException("Profiled and normal eligibility differ");
          }
          if (round >= 0) {
            System.out.printf(
                "%d,%s,%b,%d,%d%n",
                round, filter.name(), profileFirst, first.accepted(), first.nanos());
            System.out.printf(
                "%d,%s,%b,%d,%d%n",
                round, filter.name(), !profileFirst, second.accepted(), second.nanos());
          }
        }
      }
    }
  }
}
