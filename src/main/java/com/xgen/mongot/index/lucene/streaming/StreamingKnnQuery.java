package com.xgen.mongot.index.lucene.streaming;

import com.xgen.mongot.index.lucene.explain.knn.VectorSearchExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.query.custom.LeafKnnSearch;
import com.xgen.mongot.index.query.operators.StreamingOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.jetbrains.annotations.Nullable;

/**
 * Finite stage adapter. Lucene collectors and mongot rescoring still receive ordinary score docs.
 */
public final class StreamingKnnQuery {
  private StreamingKnnQuery() {}

  public static BsonDocument explainPlan(
      StreamingOptions options,
      int candidates,
      IndexReader reader,
      String field,
      VectorEncoding encoding,
      boolean openEnded) {
    return options.adaptive().isEmpty()
        ? options.toBson()
        : StreamPlan.resolve(options, candidates, reader, field, encoding)
            .describe(options, openEnded);
  }

  /** The search batch producer pins this reader until cursor close. */
  public interface Resumable {
    RankedVectorCursor openOnPinnedReader(IndexSearcher searcher) throws IOException;
  }

  /** Query that can collect a candidate pool on one pinned leaf. */
  public interface Finite {
    RankedVectorCursor openFinite(IndexSearcher searcher) throws IOException;
  }

  /** Supply leaf candidates to Lucene's existing kNN merge, with classic fallback on underfill. */
  public static LeafKnnSearch routed(StreamRouting plan, Finite source) {
    return (ctx, timeout) -> {
      if (ctx.ord >= plan.segments().size()) {
        return null;
      }
      var decision = plan.segments().get(ctx.ord);
      if (decision.context() != ctx || !decision.stream()) {
        return null;
      }
      var local = new IndexSearcher(ctx.reader());
      local.setTimeout(timeout);
      long start = System.nanoTime();
      try (var cursor = source.openFinite(local)) {
        var hits = collectHits(cursor, plan.candidates());
        if (decision.execution() != null) {
          decision.execution().putAll(cursor.explanation(true, hits == null));
          decision
              .execution()
              .put("elapsedMillis", new BsonDouble((System.nanoTime() - start) / 1e6));
        }
        if (hits == null) {
          return null;
        }
        var docs =
            hits.stream()
                .map(h -> new ScoreDoc(ctx.docBase + h.doc(), h.score()))
                .toArray(ScoreDoc[]::new);
        Arrays.sort(
            docs,
            Comparator.<ScoreDoc>comparingDouble(h -> h.score)
                .reversed()
                .thenComparingInt(h -> h.doc));
        return new TopDocs(new TotalHits(cursor.cachedNodes(), TotalHits.Relation.EQUAL_TO), docs);
      }
    };
  }

  public static final class Floats extends KnnFloatVectorQuery implements Resumable, Finite {
    private final StreamingOptions options;

    public Floats(
        String field, float[] target, int k, @Nullable Query filter, StreamingOptions options) {
      super(field, target.clone(), k, filter);
      this.options = options;
    }

    @Override
    public RankedVectorCursor openOnPinnedReader(IndexSearcher searcher) throws IOException {
      return RankedVectorCursor.open(
          searcher,
          getField(),
          this.target,
          getFilter(),
          this.options,
          RankedVectorCursor.Use.PAGED_STAGE,
          getK());
    }

    @Override
    public RankedVectorCursor openFinite(IndexSearcher searcher) throws IOException {
      return RankedVectorCursor.open(
          searcher,
          getField(),
          this.target,
          getFilter(),
          this.options,
          RankedVectorCursor.Use.FINITE_STAGE,
          getK());
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
      long start = System.nanoTime();
      try (var cursor = openFinite(searcher)) {
        long opened = System.nanoTime();
        var result = collect(searcher, cursor, getK());
        recordExplain(cursor, result == null, start, opened);
        if (result != null) {
          return result;
        }
      }
      return super.rewrite(searcher);
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other) && this.options.equals(((Floats) other).options);
    }

    @Override
    public int hashCode() {
      return 31 * super.hashCode() + this.options.hashCode();
    }

    @Override
    public String toString(String field) {
      return super.toString(field) + " " + this.options;
    }
  }

  public static final class Bytes extends KnnByteVectorQuery implements Resumable, Finite {
    private final StreamingOptions options;

    public Bytes(
        String field, byte[] target, int k, @Nullable Query filter, StreamingOptions options) {
      super(field, target.clone(), k, filter);
      this.options = options;
    }

    @Override
    public RankedVectorCursor openOnPinnedReader(IndexSearcher searcher) throws IOException {
      return RankedVectorCursor.open(
          searcher,
          getField(),
          this.target,
          getFilter(),
          this.options,
          RankedVectorCursor.Use.PAGED_STAGE,
          getK());
    }

    @Override
    public RankedVectorCursor openFinite(IndexSearcher searcher) throws IOException {
      return RankedVectorCursor.open(
          searcher,
          getField(),
          this.target,
          getFilter(),
          this.options,
          RankedVectorCursor.Use.FINITE_STAGE,
          getK());
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
      long start = System.nanoTime();
      try (var cursor = openFinite(searcher)) {
        long opened = System.nanoTime();
        var result = collect(searcher, cursor, getK());
        recordExplain(cursor, result == null, start, opened);
        if (result != null) {
          return result;
        }
      }
      return super.rewrite(searcher);
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other) && this.options.equals(((Bytes) other).options);
    }

    @Override
    public int hashCode() {
      return 31 * super.hashCode() + this.options.hashCode();
    }

    @Override
    public String toString(String field) {
      return super.toString(field) + " " + this.options;
    }
  }

  private static void recordExplain(
      RankedVectorCursor cursor, boolean fallback, long start, long opened) {
    Explain.getQueryInfo()
        .ifPresent(
            info -> {
              long finished = System.nanoTime();
              boolean execution =
                  info.getVerbosity().isGreaterThan(Explain.Verbosity.QUERY_PLANNER);
              var stats = cursor.explanation(execution, fallback);
              if (execution) {
                stats.put("startupMillis", new BsonDouble((opened - start) / 1_000_000d));
                stats.put(
                    "candidateCollectionMillis", new BsonDouble((finished - opened) / 1_000_000d));
              }
              info.getFeatureExplainer(VectorSearchExplainer.class, VectorSearchExplainer::new)
                  .setStreamStats(stats);
            });
  }

  private static @Nullable Query collect(IndexSearcher searcher, RankedVectorCursor cursor, int k)
      throws IOException {
    var hits = collectHits(cursor, k);
    if (hits == null) {
      return null;
    }
    if (hits.isEmpty()) {
      return new MatchNoDocsQuery("empty streaming result");
    }
    hits.sort(Comparator.comparingInt(RankedVectorCursor.Hit::doc));
    return new RankedHitsQuery(searcher.getIndexReader(), hits);
  }

  private static @Nullable List<RankedVectorCursor.Hit> collectHits(
      RankedVectorCursor cursor, int k) throws IOException {
    var hits = new ArrayList<RankedVectorCursor.Hit>(k);
    while (hits.size() < k) {
      var batch = cursor.poll(k - hits.size(), 256);
      hits.addAll(batch.hits());
      if (batch.exhausted()) {
        break;
      }
    }
    if (hits.size() < k && cursor.needsClassicFallback()) {
      return null;
    }

    if (hits.size() == k) {
      cursor.refineCandidates(hits);
    }
    return hits;
  }
}
