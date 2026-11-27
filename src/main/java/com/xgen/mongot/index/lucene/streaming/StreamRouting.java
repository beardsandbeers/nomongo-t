package com.xgen.mongot.index.lucene.streaming;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBinaryQuantizedVectorsFormat;
import com.xgen.mongot.util.QueryCancellation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.jetbrains.annotations.Nullable;

/** Finite ANN routing bound to the leaves of the query's pinned reader. */
public record StreamRouting(List<Segment> segments, int limit, int candidates) {
  public StreamRouting(List<Segment> segments, int limit, int candidates) {
    this.segments = List.copyOf(segments);
    this.limit = limit;
    this.candidates = candidates;
  }

  public record Segment(
      LeafReaderContext context,
      String format,
      String reason,
      int vectors,
      long minimumMatches,
      int sampled,
      int accepted,
      @Nullable BsonDocument execution) {
    public boolean stream() {
      return this.reason.equals("broadFilter");
    }

    static Segment classic(
        LeafReaderContext ctx, String format, String reason, int vectors, long minimum) {
      return new Segment(ctx, format, reason, vectors, minimum, 0, 0, null);
    }

    BsonDocument describe() {
      var doc =
          new BsonDocument("segmentOrdinal", new BsonInt32(this.context.ord))
              .append("codec", new BsonString(this.format))
              .append("selected", new BsonString(stream() ? "stream" : "classic"))
              .append("reason", new BsonString(this.reason))
              .append("sampledVectors", new BsonInt32(this.sampled));
      if (this.vectors >= 0) {
        doc.append("vectorCount", new BsonInt64(this.vectors));
      }
      if (this.minimumMatches > 0) {
        doc.append("minimumMatchingVectors", new BsonInt64(this.minimumMatches));
      }
      if (this.sampled > 0) {
        doc.append("estimatedAcceptance", new BsonDouble((double) this.accepted / this.sampled));
        doc.append(
            "estimatedMatchingVectors",
            new BsonInt64((long) this.vectors * this.accepted / this.sampled));
      }
      if (this.execution != null) {
        doc.append("execution", this.execution);
      }
      return doc;
    }
  }

  /** Plan each segment independently using vector-field metadata and sampled live membership. */
  public static StreamRouting choose(
      IndexReader reader,
      String field,
      VectorEncoding encoding,
      @Nullable Query filter,
      int candidates,
      int limit,
      boolean eligible,
      boolean executionStats)
      throws IOException {

    @Nullable String skip = unsupportedReason(eligible, filter);
    var segments = new ArrayList<Segment>();
    for (var ctx : reader.leaves()) {
      QueryCancellation.check();
      var info = ctx.reader().getFieldInfos().fieldInfo(field);
      if (info == null || !info.hasVectorValues() || info.getVectorEncoding() != encoding) {
        segments.add(Segment.classic(ctx, "unknown", "noVectors", 0, 0));
        continue;
      }
      String format = info.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY);
      String codec = format == null ? "unknown" : format + "/" + encoding;
      KnnVectorValues values =
          encoding == VectorEncoding.BYTE
              ? ctx.reader().getByteVectorValues(field)
              : ctx.reader().getFloatVectorValues(field);
      int count = values == null ? 0 : values.size();
      if (skip != null || !StreamingVectors.supported(format)) {
        segments.add(
            Segment.classic(ctx, codec, skip == null ? "unsupportedFormat" : skip, count, 0));
        continue;
      }
      // The mixed-reader holdout lost latency with raw FP32 at 312k vectors per segment.
      long minimum =
          encoding == VectorEncoding.FLOAT32 && "Lucene99HnswVectorsFormat".equals(format)
              ? 450_000L
              : 225_000L;
      if (count < minimum) {
        segments.add(Segment.classic(ctx, codec, "smallVectorPopulation", count, minimum));
        continue;
      }
      if (standaloneRange(filter)) {
        segments.add(Segment.classic(ctx, codec, "singleRange", count, minimum));
        continue;
      }
      if (!calibrated(format, encoding, limit, candidates)) {
        segments.add(Segment.classic(ctx, codec, "uncalibratedProfile", count, minimum));
        continue;
      }
      int samples = 512;
      var random = new SplittableRandom(7819);
      int[] docs = new int[samples];
      for (int i = 0; i < samples; i++) {
        long ord = random.nextLong((long) i * count / samples, (i + 1L) * count / samples);
        docs[i] = values.ordToDoc((int) ord);
      }
      boolean[] matches = new boolean[samples];
      // Keep rewriting and membership local as well: a term's weight can inspect every leaf.
      var local = new IndexSearcher(ctx.reader());
      local.setQueryCache(null);
      CandidateFilter.compile(local, ctx.reader().getContext(), filter).test(docs, matches);
      var live = ctx.reader().getLiveDocs();
      @Var int accepted = 0;
      for (int i = 0; i < samples; i++) {
        if (matches[i] && (live == null || live.get(docs[i]))) {
          accepted++;
        }
      }
      String choice =
          accepted / (double) samples < 0.70
              ? "selectiveOrUncertain"
              : (long) accepted * count / samples < minimum
                  ? "smallMatchingPopulation"
                  : "broadFilter";
      segments.add(
          new Segment(
              ctx,
              codec,
              choice,
              count,
              minimum,
              samples,
              accepted,
              executionStats && choice.equals("broadFilter") ? new BsonDocument() : null));
    }
    return new StreamRouting(segments, limit, candidates);
  }

  private static @Nullable String unsupportedReason(boolean eligible, @Nullable Query filter) {
    if (!eligible) {
      return "unsupportedShape";
    }
    if (filter == null || filter instanceof MatchAllDocsQuery) {
      return "noFilter";
    }
    if (filter instanceof MatchNoDocsQuery) {
      return "matchesNone";
    }
    return cheapMembership(filter) ? null : "unsupportedPredicate";
  }

  // A 98%-passing standalone rating range was cheap to prepare in the segment sweep.
  private static boolean standaloneRange(@Nullable Query filter) {
    if (filter instanceof ConstantScoreQuery q) {
      return standaloneRange(q.getQuery());
    }
    if (filter instanceof BoostQuery q) {
      return standaloneRange(q.getQuery());
    }
    if (filter instanceof BooleanQuery q && q.clauses().size() == 1) {
      var clause = q.clauses().getFirst();
      return !clause.isProhibited() && standaloneRange(clause.query());
    }
    return filter instanceof IndexOrDocValuesQuery q
        && q.getIndexQuery() instanceof PointRangeQuery;
  }

  private static boolean calibrated(
      String format, VectorEncoding encoding, int limit, int candidates) {
    // Interpolate only within the swept limits and candidate budgets for this top-k.
    int usual = Math.clamp(8L * limit, 100, 800);
    if (limit < 10
        || limit > 100
        || candidates < Math.min(500, usual)
        || candidates > Math.max(500, usual)) {
      return false;
    }
    if (encoding == VectorEncoding.FLOAT32
        && Mongot01042HnswBinaryQuantizedVectorsFormat.NAME.equals(format)) {
      // At top-100 / 800 candidates the binary-FP32 sweep lost recall for little latency gain.
      return candidates <= 500;
    }
    return true;
  }

  public boolean usesStream() {
    return this.segments.stream().anyMatch(Segment::stream);
  }

  public BsonDocument describe() {
    long count = this.segments.stream().filter(Segment::stream).count();
    return new BsonDocument("requested", new BsonString("auto"))
        .append(
            "selected",
            new BsonString(
                count == 0 ? "classic" : count == this.segments.size() ? "stream" : "mixed"))
        .append("limit", new BsonInt32(this.limit))
        .append("numCandidates", new BsonInt32(this.candidates))
        .append("segments", new BsonArray(this.segments.stream().map(Segment::describe).toList()));
  }

  private static boolean cheapMembership(Query q) {
    if (q instanceof BooleanQuery b) {
      return b.clauses().stream().allMatch(c -> cheapMembership(c.query()));
    }
    if (q instanceof BoostQuery b) {
      return cheapMembership(b.getQuery());
    }
    if (q instanceof ConstantScoreQuery c) {
      return cheapMembership(c.getQuery());
    }
    if (q instanceof IndexOrDocValuesQuery d) {
      return d.getIndexQuery() instanceof PointRangeQuery || cheapMembership(d.getIndexQuery());
    }
    return q instanceof TermQuery
        || q instanceof TermInSetQuery t && t.getTermsCount() <= 128
        || q instanceof MatchAllDocsQuery
        || q instanceof MatchNoDocsQuery
        || q instanceof FieldExistsQuery;
  }
}
