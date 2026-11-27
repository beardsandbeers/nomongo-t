package com.xgen.mongot.index.lucene.streaming;

import com.xgen.mongot.index.query.operators.StreamingOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.VectorEncoding;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

/** Reader-wide budgets. The initial walk leaves room to explore competitive segments. */
record StreamPlan(
    int candidateCount,
    int vectorSegmentCount,
    int initialScoreBudget,
    int lookaheadHits,
    int pilotVisits,
    int pilotWidth,
    int refillEvery,
    int refillExpansions) {
  static StreamPlan resolve(
      StreamingOptions options,
      int candidates,
      IndexReader reader,
      String field,
      VectorEncoding encoding) {
    int segments =
        (int)
            reader.leaves().stream()
                .filter(
                    ctx -> {
                      var info = ctx.reader().getFieldInfos().fieldInfo(field);
                      return info != null
                          && info.hasVectorValues()
                          && info.getVectorEncoding() == encoding;
                    })
                .count();
    int leaves = Math.max(1, segments);
    // The broad catalog benchmark uses two large leaves and a skewed 50-leaf reader.
    // Cap both work per leaf and reader-wide work as fan-out grows.
    int visitLimit = options.warmupVisits() > 0 ? options.warmupVisits() : 3200;
    long pilotCapacity = (long) visitLimit * leaves;
    long pilot =
        options.warmupVisits() > 0 ? pilotCapacity : Math.min(pilotCapacity, 45L * candidates);
    long reserve = Math.min(24L * candidates, Math.max(128L, 3 * pilot / 4));
    var adaptive = options.adaptive().orElseThrow();
    int budget =
        adaptive
            .initialScoreBudget()
            .orElse(Math.clamp(pilot + reserve, 1, options.maxCachedNodes()));
    int visits = (int) Math.min(visitLimit, Math.min(pilot, 3L * budget / 5) / leaves);
    int width =
        options.warmupWidth() > 0 ? options.warmupWidth() : Math.clamp(visits / 2, 64, 2048);
    int window =
        adaptive
            .lookaheadHits()
            .orElse(Math.min(options.maxCachedNodes(), Math.clamp(2L * candidates, 32, 1024)));
    return new StreamPlan(
        candidates,
        segments,
        budget,
        window,
        visits,
        width,
        options.refillEvery() > 0 ? options.refillEvery() : 128,
        options.refillExpansions() > 0 ? options.refillExpansions() : 256);
  }

  int prefetchWorkLimit() {
    return Math.min(1024, this.lookaheadHits);
  }

  BsonDocument describe(StreamingOptions options, boolean prefetch) {
    var description =
        new BsonDocument("candidateCount", new BsonInt32(this.candidateCount))
            .append("vectorSegmentCount", new BsonInt32(this.vectorSegmentCount))
            .append("initialScoreBudget", new BsonInt32(this.initialScoreBudget))
            .append(
                "initialScoreBudgetSource",
                new BsonString(
                    options.adaptive().orElseThrow().initialScoreBudget().isPresent()
                        ? "explicit"
                        : "derived"))
            .append(
                "lookaheadHitsSource",
                new BsonString(
                    options.adaptive().orElseThrow().lookaheadHits().isPresent()
                        ? "explicit"
                        : "derived"))
            .append("maxCachedNodes", new BsonInt32(options.maxCachedNodes()))
            .append("rescoreAll", BsonBoolean.valueOf(options.rescoreAll()))
            .append("lookaheadHits", new BsonInt32(this.lookaheadHits))
            .append("pilotVisitLimitPerSegment", new BsonInt32(this.pilotVisits))
            .append("warmupWidth", new BsonInt32(this.pilotWidth))
            .append("refillEvery", new BsonInt32(this.refillEvery))
            .append("refillExpansions", new BsonInt32(this.refillExpansions));
    if (prefetch) {
      description.put("prefetchWorkLimit", new BsonInt32(prefetchWorkLimit()));
    }
    return description;
  }
}
