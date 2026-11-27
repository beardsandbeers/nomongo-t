package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** Per-request controls for the resumable graph walk. Presence opts into streaming. */
public record StreamingOptions(
    int warmupWidth,
    int warmupVisits,
    int refillEvery,
    int refillExpansions,
    int maxCachedNodes,
    boolean rescoreAll,
    Optional<Adaptive> adaptive)
    implements DocumentEncodable {
  private static final Field.Optional<Integer> WIDTH = positive("warmupWidth", 10000);
  private static final Field.Optional<Integer> VISITS = positive("warmupVisits", 1000000);
  private static final Field.Optional<Integer> EVERY = positive("refillEvery", 10000);
  private static final Field.Optional<Integer> EXPAND = positive("refillExpansions", 10000);
  private static final int DEFAULT_CACHE = 250000;
  private static final Field.WithDefault<Integer> CACHE =
      Field.builder("maxCachedNodes")
          .intField()
          .mustBePositive()
          .optional()
          .withDefault(DEFAULT_CACHE);

  private static final Field.WithDefault<Boolean> RESCORE_ALL =
      Field.builder("rescoreAll").booleanField().optional().withDefault(false);

  /** Omitted values are resolved after the reader snapshot and candidate count are known. */
  public record Adaptive(
      Optional<Integer> initialScoreBudget, Optional<Integer> lookaheadHits, boolean automatic) {
    public Adaptive(Optional<Integer> initialScoreBudget, Optional<Integer> lookaheadHits) {
      this(initialScoreBudget, lookaheadHits, false);
    }
  }

  public boolean automatic() {
    return this.adaptive.map(Adaptive::automatic).orElse(false);
  }

  private static Field.Optional<Integer> positive(String name, int max) {
    return Field.builder(name)
        .intField()
        .mustBeWithinBounds(Range.of(1, max))
        .optional()
        .noDefault();
  }

  private static final Field.Optional<Integer> INITIAL =
      positive("initialScoreBudget", Integer.MAX_VALUE);
  private static final Field.Optional<Integer> LOOKAHEAD =
      positive("lookaheadHits", Integer.MAX_VALUE);

  public static StreamingOptions defaults() {
    return new StreamingOptions(
        0,
        0,
        0,
        0,
        DEFAULT_CACHE,
        false,
        Optional.of(new Adaptive(Optional.empty(), Optional.empty())));
  }

  static Optional<StreamingOptions> parseStream(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    if (value.isString() && value.asString().getValue().equals("auto")) {
      return Optional.of(
          new StreamingOptions(
              0,
              0,
              0,
              0,
              DEFAULT_CACHE,
              false,
              Optional.of(new Adaptive(Optional.empty(), Optional.empty(), true))));
    }
    if (value.isBoolean()) {
      return value.asBoolean().getValue() ? Optional.of(defaults()) : Optional.empty();
    }
    if (!value.isDocument()) {
      return context.handleSemanticError(
          "stream must be a boolean, an options object, or the string 'auto'");
    }
    try (var parser = BsonDocumentParser.withContext(context, value.asDocument()).build()) {
      var initial = parser.getField(INITIAL).unwrap();
      var window = parser.getField(LOOKAHEAD).unwrap();
      int cap = parser.getField(CACHE).unwrap();
      if (initial.orElse(1) > cap || window.orElse(1) > cap) {
        return context.handleSemanticError(
            "initialScoreBudget and lookaheadHits must fit maxCachedNodes");
      }
      return Optional.of(
          new StreamingOptions(
              parser.getField(WIDTH).unwrap().orElse(0),
              parser.getField(VISITS).unwrap().orElse(0),
              parser.getField(EVERY).unwrap().orElse(0),
              parser.getField(EXPAND).unwrap().orElse(0),
              cap,
              parser.getField(RESCORE_ALL).unwrap(),
              Optional.of(new Adaptive(initial, window))));
    }
  }

  public BsonValue streamBson() {
    if (automatic()) {
      return new org.bson.BsonString("auto");
    }
    var doc = toBson();
    return doc.isEmpty() ? BsonBoolean.TRUE : doc;
  }

  public StreamingOptions(
      int warmupWidth,
      int warmupVisits,
      int refillEvery,
      int refillExpansions,
      int maxCachedNodes,
      boolean rescoreAll) {
    this(
        warmupWidth,
        warmupVisits,
        refillEvery,
        refillExpansions,
        maxCachedNodes,
        rescoreAll,
        Optional.empty());
  }

  public StreamingOptions(
      int warmupWidth,
      int warmupVisits,
      int refillEvery,
      int refillExpansions,
      int maxCachedNodes) {
    this(warmupWidth, warmupVisits, refillEvery, refillExpansions, maxCachedNodes, false);
  }

  public StreamingOptions {
    adaptive.ifPresent(
        a -> {
          if (a.initialScoreBudget().map(n -> n < 1 || n > maxCachedNodes).orElse(false)
              || a.lookaheadHits().map(n -> n < 1 || n > maxCachedNodes).orElse(false)) {
            throw new IllegalArgumentException("Invalid adaptive streaming budget");
          }
        });
    int minimum = adaptive.isPresent() ? 0 : 1;
    if (warmupWidth < minimum
        || warmupWidth > 10_000
        || warmupVisits < minimum
        || warmupVisits > 1_000_000
        || refillEvery < minimum
        || refillEvery > 10_000
        || refillExpansions < minimum
        || refillExpansions > 10_000
        || maxCachedNodes < 1) {
      throw new IllegalArgumentException("Invalid streaming exploration budget");
    }
  }

  static StreamingOptions fromBson(DocumentParser parser) throws BsonParseException {
    return new StreamingOptions(
        parser.getField(WIDTH).unwrap().orElse(64),
        parser.getField(VISITS).unwrap().orElse(1024),
        parser.getField(EVERY).unwrap().orElse(8),
        parser.getField(EXPAND).unwrap().orElse(8),
        parser.getField(CACHE).unwrap(),
        parser.getField(RESCORE_ALL).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    var builder =
        BsonDocumentBuilder.builder()
            .field(WIDTH, explicit(this.warmupWidth))
            .field(VISITS, explicit(this.warmupVisits))
            .field(EVERY, explicit(this.refillEvery))
            .field(EXPAND, explicit(this.refillExpansions));
    if (this.adaptive.isPresent()) {
      builder
          .fieldOmitDefaultValue(CACHE, this.maxCachedNodes)
          .field(INITIAL, this.adaptive.get().initialScoreBudget())
          .field(LOOKAHEAD, this.adaptive.get().lookaheadHits());
    } else {
      builder.field(CACHE, this.maxCachedNodes);
    }
    return builder.fieldOmitDefaultValue(RESCORE_ALL, this.rescoreAll).build();
  }

  private static Optional<Integer> explicit(int value) {
    return value == 0 ? Optional.empty() : Optional.of(value);
  }
}
