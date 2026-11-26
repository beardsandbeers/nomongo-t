package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

/** Per-request controls for the resumable graph walk. Presence opts into streaming. */
public record StreamingOptions(
    int warmupWidth,
    int warmupVisits,
    int refillEvery,
    int refillExpansions,
    int maxCachedNodes,
    boolean rescoreAll)
    implements DocumentEncodable {
  private static Field.WithDefault<Integer> option(String name, int max, int initial) {
    return Field.builder(name)
        .intField()
        .mustBeWithinBounds(Range.of(1, max))
        .optional()
        .withDefault(initial);
  }

  private static final Field.WithDefault<Integer> WIDTH = option("warmupWidth", 10_000, 64);
  private static final Field.WithDefault<Integer> VISITS = option("warmupVisits", 1_000_000, 1024);
  private static final Field.WithDefault<Integer> EVERY = option("refillEvery", 10_000, 8);
  private static final Field.WithDefault<Integer> EXPAND = option("refillExpansions", 10_000, 8);
  private static final Field.WithDefault<Integer> CACHE =
      option("maxCachedNodes", Integer.MAX_VALUE, 250_000);

  private static final Field.WithDefault<Boolean> RESCORE_ALL =
      Field.builder("rescoreAll").booleanField().optional().withDefault(false);

  public StreamingOptions(
      int warmupWidth,
      int warmupVisits,
      int refillEvery,
      int refillExpansions,
      int maxCachedNodes) {
    this(warmupWidth, warmupVisits, refillEvery, refillExpansions, maxCachedNodes, false);
  }

  public StreamingOptions {
    if (warmupWidth < 1
        || warmupWidth > 10_000
        || warmupVisits < 1
        || warmupVisits > 1_000_000
        || refillEvery < 1
        || refillEvery > 10_000
        || refillExpansions < 1
        || refillExpansions > 10_000
        || maxCachedNodes < 1) {
      throw new IllegalArgumentException("Invalid streaming exploration budget");
    }
  }

  static StreamingOptions fromBson(DocumentParser parser) throws BsonParseException {
    return new StreamingOptions(
        parser.getField(WIDTH).unwrap(),
        parser.getField(VISITS).unwrap(),
        parser.getField(EVERY).unwrap(),
        parser.getField(EXPAND).unwrap(),
        parser.getField(CACHE).unwrap(),
        parser.getField(RESCORE_ALL).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    var builder =
        BsonDocumentBuilder.builder()
            .field(WIDTH, this.warmupWidth)
            .field(VISITS, this.warmupVisits)
            .field(EVERY, this.refillEvery)
            .field(EXPAND, this.refillExpansions)
            .field(CACHE, this.maxCachedNodes);
    if (this.rescoreAll) {
      builder.field(RESCORE_ALL, true);
    }
    return builder.build();
  }
}
