package com.xgen.mongot.index.lucene.streaming;

import com.xgen.mongot.index.query.operators.StreamingOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.Nullable;

/**
 * Finite stage adapter. Lucene collectors and mongot rescoring still receive ordinary score docs.
 */
public final class StreamingKnnQuery {
  private StreamingKnnQuery() {}

  public static final class Floats extends KnnFloatVectorQuery {
    private final StreamingOptions options;

    public Floats(
        String field, float[] target, int k, @Nullable Query filter, StreamingOptions options) {
      super(field, target.clone(), k, filter);
      this.options = options;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
      try (var cursor =
          RankedVectorCursor.open(
              searcher, getField(), this.target, getFilter(), this.options, false, getK())) {
        var result = collect(searcher, cursor, getK());
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

  public static final class Bytes extends KnnByteVectorQuery {
    private final StreamingOptions options;

    public Bytes(
        String field, byte[] target, int k, @Nullable Query filter, StreamingOptions options) {
      super(field, target.clone(), k, filter);
      this.options = options;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
      try (var cursor =
          RankedVectorCursor.open(
              searcher, getField(), this.target, getFilter(), this.options, false, getK())) {
        var result = collect(searcher, cursor, getK());
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

  private static @Nullable Query collect(IndexSearcher searcher, RankedVectorCursor cursor, int k)
      throws IOException {
    var hits = new ArrayList<RankedVectorCursor.Hit>(k);
    while (hits.size() < k) {
      var batch = cursor.poll(k - hits.size(), 256);
      hits.addAll(batch.hits());
      if (batch.exhausted()) {
        break;
      }
    }
    if (hits.size() < k && cursor.hasUnreachableVectors()) {
      return null;
    }
    if (hits.isEmpty()) {
      return new MatchNoDocsQuery("empty streaming result");
    }
    hits.sort(Comparator.comparingInt(RankedVectorCursor.Hit::doc));
    return new RankedHitsQuery(searcher.getIndexReader(), hits);
  }
}
