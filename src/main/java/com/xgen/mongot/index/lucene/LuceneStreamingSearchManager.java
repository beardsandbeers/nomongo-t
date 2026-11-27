package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.lucene.streaming.RankedVectorCursor;
import com.xgen.mongot.index.lucene.streaming.StreamingKnnQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.jetbrains.annotations.Nullable;

/**
 * Keeps one resumable walk and the last offered page. A byte-limited batch may consume only a
 * prefix.
 */
final class LuceneStreamingSearchManager
    implements LuceneSearchManager<LuceneSearchManager.QueryInfo> {
  private final Query query;
  private @Nullable RankedVectorCursor cursor;
  private List<ScoreDoc> page = List.of();
  private boolean exhausted;
  private long discovered;
  private long startupNanos;
  private long collectionNanos;

  LuceneStreamingSearchManager(Query query) {
    this.query = query instanceof WrappedKnnQuery wrapped ? wrapped.getQuery() : query;
  }

  @Override
  public boolean permitsScoreInversions() {
    return true;
  }

  @Override
  public QueryInfo initialSearch(LuceneIndexSearcherReference ref, int size)
      throws IOException, InvalidQueryException {
    if (!(this.query instanceof StreamingKnnQuery.Resumable stream)) {
      throw new InvalidQueryException(
          "Open-ended vector search requires a root HNSW query with vector similarity scoring");
    }
    long start = System.nanoTime();
    this.cursor = stream.openOnPinnedReader(ref.getIndexSearcher());
    ref.attachCursor(this.cursor);
    this.cursor.startPrefetch();
    this.startupNanos = System.nanoTime() - start;
    return new QueryInfo(next(List.of(), size), this.exhausted);
  }

  @Override
  public TopDocs getMoreTopDocs(LuceneIndexSearcherReference ref, ScoreDoc after, int size)
      throws IOException {
    int last = this.page.indexOf(after);
    if (last < 0) {
      throw new IllegalStateException("Unknown stream continuation");
    }
    return next(this.page.subList(last + 1, this.page.size()), size);
  }

  private TopDocs next(List<ScoreDoc> pending, int size) throws IOException {
    // A previous BSON batch can end at any hit. Keep its suffix even if the next request is
    // smaller.
    long start = System.nanoTime();
    var stream = this.cursor;
    var docs = new ArrayList<>(pending);
    while (docs.size() < size && stream != null && !this.exhausted) {
      com.xgen.mongot.util.QueryCancellation.check();
      var batch = stream.poll(size - docs.size(), 256);
      for (var hit : batch.hits()) {
        docs.add(new ScoreDoc(hit.doc(), hit.score()));
      }
      this.discovered += batch.hits().size();
      this.exhausted = batch.exhausted();
    }
    if (stream != null) {
      this.collectionNanos += System.nanoTime() - start;
      stream.explainPage(this.startupNanos, this.collectionNanos);
      if (this.exhausted) {
        stream.close();
        this.cursor = null;
      }
    }
    this.page = docs;
    int count = Math.min(size, docs.size());
    var hits =
        new TotalHits(
            this.discovered,
            this.exhausted
                ? TotalHits.Relation.EQUAL_TO
                : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    return new TopDocs(hits, docs.subList(0, count).toArray(ScoreDoc[]::new));
  }
}
