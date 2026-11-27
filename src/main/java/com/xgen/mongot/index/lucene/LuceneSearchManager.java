package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.query.InvalidQueryException;
import java.io.IOException;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

public interface LuceneSearchManager<T> {

  class QueryInfo {
    public final TopDocs topDocs;
    public final boolean luceneExhausted;

    public QueryInfo(TopDocs topDocs, boolean luceneExhausted) {
      this.topDocs = topDocs;
      this.luceneExhausted = luceneExhausted;
    }
  }

  /** A resumed graph exploration can discover a higher-scoring hit after a page was delivered. */
  default boolean permitsScoreInversions() {
    return false;
  }

  T initialSearch(LuceneIndexSearcherReference searcherReference, int batchSize)
      throws IOException, InvalidQueryException;

  TopDocs getMoreTopDocs(
      LuceneIndexSearcherReference searcherReference, ScoreDoc lastScoreDoc, int batchSize)
      throws IOException;
}
