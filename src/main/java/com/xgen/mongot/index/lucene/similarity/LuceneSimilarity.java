package com.xgen.mongot.index.lucene.similarity;

import com.xgen.mongot.index.analyzer.wrapper.AnalyzerMapping;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SimilarityDefinition;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.Similarity;

public class LuceneSimilarity {
  static final Similarity DEFAULT_BM25_SIMILARITY = new BM25Similarity();

  private static final Similarity BOOLEAN = new BooleanSimilarity();

  /** Indexing and search select the same model for a physical Lucene field. */
  public static Similarity from(SearchIndexDefinition indexDefinition) {
    var mapping = new AnalyzerMapping(indexDefinition);
    if (!mapping.hasSimilarity()) return DEFAULT_BM25_SIMILARITY;
    return new SimilarityWrapper(
        name ->
            switch (mapping.similarity(name).orElse(SimilarityDefinition.BM25).type()) {
              case BM25 -> DEFAULT_BM25_SIMILARITY;
              case BOOLEAN -> BOOLEAN;
              case STABLE_TFL -> StableTflSimilarity.getInstance();
            });
  }
}
