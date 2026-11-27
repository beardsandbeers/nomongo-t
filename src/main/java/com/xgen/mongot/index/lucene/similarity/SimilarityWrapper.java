package com.xgen.mongot.index.lucene.similarity;

import java.util.Map;
import java.util.function.Function;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;

/** Resolves a scoring model for each physical Lucene field. */
public class SimilarityWrapper extends PerFieldSimilarityWrapper {
  private final Function<String, Similarity> resolve;

  public SimilarityWrapper(
      Map<String, Similarity> fieldSimilarities, Similarity defaultSimilarity) {
    this(name -> fieldSimilarities.getOrDefault(name, defaultSimilarity));
  }

  SimilarityWrapper(Function<String, Similarity> resolve) {
    this.resolve = resolve;
  }

  @Override
  public final Similarity get(String name) {
    return this.resolve.apply(name);
  }
}
