package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.wrapper.LuceneAnalyzer;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.similarity.LuceneSimilarity;
import com.xgen.mongot.index.version.IndexFormatVersion;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Local ingestion schemas share one physical writer. Publication is serialized against mutations by
 * the owner; each query reader retains the immutable view with which it was constructed.
 */
final class LiveSearchSchema {
  record View(
      SearchIndexDefinition definition,
      AnalyzerRegistry registry,
      Analyzer analyzer,
      SearchFieldDefinitionResolver resolver,
      Similarity similarity) {}

  private volatile View current;
  private final IndexFormatVersion format;
  private final Analyzer writerAnalyzer =
      new DelegatingAnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
        @Override
        protected Analyzer getWrappedAnalyzer(String field) {
          return LiveSearchSchema.this.current.analyzer();
        }
      };
  private final Similarity writerSimilarity =
      new Similarity() {
        @Override
        public long computeNorm(FieldInvertState state) {
          return LiveSearchSchema.this.current.similarity().computeNorm(state);
        }

        @Override
        public SimScorer scorer(
            float boost, CollectionStatistics collection, TermStatistics... terms) {
          return LiveSearchSchema.this.current.similarity().scorer(boost, collection, terms);
        }
      };

  LiveSearchSchema(
      SearchIndexDefinition definition, AnalyzerRegistry registry, IndexFormatVersion format) {
    this.format = format;
    this.current = prepare(definition, registry);
  }

  View prepare(SearchIndexDefinition definition, AnalyzerRegistry registry) {
    return new View(
        definition,
        registry,
        LuceneAnalyzer.indexAnalyzer(definition, registry),
        definition.createFieldDefinitionResolver(this.format),
        LuceneSimilarity.from(definition));
  }

  View current() {
    return this.current;
  }

  void publish(View next) {
    this.current = next;
  }

  Analyzer writerAnalyzer() {
    return this.writerAnalyzer;
  }

  Similarity writerSimilarity() {
    return this.writerSimilarity;
  }
}
