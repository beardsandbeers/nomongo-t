package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.definition.VectorFieldDefinitionResolver;
import com.xgen.mongot.index.lucene.query.context.VectorQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.custom.WrappedKnnQuery;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.MaterializedVectorSearchQuery;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

/** This class is used for $vectorSearch queries running against vector index type. */
public class LuceneVectorQueryFactoryDistributor {

  private final VectorSearchQueryFactory vectorSearchQueryFactory;
  private final VectorFieldDefinitionResolver definitionResolver;
  private final VectorSearchFilterQueryFactory filters;

  private LuceneVectorQueryFactoryDistributor(
      VectorSearchQueryFactory vectorSearchQueryFactory,
      VectorQueryFactoryContext context,
      VectorSearchFilterQueryFactory filters) {
    this.vectorSearchQueryFactory = vectorSearchQueryFactory;
    this.definitionResolver = context.getFieldDefinitionResolver();
    this.filters = filters;
  }

  public static LuceneVectorQueryFactoryDistributor create(
      VectorQueryFactoryContext factoryContext) {
    return create(factoryContext, DynamicFeatureFlagRegistry.empty());
  }

  public static LuceneVectorQueryFactoryDistributor create(
      VectorQueryFactoryContext factoryContext,
      DynamicFeatureFlagRegistry dynamicFeatureFlagRegistry) {
    var filters = VectorSearchFilterQueryFactory.create(factoryContext, dynamicFeatureFlagRegistry);
    return new LuceneVectorQueryFactoryDistributor(
        new VectorSearchQueryFactory(factoryContext, filters, dynamicFeatureFlagRegistry),
        factoryContext,
        filters);
  }

  /** Compiles a metadata predicate independently of vector ranking, using the index settings. */
  public Query createFilter(org.bson.BsonDocument predicate, IndexReader reader)
      throws InvalidQueryException, IOException {
    try (var parser =
        com.xgen.mongot.util.bson.parser.BsonDocumentParser.fromRoot(
                new org.bson.BsonDocument("filter", predicate))
            .build()) {
      var filter =
          com.xgen.mongot.index.query.operators.VectorSearchFilter.fromBson(
                  parser, com.xgen.mongot.index.query.operators.VectorSearchFilter.Type.CLAUSE)
              .orElseThrow();
      return this.filters.createLuceneFilter(filter, reader);
    } catch (com.xgen.mongot.util.bson.parser.BsonParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  public Query createQuery(MaterializedVectorSearchQuery query, IndexReader indexReader)
      throws InvalidQueryException, IOException {
    return this.vectorSearchQueryFactory.fromQuery(
        query.materializedCriteria(), SingleQueryContext.createQueryRoot(indexReader));
  }

  /**
   * Creates a WrappedKnnQuery from an operator to be used for explain queries.
   *
   * @param materializedVectorQuery vector search query
   * @param indexReader index reader
   * @return Lucene Query
   * @throws InvalidQueryException represents a parsing exception
   */
  public Query createExplainQuery(
      MaterializedVectorSearchQuery materializedVectorQuery, IndexReader indexReader)
      throws InvalidQueryException, IOException {
    var singleQueryContext = SingleQueryContext.createExplainRoot(indexReader);
    return new WrappedKnnQuery(
        this.vectorSearchQueryFactory.fromQuery(
            materializedVectorQuery.materializedCriteria(), singleQueryContext));
  }

  public VectorFieldDefinitionResolver getDefinitionResolver() {
    return this.definitionResolver;
  }
}
