package com.xgen.mongot.featureflag.dynamic;

import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.SearchQueryParseOptions;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;

/** Shared query-language gates for coupled and standalone search entry points. */
public final class SearchQueryFeaturePolicy {
  private SearchQueryFeaturePolicy() {}

  /** Command-level validation shared by both query transports, before catalog lookup. */
  public static void validateQuery(
      SearchQuery query, boolean requireSequenceTokens, FeatureFlags featureFlags)
      throws InvalidQueryException {
    if (!(query instanceof OperatorQuery operatorQuery)) {
      return;
    }
    if (operatorQuery.operator() instanceof VectorSearchOperator && requireSequenceTokens) {
      throw new InvalidQueryException(
          "Pagination is not supported with the 'vectorSearch' operator. "
              + "Use $skip and $limit stages instead.");
    }
    if (operatorQuery.operator() instanceof VectorSearchOperator vector) {
      validateVectorCriteria(vector.criteria(), featureFlags);
    }
  }

  /** Candidate selection policy shared by search operators and vector-search transports. */
  public static void validateVectorCriteria(
      VectorSearchCriteria criteria, FeatureFlags featureFlags) throws InvalidQueryException {
    if (criteria instanceof ApproximateVectorSearchCriteria approx
        && approx.isAutoNumCandidates()
        && !approx.isUnboundedStream()
        && !featureFlags.isEnabled(Feature.AUTO_NUM_CANDIDATES)) {
      throw new InvalidQueryException("numCandidates is required for approximate vector search");
    }
  }

  /** Evaluate per request so updates to the engine's dynamic registry also affect parsing. */
  public static SearchQueryParseOptions parseOptions(
      FeatureFlags featureFlags, DynamicFeatureFlagRegistry dynamicFeatureFlags) {
    return new SearchQueryParseOptions(
        dynamicFeatureFlags.evaluateClusterInvariant(DynamicFeatureFlags.ENABLE_10K_BUCKET_LIMIT),
        featureFlags.isEnabled(Feature.SEARCH_AGGREGATION),
        dynamicFeatureFlags.evaluateClusterInvariant(
            DynamicFeatureFlags.ENABLE_EMBEDDED_SCOPED_SORT));
  }
}
