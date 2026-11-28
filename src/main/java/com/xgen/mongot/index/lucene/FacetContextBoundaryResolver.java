package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName.TypeField;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.aggregation.AggregationRegistry.GroupingArgs.BoundaryArgs;
import com.xgen.mongot.index.query.collectors.AggregationSpecBuilder;
import com.xgen.mongot.index.query.collectors.FacetDefinition.BoundaryFacetDefinition;
import com.xgen.mongot.index.query.collectors.FacetDefinition.NumericFacetDefinition;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import org.bson.BsonString;

/**
 * Resolves V2 boundary columns after aggregation validation, including alternate BSON spellings.
 */
record FacetContextBoundaryResolver(LuceneFacetContext context)
    implements AggregationSpecBuilder.BoundaryFieldResolver {
  @Override
  public FieldDefinition getFieldDefinition(FieldPath path) {
    return this.context
        .getFieldDefinitionResolver()
        .getFieldDefinition(path, Optional.empty())
        .orElseThrow(
            () -> new IllegalStateException("Unresolvable field definition for path: " + path));
  }

  @Override
  public TypeField getBoundaryFacetFieldType(BoundaryFacetDefinition<?> facet) {
    try {
      return this.context.getBoundaryFacetFieldType(facet, Optional.empty());
    } catch (InvalidQueryException e) {
      throw new IllegalStateException("Failed to get field type for " + facet.path(), e);
    }
  }

  @Override
  public BoundaryArgs resolve(BoundaryFacetDefinition<?> facet) {
    var path = FieldPath.parse(facet.path());
    boolean numeric = facet instanceof NumericFacetDefinition;
    var field =
        this.context
            .getFieldDefinitionResolver()
            .getQueryFieldDefinition(
                path,
                Optional.empty(),
                candidate ->
                    Optional.of(candidate)
                        .filter(
                            f ->
                                numeric
                                    ? f.numberFieldDefinition().isPresent()
                                    : f.dateFieldDefinition().isPresent()))
            .orElseThrow(() -> new IllegalStateException("Unresolvable boundary field: " + path));
    var type =
        numeric
            ? LuceneAggregationContextFieldResolver.getNumericTypeField(
                field.numberFieldDefinition().orElseThrow())
            : TypeField.DATE_V2;
    return new BoundaryArgs(
        facet.boundaries(),
        facet.defaultBucketName().map(BsonString::new),
        Optional.of(type),
        Optional.of(field),
        this.context.numericBucketOrdinals(facet, Optional.empty(), type));
  }
}
