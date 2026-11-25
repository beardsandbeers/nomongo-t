package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Optional;
import java.util.function.Supplier;
import org.bson.BsonDocument;

/** A BSON-vector-only dynamic mapping. Dimensions may be learned independently at each path. */
public record TypeSetVectorFieldDefinition(
    Optional<Integer> numDimensions, VectorSimilarity similarity, VectorQuantization quantization)
    implements FieldTypeDefinition {
  static TypeSetVectorFieldDefinition fromBson(DocumentParser parser) throws BsonParseException {
    var dimensions = parser.getField(VectorFieldSpecification.Fields.OPTIONAL_DIMENSIONS).unwrap();
    var similarity = parser.getField(VectorFieldSpecification.Fields.SIMILARITY).unwrap();
    var quantization = parser.getField(VectorFieldSpecification.Fields.QUANTIZATION).unwrap();
    if (dimensions.isPresent()
        && !VectorFieldSpecification.validQuantizationDimensions(dimensions.get(), quantization)) {
      parser
          .getContext()
          .handleSemanticError("numDimensions must be a multiple of 8 for binary quantization");
    }
    return new TypeSetVectorFieldDefinition(dimensions, similarity, quantization);
  }

  public Optional<VectorFieldSpecification> resolve(
      Supplier<Optional<Integer>> inferredDimensions) {
    return this.numDimensions
        .or(inferredDimensions)
        .filter(
            dimensions ->
                dimensions > 0
                    && dimensions <= VectorFieldSpecification.MAX_DIMENSIONS
                    && VectorFieldSpecification.validQuantizationDimensions(
                        dimensions, this.quantization))
        .map(
            dimensions ->
                new VectorFieldSpecification(
                    dimensions,
                    this.similarity,
                    this.quantization,
                    new VectorIndexingAlgorithm.HnswIndexingAlgorithm()));
  }

  @Override
  public Type getType() {
    return Type.VECTOR;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return BsonDocumentBuilder.builder()
        .field(VectorFieldSpecification.Fields.OPTIONAL_DIMENSIONS, this.numDimensions)
        .field(VectorFieldSpecification.Fields.SIMILARITY, this.similarity)
        .fieldOmitDefaultValue(VectorFieldSpecification.Fields.QUANTIZATION, this.quantization)
        .build();
  }
}
