package com.xgen.mongot.index.lucene.streaming;

import com.xgen.mongot.index.lucene.quantization.BinaryQuantizedFlatVectorsScorer;
import com.xgen.mongot.index.lucene.quantization.FlatBitVectorsScorer;
import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBinaryQuantizedVectorsFormat;
import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBitVectorsFormat;
import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.jetbrains.annotations.Nullable;

record StreamingVectors(
    HnswGraph graph,
    KnnVectorValues values,
    RandomVectorScorer traversal,
    RandomVectorScorer output) {
  interface Target {
    RandomVectorScorer scorer(
        FlatVectorsScorer factory, VectorSimilarityFunction similarity, KnnVectorValues values)
        throws IOException;
  }

  static @Nullable StreamingVectors open(
      LeafReaderContext leaf,
      String field,
      Target target,
      VectorEncoding encoding,
      boolean finalScores)
      throws IOException {
    var info = leaf.reader().getFieldInfos().fieldInfo(field);
    if (info == null || !info.hasVectorValues() || info.getVectorEncoding() != encoding) {
      return null;
    }
    if (!(leaf.reader() instanceof CodecReader codec)
        || !(codec.getVectorReader() instanceof PerFieldKnnVectorsFormat.FieldsReader fields)) {
      throw new IllegalArgumentException("Streaming requires a per-field vector reader");
    }
    var reader = fields.getFieldReader(field);
    if (!(reader instanceof HnswGraphProvider provider)) {
      throw new IllegalArgumentException("Streaming requires an HNSW vector field: " + field);
    }
    var format = info.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY);
    var raw =
        encoding == VectorEncoding.BYTE
            ? reader.getByteVectorValues(field)
            : reader.getFloatVectorValues(field);
    if (raw == null || raw.size() == 0) {
      return null;
    }
    var similarity = info.getVectorSimilarityFunction();
    var standard = FlatVectorScorerUtil.getLucene99FlatVectorsScorer();
    RandomVectorScorer traversal;
    RandomVectorScorer output;
    if (Mongot01042HnswBinaryQuantizedVectorsFormat.NAME.equals(format)) {
      var binary = ((QuantizedVectorsReader) reader).getQuantizedVectorValues(field);
      traversal = target.scorer(new BinaryQuantizedFlatVectorsScorer(standard), similarity, binary);
      output = finalScores ? target.scorer(standard, similarity, raw) : traversal;
    } else if (Mongot01042HnswBitVectorsFormat.NAME.equals(format)) {
      traversal = target.scorer(new FlatBitVectorsScorer(), similarity, raw);
      output = traversal;
    } else if ("Lucene99HnswScalarQuantizedVectorsFormat".equals(format)
        && encoding == VectorEncoding.FLOAT32) {
      var quantized = ((QuantizedVectorsReader) reader).getQuantizedVectorValues(field);
      traversal =
          target.scorer(
              FlatVectorScorerUtil.getLucene99ScalarQuantizedVectorsScorer(),
              similarity,
              quantized);
      output = finalScores ? target.scorer(standard, similarity, raw) : traversal;
    } else if ("Lucene99HnswVectorsFormat".equals(format)
        || "Lucene99HnswScalarQuantizedVectorsFormat".equals(format)) {
      traversal = target.scorer(standard, similarity, raw);
      output = traversal;
    } else {
      throw new IllegalArgumentException("Unsupported streaming vector format: " + format);
    }
    // Some tiny HNSW segments intentionally store no graph.
    var graph = provider.getGraph(field);
    return new StreamingVectors(graph, raw, traversal, output);
  }
}
