package com.xgen.mongot.index.lucene.streaming;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBinaryQuantizedVectorsFormat;
import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBitVectorsFormat;
import com.xgen.mongot.index.lucene.vector.HnswWriteParams;
import com.xgen.mongot.index.lucene.vector.NativeHnswScalarQuantizedVectorsFormat;
import com.xgen.mongot.index.query.VectorSearchQuery;
import com.xgen.mongot.index.query.operators.ApproximateVectorSearchCriteria;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.VectorUtil;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StreamingQuantizationTest {
  enum Format {
    FLOAT32,
    INT8,
    BIT,
    SCALAR_FLOAT32,
    BINARY_FLOAT32,
    BINARY_INT8
  }

  @Parameterized.Parameters(name = "{0}/{1}")
  public static List<Object[]> formats() {
    var cases = new ArrayList<Object[]>();
    for (Format format : Format.values()) {
      for (var metric :
          List.of(
              VectorSimilarityFunction.EUCLIDEAN,
              VectorSimilarityFunction.COSINE,
              VectorSimilarityFunction.DOT_PRODUCT)) {
        if (format != Format.BIT || metric == VectorSimilarityFunction.EUCLIDEAN) {
          cases.add(new Object[] {format, metric});
        }
      }
    }
    return cases;
  }

  @Parameterized.Parameter public Format format;

  @Parameterized.Parameter(1)
  public VectorSimilarityFunction metric;

  private KnnVectorsFormat vectorsFormat() {
    var params = new HnswWriteParams(0, Lucene99HnswVectorsFormat.VERSION_START);
    return switch (this.format) {
      case FLOAT32, INT8 -> new Lucene99HnswVectorsFormat(16, 100, 0);
      case BIT -> new Mongot01042HnswBitVectorsFormat(16, 100, 1, Optional.empty(), params);
      case SCALAR_FLOAT32 ->
          new NativeHnswScalarQuantizedVectorsFormat(16, 100, 1, Optional.empty(), params);
      case BINARY_FLOAT32, BINARY_INT8 ->
          new Mongot01042HnswBinaryQuantizedVectorsFormat(16, 100, 1, Optional.empty(), params);
    };
  }

  @Test
  public void stream_quantizedOrRawGraph_usesCorrectRetainedScoreAndStageCandidates()
      throws Exception {
    boolean bytes =
        this.format == Format.INT8
            || this.format == Format.BIT
            || this.format == Format.BINARY_INT8;
    var vectorFormat = vectorsFormat();
    var codec =
        new Lucene104Codec() {
          @Override
          public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return vectorFormat;
          }
        };
    var random = new Random(917);
    byte[][] b = new byte[128][32];
    float[][] f = new float[128][32];
    try (var directory = new ByteBuffersDirectory()) {
      try (var writer = new IndexWriter(directory, new IndexWriterConfig().setCodec(codec))) {
        for (int i = 0; i < f.length; i++) {
          random.nextBytes(b[i]);
          for (int j = 0; j < f[i].length; j++) {
            f[i][j] = b[i][j] / 128f;
          }
          if (this.metric == VectorSimilarityFunction.DOT_PRODUCT) {
            VectorUtil.l2normalize(f[i]);
          }
          var doc = new Document();
          doc.add(
              bytes
                  ? new KnnByteVectorField("v", b[i], this.metric)
                  : new KnnFloatVectorField("v", f[i], this.metric));
          writer.addDocument(doc);
        }
      }
      var criteria =
          (ApproximateVectorSearchCriteria)
              VectorSearchQuery.fromBson(
                      BsonDocument.parse(
                          "{path:'v',index:'idx',limit:4,numCandidates:32,queryVector:[1.0],"
                              + "stream:{initialScoreBudget:96,lookaheadHits:8}}"))
                  .criteria();
      var options = criteria.streaming().orElseThrow();
      try (var reader = DirectoryReader.open(directory)) {
        var searcher = new IndexSearcher(reader);
        try (var c =
            bytes
                ? RankedVectorCursor.open(searcher, "v", b[54], null, options)
                : RankedVectorCursor.open(searcher, "v", f[54], null, options)) {
          assertThat(c.cachedNodes()).isAtMost(96);
          var segment = c.explanation(true, false).getArray("segments").get(0).asDocument();
          String policy =
              switch (this.format) {
                case BINARY_FLOAT32, BINARY_INT8, SCALAR_FLOAT32 -> "retainedOnAdmission";
                default -> "none";
              };
          assertThat(segment.getString("stageRescoring").getValue()).isEqualTo(policy);
          var batch = c.poll(4, 32);
          assertThat(batch.hits()).hasSize(4);
          for (var hit : batch.hits()) {
            float expected =
                this.format == Format.BIT
                    ? 1f - VectorUtil.xorBitCount(b[54], b[hit.doc()]) / 256f
                    : bytes
                        ? this.metric.compare(b[54], b[hit.doc()])
                        : this.metric.compare(f[54], f[hit.doc()]);
            assertThat(hit.score()).isWithin(1e-6f).of(expected);
          }
        }
        var query =
            bytes
                ? new StreamingKnnQuery.Bytes("v", b[54], 32, null, options)
                : new StreamingKnnQuery.Floats("v", f[54], 32, null, options);
        assertThat(searcher.search(query, 32).scoreDocs).hasLength(32);
      }
    }
  }
}
