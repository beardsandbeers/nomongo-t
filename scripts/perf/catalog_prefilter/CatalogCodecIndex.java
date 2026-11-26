import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBinaryQuantizedVectorsFormat;
import com.xgen.mongot.index.lucene.vector.HnswWriteParams;
import com.xgen.mongot.index.lucene.vector.NativeHnswScalarQuantizedVectorsFormat;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;

/** Same catalog rows and HNSW construction settings for each scoring representation. */
public class CatalogCodecIndex {
  static KnnVectorsFormat format(String kind, int threshold, int workers, ExecutorService pool) {
    var params = new HnswWriteParams(threshold, Lucene99HnswVectorsFormat.VERSION_START);
    KnnVectorsFormat format =
        switch (kind) {
          case "f32", "i8" ->
              new Lucene99HnswVectorsFormat(
                  16, 100, workers, pool, threshold, Lucene99HnswVectorsFormat.VERSION_START);
          case "scalar" ->
              new NativeHnswScalarQuantizedVectorsFormat(
                  16, 100, workers, Optional.ofNullable(pool), params);
          case "f32-binary", "i8-binary" ->
              new Mongot01042HnswBinaryQuantizedVectorsFormat(
                  16, 100, workers, Optional.ofNullable(pool), params);
          default -> throw new IllegalArgumentException("Unknown codec " + kind);
        };
    // Lift Lucene's upstream dimension check for the 4,154-coordinate catalog fixture.
    return new KnnVectorsFormat(format.getName()) {
      @Override
      public int getMaxDimensions(String field) {
        return CatalogPrefilter.DIMS;
      }

      @Override
      public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return format.fieldsWriter(state);
      }

      @Override
      public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return format.fieldsReader(state);
      }
    };
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4 && args.length != 5)
      throw new IllegalArgumentException("codec inputs staging index [docs-per-leaf]");
    String kind = args[0];
    CatalogPrefilter.build(
        Path.of(args[1]),
        Path.of(args[2]),
        Path.of(args[3]),
        args.length == 5 ? Integer.parseInt(args[4]) : CatalogPrefilter.DOCS,
        kind.startsWith("i8") ? VectorEncoding.BYTE : VectorEncoding.FLOAT32,
        (threshold, workers, pool) -> format(kind, threshold, workers, pool));
  }
}
