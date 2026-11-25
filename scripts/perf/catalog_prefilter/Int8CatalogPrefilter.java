import com.xgen.mongot.index.lucene.quantization.Mongot01042HnswBinaryQuantizedVectorsFormat;
import com.xgen.mongot.index.lucene.vector.HnswWriteParams;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;

/** Catalog benchmark using retained int8 vectors and mongot's one-bit graph scorer. */
public class Int8CatalogPrefilter {
  private static final int DOCS = 1_130_000;

  private static KnnVectorsFormat format(int threshold, int workers, ExecutorService pool) {
    return new Mongot01042HnswBinaryQuantizedVectorsFormat(
        16,
        100,
        workers,
        Optional.ofNullable(pool),
        new HnswWriteParams(threshold, Lucene99HnswVectorsFormat.VERSION_START)) {
      @Override
      public int getMaxDimensions(String field) {
        return CatalogPrefilter.DIMS;
      }
    };
  }

  private static void verifyInt8(Path index) throws IOException {
    try (var dir = FSDirectory.open(index);
        var reader = DirectoryReader.open(dir)) {
      var leaf = (SegmentReader) reader.leaves().getFirst().reader();
      var perField = (PerFieldKnnVectorsFormat.FieldsReader) leaf.getVectorReader();
      var quantized = (QuantizedVectorsReader) perField.getFieldReader("vector");
      var state = quantized.getQuantizationState("vector");
      var raw = leaf.getByteVectorValues("vector");
      if (state == null
          || state.getBits() != 1
          || raw == null
          || raw.size() != DOCS
          || raw.dimension() != CatalogPrefilter.DIMS) {
        throw new IOException("Expected one-bit graph scoring and full-width int8 vectors");
      }
      var searcher = new IndexSearcher(reader);
      var hits = searcher.search(new KnnByteVectorQuery("vector", raw.vectorValue(0), 10), 10);
      if (hits.scoreDocs.length != 10) throw new IOException("Byte-vector kNN smoke check failed");
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      throw new IllegalArgumentException(
          "Expected build <inputs> <staging> <index> or measure <index> <csv> <evict.py>");
    }
    Path first = Path.of(args[1]), second = Path.of(args[2]), third = Path.of(args[3]);
    switch (args[0]) {
      case "build" ->
          CatalogPrefilter.build(
              first, second, third, DOCS, VectorEncoding.BYTE, Int8CatalogPrefilter::format);
      case "measure" -> {
        verifyInt8(first);
        CatalogPrefilter.measure(first, second, third, DOCS, VectorEncoding.BYTE);
      }
      default -> throw new IllegalArgumentException("Use build or measure");
    }
  }
}
