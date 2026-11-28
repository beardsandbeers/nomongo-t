package com.xgen.mongot.index.lucene.writer;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorDimensions;
import com.xgen.mongot.index.definition.VectorDimensions.Binding;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;

/** Coordinates discovery across writers and checkpoints the bindings used by this partition. */
final class TypeSetVectorWriter {
  private static final FluentLogger LOG = FluentLogger.forEnclosingClass();
  private static final String COMMIT_PREFIX = "typeSetVectorDimensions/";
  // A bare integer denotes a checkpoint written with the original physical field names.
  private static final String DIMENSIONED = "v2:";
  private final VectorDimensions shared;
  private final ConcurrentHashMap<String, Binding> local = new ConcurrentHashMap<>();

  private final Analyzer analyzer;
  // A legacy physical name is reusable only on the partition and encoding that established it.
  private final Set<String> legacyFields = new HashSet<>();

  private TypeSetVectorWriter(VectorDimensions shared, Analyzer analyzer) {
    this.analyzer = analyzer;
    this.shared = shared;
  }

  static Optional<TypeSetVectorWriter> open(
      SearchFieldDefinitionResolver resolver, IndexWriter writer) throws IOException {
    if (!resolver.hasTypeSetVectorFields()) {
      return Optional.empty();
    }
    var partition =
        new TypeSetVectorWriter(resolver.vectorDimensions(), writer.getConfig().getAnalyzer());
    var commitData = writer.getLiveCommitData();
    if (commitData != null) {
      for (var entry : commitData) {
        if (entry.getKey().startsWith(COMMIT_PREFIX)) {
          String path = entry.getKey().substring(COMMIT_PREFIX.length());
          try {
            String value = entry.getValue();
            boolean dimensionInFieldName = value.startsWith(DIMENSIONED);
            int dimensions =
                Integer.parseInt(
                    dimensionInFieldName ? value.substring(DIMENSIONED.length()) : value);
            if (dimensions < 1 || dimensions > VectorFieldSpecification.MAX_DIMENSIONS) {
              throw new IllegalArgumentException("Invalid dimensions: " + dimensions);
            }
            var binding = new Binding(dimensions, dimensionInFieldName);
            resolver.vectorDimensions().record(path, binding);
            partition.local.put(path, binding);
          } catch (IllegalArgumentException | IllegalStateException e) {
            throw new IOException("Invalid committed vector dimensions for " + path, e);
          }
        }
      }
    }
    if (partition.local.values().stream().anyMatch(binding -> !binding.dimensionInFieldName())
        && DirectoryReader.indexExists(writer.getDirectory())) {
      try (var reader = DirectoryReader.open(writer.getDirectory())) {
        partition.restorePhysicalFields(FieldInfos.getMergedFieldInfos(reader));
      }
    }
    return Optional.of(partition);
  }

  private void restorePhysicalFields(FieldInfos fields) {
    for (var field : fields) {
      if (field.getVectorDimension() == 0) {
        continue;
      }
      var type = FieldName.TypeField.getTypeOf(field.name);
      if (type.isEmpty()) {
        continue;
      }
      String path = type.get().stripPrefix(field.name);
      int dims = type.get().logicalVectorDimensions(field.getVectorDimension());
      var binding = this.local.get(path);
      if (binding != null
          && binding.dimensions() == dims
          && !binding.dimensionInFieldName()
          && !FieldName.TypeField.isInferredVectorField(field.name)) {
        this.legacyFields.add(field.name);
      }
    }
  }

  @FunctionalInterface
  interface Write {
    void run() throws IOException, FieldExceededLimitsException;
  }

  private record Candidate(Document doc, IndexableField field, String path, int dimensions) {}

  void write(List<Document> block, Write write) throws IOException, FieldExceededLimitsException {
    List<Candidate> candidates = new ArrayList<>();
    for (var doc : block) {
      var legacy = new ArrayList<IndexableField>();
      var fields = doc.iterator();
      while (fields.hasNext()) {
        var field = fields.next();
        if (field.fieldType().vectorDimension() == 0) {
          continue;
        }
        if (FieldName.TypeField.isInferredVectorField(field.name())) {
          var type = FieldName.TypeField.getTypeOf(field.name()).orElseThrow();
          String path = type.stripPrefix(field.name());
          int dimensions = type.logicalVectorDimensions(field.fieldType().vectorDimension());
          var binding = this.local.get(path);
          if (binding == null) {
            candidates.add(new Candidate(doc, field, path, dimensions));
            continue;
          }
          if (binding.dimensions() != dimensions) {
            fields.remove();
            logMismatch(path, binding.dimensions(), dimensions);
            continue;
          }
          if (!binding.dimensionInFieldName()) {
            String name = type.getLuceneFieldName(FieldPath.parse(path), Optional.empty());
            if (this.legacyFields.contains(name)) {
              fields.remove();
              legacy.add(withName(field, name));
            }
          }
        }
      }
      legacy.forEach(doc::add);
    }
    if (candidates.isEmpty()) {
      write.run();
      return;
    }
    prepareAnalysis(block);
    writeCandidates(candidates, write);
  }

  private static Field withName(IndexableField field, String name) {
    FieldType type = new FieldType(field.fieldType());
    type.freeze();
    return switch (field) {
      case KnnByteVectorField bytes -> new KnnByteVectorField(name, bytes.vectorValue(), type);
      case KnnFloatVectorField floats -> new KnnFloatVectorField(name, floats.vectorValue(), type);
      default -> throw new IllegalStateException("Unknown vector field " + field.name());
    };
  }

  private synchronized void writeCandidates(List<Candidate> candidates, Write write)
      throws IOException, FieldExceededLimitsException {
    // First writes also take the generation lock to choose one winner across partitions.
    synchronized (this.shared) {
      Map<String, Binding> decisions = new HashMap<>();
      for (var candidate : candidates) {
        var decision =
            decisions.computeIfAbsent(
                candidate.path(),
                path ->
                    new Binding(
                        this.shared
                            .binding(path)
                            .map(Binding::dimensions)
                            .orElse(candidate.dimensions()),
                        true));
        if (decision.dimensions() != candidate.dimensions()) {
          candidate.doc().removeField(candidate.field().name());
          logMismatch(candidate.path(), decision.dimensions(), candidate.dimensions());
        }
      }
      write.run();
      decisions.forEach(this.shared::record);
      this.local.putAll(decisions);
    }
  }

  // Lucene registers all field schemas before running analyzers. A failed first write must
  // never register its tentative physical field. Replay analyzed tokens during the atomic update.
  private void prepareAnalysis(List<Document> block) throws IOException {
    for (var document : block) {
      List<IndexableField> prepared = new ArrayList<>();
      for (var field : document) {
        if (!field.fieldType().tokenized()
            || field.fieldType().indexOptions() == IndexOptions.NONE
            || field.stringValue() == null) {
          prepared.add(field);
          continue;
        }
        try (var tokens = new CachingTokenFilter(field.tokenStream(this.analyzer, null))) {
          var term = tokens.addAttribute(TermToBytesRefAttribute.class);
          tokens.reset();
          while (tokens.incrementToken()) {
            if (term.getBytesRef().length > IndexWriter.MAX_TERM_LENGTH) {
              throw new IllegalArgumentException(
                  "Term exceeds the Lucene length limit in " + field.name());
            }
          }
          tokens.end();
          // Retain the original string for stored fields and highlighting.
          prepared.add(
              new Field(field.name(), field.stringValue(), field.fieldType()) {
                @Override
                public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
                  return tokens;
                }
              });
        }
      }
      document.clear();
      prepared.forEach(document::add);
    }
  }

  private static void logMismatch(String path, int expected, int actual) {
    LOG.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
        "Vector dimension mismatch for %s: expected %d, got %d. Skipping.", path, expected, actual);
  }

  /** Commit and first writes share the lock so metadata and vectors enter the same commit. */
  synchronized void commit(IndexWriter writer, Iterable<Map.Entry<String, String>> metadata)
      throws IOException {
    Map<String, String> combined = new HashMap<>();
    metadata.forEach(entry -> combined.put(entry.getKey(), entry.getValue()));
    this.local.forEach(
        (path, binding) ->
            combined.put(
                COMMIT_PREFIX + path,
                (binding.dimensionInFieldName() ? DIMENSIONED : "") + binding.dimensions()));
    writer.setLiveCommitData(combined.entrySet());
    writer.commit();
  }
}
