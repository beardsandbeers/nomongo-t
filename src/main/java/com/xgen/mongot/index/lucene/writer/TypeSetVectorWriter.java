package com.xgen.mongot.index.lucene.writer;

import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.VectorDimensions;
import com.xgen.mongot.index.definition.VectorDimensions.Binding;
import com.xgen.mongot.index.definition.VectorFieldSpecification;
import com.xgen.mongot.index.lucene.field.FieldName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;

/** Coordinates discovery across writers and checkpoints the bindings used by this partition. */
final class TypeSetVectorWriter {
  private static final FluentLogger LOG = FluentLogger.forEnclosingClass();
  private static final String COMMIT_PREFIX = "typeSetVectorDimensions/";
  // A bare integer denotes a checkpoint written with the original physical field names.
  private static final String DIMENSIONED = "v2:";
  private final VectorDimensions shared;
  private final ConcurrentHashMap<String, Binding> local = new ConcurrentHashMap<>();

  private TypeSetVectorWriter(VectorDimensions shared) {
    this.shared = shared;
  }

  static Optional<TypeSetVectorWriter> open(
      SearchFieldDefinitionResolver resolver, org.apache.lucene.index.IndexWriter writer)
      throws IOException {
    if (resolver
        .indexDefinition
        .dynamicVectorFields()
        .noneMatch(rule -> rule.numDimensions().isEmpty())) {
      return Optional.empty();
    }
    var partition = new TypeSetVectorWriter(resolver.vectorDimensions());
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
    return Optional.of(partition);
  }

  @FunctionalInterface
  interface Write {
    void run() throws IOException, FieldExceededLimitsException;
  }

  private record Candidate(Document doc, String field, String path, Binding binding) {}

  void write(List<Document> block, Write write) throws IOException, FieldExceededLimitsException {
    List<Candidate> candidates = new ArrayList<>();
    for (var doc : block) {
      var fields = doc.iterator();
      while (fields.hasNext()) {
        var field = fields.next();
        if (field.fieldType().vectorDimension() == 0) {
          continue;
        }
        var type = FieldName.TypeField.getTypeOf(field.name());
        if (type.isEmpty()) {
          continue;
        }
        String path = type.get().stripPrefix(field.name());
        int dimensions = type.get().logicalVectorDimensions(field.fieldType().vectorDimension());
        var localBinding = this.local.get(path);
        if (localBinding != null) {
          if (localBinding.dimensions() != dimensions) {
            fields.remove();
            logMismatch(path, localBinding.dimensions(), dimensions);
          }
          continue;
        }
        boolean qualified = FieldName.TypeField.isInferredVectorField(field.name());
        // Previously committed inferred fields share the ordinary vector naming scheme.
        if (qualified || this.shared.binding(path).isPresent()) {
          candidates.add(
              new Candidate(doc, field.name(), path, new Binding(dimensions, qualified)));
        }
      }
    }
    if (candidates.isEmpty()) {
      write.run();
    } else {
      writeCandidates(candidates, write);
    }
  }

  private synchronized void writeCandidates(List<Candidate> candidates, Write write)
      throws IOException, FieldExceededLimitsException {
    // First writes also take the generation lock to choose one winner across partitions.
    synchronized (this.shared) {
      Map<String, Binding> decisions = new HashMap<>();
      for (var candidate : candidates) {
        var expected =
            this.shared
                .binding(candidate.path())
                .orElse(decisions.getOrDefault(candidate.path(), candidate.binding()));
        decisions.put(candidate.path(), expected);
        if (!expected.equals(candidate.binding())) {
          candidate.doc().removeField(candidate.field());
          logMismatch(candidate.path(), expected.dimensions(), candidate.binding().dimensions());
        }
      }
      write.run();
      decisions.forEach(this.shared::record);
      this.local.putAll(decisions);
    }
  }

  private static void logMismatch(String path, int expected, int actual) {
    LOG.atWarning().atMostEvery(1, TimeUnit.HOURS).log(
        "Vector dimension mismatch for %s: expected %d, got %d. Skipping.", path, expected, actual);
  }

  /** Commit and first writes share the lock so metadata and vectors enter the same commit. */
  synchronized void commit(
      org.apache.lucene.index.IndexWriter writer, Iterable<Map.Entry<String, String>> metadata)
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
