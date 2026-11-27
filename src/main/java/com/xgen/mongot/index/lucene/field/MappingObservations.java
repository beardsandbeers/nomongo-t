package com.xgen.mongot.index.lucene.field;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.FieldPath;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;

/** Exact BSON key segments for the typed fields in one successful Lucene document. */
public final class MappingObservations extends StoredField {
  /** Transient field-type attribute carrying BSON key segments into the partition writer. */
  public static final String VECTOR_SOURCE_PATH = "mongot.vectorSourcePath";

  public static final String NAME = "$meta/mappingObservationsV1";

  public record Entry(String field, String segments) {
    public List<String> keys() {
      return List.of(this.segments.split("\0", -1));
    }
  }

  private final Set<Entry> entries = new HashSet<>();

  public MappingObservations() {
    super(NAME, new byte[0]);
  }

  public void recordScalarFields(Document document, int start, FieldPath path, int scopeDepth) {
    var fields = document.getFields();
    if (start == fields.size()) {
      return;
    }
    var parts = path.getPathHierarchy();
    String segments =
        String.join(
            "\0",
            parts.subList(scopeDepth, parts.size()).stream().map(FieldPath::getLeaf).toList());
    for (int i = start; i < fields.size(); ++i) {
      String name = fields.get(i).name();
      if (FieldName.TypeField.getTypeOf(name).isPresent()) {
        this.entries.add(new Entry(name, segments));
      }
    }
  }

  /** Called after dimension selection, including conversion to legacy physical vector names. */
  public void finish(Document document) throws IOException {
    for (var field : document) {
      if (field.fieldType().vectorDimension() == 0) {
        continue;
      }
      var type = FieldName.TypeField.getTypeOf(field.name()).orElseThrow();
      var attributes = field.fieldType().getAttributes();
      @Var String segments = attributes == null ? null : attributes.get(VECTOR_SOURCE_PATH);
      if (segments == null) {
        // Explicit vector mappings are exported from the definition. Undotted dynamic keys are
        // unambiguous.
        String path = type.stripPrefix(field.name());
        if (path.contains(".")) {
          continue;
        }
        segments = path;
      }
      this.entries.add(new Entry(field.name(), segments));
    }
    if (!this.entries.isEmpty()) {
      var bytes = new ByteArrayOutputStream();
      try (var out = new OutputStreamDataOutput(bytes)) {
        for (var entry : this.entries) {
          out.writeString(entry.field());
          out.writeString(entry.segments());
        }
      }
      setBytesValue(bytes.toByteArray());
    }
  }

  public static void read(BytesRef value, Set<Entry> entries) throws IOException {
    var data = new ByteArrayDataInput(value.bytes, value.offset, value.length);
    while (!data.eof()) {
      entries.add(new Entry(data.readString(), data.readString()));
    }
  }
}
