package com.xgen.mongot.index.lucene.field;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;

/** Exact BSON key segments for the typed fields in one successful Lucene document. */
public final class MappingObservations extends StoredField {
  /** Transient field-type attribute carrying BSON key segments into the partition writer. */
  public static final String VECTOR_SOURCE_PATH = "mongot.vectorSourcePath";

  public static final String STORED_V1 = "$meta/mappingObservationsV1";
  public static final String NAME = "$meta/mappingObservationsV2";
  public static final String LONG_ENTRY_PREFIX = NAME + "/";
  // Ordinary entry terms start with a decimal length.
  public static final String LONG_ENTRY_MARKER = "long";

  public static boolean isInternalField(String field) {
    return field.equals(STORED_V1) || field.equals(NAME) || field.startsWith(LONG_ENTRY_PREFIX);
  }

  public record Entry(String field, String segments) {
    private String encode() {
      return this.field.length() + ":" + this.field + this.segments;
    }

    public static Entry decode(String encoded) {
      int separator = encoded.indexOf(':');
      int end = separator + 1 + Integer.parseInt(encoded, 0, separator, 10);
      return new Entry(encoded.substring(separator + 1, end), encoded.substring(end));
    }

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
  public void finish(Document document) {
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
    // The empty term marks every document written in this format, even if it has no entries.
    document.removeField(NAME);
    document.add(new StringField(NAME, new BytesRef(), Field.Store.NO));
    for (var entry : this.entries) {
      String encoded = entry.encode();
      var term = new BytesRef(encoded);
      if (term.length <= IndexWriter.MAX_TERM_LENGTH) {
        document.add(new StringField(NAME, term, Field.Store.NO));
      } else {
        // Lucene caps term bytes. Field names can carry a longer spelling without truncation.
        document.add(new StringField(NAME, LONG_ENTRY_MARKER, Field.Store.NO));
        document.add(new StringField(LONG_ENTRY_PREFIX + encoded, new BytesRef(), Field.Store.NO));
      }
    }
  }

  public static void read(BytesRef value, Set<Entry> entries) throws IOException {
    var data = new ByteArrayDataInput(value.bytes, value.offset, value.length);
    while (!data.eof()) {
      entries.add(new Entry(data.readString(), data.readString()));
    }
  }
}
