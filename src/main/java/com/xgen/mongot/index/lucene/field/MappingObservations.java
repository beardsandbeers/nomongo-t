package com.xgen.mongot.index.lucene.field;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.util.FieldPath;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;

/** Exact BSON key segments for the typed fields in one successful Lucene document. */
public final class MappingObservations extends StoredField {
  /** Transient field-type attribute carrying BSON key segments into the partition writer. */
  public static final String VECTOR_SOURCE_PATH = "mongot.vectorSourcePath";

  public static final String STORED_V1 = "$meta/mappingObservationsV1";
  public static final String INDEXED_V2 = "$meta/mappingObservationsV2";
  public static final String NAME = "$meta/mappingObservationsV3";
  public static final String PRESENCE_PREFIX = "$presence/";
  public static final String LONG_ENTRY_PREFIX = NAME + "/";
  // Ordinary entry terms start with a decimal length.
  public static final String LONG_ENTRY_MARKER = "long";

  public static boolean isInternalField(String field) {
    return field.equals(STORED_V1)
        || field.equals(INDEXED_V2)
        || field.startsWith(INDEXED_V2 + "/")
        || field.equals(NAME)
        || field.startsWith(LONG_ENTRY_PREFIX);
  }

  public record Entry(String field, String segments) {
    private String encode() {
      // Original V3 writers omitted types for the empty root key. A leading zero in its
      // decimal length distinguishes complete entries and is accepted by existing decoders.
      return (hasEmptyRoot() ? "0" : "") + this.field.length() + ":" + this.field + this.segments;
    }

    public static Entry decode(String encoded) {
      int separator = encoded.indexOf(':');
      int end = separator + 1 + Integer.parseInt(encoded, 0, separator, 10);
      return new Entry(encoded.substring(separator + 1, end), encoded.substring(end));
    }

    public boolean hasEmptyRoot() {
      return this.segments.isEmpty() || this.segments.startsWith("\0");
    }

    public List<String> keys() {
      return List.of(this.segments.split("\0", -1));
    }
  }

  private final Set<Entry> entries = new HashSet<>();

  private final Set<String> presentPaths = new HashSet<>();
  private final String presenceField;

  public MappingObservations() {
    this(Optional.empty());
  }

  public MappingObservations(Optional<FieldPath> root) {
    super(NAME, new byte[0]);
    this.presenceField = PRESENCE_PREFIX + root.map(p -> p.getPathHierarchy().size()).orElse(0);
  }

  public void recordPresence(FieldPath path) {
    String segments = String.join("\0", path.getSegments());
    this.presentPaths.add(segments);
  }

  public void recordScalarFields(Document document, int start, FieldPath path) {
    var fields = document.getFields();
    if (start == fields.size()) {
      return;
    }
    String segments = String.join("\0", path.getSegments());
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
    // A typed leaf already implies its presence. Each child implies its BSON ancestors.
    var implied = new HashSet<String>();
    for (String path : this.presentPaths) {
      int split = path.lastIndexOf('\0');
      if (split >= 0) implied.add(path.substring(0, split));
    }
    this.presentPaths.removeAll(implied);
    this.entries.forEach(entry -> this.presentPaths.remove(entry.segments()));
    this.presentPaths.forEach(path -> this.entries.add(new Entry(this.presenceField, path)));
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
}
