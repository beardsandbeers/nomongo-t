package com.xgen.mongot.index.lucene;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition.Type;
import com.xgen.mongot.index.definition.HierarchicalFieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.TypeSetVectorFieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/** Builds static public mappings from configured mappings and materialized Lucene field types. */
public final class LuceneIndexMappingExporter {
  private final SearchFieldDefinitionResolver resolver;
  private final Map<String, String> dottedMappingKeys = new HashMap<>();
  private final Map<Optional<FieldPath>, ObservedField> scopes = new HashMap<>();

  private static final class ObservedField {
    private Optional<Integer> vectorDimensions = Optional.empty();
    private final EnumSet<Type> types = EnumSet.noneOf(Type.class);
    private final Map<String, ObservedField> children = new TreeMap<>();
  }

  private LuceneIndexMappingExporter(
      SearchIndexDefinition definition, IndexFormatVersion formatVersion) {
    this.resolver = definition.createFieldDefinitionResolver(formatVersion);
    for (var path : definition.getFieldHierarchyContext().getRootFields().keySet()) {
      if (path.getLeaf().contains(FieldPath.DELIMITER)) {
        this.dottedMappingKeys.put(path.toString(), path.getLeaf());
      }
    }
  }

  /**
   * Merges metadata from every partition, retaining all explicit definitions and their options.
   * Dynamic fields use the configured dynamic defaults/type set, restricted to observed types. All
   * document levels are made static. Internal fields and physical numeric/date variants are omitted
   * or collapsed to their public mapping types.
   *
   * <p>This is a reader snapshot, not a historical schema: deleted fields can remain until segments
   * merge, and values that never materialized a typed Lucene field cannot be inferred.
   */
  public static BsonDocument export(
      SearchIndexDefinition definition,
      IndexFormatVersion formatVersion,
      List<FieldInfos> fieldInfos) {
    var exporter = new LuceneIndexMappingExporter(definition, formatVersion);
    for (var partition : fieldInfos) {
      for (var field : partition) {
        exporter.observe(field);
      }
    }
    return exporter.exportDocument(
        definition.getMappings(),
        new BsonDocument(),
        exporter.scopes.getOrDefault(Optional.empty(), new ObservedField()),
        Optional.empty());
  }

  /**
   * Lucene can retain the FieldInfo of a rejected document in a segment with other live documents.
   */
  static FieldInfos publishedFields(FieldInfos fields, SearchFieldDefinitionResolver resolver) {
    if (!fields.hasVectorValues() || resolver.indexDefinition.getTypeSetsMap().isEmpty()) {
      return fields;
    }
    Predicate<FieldInfo> published =
        field -> {
          if (field.getVectorDimension() == 0) {
            return true;
          }
          String path = FieldName.stripAnyPrefixFromLuceneFieldName(field.name);
          boolean qualified = FieldName.TypeField.isInferredVectorField(field.name);
          var binding = resolver.vectorDimensions().binding(path);
          if (binding.isEmpty()) {
            // A failed discovery by a writer using the older naming scheme can also leave metadata.
            return !qualified
                && resolver
                    .getFieldDefinition(FieldPath.parse(path), Optional.empty())
                    .flatMap(FieldDefinition::typeSetVectorFieldDefinition)
                    .filter(rule -> rule.numDimensions().isEmpty())
                    .isEmpty();
          }
          return binding.get().dimensionInFieldName() == qualified
              && binding.get().dimensions()
                  == FieldName.TypeField.getTypeOf(field.name)
                      .orElseThrow()
                      .logicalVectorDimensions(field.getVectorDimension());
        };
    for (var field : fields) {
      if (!published.test(field)) {
        return new FieldInfos(
            StreamSupport.stream(fields.spliterator(), false)
                .filter(published)
                .toArray(FieldInfo[]::new));
      }
    }
    return fields;
  }

  private void observe(FieldInfo field) {
    String name = field.name;
    var physicalType = FieldName.TypeField.getTypeOf(name);
    if (physicalType.isEmpty()) {
      return;
    }
    var publicType = publicType(physicalType.get());
    if (publicType.isEmpty()) {
      return;
    }
    Optional<FieldPath> embeddedRoot =
        FieldName.EmbeddedField.isTypeOf(name)
            ? Optional.of(FieldPath.parse(FieldName.getEmbeddedRootPathOrThrow(name)))
            : Optional.empty();
    var path = FieldPath.parse(physicalType.get().stripPrefix(name));
    // Static vector definitions already contain their dimensions. Two explicitly mapped BSON
    // paths can share a dotted spelling while using different physical vector encodings.
    if (publicType.get() == Type.VECTOR
        && this.resolver
            .getFieldDefinition(path, embeddedRoot)
            .flatMap(FieldDefinition::typeSetVectorFieldDefinition)
            .isEmpty()) {
      return;
    }
    @Var var observed = this.scopes.computeIfAbsent(embeddedRoot, ignored -> new ObservedField());
    for (var key : mappingKeys(path, embeddedRoot)) {
      observed = observed.children.computeIfAbsent(key, ignored -> new ObservedField());
    }
    observed.types.add(publicType.get());
    if (publicType.get() == Type.VECTOR) {
      int dimensions = physicalType.get().logicalVectorDimensions(field.getVectorDimension());
      if (observed.vectorDimensions.isPresent() && observed.vectorDimensions.get() != dimensions) {
        throw new IllegalStateException("Conflicting vector dimensions for " + path);
      }
      observed.vectorDimensions = Optional.of(dimensions);
    }
  }

  private Iterable<String> mappingKeys(FieldPath path, Optional<FieldPath> embeddedRoot) {
    if (embeddedRoot.isPresent()) {
      return path.getPathHierarchy().stream()
          .filter(part -> part.isChildOf(embeddedRoot.get()))
          .map(FieldPath::getLeaf)
          .toList();
    }
    if (this.dottedMappingKeys.isEmpty()) {
      return path.getSegments();
    }
    // Recover declared literal keys from the flattened Lucene path (e.g. "a.b" then "v").
    var keys = new ArrayDeque<String>();
    @Var String prefix = path.toString();
    while (true) {
      String key =
          this.dottedMappingKeys.getOrDefault(
              prefix, prefix.substring(prefix.lastIndexOf('.') + 1));
      keys.addFirst(key);
      if (prefix.length() == key.length()) {
        return keys;
      }
      prefix = prefix.substring(0, prefix.length() - key.length() - 1);
    }
  }

  private BsonDocument exportDocument(
      HierarchicalFieldDefinition definition,
      BsonDocument result,
      ObservedField observed,
      Optional<FieldPath> parent) {
    var fields = new BsonDocument();
    var names = new TreeSet<>(definition.fields().keySet());
    names.addAll(observed.children.keySet());
    for (String name : names) {
      var configured = definition.getField(name);
      var effective = configured.or(() -> this.resolver.getDynamicFieldDefinition(definition));
      if (effective.isEmpty()) {
        continue;
      }
      var path = parent.map(value -> value.newChild(name)).orElseGet(() -> FieldPath.newRoot(name));
      var child = observed.children.getOrDefault(name, new ObservedField());
      var mapping = exportField(effective.get(), configured.isPresent(), child, path);
      if (!mapping.isEmpty()) {
        fields.put(name, mapping.size() == 1 ? mapping.getFirst() : new BsonArray(mapping));
      }
    }
    result.put("dynamic", BsonBoolean.FALSE);
    result.put("fields", fields);
    return result;
  }

  private List<BsonValue> exportField(
      FieldDefinition definition, boolean configured, ObservedField observed, FieldPath path) {
    var result = new ArrayList<BsonValue>();
    for (var typeDefinition : definition.getAllDefinitions().flatMap(Optional::stream).toList()) {
      var type = typeDefinition.getType();
      if (!configured
          && !observed.types.contains(type)
          && !(type == Type.DOCUMENT && !observed.children.isEmpty())) {
        continue;
      }
      BsonDocument mapping = typeDefinition.toBson();
      switch (typeDefinition) {
        case TypeSetVectorFieldDefinition vector ->
            mapping.putAll(vector.resolve(() -> observed.vectorDimensions).orElseThrow().toBson());
        case DocumentFieldDefinition document ->
            exportDocument(document, mapping, observed, Optional.of(path));
        case EmbeddedDocumentsFieldDefinition embedded ->
            exportDocument(
                embedded,
                mapping,
                this.scopes.getOrDefault(Optional.of(path), new ObservedField()),
                Optional.of(path));
        default -> {
          // All scalar options, including string multi fields, are already in toBson().
        }
      }
      result.add(mapping);
    }
    return result;
  }

  private static Optional<FieldTypeDefinition.Type> publicType(FieldName.TypeField type) {
    return switch (type) {
      case AUTOCOMPLETE -> Optional.of(Type.AUTOCOMPLETE);
      case BOOLEAN -> Optional.of(Type.BOOLEAN);
      case DATE, DATE_V2, DATE_MULTIPLE -> Optional.of(Type.DATE);
      case NUMBER_DOUBLE,
          NUMBER_DOUBLE_V2,
          NUMBER_DOUBLE_MULTIPLE,
          NUMBER_INT64,
          NUMBER_INT64_V2,
          NUMBER_INT64_MULTIPLE ->
          Optional.of(Type.NUMBER);
      case GEO_POINT, GEO_SHAPE -> Optional.of(Type.GEO);
      case OBJECT_ID -> Optional.of(Type.OBJECT_ID);
      case STRING -> Optional.of(Type.STRING);
      case TOKEN -> Optional.of(Type.TOKEN);
      case UUID -> Optional.of(Type.UUID);
      case KNN_VECTOR, KNN_BYTE, KNN_BIT, KNN_F32_Q7, KNN_F32_Q1, KNN_BYTE_Q1 ->
          Optional.of(Type.VECTOR);
      // These types are only explicitly mapped (and are retained from the definition), or have
      // no public mapping type (null). Never infer an internal storage field as a new mapping.
      case DATE_FACET,
          NUMBER_DOUBLE_FACET,
          NUMBER_INT64_FACET,
          SORTABLE_DATE_BETA_V1,
          SORTABLE_NUMBER_BETA_V1,
          SORTABLE_STRING_BETA_V1,
          NULL ->
          Optional.empty();
    };
  }
}
