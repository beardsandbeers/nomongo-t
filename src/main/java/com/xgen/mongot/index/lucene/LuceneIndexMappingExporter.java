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
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.TypeSetVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorDimensions.SourcePaths;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
  private final Map<String, List<List<String>>> vectorSourcePaths = new HashMap<>();
  private final Set<String> legacyPaths;
  private final Map<Optional<FieldPath>, ObservedField> scopes = new HashMap<>();

  private static final class ObservedField {
    private Optional<Integer> vectorDimensions = Optional.empty();
    private final EnumSet<Type> types = EnumSet.noneOf(Type.class);
    private final Map<String, ObservedField> children = new TreeMap<>();
  }

  private LuceneIndexMappingExporter(
      SearchIndexDefinition definition, IndexFormatVersion formatVersion, Set<String> legacyPaths) {
    this.resolver = definition.createFieldDefinitionResolver(formatVersion);
    this.legacyPaths = legacyPaths;
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
    return export(definition, formatVersion, fieldInfos, SourcePaths.EMPTY);
  }

  public static BsonDocument export(
      SearchIndexDefinition definition,
      IndexFormatVersion formatVersion,
      List<FieldInfos> fieldInfos,
      SourcePaths sourcePaths) {
    var exporter =
        new LuceneIndexMappingExporter(definition, formatVersion, sourcePaths.legacyPaths());
    for (String shape : sourcePaths.spellings()) {
      exporter
          .vectorSourcePaths
          .computeIfAbsent(shape.replace('\0', '.'), ignored -> new ArrayList<>())
          .add(List.of(shape.split("\0", -1)));
    }
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
          return (qualified || !binding.get().dimensionInFieldName())
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
    var root = this.scopes.computeIfAbsent(embeddedRoot, ignored -> new ObservedField());
    if (publicType.get() == Type.VECTOR) {
      var shapes = this.vectorSourcePaths.get(path.toString());
      if (shapes == null || this.legacyPaths.contains(path.toString())) {
        var legacyShape = legacyVectorShape(path);
        observeType(root, legacyShape, publicType.get(), physicalType.get(), field);
      }
      if (shapes != null) {
        for (var keys : shapes) {
          observeType(root, keys, publicType.get(), physicalType.get(), field);
        }
      }
    } else {
      // Keep scalar observations under their ordinary nested path. A literal token mapping
      // named "a.b" must not claim a string produced by the dynamic document "a".
      Iterable<String> keys =
          embeddedRoot.isEmpty()
              ? path.getSegments()
              : path.getPathHierarchy().stream()
                  .filter(part -> part.isChildOf(embeddedRoot.get()))
                  .map(FieldPath::getLeaf)
                  .toList();
      observeType(root, keys, publicType.get(), physicalType.get(), field);
    }
  }

  public static final class AmbiguousVectorPathException extends IllegalStateException {
    private AmbiguousVectorPathException(FieldPath path) {
      super(
          "Cannot export the original BSON key segments of legacy vector field '"
              + path
              + "'. Reindex the source documents to record their field paths.");
    }
  }

  private static void observeType(
      ObservedField root,
      Iterable<String> keys,
      Type publicType,
      FieldName.TypeField physicalType,
      FieldInfo field) {
    @Var var observed = root;
    for (String key : keys) {
      observed = observed.children.computeIfAbsent(key, ignored -> new ObservedField());
    }
    observed.types.add(publicType);
    if (publicType == Type.VECTOR) {
      int dimensions = physicalType.logicalVectorDimensions(field.getVectorDimension());
      if (observed.vectorDimensions.isPresent() && observed.vectorDimensions.get() != dimensions) {
        throw new IllegalStateException("Conflicting vector dimensions for " + field.name);
      }
      observed.vectorDimensions = Optional.of(dimensions);
    }
  }

  private List<String> legacyVectorShape(FieldPath path) {
    if (path.isAtRoot()) {
      return List.of(path.getLeaf());
    }
    List<List<String>> matches = new ArrayList<>(2);
    findVectorShapes(
        this.resolver.indexDefinition.getMappings(), path.toString(), new ArrayList<>(), matches);
    if (matches.size() != 1) {
      throw new AmbiguousVectorPathException(path);
    }
    return matches.getFirst();
  }

  // A dot may be a key character or a document boundary. Stop after proving ambiguity.
  private void findVectorShapes(
      DocumentFieldDefinition parent,
      String remaining,
      List<String> prefix,
      List<List<String>> matches) {
    var dynamic = this.resolver.getDynamicFieldDefinition(parent);
    if (parent.fields().isEmpty()
        && dynamic.flatMap(FieldDefinition::typeSetVectorFieldDefinition).isEmpty()) {
      return;
    }
    for (int end = remaining.length();
        end >= 0 && matches.size() < 2;
        end = remaining.lastIndexOf('.', end - 1)) {
      String key = remaining.substring(0, end);
      var field = parent.getField(key).or(() -> dynamic);
      if (field.isEmpty()) {
        continue;
      }
      prefix.add(key);
      if (end == remaining.length()) {
        if (field.get().typeSetVectorFieldDefinition().isPresent()) {
          matches.add(List.copyOf(prefix));
        }
      } else if (field.get().documentFieldDefinition().isPresent()) {
        findVectorShapes(
            field.get().documentFieldDefinition().get(),
            remaining.substring(end + 1),
            prefix,
            matches);
      }
      prefix.removeLast();
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
      @Var BsonDocument mapping = typeDefinition.toBson();
      switch (typeDefinition) {
        case TypeSetVectorFieldDefinition vector -> {
          // The queryable dotted path determines the encoding, including for literal BSON keys.
          var rule =
              this.resolver
                  .getTypeSetVectorRule(FieldPath.parse(path.toString()), Optional.empty())
                  .orElse(vector);
          var spec = rule.resolve(() -> observed.vectorDimensions).orElseThrow();
          mapping = new SearchIndexVectorFieldDefinition(spec, true).toBson();
        }
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
