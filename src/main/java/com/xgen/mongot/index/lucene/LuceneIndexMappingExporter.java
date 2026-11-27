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
import com.xgen.mongot.index.definition.VectorDimensions;
import com.xgen.mongot.index.definition.VectorDimensions.SourcePaths;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.MappingObservations;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;

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
        exporter.observe(field, Optional.empty(), false);
      }
    }
    return exporter.mappings();
  }

  private BsonDocument mappings() {
    return exportDocument(
        this.resolver.indexDefinition.getMappings(),
        new BsonDocument(),
        this.scopes.getOrDefault(Optional.empty(), new ObservedField()),
        Optional.empty());
  }

  /** Each set of fields and BSON source segments comes from the same pinned partition reader. */
  public static BsonDocument exportSnapshot(
      SearchIndexDefinition definition, IndexFormatVersion version, List<DirectoryReader> readers)
      throws IOException {
    var exporter = new LuceneIndexMappingExporter(definition, version, new HashSet<>());
    for (var reader : readers) {
      exporter.vectorSourcePaths.clear();
      exporter.legacyPaths.clear();
      var checkpoint = reader.getIndexCommit().getUserData();
      checkpoint.forEach(
          (key, value) -> {
            if (key.startsWith(VectorDimensions.PATH_PREFIX)) {
              String shape = key.substring(VectorDimensions.PATH_PREFIX.length());
              exporter
                  .vectorSourcePaths
                  .computeIfAbsent(shape.replace('\0', '.'), unused -> new ArrayList<>())
                  .add(List.of(shape.split("\0", -1)));
            } else if (key.startsWith(VectorDimensions.LEGACY_PATH_PREFIX)) {
              exporter.legacyPaths.add(key.substring(VectorDimensions.LEGACY_PATH_PREFIX.length()));
            }
          });
      for (var context : reader.leaves()) {
        exporter.observeLive(context.reader());
      }
    }
    return exporter.mappings();
  }

  private void observeLive(LeafReader leaf) throws IOException {
    if (leaf.numDocs() == 0) {
      return;
    }
    var terms = leaf.terms(MappingObservations.NAME);
    if (terms != null) {
      var iterator = terms.iterator();
      while (iterator.next() != null) {
        if (iterator.term().length == 0 || !hasLivePosting(leaf, iterator)) {
          continue;
        }
        String encoded = iterator.term().utf8ToString();
        if (encoded.equals(MappingObservations.LONG_ENTRY_MARKER)) {
          observeLongEntries(leaf);
        } else {
          observeEntry(leaf, MappingObservations.Entry.decode(encoded));
        }
      }
      // Only writers using this format populate this field, including on empty documents.
      if (terms.getDocCount() == leaf.maxDoc()) {
        return;
      }
    }

    var older = LegacyMappingObservations.forSegment(leaf);
    for (var entry : older.stored().entrySet()) {
      if (hasLiveDoc(entry.getValue().iterator(), leaf.getLiveDocs())) {
        observeEntry(leaf, entry.getKey());
      }
    }
    var legacy = older.liveUnrecorded(leaf.getLiveDocs());
    if (!legacy.scanIsEmpty()) {
      for (var field : leaf.getFieldInfos()) {
        if (FieldName.TypeField.getTypeOf(field.name)
                .flatMap(LuceneIndexMappingExporter::publicType)
                .isPresent()
            && LegacyMappingFields.contains(leaf, field, legacy)) {
          observe(field, Optional.empty(), true);
        }
      }
    }
  }

  private void observeLongEntries(LeafReader leaf) throws IOException {
    for (var field : leaf.getFieldInfos()) {
      if (!field.name.startsWith(MappingObservations.LONG_ENTRY_PREFIX)) {
        continue;
      }
      var terms = leaf.terms(field.name);
      if (terms != null) {
        var presence = terms.iterator();
        if (presence.next() != null && hasLivePosting(leaf, presence)) {
          String encoded = field.name.substring(MappingObservations.LONG_ENTRY_PREFIX.length());
          observeEntry(leaf, MappingObservations.Entry.decode(encoded));
        }
      }
    }
  }

  private void observeEntry(LeafReader leaf, MappingObservations.Entry entry) {
    observe(leaf.getFieldInfos().fieldInfo(entry.field()), Optional.of(entry.keys()), false);
  }

  private static boolean hasLivePosting(LeafReader leaf, TermsEnum term) throws IOException {
    // If more documents have the spelling than have been deleted, at least one still has it.
    return term.docFreq() > leaf.numDeletedDocs()
        || hasLiveDoc(term.postings(null, PostingsEnum.NONE), leaf.getLiveDocs());
  }

  private static boolean hasLiveDoc(DocIdSetIterator docs, @Nullable Bits live) throws IOException {
    for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
      if (live == null || live.get(doc)) {
        return true;
      }
    }
    return false;
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

  private void observe(FieldInfo field, Optional<List<String>> source, boolean liveLegacy) {
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
    if (source.isPresent()) {
      observeType(root, source.get(), publicType.get(), physicalType.get(), field);
      return;
    }
    if (publicType.get() == Type.VECTOR) {
      var shapes = this.vectorSourcePaths.get(path.toString());
      if (shapes == null
          || this.legacyPaths.contains(path.toString())
          || (liveLegacy && shapes.size() > 1)) {
        var legacyShape =
            legacyShape(path, Type.VECTOR, this.resolver.indexDefinition.getMappings());
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
      @Var
      Iterable<String> keys =
          embeddedRoot.isEmpty()
              ? path.getSegments()
              : path.getPathHierarchy().stream()
                  .filter(part -> part.isChildOf(embeddedRoot.get()))
                  .map(FieldPath::getLeaf)
                  .toList();
      if (liveLegacy) {
        var parent =
            embeddedRoot
                .<HierarchicalFieldDefinition>map(
                    scope -> this.resolver.getEmbeddedDocumentsFieldDefinition(scope).orElseThrow())
                .orElse(this.resolver.indexDefinition.getMappings());
        keys = legacyShape(FieldPath.parse(String.join(".", keys)), publicType.get(), parent);
      }
      observeType(root, keys, publicType.get(), physicalType.get(), field);
    }
  }

  public static final class AmbiguousVectorPathException extends IllegalStateException {
    private AmbiguousVectorPathException(FieldPath path, Type type) {
      super(
          "Cannot export the original BSON key segments of legacy "
              + type.name().toLowerCase(java.util.Locale.ROOT)
              + " field '"
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

  private List<String> legacyShape(FieldPath path, Type type, HierarchicalFieldDefinition parent) {
    if (path.isAtRoot()) {
      return List.of(path.getLeaf());
    }
    List<List<String>> matches = new ArrayList<>(2);
    findShapes(parent, path.toString(), type, new ArrayList<>(), matches);
    if (matches.size() != 1) {
      throw new AmbiguousVectorPathException(path, type);
    }
    return matches.getFirst();
  }

  // A dot may be a key character or a document boundary. Stop after proving ambiguity.
  private void findShapes(
      HierarchicalFieldDefinition parent,
      String remaining,
      Type type,
      List<String> prefix,
      List<List<String>> matches) {
    var dynamic = this.resolver.getDynamicFieldDefinition(parent);
    if (parent.fields().isEmpty() && dynamic.isEmpty()) {
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
        if (field
            .get()
            .getAllDefinitions()
            .flatMap(Optional::stream)
            .anyMatch(def -> def.getType() == type)) {
          matches.add(List.copyOf(prefix));
        }
      } else if (field.get().documentFieldDefinition().isPresent()) {
        findShapes(
            field.get().documentFieldDefinition().get(),
            remaining.substring(end + 1),
            type,
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
