package com.xgen.mongot.index.lucene;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.analyzer.wrapper.AnalyzerMapping;
import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition.Type;
import com.xgen.mongot.index.definition.HierarchicalFieldDefinition;
import com.xgen.mongot.index.definition.SearchFieldDefinitionResolver;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.TokenFieldDefinition;
import com.xgen.mongot.index.definition.TypeSetVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorDimensions.SourcePaths;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.field.MappingObservations;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;

/** Builds static public mappings from configured mappings and materialized Lucene field types. */
public final class LuceneIndexMappingExporter {
  private final SearchFieldDefinitionResolver resolver;
  private final AnalyzerMapping analyzers;
  private final Map<String, List<List<String>>> vectorSourcePaths = new HashMap<>();
  private final Set<String> legacyPaths;
  private final Map<List<String>, ObservedField> scopes = new HashMap<>();

  private static final class ObservedField {
    private boolean present;
    private Optional<Integer> vectorDimensions = Optional.empty();
    private final EnumSet<Type> types = EnumSet.noneOf(Type.class);
    private final Map<String, ObservedField> children = new TreeMap<>();
  }

  private LuceneIndexMappingExporter(
      SearchIndexDefinition definition, IndexFormatVersion formatVersion, Set<String> legacyPaths) {
    this.analyzers = new AnalyzerMapping(definition);
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
        exporter.observe(field, Optional.empty());
      }
    }
    return exporter.mappings();
  }

  private BsonDocument mappings() {
    return exportDocument(
        this.resolver.indexDefinition.getMappings(),
        new BsonDocument(),
        this.scopes.getOrDefault(List.of(), new ObservedField()),
        Optional.empty(),
        Optional.empty());
  }

  /** Each set of fields and BSON source segments comes from the same pinned partition reader. */
  public static BsonDocument exportSnapshot(
      SearchIndexDefinition definition, IndexFormatVersion version, List<DirectoryReader> readers)
      throws IOException {
    var exporter = new LuceneIndexMappingExporter(definition, version, Set.of());
    var olderLeaves = new ArrayList<LeafReader>();
    for (var reader : readers) {
      for (var context : reader.leaves()) {
        var leaf = context.reader();
        if (exporter.observeAndHasOlderDocuments(leaf)) {
          olderLeaves.add(leaf);
        }
      }
    }
    // All partitions must contribute exact evidence before checking an older segment.
    for (var leaf : olderLeaves) exporter.verifyOlderSegment(leaf);
    return exporter.mappings();
  }

  private boolean observeAndHasOlderDocuments(LeafReader leaf) throws IOException {
    if (leaf.numDocs() == 0) {
      return false;
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
      if (terms.getDocCount() == leaf.maxDoc()) return false;
      if (terms.getDocCount() < leaf.numDocs()) return true;
      // Current documents carry the empty term. Check gaps between runs only when
      // deletions could account for every older document in this segment.
      var current = leaf.postings(new Term(MappingObservations.NAME, new BytesRef()));
      var live = leaf.getLiveDocs();
      for (int doc = 0; doc < leaf.maxDoc(); ) {
        int next = current.advance(doc);
        while (doc < next && doc < leaf.maxDoc()) {
          if (live == null || live.get(doc)) {
            return true;
          }
          doc++;
        }
        if (next == DocIdSetIterator.NO_MORE_DOCS) break;
        doc = current.docIDRunEnd();
      }
      return false;
    }
    return true;
  }

  // FieldInfos bound the possible types. FIELD_NAMES contains paths, even for an empty
  // array or a skipped vector. Neither lookup requires stored documents or a value dictionary.
  private void verifyOlderSegment(LeafReader leaf) throws IOException {
    var definition = this.resolver.indexDefinition;
    for (var field : leaf.getFieldInfos()) {
      var physical = FieldName.TypeField.getTypeOf(field.name);
      if (physical.isEmpty()) continue;
      var type = publicType(physical.get());
      if (type.isEmpty() && physical.get() != FieldName.TypeField.NULL) continue;
      String path = physical.get().stripPrefix(field.name);
      int dimensions = physical.get().logicalVectorDimensions(field.getVectorDimension());
      if (FieldName.EmbeddedField.isTypeOf(field.name)) {
        String root = FieldName.getEmbeddedRootPathOrThrow(field.name);
        for (var scope : definition.embeddedScopes(root)) {
          verifyCovered(
              scope.mapping(),
              this.scopes.get(scope.keys()),
              path.substring(root.length() + 1),
              type,
              dimensions,
              path);
        }
      } else {
        verifyCovered(
            definition.getMappings(), this.scopes.get(List.of()), path, type, dimensions, path);
      }
    }
    var names = leaf.terms(FieldName.MetaField.FIELD_NAMES.getLuceneFieldName());
    if (names == null) return;
    var it = names.iterator();
    while (it.next() != null) {
      String path = it.term().utf8ToString();
      verifyCovered(
          definition.getMappings(), this.scopes.get(List.of()), path, Optional.empty(), 0, path);
      // The old existence field is shared by the parent and embedded documents.
      for (int end = path.indexOf('.'); end >= 0; end = path.indexOf('.', end + 1)) {
        for (var scope : definition.embeddedScopes(path.substring(0, end))) {
          verifyCovered(
              scope.mapping(),
              this.scopes.get(scope.keys()),
              path.substring(end + 1),
              Optional.empty(),
              0,
              path);
        }
      }
    }
  }

  private void verifyCovered(
      HierarchicalFieldDefinition parent,
      @Nullable ObservedField observed,
      String remaining,
      Optional<Type> type,
      int dimensions,
      String fullPath) {
    var dynamic =
        this.resolver
            .getDynamicFieldDefinition(parent)
            .filter(field -> type.isEmpty() || field.hasAnyFieldDefinitionOf(Set.of(type.get())));
    if (parent.fields().isEmpty() && dynamic.isEmpty()) return;
    for (int end = remaining.length(); end >= 0; end = remaining.lastIndexOf('.', end - 1)) {
      String key = remaining.substring(0, end);
      var configured = parent.getField(key);
      var effective = configured.or(() -> dynamic);
      if (effective.isEmpty()) continue;
      var child = observed == null ? null : observed.children.get(key);
      if (end < remaining.length()) {
        var doc = effective.get().documentFieldDefinition();
        if (doc.isPresent())
          verifyCovered(doc.get(), child, remaining.substring(end + 1), type, dimensions, fullPath);
      } else if (configured.isEmpty()
          && (type.isEmpty() || effective.get().hasAnyFieldDefinitionOf(Set.of(type.get())))) {
        if (child == null
            || (type.isPresent() && !child.types.contains(type.get()))
            || (type.orElse(null) == Type.VECTOR
                && !child.vectorDimensions.equals(Optional.of(dimensions)))) {
          throw new AmbiguousVectorPathException(
              "Reindex the source documents before exporting mappings: older index data does not"
                  + " record every BSON spelling of '"
                  + fullPath
                  + "'.");
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
    if (entry.field().startsWith(MappingObservations.PRESENCE_PREFIX)) {
      var keys = entry.keys();
      int depth =
          Integer.parseInt(entry.field().substring(MappingObservations.PRESENCE_PREFIX.length()));
      var scope =
          this.scopes.computeIfAbsent(keys.subList(0, depth), unused -> new ObservedField());
      descend(scope, keys.subList(depth, keys.size())).present = true;
      return;
    }
    observe(leaf.getFieldInfos().fieldInfo(entry.field()), Optional.of(entry.keys()));
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

  private void observe(FieldInfo field, Optional<List<String>> source) {
    String name = field.name;
    var physicalType = FieldName.TypeField.getTypeOf(name);
    if (physicalType.isEmpty()) {
      return;
    }
    var publicType = publicType(physicalType.get());
    if (publicType.isEmpty()) {
      return;
    }
    var path = FieldPath.parse(physicalType.get().stripPrefix(name));
    if (FieldName.EmbeddedField.isTypeOf(name)) {
      observeEmbedded(field, path, physicalType.get(), publicType.get(), source);
      return;
    }
    // Static vector definitions already contain their dimensions. Two explicitly mapped BSON
    // paths can share a dotted spelling while using different physical vector encodings.
    if (publicType.get() == Type.VECTOR
        && this.resolver
            .getFieldDefinition(path, Optional.empty())
            .flatMap(FieldDefinition::typeSetVectorFieldDefinition)
            .isEmpty()) {
      return;
    }
    var root = this.scopes.computeIfAbsent(List.of(), ignored -> new ObservedField());
    if (source.isPresent()) {
      observeType(root, source.get(), publicType.get(), physicalType.get(), field);
      return;
    }
    if (publicType.get() == Type.VECTOR) {
      var shapes = this.vectorSourcePaths.get(path.toString());
      if (shapes == null || this.legacyPaths.contains(path.toString())) {
        observeLegacyRoot(field, path, physicalType.get(), publicType.get());
      }
      if (shapes != null) {
        for (var keys : shapes) {
          observeType(root, keys, publicType.get(), physicalType.get(), field);
        }
      }

    } else {
      observeType(root, path.getSegments(), publicType.get(), physicalType.get(), field);
    }
  }

  private void observeLegacyRoot(
      FieldInfo field, FieldPath path, FieldName.TypeField physical, Type type) {
    if (path.isAtRoot()) {
      observeType(
          this.scopes.computeIfAbsent(List.of(), unused -> new ObservedField()),
          path.getSegments(),
          type,
          physical,
          field);
      return;
    }
    var matches = new LegacyMatches();
    findShapes(
        this.resolver.indexDefinition.getMappings(),
        path.toString(),
        type,
        List.of(),
        new ArrayList<>(),
        matches);
    observeLegacyMatch(matches, path, type, physical, field);
  }

  private void observeEmbedded(
      FieldInfo field,
      FieldPath path,
      FieldName.TypeField physical,
      Type type,
      Optional<List<String>> source) {
    if (type == Type.VECTOR) {
      return;
    }
    String root = FieldName.getEmbeddedRootPathOrThrow(field.name);
    var scopes = this.resolver.indexDefinition.embeddedScopes(root);
    if (scopes.isEmpty()) {
      return;
    }
    if (source.isPresent()) {
      var keys = source.get();
      for (var scope : scopes) {
        int depth = scope.keys().size();
        if (keys.size() > depth && keys.subList(0, depth).equals(scope.keys())) {
          observeType(
              this.scopes.computeIfAbsent(scope.keys(), unused -> new ObservedField()),
              keys.subList(depth, keys.size()),
              type,
              physical,
              field);
          return;
        }
      }
      return;
    }
    String relative = path.toString().substring(root.length() + 1);
    if (scopes.size() == 1) {
      observeType(
          this.scopes.computeIfAbsent(scopes.getFirst().keys(), unused -> new ObservedField()),
          List.of(relative.split("\\.", -1)),
          type,
          physical,
          field);
      return;
    }
    var matches = new LegacyMatches();
    for (var scope : scopes) {
      findShapes(scope.mapping(), relative, type, scope.keys(), new ArrayList<>(), matches);
    }
    observeLegacyMatch(matches, path, type, physical, field);
  }

  public static final class AmbiguousVectorPathException extends IllegalStateException {
    private AmbiguousVectorPathException(String message) {
      super(message);
    }

    private AmbiguousVectorPathException(FieldPath path, Type type) {
      super(
          "Cannot export the original BSON key segments of legacy "
              + type.name().toLowerCase(java.util.Locale.ROOT)
              + " field '"
              + path
              + "'. Reindex the source documents to record their field paths.");
    }
  }

  private static ObservedField descend(ObservedField root, Iterable<String> keys) {
    @Var var node = root;
    for (String key : keys)
      node = node.children.computeIfAbsent(key, unused -> new ObservedField());
    return node;
  }

  private static void observeType(
      ObservedField root,
      Iterable<String> keys,
      Type publicType,
      FieldName.TypeField physicalType,
      FieldInfo field) {
    var observed = descend(root, keys);
    observed.types.add(publicType);
    if (publicType == Type.VECTOR) {
      int dimensions = physicalType.logicalVectorDimensions(field.getVectorDimension());
      if (observed.vectorDimensions.isPresent() && observed.vectorDimensions.get() != dimensions) {
        throw new IllegalStateException("Conflicting vector dimensions for " + field.name);
      }
      observed.vectorDimensions = Optional.of(dimensions);
    }
  }

  // Explicit matches are already in the exported definition. Ambiguity matters only if
  // choosing between candidates would change which dynamic fields are emitted.
  private static final class LegacyMatches {
    private boolean explicit;
    private boolean ambiguous;
    private List<String> scope = List.of();
    private Optional<List<String>> dynamicKeys = Optional.empty();

    void add(List<String> scope, List<String> keys, boolean configured) {
      if (configured) {
        this.explicit = true;
      } else {
        this.ambiguous |= this.dynamicKeys.isPresent();
        this.scope = scope;
        this.dynamicKeys = Optional.of(List.copyOf(keys));
      }
      this.ambiguous |= this.explicit && this.dynamicKeys.isPresent();
    }
  }

  private void observeLegacyMatch(
      LegacyMatches matches,
      FieldPath path,
      Type type,
      FieldName.TypeField physical,
      FieldInfo field) {
    if (matches.ambiguous || (!matches.explicit && matches.dynamicKeys.isEmpty())) {
      throw new AmbiguousVectorPathException(path, type);
    }
    matches.dynamicKeys.ifPresent(
        keys ->
            observeType(
                this.scopes.computeIfAbsent(matches.scope, unused -> new ObservedField()),
                keys,
                type,
                physical,
                field));
  }

  private void findShapes(
      HierarchicalFieldDefinition parent,
      String remaining,
      Type type,
      List<String> scope,
      List<String> prefix,
      LegacyMatches matches) {
    // An implicit document inherits the same dynamic rule at every descendant.
    var dynamic =
        this.resolver
            .getDynamicFieldDefinition(parent)
            .filter(field -> field.hasAnyFieldDefinitionOf(Set.of(type)));
    if (parent.fields().isEmpty() && dynamic.isEmpty()) {
      return;
    }
    for (int end = remaining.length();
        end >= 0 && !matches.ambiguous;
        end = remaining.lastIndexOf('.', end - 1)) {
      String key = remaining.substring(0, end);
      var explicit = parent.getField(key);
      var field = explicit.or(() -> dynamic);
      if (field.isEmpty()) {
        continue;
      }
      prefix.add(key);
      if (end == remaining.length()) {
        if (field.get().hasAnyFieldDefinitionOf(Set.of(type))) {
          matches.add(scope, prefix, explicit.isPresent());
        }
      } else if (field.get().documentFieldDefinition().isPresent()) {
        findShapes(
            field.get().documentFieldDefinition().get(),
            remaining.substring(end + 1),
            type,
            scope,
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
      Optional<FieldPath> parent,
      Optional<FieldPath> embeddedRoot) {
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
      var mapping = exportField(effective.get(), configured.isPresent(), child, path, embeddedRoot);
      if (!mapping.isEmpty()) {
        fields.put(name, mapping.size() == 1 ? mapping.getFirst() : new BsonArray(mapping));
      }
    }
    result.put("dynamic", BsonBoolean.FALSE);
    result.put("fields", fields);
    return result;
  }

  private List<BsonValue> exportField(
      FieldDefinition definition,
      boolean configured,
      ObservedField observed,
      FieldPath path,
      Optional<FieldPath> embeddedRoot) {
    var result = new ArrayList<BsonValue>();
    for (var typeDefinition : definition.getAllDefinitions().flatMap(Optional::stream).toList()) {
      var type = typeDefinition.getType();
      if (!configured
          && !observed.types.contains(type)
          && !(type == Type.DOCUMENT
              && (!observed.children.isEmpty()
                  || (observed.present && observed.types.isEmpty())))) {
        continue;
      }
      @Var BsonDocument mapping = typeDefinition.toBson();
      switch (typeDefinition) {
        case StringFieldDefinition string -> {
          fixStringAnalysis(
              mapping, FieldName.TypeField.STRING.getLuceneFieldName(path, embeddedRoot));
          for (String name : string.multi().keySet()) {
            fixStringAnalysis(
                mapping.getDocument("multi").getDocument(name),
                FieldName.MultiField.getLuceneFieldName(path, name, embeddedRoot));
          }
        }
        case TokenFieldDefinition token ->
            mapping =
                this.analyzers
                    .resolve(FieldName.TypeField.TOKEN.getLuceneFieldName(path, embeddedRoot))
                    .orElse(token)
                    .toBson();
        case AutocompleteFieldDefinition ac ->
            mapping =
                this.analyzers
                    .resolve(
                        FieldName.TypeField.AUTOCOMPLETE.getLuceneFieldName(path, embeddedRoot))
                    .orElse(ac)
                    .toBson();
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
            exportDocument(document, mapping, observed, Optional.of(path), embeddedRoot);
        case EmbeddedDocumentsFieldDefinition embedded ->
            exportDocument(
                embedded,
                mapping,
                this.scopes.getOrDefault(
                    ImmutableList.copyOf(path.getSegments()), new ObservedField()),
                Optional.of(path),
                Optional.of(path));
        default -> {
          // All scalar options, including string multi fields, are already in toBson().
        }
      }
      result.add(mapping);
    }
    return result;
  }

  private void fixStringAnalysis(BsonDocument mapping, String physicalName) {
    var selected = this.analyzers.resolve(physicalName).map(StringFieldDefinition.class::cast);
    mapping.remove("analyzer");
    mapping.remove("searchAnalyzer");
    selected
        .flatMap(StringFieldDefinition::analyzerName)
        .ifPresent(name -> mapping.put("analyzer", new BsonString(name)));
    selected
        .flatMap(StringFieldDefinition::searchAnalyzerName)
        .ifPresent(name -> mapping.put("searchAnalyzer", new BsonString(name)));
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
