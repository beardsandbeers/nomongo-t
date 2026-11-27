package com.xgen.mongot.index.analyzer.wrapper;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.DynamicDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SimilarityDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

/** Configuration selected for each physical field by index-time and query-time analysis. */
public final class AnalyzerMapping {
  private final ImmutableMap<String, FieldTypeDefinition> fields;
  private final Optional<DynamicTypeSetPrefixMap<FieldTypeDefinition>> prefixes;

  private final boolean useDynamicSimilarity;
  private final boolean hasSimilarity;

  public AnalyzerMapping(SearchIndexDefinition index) {
    this.fields = staticDefinitions(index);
    var dynamic = dynamicDefinitions(index);
    this.useDynamicSimilarity = index.usesTypeSetSimilarity();
    this.hasSimilarity =
        this.fields.values().stream().anyMatch(f -> similarityOf(f).isPresent())
            || (this.useDynamicSimilarity
                && dynamic.values().stream().anyMatch(f -> similarityOf(f).isPresent()));
    this.prefixes =
        dynamic.isEmpty() ? Optional.empty() : Optional.of(new DynamicTypeSetPrefixMap<>(dynamic));
  }

  public Optional<FieldTypeDefinition> resolve(String physicalName) {
    return Optional.ofNullable(this.fields.get(physicalName))
        .or(() -> this.prefixes.flatMap(prefix -> prefix.getNearestRoot(physicalName)));
  }

  public boolean hasSimilarity() {
    return this.hasSimilarity;
  }

  /** The same physical field must use the same norms and scoring model throughout a generation. */
  public Optional<SimilarityDefinition> similarity(String name) {
    if (!this.hasSimilarity
        || !(FieldName.TypeField.STRING.isTypeOf(name)
            || FieldName.MultiField.isTypeOf(name)
            || FieldName.TypeField.AUTOCOMPLETE.isTypeOf(name))) {
      return Optional.empty();
    }
    return (this.useDynamicSimilarity ? resolve(name) : Optional.ofNullable(this.fields.get(name)))
        .flatMap(AnalyzerMapping::similarityOf);
  }

  private static Optional<SimilarityDefinition> similarityOf(FieldTypeDefinition definition) {
    return switch (definition) {
      case StringFieldDefinition string -> string.similarity();
      case AutocompleteFieldDefinition ac -> ac.getSimilarity();
      default -> Optional.empty();
    };
  }

  @FunctionalInterface
  private interface Visitor {
    void field(FieldPath path, Optional<FieldPath> root, FieldDefinition definition);
  }

  private static void visit(SearchIndexDefinition index, Visitor visitor) {
    var hierarchy = index.getFieldHierarchyContext();
    hierarchy
        .getRootFields()
        .forEach((path, field) -> visitor.field(path, Optional.empty(), field));
    hierarchy
        .getFieldsByEmbeddedRoot()
        .forEach(
            (root, children) ->
                children.forEach((path, field) -> visitor.field(path, Optional.of(root), field)));
  }

  static ImmutableMap<String, FieldTypeDefinition> staticDefinitions(SearchIndexDefinition index) {
    var fields = ImmutableMap.<String, FieldTypeDefinition>builder();
    visit(
        index,
        (path, root, field) -> add(fields, path, root, field, index.usesTypeSetSimilarity()));
    return fields.buildKeepingLast();
  }

  static ImmutableMap<String, FieldTypeDefinition> dynamicDefinitions(SearchIndexDefinition index) {
    if (index.getTypeSetsMap().isEmpty()) return ImmutableMap.of();
    var fields = ImmutableMap.<String, FieldTypeDefinition>builder();
    addDynamic(
        fields, index, FieldPath.newRoot(""), Optional.empty(), index.getMappings().dynamic());
    visit(
        index,
        (path, root, field) -> {
          field
              .documentFieldDefinition()
              .ifPresent(doc -> addDynamic(fields, index, path, root, doc.dynamic()));
          field
              .embeddedDocumentsFieldDefinition()
              .ifPresent(doc -> addDynamic(fields, index, path, Optional.of(path), doc.dynamic()));
        });
    return fields.buildKeepingLast();
  }

  private static void addDynamic(
      ImmutableMap.Builder<String, FieldTypeDefinition> fields,
      SearchIndexDefinition index,
      FieldPath path,
      Optional<FieldPath> root,
      DynamicDefinition dynamic) {
    if (dynamic instanceof DynamicDefinition.Document document) {
      add(
          fields,
          path,
          root,
          index.getTypeSetsMap().get(document.typeSet()).getFieldDefinition(),
          true);
    }
  }

  private static void add(
      ImmutableMap.Builder<String, FieldTypeDefinition> fields,
      FieldPath path,
      Optional<FieldPath> root,
      FieldDefinition field,
      boolean includeQueryAliases) {
    field
        .tokenFieldDefinition()
        .ifPresent(
            token -> fields.put(FieldName.TypeField.TOKEN.getLuceneFieldName(path, root), token));
    field
        .autocompleteFieldDefinition()
        .ifPresent(
            ac -> fields.put(FieldName.TypeField.AUTOCOMPLETE.getLuceneFieldName(path, root), ac));
    field
        .stringFieldDefinition()
        .ifPresent(
            string -> {
              fields.put(FieldName.TypeField.STRING.getLuceneFieldName(path, root), string);
              (includeQueryAliases ? string.multi() : string.declaredMulti())
                  .forEach(
                      (name, multi) ->
                          fields.put(
                              FieldName.MultiField.getLuceneFieldName(path, name, root), multi));
            });
  }
}
