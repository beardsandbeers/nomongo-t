package com.xgen.mongot.index.analyzer.wrapper;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.definition.DynamicDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

/** Configuration selected for each physical field by index-time and query-time analysis. */
public final class AnalyzerMapping {
  private final ImmutableMap<String, FieldTypeDefinition> fields;
  private final Optional<DynamicTypeSetPrefixMap<FieldTypeDefinition>> prefixes;

  public AnalyzerMapping(SearchIndexDefinition index) {
    this.fields = staticDefinitions(index);
    var dynamic = dynamicDefinitions(index);
    this.prefixes =
        dynamic.isEmpty() ? Optional.empty() : Optional.of(new DynamicTypeSetPrefixMap<>(dynamic));
  }

  public Optional<FieldTypeDefinition> resolve(String physicalName) {
    return Optional.ofNullable(this.fields.get(physicalName))
        .or(() -> this.prefixes.flatMap(prefix -> prefix.getNearestRoot(physicalName)));
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
    visit(index, (path, root, field) -> add(fields, path, root, field));
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
      add(fields, path, root, index.getTypeSetsMap().get(document.typeSet()).getFieldDefinition());
    }
  }

  private static void add(
      ImmutableMap.Builder<String, FieldTypeDefinition> fields,
      FieldPath path,
      Optional<FieldPath> root,
      FieldDefinition field) {
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
              string
                  .multi()
                  .forEach(
                      (name, multi) ->
                          fields.put(
                              FieldName.MultiField.getLuceneFieldName(path, name, root), multi));
            });
  }
}
