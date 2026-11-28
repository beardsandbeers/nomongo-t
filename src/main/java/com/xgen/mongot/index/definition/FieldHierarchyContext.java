package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.FieldPath;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link FieldHierarchyContext} aggregates information about children of a {@link
 * FieldTypeDefinition}. The {@link FieldHierarchyContext} of the {@link DocumentFieldDefinition} at
 * the root of an {@link SearchIndexDefinition} summarizes "hierarchical information" of the entire
 * index definition.
 *
 * <p>Examples of "hierarchical information" include the depth of the most deeply-layered {@link
 * EmbeddedDocumentsFieldDefinition}, and the relative {@link FieldPath}s of {@link
 * EmbeddedDocumentsFieldDefinition}s under a particular field.
 */
public class FieldHierarchyContext {
  /**
   * Maps embedded root path to {@link EmbeddedDocumentsFieldDefinition} field definitions for all
   * field definitions containing an embedded documents field.
   */
  private final ImmutableMap<FieldPath, FieldDefinition> rootFields;

  /**
   * Maps embedded root path to a map from {@link FieldPath} to {@link FieldDefinition} for all
   * statically configured fields of an {@link EmbeddedDocumentsFieldDefinition}.
   */
  private final ImmutableMap<FieldPath, ImmutableMap<FieldPath, FieldDefinition>>
      fieldsByEmbeddedRoot;

  private final ImmutableMap<FieldPath, EmbeddedDocumentsFieldDefinition>
      embeddedFieldDefinitionByEmbeddedRoot;

  /**
   * The number of embeddedDocuments "layers" of the most-deeply layered embeddedDocuments
   * descendent of this field. See {@link FieldHierarchyContext#getNumEmbeddedDocumentsLayers()} for
   * more details.
   */
  private final int numEmbeddedDocumentsLayers;

  FieldHierarchyContext(
      ImmutableMap<FieldPath, FieldDefinition> rootFields,
      ImmutableMap<FieldPath, ImmutableMap<FieldPath, FieldDefinition>> fieldsByEmbeddedRoot,
      ImmutableMap<FieldPath, EmbeddedDocumentsFieldDefinition>
          embeddedFieldDefinitionByEmbeddedRoot,
      int numEmbeddedDocumentsLayers) {
    this.rootFields = rootFields;
    this.fieldsByEmbeddedRoot = fieldsByEmbeddedRoot;
    this.embeddedFieldDefinitionByEmbeddedRoot = embeddedFieldDefinitionByEmbeddedRoot;
    this.numEmbeddedDocumentsLayers = numEmbeddedDocumentsLayers;
  }

  /**
   * An {@link EmbeddedDocumentsFieldDefinition} field may be the child of another {@link
   * EmbeddedDocumentsFieldDefinition}. An index definition with an {@link
   * EmbeddedDocumentsFieldDefinition} that itself has an {@link EmbeddedDocumentsFieldDefinition}
   * field as a child is said to have two "embeddedDocuments layers".
   *
   * <p>Atlas Search imposes limits on how many layers of {@link EmbeddedDocumentsFieldDefinition}s
   * may be in a single index definition.
   *
   * <p>{@link FieldHierarchyContext#getNumEmbeddedDocumentsLayers()} returns the number of
   * embeddedDocuments "layers" between the most-deeply layered embeddedDocuments descendent of this
   * field and this field itself. Said differently, this number represents the number of
   * embeddedDocuments layers between this field and the most-layered embeddedDocuments field that
   * is a descendent of it.
   */
  int getNumEmbeddedDocumentsLayers() {
    return this.numEmbeddedDocumentsLayers;
  }

  ImmutableSet<FieldPath> getEmbeddedDocumentsRelativeRoots() {
    return this.fieldsByEmbeddedRoot.keySet();
  }

  public ImmutableMap<FieldPath, ImmutableMap<FieldPath, FieldDefinition>>
      getFieldsByEmbeddedRoot() {
    return this.fieldsByEmbeddedRoot;
  }

  public ImmutableMap<FieldPath, EmbeddedDocumentsFieldDefinition>
      getEmbeddedFieldDefinitionByEmbeddedRoot() {
    return this.embeddedFieldDefinitionByEmbeddedRoot;
  }

  public ImmutableMap<FieldPath, FieldDefinition> getRootFields() {
    return this.rootFields;
  }

  Optional<FieldDefinition> getFieldDefinition(FieldPath fieldPath, FieldPath embeddedRoot) {
    return Optional.ofNullable(this.fieldsByEmbeddedRoot.get(embeddedRoot))
        .map(fieldMap -> fieldMap.get(fieldPath));
  }

  /**
   * Create for a {@link DocumentFieldDefinition}. Throws a {@link IllegalEmbeddedFieldException} if
   * that field has two {@link EmbeddedDocumentsFieldDefinition} children at the same path.
   */
  static FieldHierarchyContext createForDocumentsField(Map<String, FieldDefinition> fields)
      throws IllegalEmbeddedFieldException {
    return create(fields, false, true);
  }

  /**
   * Create for an {@link EmbeddedDocumentsFieldDefinition}. Throws a {@link
   * IllegalEmbeddedFieldException} if that field has two {@link EmbeddedDocumentsFieldDefinition}
   * children at the same path.
   */
  static FieldHierarchyContext createForEmbeddedDocumentsField(Map<String, FieldDefinition> fields)
      throws IllegalEmbeddedFieldException {
    return create(fields, true, true);
  }

  /** Reconstruct the whole-field, last-declaration-wins view used by older Lucene generations. */
  static FieldHierarchyContext legacy(HierarchicalFieldDefinition field) {
    try {
      return create(field.fields(), field instanceof EmbeddedDocumentsFieldDefinition, false);
    } catch (IllegalEmbeddedFieldException e) {
      throw new IllegalStateException(
          "Previously validated mapping has conflicting embedded roots", e);
    }
  }

  private static FieldHierarchyContext create(
      Map<String, FieldDefinition> fields, boolean isEmbeddedDocs, boolean mergeAliases)
      throws IllegalEmbeddedFieldException {
    Map<FieldPath, Map<FieldPath, FieldDefinition>> fieldsByEmbeddedRoot = new HashMap<>();
    Map<FieldPath, FieldDefinition> rootFields = new HashMap<>();
    Map<FieldPath, EmbeddedDocumentsFieldDefinition> embeddedFieldDefinitionByEmbeddedRoot =
        new HashMap<>();
    CheckedStream.from(fields.entrySet())
        .forEachChecked(
            field ->
                populateRelativeRoots(
                    field.getKey(),
                    field.getValue(),
                    mergeAliases,
                    fieldsByEmbeddedRoot,
                    rootFields,
                    embeddedFieldDefinitionByEmbeddedRoot));

    int numEmbeddedDocumentLayers =
        fields.values().stream()
                .mapToInt(FieldHierarchyContext::getNumEmbeddedLayers)
                .max()
                .orElse(0)
            + (isEmbeddedDocs ? 1 : 0);

    return new FieldHierarchyContext(
        ImmutableMap.copyOf(rootFields),
        fieldsByEmbeddedRoot.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue()))),
        ImmutableMap.copyOf(embeddedFieldDefinitionByEmbeddedRoot),
        numEmbeddedDocumentLayers);
  }

  private static void populateRelativeRoots(
      String leafPath,
      FieldDefinition field,
      boolean mergeAliases,
      Map<FieldPath, Map<FieldPath, FieldDefinition>> fieldsByEmbeddedRoot,
      Map<FieldPath, FieldDefinition> rootFields,
      Map<FieldPath, EmbeddedDocumentsFieldDefinition> embeddedFieldDefinitionByEmbeddedRoot)
      throws IllegalEmbeddedFieldException {
    Set<FieldPath> embeddedDocumentsRelativeRoots =
        field.embeddedDocumentsFieldDefinition().stream()
            .map(EmbeddedDocumentsFieldDefinition::fieldHierarchyContext)
            .map(FieldHierarchyContext::getEmbeddedDocumentsRelativeRoots)
            .flatMap(Collection::stream)
            .map(relativeRoot -> relativeRoot.withNewRoot(leafPath))
            .collect(Collectors.toSet());
    Set<FieldPath> documentRelativeRoots =
        field.documentFieldDefinition().stream()
            .map(DocumentFieldDefinition::fieldHierarchyContext)
            .map(FieldHierarchyContext::getEmbeddedDocumentsRelativeRoots)
            .flatMap(Collection::stream)
            .map(relativeRoot -> relativeRoot.withNewRoot(leafPath))
            .collect(Collectors.toSet());

    Set<FieldPath> conflictingRelativeRoots =
        Sets.intersection(embeddedDocumentsRelativeRoots, documentRelativeRoots);

    if (!conflictingRelativeRoots.isEmpty()) {
      throw IllegalEmbeddedFieldException.withConflictingRelativeRoots(conflictingRelativeRoots);
    }

    var embedded =
        field
            .embeddedDocumentsFieldDefinition()
            .map(doc -> mergeAliases ? doc.fieldHierarchyContext() : legacy(doc));
    var document =
        field
            .documentFieldDefinition()
            .map(doc -> mergeAliases ? doc.fieldHierarchyContext() : legacy(doc));
    embedded.ifPresent(
        context ->
            context.prependEmbeddedFields(
                leafPath,
                fieldsByEmbeddedRoot,
                embeddedFieldDefinitionByEmbeddedRoot,
                mergeAliases));
    document.ifPresent(
        context ->
            context.prependEmbeddedFields(
                leafPath,
                fieldsByEmbeddedRoot,
                embeddedFieldDefinitionByEmbeddedRoot,
                mergeAliases));

    embedded.ifPresent(
        context ->
            putEmbeddedFields(
                fieldsByEmbeddedRoot,
                FieldPath.newRoot(leafPath),
                prependPaths(context.rootFields, leafPath),
                mergeAliases));
    field
        .embeddedDocumentsFieldDefinition()
        .ifPresent(
            def -> embeddedFieldDefinitionByEmbeddedRoot.put(FieldPath.newRoot(leafPath), def));
    document.ifPresent(
        context ->
            prependPaths(context.rootFields, leafPath)
                .forEach((path, child) -> putField(rootFields, path, child, mergeAliases)));
    putField(rootFields, FieldPath.newRoot(leafPath), field, mergeAliases);
  }

  private void prependEmbeddedFields(
      String leafPath,
      Map<FieldPath, Map<FieldPath, FieldDefinition>> scopes,
      Map<FieldPath, EmbeddedDocumentsFieldDefinition> roots,
      boolean mergeAliases) {
    this.fieldsByEmbeddedRoot.forEach(
        (path, children) ->
            putEmbeddedFields(
                scopes,
                path.withNewRoot(leafPath),
                prependPaths(children, leafPath),
                mergeAliases));
    this.embeddedFieldDefinitionByEmbeddedRoot.forEach(
        (path, definition) -> roots.put(path.withNewRoot(leafPath), definition));
  }

  private static void putEmbeddedFields(
      Map<FieldPath, Map<FieldPath, FieldDefinition>> scopes,
      FieldPath root,
      Map<FieldPath, FieldDefinition> children,
      boolean mergeAliases) {
    if (!mergeAliases) {
      scopes.put(root, children);
      return;
    }
    var fields = scopes.computeIfAbsent(root, unused -> new HashMap<>());
    children.forEach(
        (path, child) -> fields.merge(path, child, FieldDefinition::mergeQueryAliases));
  }

  private static void putField(
      Map<FieldPath, FieldDefinition> fields,
      FieldPath path,
      FieldDefinition child,
      boolean mergeAliases) {
    if (mergeAliases) {
      fields.merge(path, child, FieldDefinition::mergeQueryAliases);
    } else {
      fields.put(path, child);
    }
  }

  private static <T> ImmutableMap<FieldPath, T> prependPaths(
      Map<FieldPath, T> fieldMap, String newRootPath) {
    return fieldMap.entrySet().stream()
        .collect(
            ImmutableMap.toImmutableMap(
                entry -> entry.getKey().withNewRoot(newRootPath), Map.Entry::getValue));
  }

  private static int getNumEmbeddedLayers(FieldDefinition field) {
    return Math.max(
        field
            .embeddedDocumentsFieldDefinition()
            .map(EmbeddedDocumentsFieldDefinition::fieldHierarchyContext)
            .map(FieldHierarchyContext::getNumEmbeddedDocumentsLayers)
            .orElse(0),
        field
            .documentFieldDefinition()
            .map(DocumentFieldDefinition::fieldHierarchyContext)
            .map(FieldHierarchyContext::getNumEmbeddedDocumentsLayers)
            .orElse(0));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldHierarchyContext that = (FieldHierarchyContext) o;
    return this.numEmbeddedDocumentsLayers == that.numEmbeddedDocumentsLayers
        && this.rootFields.equals(that.rootFields)
        && this.fieldsByEmbeddedRoot.equals(that.fieldsByEmbeddedRoot);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this.rootFields, this.fieldsByEmbeddedRoot, this.numEmbeddedDocumentsLayers);
  }
}
