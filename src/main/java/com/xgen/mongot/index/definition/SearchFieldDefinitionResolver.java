package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.version.IndexCapabilities;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import java.util.function.Function;

/**
 * FieldDefinitionResolver retrieves the correct FieldDefinition for a combination of path +
 * embeddedRoot by checking to see if there exists a statically defined FieldDefinition else
 * defaulting to the IndexFormatVersion-specific dynamic FieldDefinition.
 */
public class SearchFieldDefinitionResolver implements FieldDefinitionResolver {

  public final SearchIndexDefinition indexDefinition;
  public final SearchIndexCapabilities indexCapabilities;

  private final VectorDimensions vectorDimensions;

  public boolean hasTypeSetVectorFields() {
    return this.indexDefinition.hasTypeSetVectorFields();
  }

  public VectorDimensions vectorDimensions() {
    return this.vectorDimensions;
  }

  public SearchFieldDefinitionResolver(
      SearchIndexDefinition indexDefinition,
      IndexFormatVersion indexFormatVersion,
      VectorDimensions vectorDimensions) {
    this.indexDefinition = indexDefinition;
    this.indexCapabilities = indexDefinition.getIndexCapabilities(indexFormatVersion);
    this.vectorDimensions = vectorDimensions;
  }

  SearchFieldDefinitionResolver(
      SearchIndexDefinition indexDefinition, IndexFormatVersion indexFormatVersion) {
    this(indexDefinition, indexFormatVersion, new VectorDimensions());
  }

  public Optional<FieldDefinition> getFieldDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    return embeddedRoot
        .flatMap(
            rootPath ->
                this.indexDefinition
                    .getMappings()
                    .fieldHierarchyContext()
                    .getFieldDefinition(path, rootPath))
        .or(() -> resolveFieldDefinition(path, embeddedRoot));
  }

  public Optional<StringFieldDefinition> getStringFieldDefinition(
      StringPath stringPath, Optional<FieldPath> embeddedRoot) {
    Optional<StringFieldDefinition> baseStringFieldDefinition =
        getFieldDefinition(stringPath.getBaseFieldPath(), embeddedRoot)
            .flatMap(FieldDefinition::stringFieldDefinition);

    if (stringPath.isField()) {
      return baseStringFieldDefinition;
    }

    String multi = stringPath.asMultiField().getMulti();
    return baseStringFieldDefinition
        .flatMap(field -> field.getMultiDefinition(multi))
        .or(
            () ->
                findAlias(
                    stringPath.getBaseFieldPath(),
                    embeddedRoot,
                    field ->
                        field
                            .stringFieldDefinition()
                            .flatMap(string -> string.getMultiDefinition(multi))));
  }

  /**
   * Fetches the EmbeddedDocumentsFieldDefinition for an embeddedRoot if present, empty if not
   *
   * @param embeddedRoot EmbeddedRoot path
   * @return EmbeddedDocumentsFieldDefinition
   */
  public Optional<EmbeddedDocumentsFieldDefinition> getEmbeddedDocumentsFieldDefinition(
      FieldPath embeddedRoot) {
    return Optional.ofNullable(
        this.indexDefinition
            .getFieldHierarchyContext()
            .getEmbeddedFieldDefinitionByEmbeddedRoot()
            .get(embeddedRoot));
  }

  @Override
  public boolean isUsed(FieldPath path) {
    return getFieldDefinition(path, Optional.empty()).isPresent()
        || this.indexDefinition
            .getMappings()
            .fieldHierarchyContext()
            .getEmbeddedDocumentsRelativeRoots()
            .stream()
            .anyMatch(
                embeddedRoot -> getFieldDefinition(path, Optional.of(embeddedRoot)).isPresent())
        || this.indexDefinition.getStoredSource().isPathToStored(path);
  }

  @Override
  public Optional<VectorFieldSpecification> getVectorFieldSpecification(FieldPath path) {
    return getVectorFieldSpecification(path, Optional.empty());
  }

  public Optional<VectorFieldSpecification> getVectorFieldSpecification(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    var field = getFieldDefinition(path, embeddedRoot);
    var explicit = field.flatMap(FieldDefinition::vectorFieldSpecification);
    return explicit.or(
        () ->
            getTypeSetVectorRule(path, embeddedRoot)
                .flatMap(rule -> rule.resolve(() -> this.vectorDimensions.get(path))));
  }

  public Optional<TypeSetVectorFieldDefinition> getTypeSetVectorRule(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    if (embeddedRoot.isPresent() || !hasTypeSetVectorFields()) return Optional.empty();
    var canonical = getFieldDefinition(path, embeddedRoot);
    if (canonical.flatMap(FieldDefinition::vectorFieldSpecification).isPresent())
      return Optional.empty();
    return canonical
        .flatMap(FieldDefinition::typeSetVectorFieldDefinition)
        .or(() -> findAlias(path, embeddedRoot, FieldDefinition::typeSetVectorFieldDefinition));
  }

  public boolean hasGeoField(FieldPath path, Optional<FieldPath> embeddedRoot, boolean shapes) {
    Function<FieldDefinition, Optional<GeoFieldDefinition>> select =
        field -> field.geoFieldDefinition().filter(geo -> !shapes || geo.indexShapes());
    return getFieldDefinition(path, embeddedRoot)
        .flatMap(select)
        .or(() -> findAlias(path, embeddedRoot, select))
        .isPresent();
  }

  /** Resolve alternate BSON key boundaries against the configured schema alone. */
  private <T> Optional<T> findAlias(
      FieldPath path, Optional<FieldPath> root, Function<FieldDefinition, Optional<T>> select) {
    String dotted = path.toString();
    if (root.isEmpty()) {
      return dotted.contains(".")
          ? findAlias(this.indexDefinition.getMappings(), dotted, select)
          : Optional.empty();
    }
    String prefix = root.get() + ".";
    if (!dotted.startsWith(prefix)) return Optional.empty();
    for (var scope : this.indexDefinition.embeddedScopes(root.get().toString())) {
      var match = findAlias(scope.mapping(), dotted.substring(prefix.length()), select);
      if (match.isPresent()) return match;
    }
    return Optional.empty();
  }

  private <T> Optional<T> findAlias(
      HierarchicalFieldDefinition parent,
      String remaining,
      Function<FieldDefinition, Optional<T>> select) {
    if (parent.fields().isEmpty()) return getDynamicFieldDefinition(parent).flatMap(select);
    var whole = parent.getField(remaining);
    var exact = whole.flatMap(select);
    if (exact.isPresent()) return exact;
    @Var boolean implicitDocument = false;
    for (int end = remaining.lastIndexOf('.');
        end >= 0;
        end = remaining.lastIndexOf('.', end - 1)) {
      var prefix = parent.getField(remaining.substring(0, end));
      if (prefix.isEmpty()) {
        implicitDocument = true;
      } else if (prefix.get().documentFieldDefinition().isPresent()) {
        var match =
            findAlias(
                prefix.get().documentFieldDefinition().get(), remaining.substring(end + 1), select);
        if (match.isPresent()) return match;
      }
    }
    var dynamic = getDynamicFieldDefinition(parent);
    if (whole.isEmpty()
        || (implicitDocument
            && dynamic.flatMap(FieldDefinition::documentFieldDefinition).isPresent())) {
      return dynamic.flatMap(select);
    }
    return Optional.empty();
  }

  public IndexCapabilities getIndexCapabilities() {
    return this.indexCapabilities;
  }

  /**
   * Returns the correct <code>dynamic</code> {@link FieldDefinition} based on the {@link
   * DynamicDefinition} supplied.
   */
  public Optional<FieldDefinition> getDynamicFieldDefinition(
      HierarchicalFieldDefinition hierarchicalFieldDefinition) {
    return switch (hierarchicalFieldDefinition.dynamic()) {
      case DynamicDefinition.Boolean bool ->
          bool.value() ? Optional.of(FieldDefinition.DYNAMIC_FIELD_DEFINITION) : Optional.empty();

      case DynamicDefinition.Document document -> {
        // If dynamicDefinition is of type document, validation has already occurred during
        // deserialization that it references a unique valid typeSet defined in the IndexDefinition
        Optional<TypeSetDefinition> matchingTypeSet =
            Optional.ofNullable(this.indexDefinition.getTypeSetsMap().get(document.typeSet()));
        yield Optional.of(Check.isPresent(matchingTypeSet, "matchingTypeSet").getFieldDefinition());
      }
    };
  }

  private Optional<FieldDefinition> resolveFieldDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    Optional<FieldDefinition> staticDefinition = getStaticDefinition(path, embeddedRoot);
    if (staticDefinition.isPresent()) {
      return staticDefinition;
    }

    // The path is not statically configured, so we need to see if it is dynamically configured.
    // Find the closest ancestor (starting with the parent) that has a configured FieldDefinition.
    Optional<FieldDefinition> optionalAncestorDefinition =
        path.ancestorPaths()
            .map(ancestorPath -> getStaticDefinition(ancestorPath, embeddedRoot))
            .flatMap(Optional::stream)
            .findFirst();

    // If we didn't find an ancestor of the path, fall back to the root mappings.
    if (optionalAncestorDefinition.isEmpty()) {
      if (embeddedRoot.isEmpty()) {
        return getDynamicFieldDefinition(this.indexDefinition.getMappings());
      }

      return Optional.ofNullable(
              this.indexDefinition
                  .getMappings()
                  .fieldHierarchyContext()
                  .getRootFields()
                  .get(embeddedRoot.get()))
          .flatMap(FieldDefinition::embeddedDocumentsFieldDefinition)
          .flatMap(this::getDynamicFieldDefinition);
    }

    FieldDefinition ancestorDefinition = optionalAncestorDefinition.get();
    Optional<DocumentFieldDefinition> optionalDocumentDefinition =
        ancestorDefinition.documentFieldDefinition();

    // If the statically configured ancestor was not configured to be a document, then this field
    // should not be resolved.
    if (optionalDocumentDefinition.isEmpty()) {
      return Optional.empty();
    }

    // Finally, if we found a statically configured ancestor which is configured to be a document,
    // check whether it is configured to have its children be dynamically discovered.
    DocumentFieldDefinition documentDefinition = optionalDocumentDefinition.get();
    return getDynamicFieldDefinition(documentDefinition);
  }

  private Optional<FieldDefinition> getStaticDefinition(
      FieldPath path, Optional<FieldPath> embeddedRoot) {
    if (embeddedRoot.isPresent()) {
      return this.indexDefinition
          .getMappings()
          .fieldHierarchyContext()
          .getFieldDefinition(path, embeddedRoot.get());
    }

    return Optional.ofNullable(this.indexDefinition.getStaticFields().get(path.toString()));
  }

  public ImmutableMap<String, FieldDefinition> getFields() {
    return this.indexDefinition.getMappings().fields();
  }
}
