package com.xgen.mongot.index.analyzer.wrapper;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.FieldTypeDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.TokenFieldDefinition;
import java.util.Optional;

/** Resolves configured analysis options against the registry for indexing or querying. */
abstract class AnalyzerWrapperBuilder<T> {
  abstract Optional<T> getFieldNormalizer(AnalyzerRegistry registry, TokenFieldDefinition field);

  abstract Optional<T> getFieldAnalyzer(AnalyzerRegistry registry, StringFieldDefinition field);

  abstract T getAutoCompleteAnalyzer(AnalyzerRegistry registry, AutocompleteFieldDefinition field);

  ImmutableMap<String, T> buildStaticMappings(
      SearchIndexDefinition index,
      AnalyzerRegistry registry,
      T defaultAnalyzer,
      T defaultNormalizer) {
    return resolve(
        AnalyzerMapping.staticDefinitions(index), registry, defaultAnalyzer, defaultNormalizer);
  }

  Optional<DynamicTypeSetPrefixMap<T>> buildDynamicMappings(
      SearchIndexDefinition index,
      AnalyzerRegistry registry,
      T defaultAnalyzer,
      T defaultNormalizer) {
    var fields =
        resolve(
            AnalyzerMapping.dynamicDefinitions(index),
            registry,
            defaultAnalyzer,
            defaultNormalizer);
    return fields.isEmpty() ? Optional.empty() : Optional.of(new DynamicTypeSetPrefixMap<>(fields));
  }

  private ImmutableMap<String, T> resolve(
      ImmutableMap<String, FieldTypeDefinition> fields,
      AnalyzerRegistry registry,
      T defaultAnalyzer,
      T defaultNormalizer) {
    var result = ImmutableMap.<String, T>builder();
    fields.forEach(
        (name, field) ->
            result.put(
                name,
                switch (field) {
                  case StringFieldDefinition string ->
                      getFieldAnalyzer(registry, string).orElse(defaultAnalyzer);
                  case TokenFieldDefinition token ->
                      getFieldNormalizer(registry, token).orElse(defaultNormalizer);
                  case AutocompleteFieldDefinition ac -> getAutoCompleteAnalyzer(registry, ac);
                  default ->
                      throw new IllegalStateException(
                          "Unexpected analyzer field " + field.getType());
                }));
    return result.buildOrThrow();
  }
}
