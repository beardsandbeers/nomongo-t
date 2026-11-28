package com.xgen.mongot.index.query.highlights;

import com.google.common.collect.ImmutableList;
import java.util.Map;
import java.util.Set;

/**
 * Represents the configuration for highlighting, including mapping from resolved field names to
 * their actual stored Lucene field names and highlighting limits.
 *
 * @param storedLuceneFieldNameMap The mapping from each resolved field name to the actual stored
 *     Lucene field name used to load string values; typically maps multi-fields to their base field
 *     if the base field is stored (e.g., {@code $multi/title.french} → {@code $type:string/title})
 * @param fieldsWithPartialStorage Fields where indexed offsets can include values omitted from
 *     stored text
 * @param maxNumPassages The maximum number of passages per document to highlight
 * @param maxCharsToExamine The maximum number of characters to examine per field during
 *     highlighting
 */
public record Highlight(
    Map<String, String> storedLuceneFieldNameMap,
    int maxNumPassages,
    int maxCharsToExamine,
    Set<String> fieldsWithPartialStorage) {

  public Highlight(
      Map<String, String> storedLuceneFieldNameMap, int maxNumPassages, int maxCharsToExamine) {
    this(storedLuceneFieldNameMap, maxNumPassages, maxCharsToExamine, Set.of());
  }

  public static Highlight create(
      Map<String, String> storedLuceneFieldNameMap, UnresolvedHighlight unresolvedHighlight) {
    return new Highlight(
        storedLuceneFieldNameMap,
        unresolvedHighlight.maxNumPassages(),
        unresolvedHighlight.maxCharsToExamine());
  }

  public static Highlight create(
      Map<String, String> storedLuceneFieldNameMap, int maxNumPassages, int maxCharsToExamine) {
    return new Highlight(storedLuceneFieldNameMap, maxNumPassages, maxCharsToExamine);
  }

  /** The list of resolved Lucene field names requested for highlighting */
  public ImmutableList<String> resolvedLuceneFieldNames() {
    return ImmutableList.copyOf(this.storedLuceneFieldNameMap.keySet());
  }
}
