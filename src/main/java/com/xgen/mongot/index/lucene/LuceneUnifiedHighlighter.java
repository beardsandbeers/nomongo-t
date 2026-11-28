package com.xgen.mongot.index.lucene;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableListMultimap;
import com.xgen.mongot.index.SearchHighlight;
import com.xgen.mongot.index.lucene.explain.explainers.HighlightFeatureExplainer;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.highlights.Highlight;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.uhighlight.UHComponents;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.util.UnicodeUtil;

class LuceneUnifiedHighlighter {
  private final PublicUnifiedHighlighter luceneHighlighter;
  private final Highlight highlight;
  private final Query searchQuery;
  private final Optional<HighlightFeatureExplainer> explainer;

  @VisibleForTesting
  static final Comparator<SearchHighlight> BY_PATH = Comparator.comparing(h -> h.path().toString());

  private LuceneUnifiedHighlighter(
      PublicUnifiedHighlighter luceneHighlighter,
      Highlight highlight,
      Query searchQuery,
      Optional<HighlightFeatureExplainer> explainer) {
    this.luceneHighlighter = luceneHighlighter;
    this.highlight = highlight;
    this.searchQuery = searchQuery;
    this.explainer = explainer;
  }

  static LuceneUnifiedHighlighter create(
      IndexSearcher indexSearcher,
      Analyzer indexAnalyzer,
      Highlight highlight,
      Query searchQuery,
      Optional<HighlightFeatureExplainer> explainer) {
    UnifiedHighlighter.Builder highlighterBuilder =
        UnifiedHighlighter.builder(indexSearcher, indexAnalyzer)
            .withFormatter(new LuceneSearchHighlightsFormatter())
            // If a field specified for highlighting is part of a multi-term query (i.e. fuzzy or
            // wildcard), Lucene re-analyzes the field in memory. Under the hood, this requires
            // converting
            // each supplied query term from UTF8 to UTF16, a process that mandates that the terms
            // should
            // not be bytes (i.e. ObjectId types.)
            //
            // To determine the query terms used to perform the UTF16 conversion above, Lucene uses
            // a FieldMatcher earlier on in the process to keep the appropriate query terms. We
            // supply the
            // FieldMatcher below to keep only the autocomplete and string-typed query terms, since
            // they are
            // the field types we currently support highlighting for.
            .withFieldMatcher(
                FieldName.MultiField.isType()
                    .or(FieldName.TypeField.AUTOCOMPLETE.isType())
                    .or(FieldName.TypeField.STRING.isType()))
            .withMaxLength(highlight.maxCharsToExamine())
            // keep weightMatches off to preserve default behavior we had before upgrade to Lucene 9
            .withWeightMatches(false);
    PublicUnifiedHighlighter luceneHighlighter =
        new PublicUnifiedHighlighter(highlighterBuilder, highlight);

    return new LuceneUnifiedHighlighter(luceneHighlighter, highlight, searchQuery, explainer);
  }

  /**
   * Performs validation on the searchQuery used to process documents for highlighting to ensure
   * that Lucene will not throw an exception while processing highlights.
   *
   * <p>Notably, Lucene assumes that the input query terms are all proper UTF-8 strings; attempting
   * to highlight a query with non-UTF-8 term values results in a non-intuitive OOBE, which is what
   * we check for here. This is in lieu of "proper" UTF-8 validation since it more closely emulates
   * the Lucene behavior and isn't dependent on the implementation of {@link UnicodeUtil}.
   *
   * @throws InvalidQueryException if searchQuery is invalid for highlighting
   */
  public void assertHighlightedTermsValid() throws InvalidQueryException {
    Set<Term> potentialHighlightTerms =
        PublicUnifiedHighlighter.extractTerms(this.searchQuery).stream()
            .filter(term -> this.highlight.resolvedLuceneFieldNames().contains(term.field()))
            .collect(Collectors.toUnmodifiableSet());

    for (Term term : potentialHighlightTerms) {
      char[] charsOut = new char[term.bytes().length];

      try {
        UnicodeUtil.UTF8toUTF16(term.bytes(), charsOut);
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new InvalidQueryException(
            String.format(
                "highlight: query value must be utf-8: %s",
                fromLuceneStringOrAutocompleteFieldName(term.field())));
      }
    }
  }

  /**
   * Return a vector of SearchHighlights where the index corresponds to the document index in the
   * topDocs array.
   */
  public List<List<SearchHighlight>> highlightsAsSearchHighlightsArray(TopDocs topDocs)
      throws IOException {
    if (Explain.isEnabled()) {
      ExplainTimings highlightTimings = this.explainer.get().getTimings();
      try (var unused = highlightTimings.split(ExplainTimings.Type.EXECUTE_HIGHLIGHT)) {
        return performHighlighting(topDocs);
      }
    }

    return performHighlighting(topDocs);
  }

  private List<List<SearchHighlight>> performHighlighting(TopDocs topDocs) throws IOException {
    // Lucene's UnifiedHighlighter returns a Map where the key is the path and the value is an
    // array of LuceneSearchHighlights. The index of the LuceneSearchHighlights[] corresponds to
    // the index of the associated document in the scoreDocs array.
    //
    // In the mongot SearchHighlights format, each searchHighlight is aware of it's own path, and
    // all the SearchHighlights (regardless of path) are associated with their documents in a
    // single "docSearchHighlights" array.
    //
    // Get Map of SearchHighlights where key = path and value == Object[] (which should be a
    // LuceneSearchHighlights[].

    Map<String, Object[]> searchHighlightsArrayByFieldMap =
        getLuceneSearchHighlightsArrayByPathMap(topDocs);

    // Create return array
    List<List<SearchHighlight>> docSearchHighlights = new ArrayList<>(topDocs.scoreDocs.length);
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      docSearchHighlights.add(new ArrayList<>());
    }

    mergeSearchHighlightsArray(searchHighlightsArrayByFieldMap, docSearchHighlights);

    // Lucene's UnifiedHighlighter returns a HashMap whose iteration order depends on field-name
    // hashes and the map's internal capacity (sized to the resolved field count). That order is
    // not a contract we want to expose, and it can shift between shard topologies and Lucene/JDK
    // versions. Sort each doc's highlights by path so callers (and tests) see a stable order.
    for (List<SearchHighlight> doc : docSearchHighlights) {
      doc.sort(BY_PATH);
    }

    return docSearchHighlights;
  }

  static void mergeSearchHighlightsArray(
      Map<String, Object[]> searchHighlightsArrayByFieldMap,
      List<List<SearchHighlight>> docSearchHighlights) {
    searchHighlightsArrayByFieldMap.forEach(
        (path, objects) ->
            mergeObjectArrayIntoDocSearchHighlightsArray(
                fromLuceneStringOrAutocompleteFieldName(path), objects, docSearchHighlights));
  }

  @VisibleForTesting
  Highlight getHighlight() {
    return this.highlight;
  }

  @VisibleForTesting
  static StringPath fromLuceneStringOrAutocompleteFieldName(String luceneFieldName) {
    if (FieldName.TypeField.STRING.isTypeOf(luceneFieldName)) {
      return new StringFieldPath(
          FieldPath.parse(FieldName.TypeField.STRING.stripPrefix(luceneFieldName)));
    }

    if (FieldName.TypeField.AUTOCOMPLETE.isTypeOf(luceneFieldName)) {
      return new StringFieldPath(
          FieldPath.parse(FieldName.TypeField.AUTOCOMPLETE.stripPrefix(luceneFieldName)));
    }

    if (FieldName.MultiField.isTypeOf(luceneFieldName)) {
      return FieldName.MultiField.getFieldPath(luceneFieldName);
    }

    throw new IllegalArgumentException(
        String.format("lucene field name %s did not match expected format", luceneFieldName));
  }

  /**
   * Merge the LuceneSearchHighlights array returned for a given path into the by document
   * SearchHighlights array.
   *
   * @param path - the path of the SearchHighlights that have been returned
   * @param objects - an array of Objects where the index corresponds with the associated document
   *     in the scoreDocs array, and the Object may be a LuceneSearchHighlights
   * @param docSearchHighlights - an array of SearchHighlights associated with each document
   */
  private static void mergeObjectArrayIntoDocSearchHighlightsArray(
      StringPath path, Object[] objects, List<List<SearchHighlight>> docSearchHighlights) {
    for (int docIndex = 0; docIndex < docSearchHighlights.size(); docIndex++) {
      int index = docIndex; // final for use in lambda
      convertObjectToSearchHighlights(objects[docIndex], path)
          .ifPresent(searchHighlights -> docSearchHighlights.get(index).addAll(searchHighlights));
    }
  }

  private Map<String, Object[]> getLuceneSearchHighlightsArrayByPathMap(TopDocs topDocs)
      throws IOException {

    // Convert inputs into formats required by Lucene
    int[] docIds = Arrays.stream(topDocs.scoreDocs).mapToInt(scoreDoc -> scoreDoc.doc).toArray();

    String[] paths = this.highlight.resolvedLuceneFieldNames().toArray(String[]::new);
    if (paths.length == 0) {
      // lucene doesn't like an empty input
      return Collections.emptyMap();
    }

    int[] numSearchHighlights = new int[paths.length];
    Arrays.fill(numSearchHighlights, this.highlight.maxNumPassages());

    return this.luceneHighlighter.highlightFieldsAsObjects(
        paths, this.searchQuery, docIds, numSearchHighlights);
  }

  /**
   * Lucene returns a nullable Object from the PassageFormatter. Null means that there are no
   * SearchHighlights for this path in this document.
   *
   * <p>Convert that nullable Object into an Optional SearchHighlights
   *
   * @param object - nullable Object which may be a LuceneSearchHighlights
   * @param path - the path of the SearchHighlights that have been returned
   * @return Optional SearchHighlights
   */
  private static Optional<List<SearchHighlight>> convertObjectToSearchHighlights(
      Object object, StringPath path) {
    Optional<Object> objectOptional = Optional.ofNullable(object);

    if (objectOptional.isPresent() && !(objectOptional.get() instanceof LuceneSearchHighlights)) {
      throw new RuntimeException("Should have been a SearchHighlights Object.");
    }

    return objectOptional.map(o -> ((LuceneSearchHighlights) o).toSearchHighlights(path));
  }

  /**
   * PublicUnifiedHighlighter extends Lucene's {@link UnifiedHighlighter} to allow:
   *
   * <ul>
   *   <li>Direct invocation of {@code highlightFieldsAsObjects} and {@code extractTerms} in tests
   *   <li>Custom field value loading logic via {@code loadFieldValues}, enabling the use of stored
   *       base fields instead of multi fields when highlighting
   * </ul>
   *
   * <p>This class is used to simulate real-world field resolution behavior in highlight tests
   * without modifying Lucene's internals. It relies on a resolved mapping of {@code
   * resolvedLuceneFieldName → storedLuceneFieldName} provided externally.
   */
  @VisibleForTesting
  static class PublicUnifiedHighlighter extends UnifiedHighlighter {

    // Maps resolved user-specified Lucene field names to their corresponding stored field names.
    // Used during {@link #loadFieldValues} to determine which fields to load from disk.
    private final Map<String, String> storedLuceneFieldNameMap;
    private final Set<String> fieldsWithPartialStorage;

    private PublicUnifiedHighlighter(UnifiedHighlighter.Builder builder, Highlight highlight) {
      super(builder);
      this.storedLuceneFieldNameMap = highlight.storedLuceneFieldNameMap();
      this.fieldsWithPartialStorage = highlight.fieldsWithPartialStorage();
    }

    @Override
    public Map<String, Object[]> highlightFieldsAsObjects(
        String[] fieldsIn, Query query, int[] docIdsIn, int[] maxPassagesIn) throws IOException {
      return super.highlightFieldsAsObjects(fieldsIn, query, docIdsIn, maxPassagesIn);
    }

    public static Set<Term> extractTerms(Query query) {
      return UnifiedHighlighter.extractTerms(query);
    }

    @Override
    protected OffsetSource getOptimizedOffsetSource(UHComponents components) {
      // Postings offsets include every indexed value. When a BSON alias opted out of storage,
      // those offsets cannot address the shorter stored text of a document containing both.
      OffsetSource optimized = super.getOptimizedOffsetSource(components);
      OffsetSource result =
          optimized != OffsetSource.NONE_NEEDED
                  && this.fieldsWithPartialStorage.contains(components.field())
              ? OffsetSource.ANALYSIS
              : optimized;
      Explain.getQueryInfo()
          .flatMap(e -> e.getFeatureExplainer(HighlightFeatureExplainer.class))
          .ifPresent(h -> h.addOffsetSource(components.field(), result));
      return result;
    }

    /**
     * A stored value can back several requested multi-fields. Deliver it to each in document order,
     * including when another BSON spelling stores its value directly on the multi-field.
     */
    @Override
    protected LimitedStoredFieldVisitor newLimitedStoredFieldsVisitor(String[] fields) {
      if (Arrays.stream(fields)
          .allMatch(field -> field.equals(this.storedLuceneFieldNameMap.get(field)))) {
        return super.newLimitedStoredFieldsVisitor(fields);
      }
      var targets = storedFieldTargets(fields, this.storedLuceneFieldNameMap);
      return new LimitedStoredFieldVisitor(fields, '\0', getMaxLength()) {
        @Override
        public Status needsField(FieldInfo info) {
          for (int index : targets.get(info.name)) {
            var value = this.values[index];
            if (value == null || value.length() < this.maxLength) {
              return Status.YES;
            }
          }
          return Status.NO;
        }

        @Override
        public void stringField(FieldInfo info, String value) throws IOException {
          for (int index : targets.get(info.name)) {
            if (this.values[index] == null || this.values[index].length() < this.maxLength) {
              this.currentField = index;
              super.stringField(info, value);
            }
          }
        }
      };
    }

    static ImmutableListMultimap<String, Integer> storedFieldTargets(
        String[] fields, Map<String, String> storedNames) {
      var targets = ImmutableListMultimap.<String, Integer>builder();
      for (int i = 0; i < fields.length; i++) {
        String field = fields[i];
        String stored = storedNames.get(field);
        targets.put(stored, i);
        if (!field.equals(stored)) {
          targets.put(field, i);
        }
      }
      return targets.build();
    }
  }
}
