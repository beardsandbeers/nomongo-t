package com.xgen.mongot.index.lucene.query;

import com.xgen.mongot.index.SearchIndexReader.QuerySnapshot.OrdinalJoinPartition;
import com.xgen.mongot.index.query.operators.AllDocumentsOperator;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/** Adapts the production join algorithm to plain Lucene readers in tests and benchmarks. */
public final class OrdinalJoinFixture {
  private OrdinalJoinFixture() {}

  public record Preparation(Query query, long distinctForeignKeys, long peakBitmapBytes) {}

  public static Preparation prepare(
      List<IndexSearcher> foreign,
      Query predicate,
      String foreignField,
      List<? extends IndexReader> local,
      String localField,
      Runnable check,
      int sequentialSteps)
      throws Exception {
    List<OrdinalJoinPartition> sources =
        foreign.stream()
            .map(
                searcher ->
                    new OrdinalJoinPartition(
                        searcher.getIndexReader(),
                        (unused, collector, cancellation) -> searcher.search(predicate, collector)))
            .toList();
    var targets =
        local.stream()
            .map(
                reader ->
                    new OrdinalJoinPartition(
                        reader,
                        (unused, collector, cancellation) -> {
                          throw new UnsupportedOperationException();
                        }))
            .toList();
    var prepared =
        OrdinalJoinPreparation.prepare(
            sources,
            AllDocumentsOperator.INSTANCE,
            foreignField,
            targets,
            localField,
            "fixture",
            check,
            ignored -> {},
            sequentialSteps);
    return new Preparation(
        new OrdinalJoinQuery(localField, prepared.membership(), Optional.empty()),
        prepared.membership().size(),
        prepared.peakBitmapBytes());
  }

  public static Preparation prepare(
      List<IndexSearcher> foreign,
      Query predicate,
      String foreignField,
      List<? extends IndexReader> local,
      String localField,
      Runnable check)
      throws Exception {
    return prepare(foreign, predicate, foreignField, local, localField, check, 0);
  }
}
