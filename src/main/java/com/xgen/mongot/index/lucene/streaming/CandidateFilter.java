package com.xgen.mongot.index.lucene.streaming;

import com.google.errorprone.annotations.Var;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.Nullable;

/** Membership of an ascending batch of leaf-local IDs. Iterators are fresh for each batch. */
interface CandidateFilter {
  void test(int[] docs, boolean[] matches) throws IOException;

  CandidateFilter ALL = (docs, matches) -> Arrays.fill(matches, true);
  CandidateFilter NONE = (docs, matches) -> Arrays.fill(matches, false);

  /** A cheap superset small enough to check exhaustively, or null when the size is unknown. */
  static @Nullable DocIdSetIterator smallLead(
      LeafReaderContext leaf, @Nullable Query query, int limit) throws IOException {
    if (query instanceof MatchNoDocsQuery) {
      return DocIdSetIterator.empty();
    }
    if (query instanceof BoostQuery q) {
      return smallLead(leaf, q.getQuery(), limit);
    }
    if (query instanceof ConstantScoreQuery q) {
      return smallLead(leaf, q.getQuery(), limit);
    }
    if (query instanceof IndexOrDocValuesQuery q) {
      return smallLead(leaf, q.getIndexQuery(), limit);
    }
    if (query instanceof TermQuery q && leaf.reader().docFreq(q.getTerm()) <= limit) {
      var postings = leaf.reader().postings(q.getTerm(), PostingsEnum.NONE);
      return postings == null ? DocIdSetIterator.empty() : postings;
    }
    if (query instanceof BooleanQuery q) {
      for (var clause : q) {
        if (clause.isRequired()) {
          var lead = smallLead(leaf, clause.query(), limit);
          if (lead != null) {
            return lead;
          }
        }
      }
    }
    return null;
  }

  static CandidateFilter compile(
      IndexSearcher searcher, LeafReaderContext leaf, @Nullable Query query) throws IOException {
    if (query == null || query instanceof MatchAllDocsQuery) {
      return ALL;
    }
    if (query instanceof MatchNoDocsQuery) {
      return NONE;
    }
    if (query instanceof BoostQuery q) {
      return compile(searcher, leaf, q.getQuery());
    }
    if (query instanceof ConstantScoreQuery q) {
      return compile(searcher, leaf, q.getQuery());
    }
    if (query instanceof BooleanQuery q) {
      return compound(searcher, leaf, q);
    }
    if (query instanceof IndexOrDocValuesQuery q) {
      if (q.getIndexQuery() instanceof PointRangeQuery) {
        return iterator(searcher, leaf, q.getRandomAccessQuery());
      }
      return compile(searcher, leaf, q.getIndexQuery());
    }
    if (query instanceof TermQuery || query instanceof FieldExistsQuery) {
      return iterator(searcher, leaf, query);
    }
    if (query instanceof TermInSetQuery terms) {
      var clauses = new ArrayList<CandidateFilter>();
      var it = terms.getBytesRefIterator();
      for (BytesRef term; (term = it.next()) != null; ) {
        clauses.add(
            iterator(
                searcher,
                leaf,
                new TermQuery(new Term(terms.getField(), BytesRef.deepCopyOf(term)))));
      }
      return (docs, matches) -> {
        boolean[] scratch = new boolean[docs.length];
        Arrays.fill(matches, false);
        for (var clause : clauses) {
          clause.test(docs, scratch);
          for (int i = 0; i < docs.length; i++) {
            matches[i] |= scratch[i];
          }
        }
      };
    }
    if (query instanceof PointRangeQuery p) {
      return PointRangeMembership.compile(leaf.reader(), p, searcher.getTimeout());
    }
    throw new IllegalArgumentException(
        "Streaming does not support candidate membership for " + query);
  }

  private static CandidateFilter iterator(IndexSearcher searcher, LeafReaderContext leaf, Query q)
      throws IOException {
    var weight = searcher.rewrite(q).createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1);
    return (docs, matches) -> {
      var scorer = weight.scorer(leaf);
      Arrays.fill(matches, false);
      if (scorer == null) {
        return;
      }
      var twoPhase = scorer.twoPhaseIterator();
      var it = twoPhase == null ? scorer.iterator() : twoPhase.approximation();
      for (int i = 0; i < docs.length; i++) {
        if (it.docID() < docs[i]) {
          it.advance(docs[i]);
        }
        matches[i] = it.docID() == docs[i] && (twoPhase == null || twoPhase.matches());
      }
    };
  }

  private static CandidateFilter compound(
      IndexSearcher searcher, LeafReaderContext leaf, BooleanQuery q) throws IOException {
    record Clause(BooleanClause.Occur occur, CandidateFilter filter) {}

    List<Clause> clauses = new ArrayList<>();
    @Var boolean required = false;
    for (var c : q) {
      clauses.add(new Clause(c.occur(), compile(searcher, leaf, c.query())));
      required |= c.isRequired();
    }
    int minimum = Math.max(q.getMinimumNumberShouldMatch(), required ? 0 : 1);
    return (docs, matches) -> {
      boolean[] scratch = new boolean[docs.length];
      int[] should = new int[docs.length];
      Arrays.fill(matches, true);
      for (Clause c : clauses) {
        c.filter.test(docs, scratch);
        for (int i = 0; i < docs.length; i++) {
          switch (c.occur) {
            case MUST, FILTER -> matches[i] &= scratch[i];
            case MUST_NOT -> matches[i] &= !scratch[i];
            case SHOULD -> {
              if (scratch[i]) {
                should[i]++;
              }
            }
          }
        }
      }
      for (int i = 0; i < docs.length; i++) {
        matches[i] &= should[i] >= minimum;
      }
    };
  }
}
