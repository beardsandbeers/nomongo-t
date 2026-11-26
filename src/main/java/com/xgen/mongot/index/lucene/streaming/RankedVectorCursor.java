package com.xgen.mongot.index.lucene.streaming;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.query.DeadlineExceededException;
import com.xgen.mongot.index.query.operators.StreamingOptions;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.jetbrains.annotations.Nullable;

/** Single-owner ranked stream on one reader snapshot. Later refills can find higher scores. */
public final class RankedVectorCursor implements Closeable {
  public record Hit(int doc, float score) {}

  public record Batch(List<Hit> hits, boolean exhausted) {}

  private record Node(int ord, int doc, float score) {}

  private static final Comparator<Hit> BEST =
      Comparator.comparingDouble(Hit::score).reversed().thenComparingInt(Hit::doc);
  private static final Comparator<Node> NEXT =
      Comparator.comparingDouble(Node::score).reversed().thenComparingInt(Node::ord);

  private final IndexReader reader;
  private final @Nullable QueryTimeout timeout;
  private final StreamingOptions options;
  private final AtomicInteger cached = new AtomicInteger();
  private final List<Leaf> leaves = new ArrayList<>();
  private final PriorityQueue<Leaf> heads =
      new PriorityQueue<>((a, b) -> BEST.compare(a.ready.element(), b.ready.element()));
  private int sinceRefill;
  private int nextLeaf;
  private int winner;
  private boolean fairTurn = true;
  private volatile boolean closeRequested;
  private boolean closed;

  public static RankedVectorCursor open(
      IndexSearcher searcher,
      String field,
      float[] query,
      @Nullable Query filter,
      StreamingOptions options)
      throws IOException {
    return open(searcher, field, query, filter, options, true, options.warmupWidth());
  }

  public static RankedVectorCursor open(
      IndexSearcher searcher,
      String field,
      byte[] query,
      @Nullable Query filter,
      StreamingOptions options)
      throws IOException {
    return open(searcher, field, query, filter, options, true, options.warmupWidth());
  }

  static RankedVectorCursor open(
      IndexSearcher searcher,
      String field,
      float[] query,
      @Nullable Query filter,
      StreamingOptions options,
      boolean finalScores,
      int smallSetLimit)
      throws IOException {
    float[] target = query.clone();
    return new RankedVectorCursor(
        searcher,
        field,
        filter,
        options,
        VectorEncoding.FLOAT32,
        (factory, similarity, values) -> factory.getRandomVectorScorer(similarity, values, target),
        finalScores,
        smallSetLimit);
  }

  static RankedVectorCursor open(
      IndexSearcher searcher,
      String field,
      byte[] query,
      @Nullable Query filter,
      StreamingOptions options,
      boolean finalScores,
      int smallSetLimit)
      throws IOException {
    byte[] target = query.clone();
    return new RankedVectorCursor(
        searcher,
        field,
        filter,
        options,
        VectorEncoding.BYTE,
        (factory, similarity, values) -> factory.getRandomVectorScorer(similarity, values, target),
        finalScores,
        smallSetLimit);
  }

  private RankedVectorCursor(
      IndexSearcher searcher,
      String field,
      @Nullable Query filter,
      StreamingOptions options,
      VectorEncoding encoding,
      StreamingVectors.Target target,
      boolean finalScores,
      int smallSetLimit)
      throws IOException {
    this.reader = searcher.getIndexReader();
    this.timeout = searcher.getTimeout();
    this.options = options;
    this.reader.incRef();
    @Var boolean opened = false;
    try {
      // The request searcher may have a thread-confined explain profiler.
      var membershipSearcher = new IndexSearcher(this.reader);
      membershipSearcher.setQueryCache(null);
      membershipSearcher.setTimeout(this.timeout);
      var tasks = new ArrayList<Callable<Leaf>>();
      for (var ctx : this.reader.leaves()) {
        tasks.add(
            () -> {
              checkTime();
              var vectors = StreamingVectors.open(ctx, field, target, encoding, finalScores);
              if (vectors == null) {
                return null;
              }
              var state =
                  new Leaf(ctx, vectors, CandidateFilter.compile(membershipSearcher, ctx, filter));
              var small = CandidateFilter.smallLead(ctx, filter, smallSetLimit);
              if (small != null) {
                state.scoreSmallSet(small);
              } else if (state.filter != CandidateFilter.NONE) {
                state.warm();
              }
              return state;
            });
      }
      for (Leaf leaf : searcher.getTaskExecutor().invokeAll(tasks)) {
        if (leaf == null) {
          continue;
        }
        leaf.index = this.leaves.size();
        this.leaves.add(leaf);
        if (!leaf.ready.isEmpty()) {
          this.heads.add(leaf);
        }
      }
      opened = true;
    } finally {
      if (!opened) {
        close();
      }
    }
  }

  private void checkTime() throws InterruptedIOException {
    if (this.timeout != null && this.timeout.shouldExit()) {
      throw new DeadlineExceededException();
    }
    if (this.closeRequested || Thread.currentThread().isInterrupted()) {
      throw new InterruptedIOException("Streaming vector search interrupted");
    }
  }

  /** Number of distinct scored ordinals retained across the request. */
  public int cachedNodes() {
    return this.cached.get();
  }

  /** Each work unit permits one leaf refill or one hit delivery. An empty batch can be pending. */
  public synchronized Batch poll(int maxHits, @Var int workUnits) throws IOException {
    if (maxHits < 1 || workUnits < 1) {
      throw new IllegalArgumentException("Expected positive poll limits");
    }
    if (this.closed) {
      throw new IllegalStateException("Vector cursor closed");
    }
    var hits = new ArrayList<Hit>();
    try {
      while (workUnits-- > 0 && hits.size() < maxHits) {
        checkTime();
        if (this.heads.isEmpty() || this.sinceRefill >= this.options.refillEvery()) {
          Leaf leaf = refillLeaf();
          this.sinceRefill = 0;
          if (leaf != null) {
            this.heads.remove(leaf);
            leaf.expand(this.options.refillExpansions(), Integer.MAX_VALUE);
            if (!leaf.ready.isEmpty()) {
              this.heads.add(leaf);
            }
            continue;
          }
        }
        if (this.heads.isEmpty()) {
          break;
        }
        var leaf = this.heads.remove();
        hits.add(leaf.ready.remove());
        this.winner = leaf.index;
        this.sinceRefill++;
        if (!leaf.ready.isEmpty()) {
          this.heads.add(leaf);
        }
      }
      return new Batch(
          List.copyOf(hits),
          this.heads.isEmpty() && this.leaves.stream().allMatch(l -> l.frontier.isEmpty()));
    } catch (IOException | RuntimeException e) {
      close();
      throw e;
    }
  }

  /** Terminal operation for the finite stage, whose provisional hits use traversal scores. */
  void refineCandidates(List<Hit> hits) throws IOException {
    this.heads.clear();
    var best = new PriorityQueue<Hit>(hits.size(), BEST.reversed());
    best.addAll(hits);
    // Every already-scored, passing hit may compete, including the final refill's buffered hits.
    for (var leaf : this.leaves) {
      leaf.promote(best);
    }
    // Spend at most k extra adjacency expansions across the entire request. A low pass
    // rate can otherwise keep the passing cutoff below most of the graph's traversal scores.
    @Var int expansions = hits.size();
    while (expansions > 0) {
      int remaining = this.options.maxCachedNodes() - cachedNodes();
      if (remaining == 0) {
        break;
      }
      @Var Leaf next = null;
      @Var float cutoff = best.element().score;
      for (Leaf leaf : this.leaves) {
        if (!leaf.frontier.isEmpty() && leaf.frontier.element().score > cutoff) {
          next = leaf;
          cutoff = leaf.frontier.element().score;
        }
      }
      if (next == null) {
        break;
      }
      expansions -= next.expand(Math.min(expansions, this.options.refillExpansions()), remaining);
      next.promote(best);
    }
    hits.clear();
    hits.addAll(best);
  }

  /** Only queried after graph exhaustion when the finite stage still needs more candidates. */
  boolean hasUnreachableVectors() {
    return this.leaves.stream()
        .anyMatch(
            leaf ->
                leaf.walkedGraph && leaf.seen.cardinality() < leaf.vectors.traversal().maxOrd());
  }

  private @Nullable Leaf refillLeaf() {
    if (!this.leaves.isEmpty()
        && !this.fairTurn
        && !this.leaves.get(this.winner).frontier.isEmpty()) {
      this.fairTurn = true;
      return this.leaves.get(this.winner);
    }
    for (int i = 0; i < this.leaves.size(); i++) {
      Leaf leaf = this.leaves.get(this.nextLeaf);
      this.nextLeaf = (this.nextLeaf + 1) % this.leaves.size();
      if (!leaf.frontier.isEmpty()) {
        this.fairTurn = false;
        return leaf;
      }
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    this.closeRequested = true;
    synchronized (this) {
      if (this.closed) {
        return;
      }
      this.closed = true;
      this.heads.clear();
      this.leaves.clear();
      this.reader.decRef();
    }
  }

  private final class Leaf {
    final LeafReaderContext ctx;
    final StreamingVectors vectors;
    final CandidateFilter filter;
    final SparseFixedBitSet seen;
    final PriorityQueue<Node> frontier = new PriorityQueue<>(NEXT);
    final PriorityQueue<Hit> ready = new PriorityQueue<>(BEST);
    final ArrayList<Node> pending = new ArrayList<>();
    int index;
    boolean walkedGraph;

    Leaf(LeafReaderContext ctx, StreamingVectors vectors, CandidateFilter filter) {
      this.ctx = ctx;
      this.vectors = vectors;
      this.seen = new SparseFixedBitSet(vectors.traversal().maxOrd());
      this.filter = filter;
    }

    void remember(int ord, float score) throws IOException {
      if (this.seen.get(ord)) {
        return;
      }
      this.seen.set(ord);
      if (RankedVectorCursor.this.cached.incrementAndGet()
          > RankedVectorCursor.this.options.maxCachedNodes()) {
        throw new IllegalArgumentException(
            "Streaming vector search exceeded maxCachedNodes="
                + RankedVectorCursor.this.options.maxCachedNodes());
      }
      Node node = new Node(ord, this.vectors.traversal().ordToDoc(ord), score);
      if (this.vectors.graph().size() > 0) {
        this.frontier.add(node);
      }
      var live = this.ctx.reader().getLiveDocs();
      if (live == null || live.get(node.doc)) {
        this.pending.add(node);
      }
    }

    void admit() throws IOException {
      if (this.pending.isEmpty()) {
        return;
      }
      this.pending.sort(Comparator.comparingInt(Node::doc));
      int[] docs = new int[this.pending.size()];
      for (int i = 0; i < docs.length; i++) {
        docs[i] = this.pending.get(i).doc;
      }
      boolean[] matches = new boolean[docs.length];
      this.filter.test(docs, matches);
      for (int i = 0; i < docs.length; i++) {
        if ((i & 63) == 0) {
          checkTime();
        }
        if (matches[i]) {
          var n = this.pending.get(i);
          float output =
              this.vectors.output() == this.vectors.traversal()
                  ? n.score
                  : this.vectors.output().score(n.ord);
          this.ready.add(new Hit(this.ctx.docBase + n.doc, output));
        }
      }
      this.pending.clear();
    }

    void scoreSmallSet(DocIdSetIterator candidates) throws IOException {
      var ords = this.vectors.values().iterator();
      for (int doc; (doc = candidates.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS; ) {
        checkTime();
        if (ords.docID() < doc) {
          ords.advance(doc);
        }
        if (ords.docID() == doc) {
          int ord = ords.index();
          remember(ord, this.vectors.traversal().score(ord));
        }
      }
      // Every match must occur in the lead. No graph continuation is necessary.
      this.frontier.clear();
      admit();
    }

    void warm() throws IOException {
      var scorer = this.vectors.traversal();
      if (this.vectors.graph().size() == 0) {
        for (int ord = 0; ord < scorer.maxOrd(); ord++) {
          if ((ord & 63) == 0) {
            checkTime();
          }
          remember(ord, scorer.score(ord));
        }
      } else {
        this.walkedGraph = true;
        RandomVectorScorer recording =
            new RandomVectorScorer() {
              @Override
              public float score(int ord) throws IOException {
                checkTime();
                float value = scorer.score(ord);
                remember(ord, value);
                return value;
              }

              @Override
              public float bulkScore(int[] ords, float[] scores, int count) throws IOException {
                checkTime();
                float max = scorer.bulkScore(ords, scores, count);
                for (int i = 0; i < count; i++) {
                  remember(ords[i], scores[i]);
                }
                return max;
              }

              @Override
              public int maxOrd() {
                return scorer.maxOrd();
              }
            };
        var graph = new WarmupGraph(this.vectors.graph());
        var collector =
            new TopKnnCollector(
                RankedVectorCursor.this.options.warmupWidth(),
                RankedVectorCursor.this.options.warmupVisits());
        HnswGraphSearcher.search(recording, collector, graph, null);
        // A very small visit limit may stop the upper-level descent before a level-zero hit.
        if (this.frontier.isEmpty()) {
          int entry = this.vectors.graph().entryNode();
          remember(entry, scorer.score(entry));
        }
        var completed = graph.completedNodes(collector.earlyTerminated());
        this.frontier.removeIf(node -> completed.contains(node.ord));
      }
      admit();
    }

    private void promote(PriorityQueue<Hit> best) throws IOException {
      while (!this.ready.isEmpty() && BEST.compare(this.ready.element(), best.element()) < 0) {
        checkTime();
        best.remove();
        best.add(this.ready.remove());
      }
    }

    // A finite refinement may stop in its final neighborhood when it spends its remaining cache.
    int expand(int count, @Var int newNodes) throws IOException {
      var graph = this.vectors.graph();
      @Var int expanded = 0;
      while (expanded < count && newNodes > 0 && !this.frontier.isEmpty()) {
        expanded++;
        checkTime();
        graph.seek(0, this.frontier.remove().ord);
        for (int ord; (ord = graph.nextNeighbor()) != DocIdSetIterator.NO_MORE_DOCS; ) {
          if (!this.seen.get(ord)) {
            remember(ord, this.vectors.traversal().score(ord));
            if (--newNodes == 0) {
              break;
            }
          }
        }
      }
      admit();
      return expanded;
    }
  }
}
