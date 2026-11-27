package com.xgen.mongot.index.lucene.streaming;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.lucene.explain.knn.VectorSearchExplainer;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.DeadlineExceededException;
import com.xgen.mongot.index.query.operators.StreamingOptions;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.jetbrains.annotations.Nullable;

/** Ranked stream on one reader snapshot. Later refills can find higher scores. */
public final class RankedVectorCursor implements Closeable {
  public record Hit(int doc, float score) {}

  public record Batch(List<Hit> hits, boolean exhausted) {}

  private record Node(int ord, int doc, float score) {}

  private static final Comparator<Hit> BEST =
      Comparator.comparingDouble(Hit::score).reversed().thenComparingInt(Hit::doc);
  private static final Comparator<Node> NEXT =
      Comparator.comparingDouble(Node::score).reversed().thenComparingInt(Node::ord);

  private final IndexReader reader;

  enum Use {
    CURSOR,
    FINITE_STAGE,
    PAGED_STAGE
  }

  private final Use use;
  private final @Nullable QueryTimeout timeout;
  private final StreamingOptions options;
  private final AtomicInteger cached = new AtomicInteger();
  private final List<Leaf> leaves = new ArrayList<>();
  private final PriorityQueue<Leaf> heads =
      new PriorityQueue<>((a, b) -> BEST.compare(a.ready.element(), b.ready.element()));
  private final @Nullable StreamPlan plan;
  private boolean cacheLimited;

  private record BufferedHit(Hit hit, Leaf owner) {}

  private final TreeSet<BufferedHit> window =
      new TreeSet<>(Comparator.comparing(BufferedHit::hit, BEST));
  private int refillTurn;
  private final int startupScored;
  private int sinceRefill;
  private int nextLeaf;
  private int winner;
  private boolean fairTurn = true;
  private volatile boolean closeRequested;
  private boolean closed;

  // A worker owns traversal for one slice. Only the cursor monitor publishes its hits.
  private @Nullable Executor prefetchExecutor;
  private boolean prefetchScheduled;
  private boolean prefetchRunning;
  private int prefetchCredit;
  private int prefetchRefills;
  private @Nullable Throwable prefetchFailure;

  private static final class PrefetchPool {
    private static final int THREADS = Math.min(4, Runtime.getRuntime().availableProcessors());
    private static final ThreadPoolExecutor INSTANCE =
        new ThreadPoolExecutor(
            THREADS,
            THREADS,
            30,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(64),
            Thread.ofPlatform().daemon().name("vector-stream-prefetch-", 0).factory());

    static {
      INSTANCE.allowCoreThreadTimeOut(true);
    }
  }

  /**
   * Starts bounded read-ahead for an adaptive cursor. Its owner must close it before its reader.
   */
  public void startPrefetch() {
    startPrefetch(PrefetchPool.INSTANCE);
  }

  synchronized void startPrefetch(Executor executor) {
    if (this.closed || this.use == Use.FINITE_STAGE || this.plan == null) {
      throw new IllegalStateException("Read-ahead requires an open adaptive cursor");
    }
    if (this.prefetchExecutor == null) {
      this.prefetchExecutor = executor;
      this.prefetchCredit = this.plan.prefetchWorkLimit();
      schedulePrefetch();
    }
  }

  // Called with the cursor monitor held. Saturation leaves demand-driven work available to poll().
  private void schedulePrefetch() {
    if (this.prefetchExecutor == null
        || this.prefetchScheduled
        || this.closeRequested
        || this.prefetchFailure != null
        || this.prefetchCredit == 0
        || cachedNodes() >= this.options.maxCachedNodes()
        || this.leaves.stream().noneMatch(Leaf::canExpand)) {
      return;
    }
    this.prefetchScheduled = true;
    try {
      this.prefetchExecutor.execute(this::prefetch);
    } catch (RejectedExecutionException saturated) {
      this.prefetchScheduled = false;
    }
  }

  private void prefetch() {
    Leaf leaf;
    int block;
    int before;
    synchronized (this) {
      block =
          Math.min(
              64, Math.min(this.prefetchCredit, this.options.maxCachedNodes() - cachedNodes()));
      if (this.closeRequested || block <= 0 || (leaf = selectAdaptiveLeaf()) == null) {
        this.prefetchScheduled = false;
        return;
      }
      this.prefetchRunning = true;
      this.prefetchCredit -= block;
      before = leaf.scored;
    }
    // Poll can deliver existing hits while this worker is doing graph, predicate or vector I/O.
    var found = new ArrayList<Hit>();
    try {
      checkTime();
      advance(leaf, block, block, found);
      synchronized (this) {
        this.heads.remove(leaf);
        leaf.ready.addAll(found);
        finishRefill(leaf, before);
        this.sinceRefill = 0;
        this.prefetchRefills++;
      }
    } catch (IOException | RuntimeException | Error failure) {
      synchronized (this) {
        this.prefetchFailure = failure;
      }
    } finally {
      synchronized (this) {
        this.prefetchRunning = false;
        this.prefetchScheduled = false;
        notifyAll();
        schedulePrefetch();
      }
    }
  }

  public static RankedVectorCursor open(
      IndexSearcher searcher,
      String field,
      float[] query,
      @Nullable Query filter,
      StreamingOptions options)
      throws IOException {
    return open(
        searcher,
        field,
        query,
        filter,
        options,
        Use.CURSOR,
        options.adaptive().isPresent() ? 256 : options.warmupWidth());
  }

  public static RankedVectorCursor open(
      IndexSearcher searcher,
      String field,
      byte[] query,
      @Nullable Query filter,
      StreamingOptions options)
      throws IOException {
    return open(
        searcher,
        field,
        query,
        filter,
        options,
        Use.CURSOR,
        options.adaptive().isPresent() ? 256 : options.warmupWidth());
  }

  static RankedVectorCursor open(
      IndexSearcher searcher,
      String field,
      float[] query,
      @Nullable Query filter,
      StreamingOptions options,
      Use use,
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
        use,
        smallSetLimit);
  }

  static RankedVectorCursor open(
      IndexSearcher searcher,
      String field,
      byte[] query,
      @Nullable Query filter,
      StreamingOptions options,
      Use use,
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
        use,
        smallSetLimit);
  }

  private RankedVectorCursor(
      IndexSearcher searcher,
      String field,
      @Nullable Query filter,
      StreamingOptions options,
      VectorEncoding encoding,
      StreamingVectors.Target target,
      Use use,
      int smallSetLimit)
      throws IOException {
    this.use = use;
    this.reader = searcher.getIndexReader();
    this.timeout = searcher.getTimeout();
    this.options = options;
    this.plan =
        options.adaptive().isPresent()
            ? StreamPlan.resolve(options, smallSetLimit, this.reader, field, encoding)
            : null;
    if (this.use != Use.PAGED_STAGE) {
      this.reader.incRef();
    }
    @Var boolean opened = false;
    try {
      // The request searcher may have a thread-confined explain profiler.
      var membershipSearcher = new IndexSearcher(this.reader);
      membershipSearcher.setQueryCache(null);
      membershipSearcher.setTimeout(
          () -> this.closeRequested || (this.timeout != null && this.timeout.shouldExit()));
      var tasks = new ArrayList<Callable<Leaf>>();
      for (var ctx : this.reader.leaves()) {
        tasks.add(
            () -> {
              checkTime();
              var vectors =
                  StreamingVectors.open(ctx, field, target, encoding, use != Use.FINITE_STAGE);
              if (vectors == null) {
                return null;
              }
              var state =
                  new Leaf(ctx, vectors, CandidateFilter.compile(membershipSearcher, ctx, filter));
              if (state.filter == CandidateFilter.NONE) {
                return state;
              }
              var small = CandidateFilter.smallLead(ctx, filter, smallSetLimit);
              if (this.plan != null && (small != null || vectors.graph().size() == 0)) {
                state.scan = small != null ? small : DocIdSetIterator.all(ctx.reader().maxDoc());
                state.scanVectors = vectors.values().iterator();
                state.scan(this.plan.pilotVisits(), Integer.MAX_VALUE, state.ready);
              } else if (small != null) {
                state.scoreSmallSet(small);
              } else if (this.plan != null && this.plan.pilotVisits() == 0) {
                state.needsEntry = true;
              } else {
                state.warm();
              }
              state.pilotScored = state.scored;
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
      if (this.plan != null) {
        rebalance();
        while (cachedNodes() < this.plan.initialScoreBudget()
            && refillAdaptive(this.plan.initialScoreBudget() - cachedNodes())) {
          checkTime();
        }
      }
      this.startupScored = cachedNodes();
      this.leaves.forEach(l -> l.startupScored = l.scored);
      opened = true;
    } finally {
      if (!opened) {
        close();
      }
    }
  }

  // Only discovered scores participate in this merge. Displaced hits remain eligible locally.
  private int rebalance() {
    @Var int promoted = 0;
    int capacity = Objects.requireNonNull(this.plan).lookaheadHits();
    while (!this.heads.isEmpty()) {
      Leaf leaf = this.heads.element();
      if (this.window.size() == capacity
          && BEST.compare(leaf.ready.element(), this.window.last().hit()) >= 0) {
        break;
      }
      this.heads.remove();
      Hit hit = leaf.ready.remove();
      if (!leaf.ready.isEmpty()) {
        this.heads.add(leaf);
      }
      if (this.window.size() == capacity) {
        var evicted = this.window.removeLast();
        Leaf owner = evicted.owner();
        this.heads.remove(owner);
        owner.ready.add(evicted.hit());
        this.heads.add(owner);
      }
      this.window.add(new BufferedHit(hit, leaf));
      promoted++;
    }
    return promoted;
  }

  private @Nullable Leaf selectAdaptiveLeaf() {
    @Var Leaf best = null;
    boolean probe = (++this.refillTurn % 8) == 0;
    for (int i = 0; i < this.leaves.size(); i++) {
      Leaf leaf = this.leaves.get((this.nextLeaf + i) % this.leaves.size());
      if (!leaf.canExpand()) {
        continue;
      }
      if (probe) {
        best = leaf;
        this.nextLeaf = (leaf.index + 1) % this.leaves.size();
        break;
      }
      if (best == null || leaf.priority() > best.priority()) {
        best = leaf;
      }
    }
    return best;
  }

  private boolean refillAdaptive(int available) throws IOException {
    Leaf leaf = selectAdaptiveLeaf();
    if (leaf == null) {
      return false;
    }
    this.heads.remove(leaf);
    int before = leaf.scored;
    advance(leaf, Math.min(available, 256), Integer.MAX_VALUE, leaf.ready);
    finishRefill(leaf, before);
    return true;
  }

  private void advance(Leaf leaf, int scores, int steps, Collection<Hit> found) throws IOException {
    if (leaf.scan != null) {
      leaf.scan(scores, steps, found);
    } else if (leaf.needsEntry) {
      leaf.needsEntry = false;
      leaf.walkedGraph = true;
      int ord = leaf.vectors.graph().entryNode();
      leaf.remember(ord, leaf.vectors.traversal().score(ord));
      leaf.admit(found);
    } else {
      leaf.expand(
          Math.min(Objects.requireNonNull(this.plan).refillExpansions(), steps), scores, found);
    }
  }

  private void finishRefill(Leaf leaf, int before) {
    leaf.refills++;
    if (!leaf.ready.isEmpty()) {
      this.heads.add(leaf);
    }
    int admitted = rebalance();
    leaf.admissionRate = (admitted + 1f) / (leaf.scored - before + 16f);
  }

  private Batch pollAdaptive(int maxHits, @Var int units) throws IOException {
    var hits = new ArrayList<Hit>();
    while (units-- > 0 && hits.size() < maxHits) {
      checkTime();
      if (this.prefetchFailure != null) {
        throw IOUtils.rethrowAlways(this.prefetchFailure);
      }
      if (this.window.isEmpty()
          || this.sinceRefill >= Objects.requireNonNull(this.plan).refillEvery()) {
        if (this.prefetchRunning) {
          if (!hits.isEmpty()) {
            break;
          }
          try {
            wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Interrupted waiting for vector read-ahead");
          }
          continue;
        }
        this.sinceRefill = 0;
        int remaining = this.options.maxCachedNodes() - cachedNodes();
        // Only a finite stage can restart with classic ANN. A generic cursor has already
        // published its prefix and must report the cap if another discovery is required.
        if (remaining == 0 && this.window.isEmpty() && this.use == Use.FINITE_STAGE) {
          this.cacheLimited =
              this.leaves.stream()
                  .anyMatch(
                      leaf -> leaf.canExpand() && leaf.scored < leaf.vectors.traversal().maxOrd());
          return new Batch(List.copyOf(hits), true);
        }
        if (remaining == 0 && this.window.isEmpty() && !hits.isEmpty()) {
          break;
        }
        if ((remaining > 0 || this.window.isEmpty()) && refillAdaptive(Math.max(1, remaining))) {
          continue;
        }
      }
      if (this.window.isEmpty()) {
        break;
      }
      var top = this.window.removeFirst();
      hits.add(top.hit());
      top.owner().delivered++;
      this.sinceRefill++;
      rebalance();
    }
    boolean exhausted =
        !this.prefetchRunning
            && this.window.isEmpty()
            && this.leaves.stream().noneMatch(Leaf::canExpand);
    if (this.prefetchExecutor != null && !exhausted) {
      this.prefetchCredit +=
          Math.min(
              Objects.requireNonNull(this.plan).prefetchWorkLimit() - this.prefetchCredit,
              hits.size());
      schedulePrefetch();
    }
    return new Batch(List.copyOf(hits), exhausted);
  }

  /** Publish cumulative execution counters for a live consumer. */
  public void explainPage(long startupNanos, long collectionNanos) {
    Explain.getQueryInfo()
        .ifPresent(
            info -> {
              var stats =
                  explanation(
                      info.getVerbosity().isGreaterThan(Explain.Verbosity.QUERY_PLANNER), false);
              if (info.getVerbosity().isGreaterThan(Explain.Verbosity.QUERY_PLANNER)) {
                stats.put("startupMillis", new org.bson.BsonDouble(startupNanos / 1_000_000d));
                stats.put(
                    "candidateCollectionMillis",
                    new org.bson.BsonDouble(collectionNanos / 1_000_000d));
              }
              stats.put("openEnded", BsonBoolean.TRUE);
              info.getFeatureExplainer(VectorSearchExplainer.class, VectorSearchExplainer::new)
                  .setStreamStats(stats);
            });
  }

  synchronized BsonDocument explanation(boolean execution, boolean fallback) {
    var doc =
        this.plan != null
            ? this.plan.describe(this.options, this.prefetchExecutor != null)
            : this.options.toBson();
    doc.put("maxCachedNodes", new BsonInt32(this.options.maxCachedNodes()));
    doc.put("rescoreAll", BsonBoolean.valueOf(this.options.rescoreAll()));
    if (execution) {
      // Segment counters reflect complete slices, including for explain on a live paged cursor.
      awaitPrefetch();
      if (this.prefetchExecutor != null) {
        doc.put("prefetchRefills", new BsonInt32(this.prefetchRefills));
      }
      doc.put("startupScored", new BsonInt32(this.startupScored));
      doc.put("totalDistinctScored", new BsonInt32(cachedNodes()));
      doc.put("fallbackToClassic", BsonBoolean.valueOf(fallback));
      if (fallback) {
        doc.put(
            "fallbackReason",
            new BsonString(this.cacheLimited ? "cacheLimit" : "unreachableVectors"));
      }
      var segments = new BsonArray();
      for (Leaf leaf : this.leaves) {
        segments.add(
            new BsonDocument("ordinal", new BsonInt32(leaf.ctx.ord))
                .append("strategy", new BsonString(leaf.strategy()))
                .append("format", new BsonString(leaf.vectors.format()))
                .append("graphScoring", new BsonString(leaf.vectors.scoring().name()))
                .append(
                    "stageRescoring",
                    new BsonString(
                        this.use == Use.FINITE_STAGE
                            ? leaf.vectors.scoring().stageRescoring(this.options.rescoreAll())
                            : (leaf.vectors.output() == leaf.vectors.traversal()
                                ? "none"
                                : "retainedOnAdmission")))
                .append("encoding", new BsonString(leaf.vectors.values().getEncoding().name()))
                .append(
                    "startupAdaptiveScored", new BsonInt32(leaf.startupScored - leaf.pilotScored))
                .append("pilotScored", new BsonInt32(leaf.pilotScored))
                .append("totalDistinctScored", new BsonInt32(leaf.scored))
                .append("filterAccepted", new BsonInt32(leaf.accepted))
                .append("refills", new BsonInt32(leaf.refills))
                .append("deliveredCandidates", new BsonInt32(leaf.delivered)));
      }
      doc.put("segments", segments);
    }
    return doc;
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
      if (this.plan != null) {
        return pollAdaptive(maxHits, workUnits);
      }
      while (workUnits-- > 0 && hits.size() < maxHits) {
        checkTime();
        if (this.heads.isEmpty() || this.sinceRefill >= this.options.refillEvery()) {
          Leaf leaf = refillLeaf();
          this.sinceRefill = 0;
          if (leaf != null) {
            this.heads.remove(leaf);
            leaf.expand(this.options.refillExpansions(), Integer.MAX_VALUE, leaf.ready);
            leaf.refills++;
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
        leaf.delivered++;
        this.winner = leaf.index;
        this.sinceRefill++;
        if (!leaf.ready.isEmpty()) {
          this.heads.add(leaf);
        }
      }
      return new Batch(
          List.copyOf(hits),
          this.heads.isEmpty() && this.leaves.stream().allMatch(l -> l.frontier.isEmpty()));
    } catch (IOException | RuntimeException | Error e) {
      close();
      throw e;
    }
  }

  /** Terminal operation for the finite stage, whose provisional hits use traversal scores. */
  void refineCandidates(List<Hit> hits) throws IOException {
    this.heads.clear();
    for (var buffered : this.window) {
      buffered.owner().ready.add(buffered.hit());
    }
    this.window.clear();
    var best = new PriorityQueue<Hit>(hits.size(), BEST.reversed());
    best.addAll(hits);
    // Every already-scored, passing hit may compete, including the final refill's buffered hits.
    for (var leaf : this.leaves) {
      leaf.promote(best);
    }
    // Spend at most k extra adjacency expansions across the entire request. A low pass
    // rate can otherwise keep the passing cutoff below most of the graph's traversal scores.
    @Var int expansions = this.plan == null ? hits.size() : 0;
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
      expansions -=
          next.expand(Math.min(expansions, this.options.refillExpansions()), remaining, next.ready);
      next.promote(best);
    }
    hits.clear();
    hits.addAll(best);
  }

  /** The available walk ended before the finite stage could fill its candidate pool. */
  boolean needsClassicFallback() {
    return this.cacheLimited
        || this.leaves.stream()
            .anyMatch(
                leaf ->
                    leaf.walkedGraph
                        && leaf.seen.cardinality() < leaf.vectors.traversal().maxOrd());
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
      awaitPrefetch();
      if (this.closed) {
        return;
      }
      this.closed = true;
      this.heads.clear();
      this.leaves.clear();
      this.window.clear();
      if (this.use != Use.PAGED_STAGE) {
        this.reader.decRef();
      }
    }
  }

  // Cleanup must retain the pinned reader until an active worker has left Lucene.
  private void awaitPrefetch() {
    @Var boolean interrupted = false;
    while (this.prefetchRunning) {
      try {
        wait();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /** Lucene may score a final batch beyond its visit limit. Retain that unfinished neighborhood. */
  private static final class PilotComplete extends RuntimeException {
    static final PilotComplete INSTANCE = new PilotComplete();

    private PilotComplete() {
      super(null, null, false, false);
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
    boolean needsEntry;
    @Nullable DocIdSetIterator scan;
    @Nullable KnnVectorValues.DocIndexIterator scanVectors;
    int scored;
    int pilotScored;
    int startupScored;
    int accepted;
    int delivered;
    int refills;
    float admissionRate;

    String strategy() {
      if (this.filter == CandidateFilter.NONE) {
        return "noMatches";
      }
      if (this.walkedGraph || this.needsEntry) {
        return "hnsw";
      }
      return this.vectors.graph().size() == 0 ? "graphlessScan" : "smallSetScan";
    }

    boolean canExpand() {
      return this.needsEntry || this.scan != null || !this.frontier.isEmpty();
    }

    float priority() {
      return this.frontier.isEmpty()
          ? Float.POSITIVE_INFINITY
          : this.frontier.element().score() * (1f + 0.1f * this.admissionRate);
    }

    void scan(@Var int budget, @Var int steps, Collection<Hit> found) throws IOException {
      var docs = Objects.requireNonNull(this.scan);
      var vectors = Objects.requireNonNull(this.scanVectors);
      while (budget > 0 && steps-- > 0) {
        checkTime();
        int doc = docs.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          this.scan = null;
          break;
        }
        if (vectors.docID() < doc) {
          vectors.advance(doc);
        }
        if (vectors.docID() == doc) {
          int ord = vectors.index();
          remember(ord, this.vectors.traversal().score(ord));
          budget--;
        }
      }
      admit(found);
    }

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
      this.scored++;
      if (RankedVectorCursor.this.cached.incrementAndGet()
          > RankedVectorCursor.this.options.maxCachedNodes()) {
        throw new IllegalArgumentException(
            "Streaming vector search exceeded maxCachedNodes="
                + RankedVectorCursor.this.options.maxCachedNodes());
      }
      Node node = new Node(ord, this.vectors.traversal().ordToDoc(ord), score);
      if (this.scan == null && this.vectors.graph().size() > 0) {
        this.frontier.add(node);
      }
      var live = this.ctx.reader().getLiveDocs();
      if (live == null || live.get(node.doc)) {
        this.pending.add(node);
      }
    }

    void admit(Collection<Hit> found) throws IOException {
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
          this.accepted++;
          var n = this.pending.get(i);
          float output =
              this.vectors.output() == this.vectors.traversal()
                  ? n.score
                  : this.vectors.output().score(n.ord);
          found.add(new Hit(this.ctx.docBase + n.doc, output));
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
      admit(this.ready);
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
        int pilotLimit =
            RankedVectorCursor.this.plan == null
                ? Integer.MAX_VALUE
                : RankedVectorCursor.this.plan.pilotVisits();
        RandomVectorScorer recording =
            new RandomVectorScorer() {
              @Override
              public float score(int ord) throws IOException {
                checkTime();
                if (Leaf.this.scored >= pilotLimit && !Leaf.this.seen.get(ord)) {
                  throw PilotComplete.INSTANCE;
                }
                float value = scorer.score(ord);
                remember(ord, value);
                return value;
              }

              @Override
              public float bulkScore(int[] ords, float[] scores, int count) throws IOException {
                checkTime();
                int allowed = Math.min(count, pilotLimit - Leaf.this.scored);
                if (allowed == 0) {
                  throw PilotComplete.INSTANCE;
                }
                float max = scorer.bulkScore(ords, scores, allowed);
                for (int i = 0; i < allowed; i++) {
                  remember(ords[i], scores[i]);
                }
                if (allowed < count) {
                  throw PilotComplete.INSTANCE;
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
                RankedVectorCursor.this.plan == null
                    ? RankedVectorCursor.this.options.warmupWidth()
                    : RankedVectorCursor.this.plan.pilotWidth(),
                RankedVectorCursor.this.plan == null
                    ? RankedVectorCursor.this.options.warmupVisits()
                    : pilotLimit);
        @Var boolean stopped = false;
        try {
          HnswGraphSearcher.search(recording, collector, graph, null);
        } catch (PilotComplete exhausted) {
          stopped = true;
        }
        // A very small visit limit may stop the upper-level descent before a level-zero hit.
        if (this.frontier.isEmpty()) {
          int entry = this.vectors.graph().entryNode();
          remember(entry, scorer.score(entry));
        }
        var completed = graph.completedNodes(stopped || collector.earlyTerminated());
        this.frontier.removeIf(node -> completed.contains(node.ord));
      }
      admit(this.ready);
    }

    private void promote(PriorityQueue<Hit> best) throws IOException {
      while (!this.ready.isEmpty() && BEST.compare(this.ready.element(), best.element()) < 0) {
        checkTime();
        best.remove();
        best.add(this.ready.remove());
      }
    }

    // A finite refinement may stop in its final neighborhood when it spends its remaining cache.
    int expand(int count, @Var int newNodes, Collection<Hit> found) throws IOException {
      var graph = this.vectors.graph();
      @Var int expanded = 0;
      while (expanded < count && newNodes > 0 && !this.frontier.isEmpty()) {
        expanded++;
        checkTime();
        Node parent = this.frontier.remove();
        graph.seek(0, parent.ord);
        for (int ord; (ord = graph.nextNeighbor()) != DocIdSetIterator.NO_MORE_DOCS; ) {
          if (!this.seen.get(ord)) {
            if (newNodes == 0) {
              if (RankedVectorCursor.this.plan != null) {
                this.frontier.add(parent);
              }
              break;
            }
            remember(ord, this.vectors.traversal().score(ord));
            newNodes--;
          }
        }
      }
      admit(found);
      return expanded;
    }
  }
}
