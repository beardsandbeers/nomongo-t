package com.xgen.mongot.standalone.coexistence;

import com.google.errorprone.annotations.Var;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.catalogservice.MongodTopologyMonitor;
import com.xgen.mongot.config.manager.ConfigState.SyncSourceSnapshot;
import com.xgen.mongot.config.manager.QuarantinedIndexLookup;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.extensions.join.JoinExecutionContext;
import com.xgen.mongot.extensions.join.JoinPlanner;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.SearchIndexReader.QuerySnapshot;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.operators.JoinField;
import com.xgen.mongot.index.query.operators.JoinSource;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.types.ObjectId;

/** Native adapter. Foreign end-user authorization is deferred: see repository issue #3. */
public final class CoupledJoinService implements AutoCloseable {
  private final IndexCatalog indexes;
  private final InitializedIndexCatalog initialized;
  private final MongodTopologyMonitor topology;
  private final QuarantinedIndexLookup quarantine;
  private final Supplier<SyncSourceSnapshot> source;
  private final SyncSourceConfig configuredSource;
  private final MongoClient metadata;
  private final MeterRegistry metrics;
  private final JoinPlanner.MemoryLimits memoryLimits;
  private final Semaphore preparation = new Semaphore(4);

  /** Creates a bounded metadata pool isolated from replication and ordinary queries. */
  public CoupledJoinService(
      IndexCatalog indexes,
      InitializedIndexCatalog initialized,
      MongodTopologyMonitor topology,
      QuarantinedIndexLookup quarantine,
      SyncSourceConfig configuredSource,
      Supplier<SyncSourceSnapshot> source,
      MeterRegistry metrics,
      JoinPlanner.MemoryLimits memoryLimits) {
    this(
        indexes,
        initialized,
        topology,
        quarantine,
        configuredSource,
        source,
        metrics,
        memoryLimits,
        createMetadataClient(configuredSource));
  }

  CoupledJoinService(
      IndexCatalog indexes,
      InitializedIndexCatalog initialized,
      MongodTopologyMonitor topology,
      QuarantinedIndexLookup quarantine,
      SyncSourceConfig configuredSource,
      Supplier<SyncSourceSnapshot> source,
      MeterRegistry metrics,
      JoinPlanner.MemoryLimits memoryLimits,
      MongoClient metadata) {
    this.metrics = metrics;
    this.memoryLimits = memoryLimits;
    metrics.gauge("coupled.joins.reservedBytes", this, ignored -> JoinPlanner.retainedBytes());
    this.indexes = indexes;
    this.initialized = initialized;
    this.topology = topology;
    this.quarantine = quarantine;
    this.configuredSource = configuredSource;
    this.source = source;
    this.metadata = metadata;
  }

  private static MongoClient createMetadataClient(SyncSourceConfig configuredSource) {
    var connection = configuredSource.mongodClusterReadWriteUri;
    var settings =
        MongoClientSettings.builder()
            .applyConnectionString(connection.uri())
            .applicationName("coupled-join-metadata")
            .timeout(30, TimeUnit.SECONDS)
            .applyToConnectionPoolSettings(b -> b.maxSize(4).maxWaitTime(1, TimeUnit.SECONDS))
            .applyToClusterSettings(b -> b.serverSelectionTimeout(2, TimeUnit.SECONDS))
            .applyToSocketSettings(
                b -> b.connectTimeout(2, TimeUnit.SECONDS).readTimeout(2, TimeUnit.SECONDS));
    connection.oidcCredential().ifPresent(settings::credential);
    connection
        .sslContextFactory()
        .ifPresent(
            factory -> settings.applyToSslSettings(b -> b.context(factory.getJavaContext())));
    return MongoClients.create(settings.build());
  }

  public static boolean contains(SearchQuery query) {
    return JoinPlanner.containsJoin(JoinPlanner.operator(query));
  }

  /** Missing outer indexes preserve the ordinary native empty-cursor behavior. */
  public Optional<Prepared> prepare(String db, UUID uuid, Optional<String> view, SearchQuery query)
      throws Exception {
    var outer = this.indexes.getIndex(db, uuid, view, query.index());
    if (outer.isEmpty()
        || this.initialized.getIndex(outer.get().getGenerationId()).isEmpty()
        || outer.get().getIndex().getStatus().getStatusCode()
            == IndexStatus.StatusCode.DOES_NOT_EXIST) {
      return Optional.empty();
    }
    if (!this.preparation.tryAcquire()) {
      throw new InvalidQueryException("Join preparation capacity exhausted");
    }
    try {
      var invocation = JoinExecutionContext.capture();
      long deadline =
          System.nanoTime()
              + Math.min(TimeUnit.SECONDS.toNanos(30), JoinExecutionContext.remainingNanos());
      Runnable check =
          () -> {
            invocation.run();
            if (System.nanoTime() >= deadline) {
              throw new Failure("Join preparation deadline exceeded");
            }
          };
      check.run();
      if (view.isPresent() || this.configuredSource.isSharded || this.topology.isShardedCluster()) {
        throw new InvalidQueryException(
            "Coupled joins require non-sharded collections, without views");
      }
      var capturedSource = this.source.get();
      var sourceConfig =
          capturedSource.config().orElseThrow(() -> new Failure("Join source unavailable"));
      if (!sourceConfig.mongodClusterReadWriteUri.equals(
          this.configuredSource.mongodClusterReadWriteUri)) {
        throw new Failure("Join source configuration changed; restart required");
      }
      Map<String, UUID> namespaces = new LinkedHashMap<>();
      JoinPlanner.Environment environment =
          new JoinPlanner.Environment() {
            public String resolve(JoinSource source) throws Exception {
              if (!(source instanceof JoinSource.CollectionIndex target)) {
                throw invalid(
                    "Coupled joins require from: {collection, index}; string sources are"
                        + " standalone-only");
              }
              @Var UUID foreignUuid = namespaces.get(target.collection());
              if (foreignUuid == null) {
                foreignUuid = lookup(db, target.collection(), deadline, check);
                namespaces.put(target.collection(), foreignUuid);
              }
              CoupledJoinService.this.quarantine.throwIfQuarantined(
                  db, foreignUuid, target.index());
              return CoupledJoinService.this
                  .indexes
                  .getIndex(db, foreignUuid, Optional.empty(), target.index())
                  .orElseThrow(() -> invalid("Foreign join index does not exist"))
                  .getGenerationId()
                  .indexId
                  .toHexString();
            }

            public GenerationId pin(String id, Optional<GenerationId> expected) throws Exception {
              var index =
                  CoupledJoinService.this
                      .indexes
                      .getIndexById(new ObjectId(id))
                      .orElseThrow(() -> invalid("Join dependency does not exist"));
              var generation = index.getGenerationId();
              if (expected.isPresent() && !expected.get().equals(generation)) {
                throw invalid("Join generation changed during preparation");
              }
              verify(id, generation);
              var ready = CoupledJoinService.this.initialized.getIndex(generation).orElseThrow();
              ready.throwIfUnavailableForQuerying();
              if (expected.isPresent()
                  && Explain.getExplainQueryState().isPresent()
                  && Explain.getExplainQueryState().get().getNumPartitions()
                      != ready.asSearchIndex().getDefinition().getNumPartitions()) {
                throw invalid("Join generation changed the explain partition layout");
              }
              return generation;
            }

            public QuerySnapshot acquire(GenerationId generation, boolean concurrent)
                throws Exception {
              return CoupledJoinService.this
                  .initialized
                  .getIndex(generation)
                  .orElseThrow()
                  .asSearchIndex()
                  .getReader()
                  .acquireQuerySnapshot(concurrent);
            }

            public void verify(String id, GenerationId generation) {
              if (CoupledJoinService.this.source.get().epoch() != capturedSource.epoch()) {
                throw invalid("Join source configuration changed");
              }
              var live = CoupledJoinService.this.indexes.getIndexById(new ObjectId(id));
              var ready = CoupledJoinService.this.initialized.getIndex(generation);
              if (live.isEmpty()
                  || !live.get().getGenerationId().equals(generation)
                  || ready.isEmpty()
                  || !(ready.get() instanceof InitializedSearchIndex)
                  || ready.get().getDefinition().getView().isPresent()
                  || ready.get().getStatus().getStatusCode()
                      == IndexStatus.StatusCode.DOES_NOT_EXIST) {
                throw invalid(
                    "Join dependency is unavailable, retired, a view, or not a search index");
              }
              try {
                ready.get().throwIfUnavailableForQuerying();
              } catch (Exception e) {
                throw invalid("Join dependency is unavailable");
              }
            }

            public void elapsed(String name, long start) {
              CoupledJoinService.this
                  .metrics
                  .timer("coupled." + name)
                  .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            }

            public void count(String name, int value) {
              CoupledJoinService.this.metrics.counter("coupled." + name).increment(value);
            }

            public Runnable cancellation() {
              return check;
            }

            public RuntimeException invalid(String message) {
              return new Failure(message);
            }

            public RuntimeException exhausted(String message) {
              return new Failure(message);
            }
          };
      var plan =
          new JoinPlanner(environment, this.memoryLimits)
              .prepare(query, outer.get().getGenerationId());
      var prepared = new Prepared(plan, outer.get().getGenerationId());
      prepared.validateInitial =
          () -> {
            if (!this.preparation.tryAcquire()) {
              throw new Failure("Join preparation capacity exhausted");
            }
            try {
              for (var entry : namespaces.entrySet()) {
                if (!lookup(db, entry.getKey(), deadline, check).equals(entry.getValue())) {
                  throw new Failure("Join collection identity changed during preparation");
                }
              }
              plan.verifyDependencies();
            } finally {
              this.preparation.release();
            }
          };
      return Optional.of(prepared);
    } finally {
      this.preparation.release();
    }
  }

  private UUID lookup(String db, String collection, long deadline, Runnable check) {
    check.run();
    long remaining = Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
    try (var cursor =
        this.metadata
            .getDatabase(db)
            .withTimeout(Math.min(remaining, 2000), TimeUnit.MILLISECONDS)
            .listCollections(BsonDocument.class)
            .filter(new BsonDocument("name", new BsonString(collection)))
            .batchSize(1)
            .iterator()) {
      if (!cursor.hasNext()) {
        throw new Failure("Foreign join collection does not exist");
      }
      var info = cursor.next();
      if (!info.getString("type").getValue().equals("collection")) {
        throw new Failure("Joins do not support views");
      }
      var uuid = info.getDocument("info").getBinary("uuid").asUuid();
      check.run();
      return uuid;
    }
  }

  @Override
  public void close() {
    this.metadata.close();
  }

  static final class Failure extends RuntimeException {
    Failure(String message) {
      super(message);
    }
  }

  /** Ownership transfers to the native producer, including idle expiry and index retirement. */
  static final class Prepared implements QuerySnapshot {
    final JoinPlanner.Plan plan;
    final GenerationId generation;
    private Optional<BatchProducer> producer = Optional.empty();
    private boolean closed;
    private Runnable onClose = () -> {};
    Runnable validateInitial = () -> {};

    Prepared(JoinPlanner.Plan plan, GenerationId generation) {
      this.plan = plan;
      this.generation = generation;
    }

    synchronized void registered(Runnable onClose) {
      if (this.closed) {
        onClose.run();
      } else {
        this.onClose = onClose;
      }
    }

    synchronized boolean consumed() {
      return this.producer.isPresent();
    }

    private void activate() {
      this.plan.activate(JoinExecutionContext.capture());
      this.plan.verifyDependencies();
    }

    public int partitions() {
      return this.plan.snapshot().partitions();
    }

    public void setCancellationCheck(Runnable check) {
      this.plan.activate(check);
    }

    public List<JoinField> joinFields(FieldPath path) throws InvalidQueryException {
      return this.plan.snapshot().joinFields(path);
    }

    public void collectKeys(
        Operator op, FieldPath path, JoinField.Type type, Consumer<BytesRef> keys, Runnable check) {
      throw new UnsupportedOperationException(
          "Prepared native join cannot be used as a foreign snapshot");
    }

    public SearchIndexReader.SearchProducerAndMetaProducer intermediateQuery(
        SearchQuery q, QueryCursorOptions o, BatchSizeStrategy s, QueryOptimizationFlags f) {
      throw new UnsupportedOperationException("Intermediate joins are not supported");
    }

    public SearchIndexReader.SearchProducerAndMetaResults query(
        Query q, QueryCursorOptions o, BatchSizeStrategy s, QueryOptimizationFlags f)
        throws IOException, InvalidQueryException, InterruptedException {
      activate();
      var result = this.plan.snapshot().query(q, o, s, f);
      this.producer = Optional.of(result.searchBatchProducer);
      this.plan.verifyDependencies();
      return new SearchIndexReader.SearchProducerAndMetaResults(
          new BatchProducer() {
            public void execute(Bytes limit, BatchCursorOptions options) throws IOException {
              activate();
              Prepared.this.producer.orElseThrow().execute(limit, options);
              Prepared.this.plan.verifyDependencies();
            }

            public BsonArray getNextBatch(Bytes limit) throws IOException {
              activate();
              var batch = Prepared.this.producer.orElseThrow().getNextBatch(limit);
              Prepared.this.plan.verifyDependencies();
              return batch;
            }

            public boolean isExhausted() {
              return Prepared.this.producer.orElseThrow().isExhausted();
            }

            public void close() throws IOException {
              Prepared.this.close();
            }
          },
          result.metaResults);
    }

    public synchronized void close() throws IOException {
      if (!this.closed) {
        this.closed = true;
        try {
          if (this.producer.isPresent()) {
            this.producer.orElseThrow().close();
          }
        } finally {
          try {
            this.plan.close();
          } finally {
            this.onClose.run();
          }
        }
      }
    }
  }
}
