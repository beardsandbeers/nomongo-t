package com.xgen.mongot.standalone.runtime;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.QuarantinePolicy;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.config.manager.IncompatibleIndexes;
import com.xgen.mongot.config.util.HysteresisConfig;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.MongotCursorManagerImpl;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.lucene.LuceneGlobalSettings;
import com.xgen.mongot.index.lucene.LuceneIndexFactory;
import com.xgen.mongot.index.lucene.LuceneIndexMappingExporter;
import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.mongot.index.lucene.directory.EnvironmentVariantPerfConfig;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.SearchQueryParseOptions;
import com.xgen.mongot.index.query.aggregation.SearchAggregationBootstrap;
import com.xgen.mongot.lifecycle.DefaultLifecycleManager;
import com.xgen.mongot.lifecycle.LifecycleConfig;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.GetIndexMappingsRequest;
import com.xgen.mongot.standalone.grpc.GetIndexMappingsResponse;
import com.xgen.mongot.standalone.grpc.IndexDescription;
import com.xgen.mongot.standalone.grpc.ServiceMetric;
import com.xgen.mongot.standalone.grpc.ServiceStatus;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import com.xgen.mongot.standalone.ingestion.ApiIngestionManager;
import com.xgen.mongot.standalone.metrics.StandaloneMetrics;
import io.grpc.Status;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

/**
 * Standalone composition with local desired state, standard physical lifecycle and API ingestion.
 */
public final class StandaloneRuntime implements Closeable {
  private final StandaloneConfig config;
  private final LocalIndexCatalog catalog;
  private final IndexCatalog liveIndexes;
  private final Optional<Runnable> externalReconcile;
  private final InitializedIndexCatalog initializedIndexes;
  private final MongotCursorManagerImpl cursors;
  private final ApiIngestionManager ingestion;
  private final StandaloneIndexStatistics statistics;
  private final DirectReadCatalog reads;
  private com.google.common.base.Supplier<
          com.xgen.mongot.embedding.providers.EmbeddingServiceManager>
      embedding = () -> null;

  public void setEmbeddingService(
      com.google.common.base.Supplier<com.xgen.mongot.embedding.providers.EmbeddingServiceManager>
          embedding) {
    this.embedding = embedding;
  }

  public com.xgen.mongot.index.query.MaterializedVectorSearchQuery materializeVectorQuery(
      com.xgen.mongot.index.query.VectorSearchQuery query,
      com.xgen.mongot.index.definition.IndexDefinition definition)
      throws Exception {
    this.configManager.validateVectorQuery(query);
    return com.xgen.mongot.server.command.search.VectorQueryEmbedder.prepare(
        this.reads.liveIndexes(), this.embedding, definition, query);
  }

  public SearchQuery materializeSearchQuery(
      SearchQuery query, com.xgen.mongot.index.definition.IndexDefinition definition)
      throws InvalidQueryException {
    return com.xgen.mongot.server.command.search.SearchAutoEmbeddingHelper.prepare(
        this.reads.liveIndexes(),
        this.embedding,
        definition.getDatabase(),
        definition.getCollectionUuid(),
        definition.getView().map(view -> view.getName()),
        query,
        definition);
  }

  private final StandaloneIndexUpdates updates;
  private final DefaultConfigManager configManager;
  private final MeterAndFtdcRegistry metrics;
  private final StandaloneMetrics serviceMetrics;
  private final ArrayDeque<AutoCloseable> resources = new ArrayDeque<>();
  private final ScheduledThreadPoolExecutor reconciler;
  private final AtomicBoolean reconcileQueued = new AtomicBoolean();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<Optional<String>> reconciliationFailure =
      new AtomicReference<>(Optional.empty());

  /** Opens the index metadata and composes the Lucene lifecycle without a source database. */
  public StandaloneRuntime(StandaloneConfig config) throws Exception {
    this(config, FeatureFlags.communityDefaults(), DynamicFeatureFlagRegistry.communityDefault());
  }

  /** Composes readers and query parsing with the same supplied feature configuration. */
  public StandaloneRuntime(
      StandaloneConfig config, FeatureFlags flags, DynamicFeatureFlagRegistry dynamicFlags)
      throws Exception {
    this.config = config;
    this.externalReconcile = Optional.empty();
    if (Files.exists(config.dataPath().resolve("coexistence.json"))) {
      throw new IOException(
          "This data directory contains replicated indexes; configure syncSource to reopen it");
    }
    this.catalog = new LocalIndexCatalog(config.dataPath());
    this.resources.addFirst(this.catalog);
    try {
      StandaloneIndexUpdates.recover(config.dataPath(), this.catalog);
      var previous =
          ConfigJournalV1.fromFileIfExists(
              config.dataPath().resolve("configJournal.json"), QuarantinePolicy.QUARANTINE);
      if (previous.isPresent()
          && Stream.of(
                  previous.get().getLiveEntries(),
                  previous.get().getStagedEntries(),
                  previous.get().getDeletedEntries())
              .flatMap(Collection::stream)
              .anyMatch(entry -> !this.catalog.owns(entry.getGenerationId().indexId))) {
        throw new IOException("Replicated generations require the native mongot launcher");
      }
      this.metrics = MeterAndFtdcRegistry.createWithSimpleRegistries();
      this.serviceMetrics = new StandaloneMetrics(this.metrics.meterRegistry());
      this.resources.addFirst(
          () -> {
            this.metrics.ftdcRegistry().close();
            this.metrics.meterRegistry().close();
          });
      this.liveIndexes = new DefaultIndexCatalog();
      this.initializedIndexes = new InitializedIndexCatalog();
      var initialized = this.initializedIndexes;
      SearchAggregationBootstrap.installRegistryIfEnabled(flags);
      var diskMonitor = new StandaloneDiskMonitor(config.dataPath());
      this.resources.addFirst(diskMonitor::stop);
      diskMonitor.start(Duration.ofSeconds(1));
      var luceneConfig = luceneConfig(config.dataPath(), config.refreshInterval());
      LuceneGlobalSettings.apply(luceneConfig);
      var factory =
          LuceneIndexFactory.fromConfig(
              luceneConfig,
              flags,
              dynamicFlags,
              EnvironmentVariantPerfConfig.getDefault(),
              this.metrics,
              Optional.empty(),
              AnalyzerRegistry.factory(),
              diskMonitor);
      StandaloneIndexUpdates.configureFactory(factory, this.catalog, config.dataPath());
      this.resources.addFirst(factory);
      this.cursors =
          MongotCursorManagerImpl.fromConfig(
              CursorConfig.getDefault(),
              this.metrics.meterRegistry(),
              this.liveIndexes,
              initialized);
      AutoCloseable cursorOwner = this.cursors::close;
      this.resources.addFirst(cursorOwner);
      var schedulers =
          IndexingWorkSchedulerFactory.createWithoutEmbeddingStrategy(
              4, this.metrics.meterRegistry(), false);
      this.resources.addFirst(
          () -> {
            for (var scheduler : Set.copyOf(schedulers.getIndexingWorkSchedulers().values())) {
              scheduler.shutdown().get(30, TimeUnit.SECONDS);
            }
          });
      this.ingestion =
          new ApiIngestionManager(
              this.liveIndexes,
              initialized,
              config.documentLimits(),
              config.commitInterval(),
              schedulers
                  .getIndexingWorkSchedulers()
                  .get(IndexingWorkSchedulerFactory.IndexingStrategy.DEFAULT));
      this.resources.addFirst(() -> this.ingestion.shutdown().get(30, TimeUnit.SECONDS));
      var lifecycle =
          new DefaultLifecycleManager(
              unused -> this.ingestion,
              Optional.empty(),
              initialized,
              factory,
              Optional.empty(),
              unused -> Optional.empty(),
              this.metrics.meterRegistry(),
              ReplicationStateMonitor.enabled(),
              LifecycleConfig.getDefault(),
              false,
              this.metrics.threadPoolResourceBindings());
      AutoCloseable lifecycleOwner = () -> lifecycle.shutdown().get(30, TimeUnit.SECONDS);
      this.resources.addFirst(lifecycleOwner);
      var desiredIds =
          this.catalog.definitions().stream()
              .map(index -> index.getIndexId())
              .collect(Collectors.toSet());
      var retirements =
          java.util.concurrent.Executors.newFixedThreadPool(
              2, Thread.ofPlatform().name("standalone-retirement-", 0).factory());
      this.resources.addFirst(
          () -> {
            retirements.shutdown();
            if (!retirements.awaitTermination(30, TimeUnit.SECONDS)) {
              throw new IOException("Standalone retirements did not drain");
            }
          });
      this.configManager =
          DefaultConfigManager.initialize(
              this.liveIndexes,
              Optional.of(desiredIds),
              initialized,
              new IncompatibleIndexes(this.metrics.meterRegistry()),
              this.cursors,
              factory,
              Optional.empty(),
              config.dataPath().resolve("configJournal.json"),
              this.metrics.meterRegistry(),
              lifecycle,
              ReplicationStateMonitor.enabled(),
              flags,
              config.dataPath().resolve("dynamicFeatureFlags.json"),
              dynamicFlags);
      synchronized (this.configManager) {
        this.configManager.configState.enableDeferredRetirement(retirements);
      }
      // The config manager now owns these three resources, including their drain/close ordering.
      this.resources.remove(factory);
      this.resources.remove(cursorOwner);
      this.resources.remove(lifecycleOwner);
      this.resources.addFirst(this.configManager::close);
      this.reads =
          new DirectReadCatalog(this.catalog, this.configManager, this.liveIndexes, initialized);
      this.reads.observe(this.serviceMetrics);
      this.updates =
          new StandaloneIndexUpdates(
              config,
              this.catalog,
              this.liveIndexes,
              this.ingestion,
              this.configManager,
              this.serviceMetrics);
      this.resources.addFirst(this.updates);
      this.statistics =
          new StandaloneIndexStatistics(this.reads, config.maxRpcDuration(), this.serviceMetrics);
      this.resources.addFirst(this.statistics);
      reconcile();
      this.reconciler =
          new ScheduledThreadPoolExecutor(
              1, Thread.ofPlatform().name("standalone-catalog").factory());
      this.reconciler.setRemoveOnCancelPolicy(true);
      this.resources.addFirst(
          () -> {
            this.reconciler.shutdown();
            if (!this.reconciler.awaitTermination(30, TimeUnit.SECONDS)) {
              throw new IOException("Local reconciliation did not drain before shutdown");
            }
          });
      this.reconciler.scheduleWithFixedDelay(
          this::reconcileSafely, 100, 100, TimeUnit.MILLISECONDS);
    } catch (Exception | Error e) {
      try {
        closeResources();
      } catch (IOException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
  }

  /** Attaches local services to an externally owned engine without changing its configuration. */
  public StandaloneRuntime(
      StandaloneConfig config,
      LocalIndexCatalog catalog,
      IndexCatalog indexes,
      InitializedIndexCatalog initialized,
      MongotCursorManagerImpl cursors,
      ApiIngestionManager ingestion,
      DefaultConfigManager manager,
      MeterAndFtdcRegistry metrics,
      Runnable reconcile,
      DirectReadCatalog.RemoteDefinitions remoteDefinitions,
      Object metadataMonitor) {
    this.config = config;
    this.catalog = catalog;
    this.liveIndexes = indexes;
    this.initializedIndexes = initialized;
    this.cursors = cursors;
    this.ingestion = ingestion;
    this.configManager = manager;
    this.metrics = metrics;
    this.externalReconcile = Optional.of(reconcile);
    this.serviceMetrics = new StandaloneMetrics(metrics.meterRegistry());
    this.reads =
        new DirectReadCatalog(
            catalog,
            manager,
            manager.configState.indexCatalog,
            initialized,
            remoteDefinitions,
            metadataMonitor);
    this.reads.observe(this.serviceMetrics);
    this.updates =
        new StandaloneIndexUpdates(
            config, catalog, indexes, ingestion, manager, this.serviceMetrics);
    this.resources.addFirst(this.updates);
    this.statistics =
        new StandaloneIndexStatistics(this.reads, config.maxRpcDuration(), this.serviceMetrics);
    this.resources.addFirst(this.statistics);
    this.reconciler =
        new ScheduledThreadPoolExecutor(
            1, Thread.ofPlatform().name("standalone-catalog").factory());
    this.reconciler.setRemoveOnCancelPolicy(true);
    this.resources.addFirst(
        () -> {
          this.reconciler.shutdown();
          if (!this.reconciler.awaitTermination(30, TimeUnit.SECONDS)) {
            throw new IOException("Local reconciliation did not drain");
          }
        });
    this.reconciler.scheduleWithFixedDelay(this::reconcileSafely, 0, 1, TimeUnit.SECONDS);
  }

  public StandaloneConfig config() {
    return this.config;
  }

  public void validateSearchQuery(SearchQuery query) throws InvalidQueryException {
    this.configManager.validateSearchQuery(query);
  }

  public SearchQueryParseOptions searchQueryParseOptions() {
    return this.configManager.getSearchQueryParseOptions();
  }

  public DirectReadCatalog reads() {
    return this.reads;
  }

  public LocalIndexCatalog catalog() {
    return this.catalog;
  }

  public IndexDescription updateIndex(UpdateIndexRequest request) {
    var result = this.updates.update(request);
    return result.getDesiredRevision() == request.getExpectedRevision()
        ? describeIndex(request.getIndexId())
        : result;
  }

  StandaloneIndexUpdates updatesForTesting() {
    return this.updates;
  }

  /** Publication notifications identify only the retired source generation. */
  public void onIndexPublication(java.util.function.Consumer<String> listener) {
    this.updates.onPublication(listener);
  }

  public ApiIngestionManager ingestion() {
    return this.ingestion;
  }

  public StandaloneIndexStatistics statistics() {
    return this.statistics;
  }

  public MongotCursorManagerImpl cursors() {
    return this.cursors;
  }

  public IndexCatalog liveIndexes() {
    return this.liveIndexes;
  }

  /** Acquires managed search snapshots without exposing raw Lucene readers to transport code. */
  public SearchIndexReader.QuerySnapshot acquireQuerySnapshot(
      com.xgen.mongot.config.manager.GenerationReadLeases.Lease lease, boolean concurrent)
      throws IOException, ReaderClosedException {
    if (lease.retired()) {
      throw Status.UNAVAILABLE.withDescription("Query generation was retired").asRuntimeException();
    }
    if (!(lease.reader() instanceof SearchIndexReader reader)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Query snapshots require search indexes")
          .asRuntimeException();
    }
    return reader.acquireQuerySnapshot(concurrent);
  }

  /** Exports one active generation under a producer-independent resource lease. */
  public GetIndexMappingsResponse getIndexMappings(GetIndexMappingsRequest request) {
    try (var lease = this.reads.acquire(request.getIndexId(), request.getGenerationId())) {
      var definition = lease.definition();
      var external = this.reads.publicDefinition(definition);
      var response =
          GetIndexMappingsResponse.newBuilder()
              .setIndexId(request.getIndexId())
              .setGenerationId(lease.index().getGenerationId().uniqueString())
              .setActiveRevision(this.reads.revision(definition))
              .setActiveVersion(this.reads.version(definition));
      if (lease.reader() instanceof SearchIndexReader reader) {
        reader.refresh();
        var search = definition.asSearchDefinition();
        var version = lease.index().getGenerationId().generation.formatVersion.asLucene();
        org.bson.BsonDocument mappings;
        if (search.dynamicVectorFields().findAny().isPresent()) {
          try (var snapshot = reader.acquireStatisticsSnapshot()) {
            mappings =
                LuceneIndexMappingExporter.exportSnapshot(search, version, snapshot.readers());
          }
        } else {
          mappings = LuceneIndexMappingExporter.export(search, version, reader.getFieldInfos());
        }
        external.put("mappings", mappings);
        response
            .setMappingsJson(mappings.toJson())
            .setIndexType(com.xgen.mongot.standalone.grpc.IndexType.INDEX_TYPE_SEARCH);
      } else {
        response
            .setIndexType(com.xgen.mongot.standalone.grpc.IndexType.INDEX_TYPE_VECTOR_SEARCH)
            .setVectorFieldsJson(vectorFieldsJson(external));
      }
      if (lease.retired()) {
        throw Status.UNAVAILABLE
            .withDescription("Mapping generation was retired")
            .asRuntimeException();
      }
      return response.setDefinitionJson(external.toJson()).build();
    } catch (LuceneIndexMappingExporter.AmbiguousVectorPathException error) {
      throw Status.FAILED_PRECONDITION
          .withDescription(error.getMessage())
          .withCause(error)
          .asRuntimeException();
    } catch (IOException | ReaderClosedException error) {
      throw Status.UNAVAILABLE
          .withDescription("Cannot read index field metadata")
          .withCause(error)
          .asRuntimeException();
    }
  }

  private static String vectorFieldsJson(org.bson.BsonDocument definition) {
    return definition.getArray("fields", new org.bson.BsonArray()).stream()
        .map(value -> value.asDocument().toJson())
        .collect(java.util.stream.Collectors.joining(",", "[", "]"));
  }

  /**
   * Coalesces notifications; the periodic backstop also finishes swaps and retiring generations.
   */
  public void catalogChanged() {
    if (!this.closed.get() && this.reconcileQueued.compareAndSet(false, true)) {
      this.reconciler.execute(
          () -> {
            this.reconcileQueued.set(false);
            reconcileSafely();
          });
    }
  }

  private void reconcileSafely() {
    if (this.closed.get()) {
      return;
    }
    try {
      reconcile();
      this.reconciliationFailure.set(Optional.empty());
    } catch (Exception e) {
      this.reconciliationFailure.set(Optional.of("Local catalog reconciliation failed"));
    }
  }

  private void reconcile() throws Exception {
    this.updates.reconcile();
    if (this.externalReconcile.isPresent()) {
      this.externalReconcile.get().run();
      return;
    }
    synchronized (this.configManager) {
      var desired =
          this.catalog.reconciliationDefinitions(
              this.liveIndexes.getIndexes().stream()
                  .collect(
                      java.util.stream.Collectors.toMap(
                          index -> index.getGenerationId().indexId.toHexString(),
                          com.xgen.mongot.index.IndexGeneration::getDefinition,
                          (first, second) -> {
                            throw new IllegalStateException("Duplicate physical index identity");
                          })));
      this.configManager.update(
          desired.stream()
              .filter(VectorIndexDefinition.class::isInstance)
              .map(VectorIndexDefinition.class::cast)
              .toList(),
          desired.stream()
              .filter(SearchIndexDefinition.class::isInstance)
              .map(SearchIndexDefinition.class::cast)
              .toList(),
          List.of(),
          Set.of(),
          Set.of());
    }
  }

  public boolean managementReady() {
    return !this.closed.get() && this.reconciliationFailure.get().isEmpty();
  }

  public StandaloneMetrics metrics() {
    return this.serviceMetrics;
  }

  /** Returns instance status and native engine meters; the RPC restricts this to administrators. */
  public ServiceStatus serviceStatus() throws Exception {
    var result =
        ServiceStatus.newBuilder()
            .setManagementReady(managementReady())
            .setFailureMessage(this.reconciliationFailure.get().orElse(""))
            .setFreeDiskBytes(Files.getFileStore(this.config.dataPath()).getUsableSpace());
    for (var index : this.reads.snapshot()) {
      result.addIndexes(
          index.toBuilder().clearDesiredDefinitionBson().clearActiveDefinitionBson().clearBuild());
    }
    for (var meter : this.metrics.meterRegistry().getMeters()) {
      if (this.externalReconcile.isPresent()
          && !meter.getId().getName().startsWith("standalone.")) {
        continue;
      }
      var metric = ServiceMetric.newBuilder().setName(meter.getId().getName());
      meter.getId().getTags().forEach(tag -> metric.putLabels(tag.getKey(), tag.getValue()));
      meter
          .measure()
          .forEach(
              measurement ->
                  metric.putMeasurements(
                      measurement.getStatistic().getTagValueRepresentation(),
                      measurement.getValue()));
      result.addMetrics(metric);
    }
    return result.build();
  }

  /** Checks Lucene disk headroom before applying a pushed batch. */
  public void checkWriteAdmission(long additionalBytes) {
    if (!managementReady()) {
      throw Status.UNAVAILABLE.withDescription("Index catalog is not ready").asRuntimeException();
    }
    try {
      long available = Files.getFileStore(this.config.dataPath()).getUsableSpace();
      if (additionalBytes < 0
          || available < additionalBytes
          || available - additionalBytes < this.config.minFreeDiskBytes()) {
        throw Status.RESOURCE_EXHAUSTED
            .withDescription("Lucene disk headroom is exhausted")
            .asRuntimeException();
      }
    } catch (IOException e) {
      throw Status.UNAVAILABLE
          .withDescription("Cannot verify disk headroom")
          .withCause(e)
          .asRuntimeException();
    }
  }

  /**
   * Desired-catalog status includes uninitialized/failed resources and exact live/build identities.
   */
  public IndexDescription describeIndex(String id) {
    return this.reads.describe(id);
  }

  @Override
  public void close() throws IOException {
    if (this.closed.compareAndSet(false, true)) {
      closeResources();
    }
  }

  private void closeResources() throws IOException {
    @Var
    @Nullable
    IOException failure = null;
    while (!this.resources.isEmpty()) {
      try {
        this.resources.removeFirst().close();
      } catch (Exception e) {
        if (failure == null) {
          failure = new IOException("Standalone shutdown failed", e);
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static LuceneConfig luceneConfig(Path path, Duration refreshInterval) {
    return LuceneConfig.create(
        path,
        Optional.of(refreshInterval),
        Optional.of(2),
        Optional.of(2),
        Optional.of(4),
        Optional.of(32.0),
        Optional.of(false),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(true), // Reuse the bounded search pool for exhaustive delete collectors.
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.of(new HysteresisConfig(0.90, 0.95)),
        Optional.empty(),
        Optional.empty());
  }
}
