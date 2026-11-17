package com.xgen.mongot.standalone.coexistence;

import com.xgen.mongot.config.manager.ConfigManager;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.config.manager.QuarantinedIndexInfos;
import com.xgen.mongot.config.manager.metrics.GroupedIndexGenerationMetrics;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.lifecycle.LifecycleManager;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.replication.ReplicationStatus;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.runtime.DirectReadCatalog;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.types.ObjectId;
import org.slf4j.LoggerFactory;

/**
 * Native config facade and the sole union reconciler. Network fetches happen in the native updater.
 */
public final class CoexistenceConfigManager
    implements ConfigManager, DirectReadCatalog.RemoteDefinitions {
  private final DefaultConfigManager delegate;
  private final LocalIndexCatalog local;
  private final LifecycleManager sourceLifecycle;
  private final ReplicationStateMonitor sourceMonitor;
  private volatile Instant sourceStatusChanged;
  private final ExecutorService sourceUpdates =
      Executors.newSingleThreadExecutor(Thread.ofPlatform().name("coexistence-source").factory());
  private final AtomicReference<SyncSourceConfig> desiredSource = new AtomicReference<>();
  private final AtomicBoolean sourceUpdateQueued = new AtomicBoolean();
  private final AtomicBoolean replicationInitialized = new AtomicBoolean();
  // Accessed only by the source executor. A timeout must not start a second native shutdown.
  private Optional<CompletableFuture<Void>> sourceDrain = Optional.empty();
  private Map<ObjectId, IndexDefinition> remote;
  private List<OverriddenBaseAnalyzerDefinition> analyzers = List.of();
  private Set<UUID> collections = Set.of();
  private Set<ObjectId> unreadable = Set.of();

  public CoexistenceConfigManager(
      DefaultConfigManager delegate,
      LocalIndexCatalog local,
      LifecycleManager sourceLifecycle,
      ReplicationStateMonitor sourceMonitor) {
    this.delegate = delegate;
    this.local = local;
    this.sourceLifecycle = sourceLifecycle;
    this.sourceMonitor = sourceMonitor;
    this.sourceStatusChanged = delegate.getReplicationStatus().startTime();
    var recovered = new LinkedHashMap<ObjectId, IndexDefinition>();
    delegate.getLiveIndexes().stream()
        .filter(i -> !local.owns(i.getIndexId()))
        .forEach(i -> recovered.put(i.getIndexId(), i));
    delegate.configState.staged.getIndexes().stream()
        .map(IndexGeneration::getDefinition)
        .filter(i -> !local.owns(i.getIndexId()))
        .forEach(i -> recovered.put(i.getIndexId(), i));
    this.remote = copyDefinitions(recovered.values());
    var quarantined = delegate.getQuarantinedIndexInfos();
    this.unreadable =
        Stream.concat(quarantined.liveIndexes().stream(), quarantined.stagedIndexes().stream())
            .map(i -> i.indexId())
            .filter(id -> !local.owns(id))
            .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public synchronized void update(
      List<VectorIndexDefinition> vectors,
      List<SearchIndexDefinition> searches,
      List<OverriddenBaseAnalyzerDefinition> analyzers,
      Set<UUID> collections,
      Set<ObjectId> unreadable) {
    var next = new ArrayList<IndexDefinition>();
    next.addAll(vectors);
    next.addAll(searches);
    if (next.stream().anyMatch(i -> this.local.owns(i.getIndexId()))
        || next.stream().map(IndexDefinition::getIndexId).distinct().count() != next.size()) {
      LoggerFactory.getLogger(CoexistenceConfigManager.class)
          .error("MongoDB catalog has conflicting index identities; retaining accepted" + " state");
      return;
    }
    this.remote = copyDefinitions(next);
    this.analyzers = List.copyOf(analyzers);
    this.collections = Set.copyOf(collections);
    this.unreadable = Set.copyOf(unreadable);
    reconcileLocal();
  }

  @Override
  public synchronized List<IndexDefinition> snapshot() {
    return this.remote.values().stream().map(CoexistenceConfigManager::copy).toList();
  }

  @Override
  public synchronized Optional<IndexDefinition> get(ObjectId id) {
    return Optional.ofNullable(this.remote.get(id)).map(CoexistenceConfigManager::copy);
  }

  private static Map<ObjectId, IndexDefinition> copyDefinitions(
      java.util.Collection<IndexDefinition> definitions) {
    var copies = new LinkedHashMap<ObjectId, IndexDefinition>();
    definitions.forEach(definition -> copies.put(definition.getIndexId(), copy(definition)));
    return java.util.Collections.unmodifiableMap(copies);
  }

  public synchronized void reconcileLocal() {
    // Replication updates lastObservedCollectionName in its working definitions. Keep accepted
    // desired state separate so source observation cannot mutate the snapshot used during outages.
    var union = new ArrayList<>(snapshot());
    union.addAll(this.local.definitions());
    this.delegate.update(
        union.stream()
            .filter(VectorIndexDefinition.class::isInstance)
            .map(VectorIndexDefinition.class::cast)
            .toList(),
        union.stream()
            .filter(SearchIndexDefinition.class::isInstance)
            .map(SearchIndexDefinition.class::cast)
            .toList(),
        this.analyzers,
        this.collections,
        this.unreadable);
  }

  private static IndexDefinition copy(IndexDefinition definition) {
    try (var parser = BsonDocumentParser.fromRoot(definition.toBson()).build()) {
      return IndexDefinition.fromBson(parser);
    } catch (BsonParseException error) {
      throw new IllegalArgumentException("Cannot copy an accepted index definition", error);
    }
  }

  @Override
  public void handleReplicationAndSyncSourceUpdate(SyncSourceConfig config) {
    this.desiredSource.set(config);
    if (!this.sourceUpdateQueued.compareAndSet(false, true)) {
      return;
    }
    this.sourceUpdates.execute(
        () -> {
          try {
            var desired = this.desiredSource.get();
            boolean changed =
                !this.sourceLifecycle.getSyncSourceConfig().filter(desired::equals).isPresent();
            boolean paused =
                this.sourceMonitor.getReplicationGate().isClosed()
                    || !desired.hasReplicationUrisAvailable();
            if (changed || paused == this.sourceLifecycle.isReplicationSupported()) {
              this.sourceStatusChanged = Instant.now();
              if (this.sourceDrain.isEmpty()) {
                this.sourceDrain = Optional.of(this.sourceLifecycle.shutdownReplication());
              }
              this.sourceDrain.orElseThrow().get(30, TimeUnit.SECONDS);
              // Network shutdown must not hold the union manager's monitor. Publish the new
              // source under its short mutation lock, including native materialized-view factories
              // and unresolved auto-embedding generations, before restarting remote replication.
              synchronized (this.delegate) {
                this.delegate.configState.updateSyncSource(desired);
              }
              this.sourceDrain = Optional.empty();
              this.sourceLifecycle.restartReplication();
              this.sourceStatusChanged = Instant.now();
            } else if (desired.hasReplicationUrisAvailable()
                && this.sourceLifecycle.hasAutoEmbeddingReplication()) {
              boolean pauseEmbedding =
                  this.sourceMonitor.getAutoEmbeddingReplicationGate().isClosed();
              if (pauseEmbedding == this.sourceLifecycle.isAutoEmbeddingReplicationSupported()) {
                if (pauseEmbedding) {
                  this.sourceLifecycle.shutdownAutoEmbeddingReplication().get(30, TimeUnit.SECONDS);
                } else {
                  this.sourceLifecycle.restartAutoEmbeddingReplication(desired);
                }
              }
            }
          } catch (Exception e) {
            LoggerFactory.getLogger(CoexistenceConfigManager.class)
                .error("Source reconfiguration failed; local ingestion remains available", e);
          } finally {
            this.sourceUpdateQueued.set(false);
          }
        });
  }

  @Override
  public List<IndexDefinition> getLiveIndexes() {
    return this.delegate.getLiveIndexes().stream()
        .filter(i -> !this.local.owns(i.getIndexId()))
        .toList();
  }

  @Override
  public List<IndexGeneration> getLiveIndexGenerations() {
    return this.delegate.getLiveIndexGenerations().stream()
        .filter(i -> !this.local.owns(i.getDefinition().getIndexId()))
        .toList();
  }

  @Override
  public boolean isReplicationInitialized() {
    if (this.replicationInitialized.get()) {
      return true;
    }
    boolean initialized =
        getLiveIndexGenerations().stream().noneMatch(i -> i.getIndex().getStatus().isUnknown());
    if (initialized) {
      this.replicationInitialized.set(true);
    }
    return initialized;
  }

  @Override
  public void refreshIndexInfos() {
    this.delegate.refreshIndexInfos();
  }

  @Override
  public List<IndexInformation> getIndexInfos() {
    return this.delegate.getIndexInfos().stream()
        .filter(i -> !this.local.owns(i.getDefinition().getIndexId()))
        .toList();
  }

  @Override
  public QuarantinedIndexInfos getQuarantinedIndexInfos() {
    var all = this.delegate.getQuarantinedIndexInfos();
    return new QuarantinedIndexInfos(
        all.liveIndexes().stream().filter(i -> !this.local.owns(i.indexId())).toList(),
        all.stagedIndexes().stream().filter(i -> !this.local.owns(i.indexId())).toList());
  }

  @Override
  public List<GroupedIndexGenerationMetrics> getGroupedIndexGenerationMetrics() {
    return this.delegate.getGroupedIndexGenerationMetrics().stream()
        .filter(i -> !this.local.owns(i.indexId()))
        .toList();
  }

  @Override
  public int getCurrentIndexFormatVersion() {
    return this.delegate.getCurrentIndexFormatVersion();
  }

  @Override
  public ReplicationStatus getReplicationStatus() {
    var status = this.delegate.getReplicationStatus();
    return new ReplicationStatus(
        status.lastUpdateTime(),
        this.sourceStatusChanged,
        status.isReplicationStopped(),
        status.isAutoEmbeddingMaterializedViewStopped(),
        status.initialSyncStatus());
  }

  @Override
  public void promoteReadyIndexes() {
    this.delegate.promoteReadyIndexes();
  }

  @Override
  public void close() {
    /* The shared runtime owns the physical config manager. */
  }

  public void stopSourceUpdates() {
    this.sourceUpdates.shutdown();
    try {
      if (!this.sourceUpdates.awaitTermination(35, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Source reconfiguration did not drain");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
