package com.xgen.mongot.standalone.coexistence;

import com.google.common.base.Supplier;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.config.manager.IncompatibleIndexes;
import com.xgen.mongot.config.manager.QuarantinedIndexLookup;
import com.xgen.mongot.cursor.CursorConfig;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorManagerImpl;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.index.IndexFactory;
import com.xgen.mongot.lifecycle.DefaultLifecycleManager;
import com.xgen.mongot.lifecycle.LifecycleConfig;
import com.xgen.mongot.lifecycle.LifecycleManager;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.ingestion.ApiIngestionManager;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import com.xgen.mongot.standalone.server.StandaloneGrpcServer;
import com.xgen.mongot.util.FileUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.jetbrains.annotations.Nullable;

/** Opt-in native-bootstrap extension. Owns only additive local state and shared scheduling. */
public final class CoexistenceRuntime {
  private final ExecutorService retirements =
      Executors.newFixedThreadPool(
          2, Thread.ofPlatform().name("coexistence-retirement-", 0).factory());
  private final StandaloneConfig config;
  private final LocalIndexCatalog catalog;
  private final IndexCatalog localIndexes;
  private final IndexCatalog remoteIndexes;
  private final InitializedIndexCatalog initialized;
  private final MongotCursorManagerImpl cursors;
  private final MeterAndFtdcRegistry metrics;
  private @Nullable IndexingWorkSchedulerFactory schedulers;
  private @Nullable ApiIngestionManager ingestion;
  private @Nullable LifecycleManager sourceLifecycle;
  private @Nullable ReplicationStateMonitor sourceMonitor;
  private @Nullable CoexistenceConfigManager coordinator;
  private @Nullable StandaloneRuntime runtime;
  private @Nullable StandaloneGrpcServer services;

  /** Opens additive ownership metadata over an externally owned native engine. */
  public CoexistenceRuntime(
      StandaloneConfig config,
      IndexCatalog indexes,
      InitializedIndexCatalog initialized,
      MongotCursorManagerImpl cursors,
      MeterAndFtdcRegistry metrics)
      throws IOException {
    if ((long) config.documentLimits().maxBatchBytes() + 65536
        > CursorConfig.DEFAULT_MESSAGE_SIZE_LIMIT.toBytes()) {
      throw new IllegalArgumentException(
          "Standalone batch limit exceeds the shared native listener limit");
    }
    this.config = config;
    this.initialized = initialized;
    this.cursors = cursors;
    this.metrics = metrics;
    var marker = config.dataPath().resolve("coexistence.json");
    if (Files.exists(marker)) {
      try {
        int version = BsonDocument.parse(Files.readString(marker)).getInt32("version").getValue();
        if (version != 1) {
          throw new IOException("Unsupported coexistence metadata version: " + version);
        }
      } catch (RuntimeException error) {
        throw new IOException("Invalid coexistence metadata", error);
      }
    }
    this.catalog = new LocalIndexCatalog(config.dataPath());
    try {
      com.xgen.mongot.standalone.runtime.StandaloneIndexUpdates.recover(
          config.dataPath(), this.catalog);
      if (!Files.exists(marker)) {
        FileUtils.atomicallyReplace(marker, "{\"version\":1}");
      }
    } catch (IOException | RuntimeException error) {
      this.catalog.close();
      throw error;
    }
    this.localIndexes = new OwnedIndexCatalog(indexes, this.catalog::owns);
    this.remoteIndexes = new OwnedIndexCatalog(indexes, id -> !this.catalog.owns(id));
    cursors.restrictNamespaceLookup(id -> !this.catalog.owns(id));
  }

  public IndexCatalog nativeIndexes() {
    return this.remoteIndexes;
  }

  public QuarantinedIndexLookup nativeQuarantine(IncompatibleIndexes indexes) {
    return (database, collectionUuid, name) -> {
      if (indexes.getLiveIndexes().stream()
          .anyMatch(
              index ->
                  !this.catalog.owns(index.indexId())
                      && index.database().equals(database)
                      && index.collectionUuid().equals(collectionUuid)
                      && index.name().equals(name))) {
        indexes.throwIfQuarantined(database, collectionUuid, name);
      }
    };
  }

  public MongotCursorManager nativeCursors() {
    return new NativeCursorManager(this.cursors);
  }

  public IndexingWorkSchedulerFactory createSchedulers(
      int threads, Supplier<EmbeddingServiceManager> embedding, boolean attributionEnabled) {
    this.schedulers =
        IndexingWorkSchedulerFactory.create(
            threads,
            embedding,
            this.metrics.meterRegistry(),
            attributionEnabled
                ? Optional.of(this.metrics.threadPoolResourceBindings())
                : Optional.empty());
    this.ingestion =
        new ApiIngestionManager(
            this.localIndexes,
            this.initialized,
            this.config.documentLimits(),
            this.config.commitInterval(),
            this.schedulers
                .getIndexingWorkSchedulers()
                .get(IndexingWorkSchedulerFactory.IndexingStrategy.DEFAULT));
    return this.schedulers;
  }

  public void configurePreservingUpdates(com.xgen.mongot.index.lucene.LuceneIndexFactory factory) {
    com.xgen.mongot.standalone.runtime.StandaloneIndexUpdates.configureFactory(
        factory, this.catalog, this.config.dataPath());
  }

  public LifecycleManager routeLifecycle(
      LifecycleManager nativeLifecycle,
      IndexFactory factory,
      ReplicationStateMonitor sourceMonitor) {
    this.sourceLifecycle = nativeLifecycle;
    this.sourceMonitor = sourceMonitor;
    var localIngestion = Objects.requireNonNull(this.ingestion);
    var localLifecycle =
        new DefaultLifecycleManager(
            unused -> localIngestion,
            Optional.empty(),
            this.initialized,
            factory,
            Optional.empty(),
            unused -> Optional.empty(),
            this.metrics.meterRegistry(),
            ReplicationStateMonitor.enabled(),
            LifecycleConfig.getDefault(),
            false,
            this.metrics.threadPoolResourceBindings());
    return new OwnedLifecycleManager(nativeLifecycle, localLifecycle, this.catalog::owns);
  }

  /** Attaches local services after the native manager has recovered the combined journal. */
  public void attach(DefaultConfigManager manager) throws IOException {
    synchronized (manager) {
      manager.configState.enableDeferredRetirement(this.retirements);
    }
    this.coordinator =
        new CoexistenceConfigManager(
            manager,
            this.catalog,
            Objects.requireNonNull(this.sourceLifecycle),
            Objects.requireNonNull(this.sourceMonitor));
    this.runtime =
        new StandaloneRuntime(
            this.config,
            this.catalog,
            this.localIndexes,
            this.initialized,
            this.cursors,
            Objects.requireNonNull(this.ingestion),
            manager,
            this.metrics,
            this.coordinator::reconcileLocal);
    this.services = new StandaloneGrpcServer(this.runtime, true);
  }

  public CoexistenceConfigManager nativeConfig() {
    return Objects.requireNonNull(this.coordinator);
  }

  public StandaloneGrpcServer services() {
    return Objects.requireNonNull(this.services);
  }

  public void stopServices() {
    try {
      if (this.services != null) {
        this.services.close();
      }
      if (this.runtime != null) {
        this.runtime.close();
      }
      if (this.coordinator != null) {
        this.coordinator.stopSourceUpdates();
      }
    } catch (Exception e) {
      throw new IllegalStateException("Cannot drain standalone services", e);
    }
  }

  public void closeResources() {
    try {
      if (this.ingestion != null) {
        this.ingestion.shutdown().get(30, TimeUnit.SECONDS);
      }
      if (this.schedulers != null) {
        for (var scheduler : Set.copyOf(this.schedulers.getIndexingWorkSchedulers().values())) {
          scheduler.shutdown().get(30, TimeUnit.SECONDS);
        }
      }
      this.retirements.shutdown();
      if (!this.retirements.awaitTermination(30, TimeUnit.SECONDS)) {
        throw new IOException("Retirement workers did not stop");
      }
      this.catalog.close();
    } catch (Exception e) {
      throw new IllegalStateException("Cannot close coexistence resources", e);
    }
  }
}
