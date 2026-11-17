package com.xgen.mongot.standalone.runtime;

import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.backup.QuarantinePolicy;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.lucene.LuceneIndexFactory;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.standalone.catalog.IndexInitialization;
import com.xgen.mongot.standalone.catalog.IndexMigration;
import com.xgen.mongot.standalone.catalog.IndexMigration.State;
import com.xgen.mongot.standalone.catalog.IndexSchemaUpdate;
import com.xgen.mongot.standalone.catalog.IndexUpdateCompatibility;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.IndexDescription;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import com.xgen.mongot.standalone.ingestion.ApiIngestionManager;
import com.xgen.mongot.util.FileUtils;
import io.grpc.Status;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bson.types.ObjectId;

/**
 * Publishes local schemas on stable storage and recovers copy operations accepted by old builds.
 */
public final class StandaloneIndexUpdates implements Closeable {
  private final StandaloneConfig config;
  private final LocalIndexCatalog catalog;
  private final IndexCatalog indexes;
  private final ApiIngestionManager ingestion;
  private final DefaultConfigManager manager;
  private final com.xgen.mongot.standalone.metrics.StandaloneMetrics metrics;
  private final ExecutorService worker =
      Executors.newSingleThreadExecutor(
          Thread.ofPlatform().name("standalone-index-update").factory());
  // Accessed under manager, matching config-state mutation order.
  private final Map<String, CompletableFuture<Void>> tasks = new HashMap<>();

  enum Checkpoint {
    SCHEMA_PREPARED,
    SCHEMA_JOURNALED,
    SOURCE_VALIDATED,
    ACCEPTED,
    COMMITTED,
    INSTALLED,
    PREPARED,
    PUBLISHED
  }

  private volatile java.util.function.Consumer<Checkpoint> checkpoint = unused -> {};

  void atCheckpoint(java.util.function.Consumer<Checkpoint> observer) {
    this.checkpoint = observer;
  }

  /** Creates an old-format copy operation solely for backward-compatibility recovery tests. */
  @org.jetbrains.annotations.TestOnly
  IndexDescription startLegacyCopyForTesting(UpdateIndexRequest request) {
    return updateLegacyOrRebuild(request);
  }

  private volatile boolean stopping;
  private volatile java.util.function.Consumer<String> publicationListener = unused -> {};
  private final java.util.List<String> published = new java.util.ArrayList<>();

  /** Supplies the retired source generation, even when delivery follows a newer publication. */
  public void onPublication(java.util.function.Consumer<String> listener) {
    this.publicationListener = listener;
  }

  StandaloneIndexUpdates(
      StandaloneConfig config,
      LocalIndexCatalog catalog,
      IndexCatalog indexes,
      ApiIngestionManager ingestion,
      DefaultConfigManager manager,
      com.xgen.mongot.standalone.metrics.StandaloneMetrics metrics) {
    this.config = config;
    this.catalog = catalog;
    this.indexes = indexes;
    this.ingestion = ingestion;
    this.manager = manager;
    this.metrics = metrics;
    synchronized (manager) {
      for (var initialization : catalog.initializations().values()) {
        initialization
            .generation()
            .ifPresent(
                generation ->
                    manager.configState.reserveUserIndexVersion(
                        generation.getIndexId(),
                        generation.generation().userIndexVersion.versionNumber));
      }
      for (var migration : catalog.migrations().values()) {
        manager.configState.reserveUserIndexVersion(
            migration.target().getIndexId(),
            migration.target().generation().userIndexVersion.versionNumber);
      }
    }
  }

  /** Must run before the generic journal initializer, upgrader, or lifecycle sees local indexes. */
  public static void recover(Path root, LocalIndexCatalog catalog) throws IOException {
    try {
      recoverChecked(root, catalog);
    } catch (com.xgen.mongot.util.bson.parser.BsonParseException e) {
      throw new IOException("Unreadable index migration journal", e);
    }
  }

  private static void recoverChecked(Path root, LocalIndexCatalog catalog)
      throws IOException, com.xgen.mongot.util.bson.parser.BsonParseException {
    for (var initialization : catalog.initializations().values()) {
      if (initialization.state() == IndexInitialization.State.INITIALIZING
          && !initialization.generation().orElseThrow().hasCurrentFormatVersion()) {
        throw new IOException("Unfinished local initialization has a noncurrent format");
      }
    }
    var previous =
        ConfigJournalV1.fromFileIfExists(
            root.resolve("configJournal.json"), QuarantinePolicy.QUARANTINE);
    if (previous.isEmpty()) {
      if (!catalog.migrations().isEmpty() || !catalog.schemas().isEmpty()) {
        throw new IOException("Missing journal for preserved index migration");
      }
      return;
    }
    @com.google.errorprone.annotations.Var var journal = previous.orElseThrow();
    // Parsed-definition accessors omit quarantined entries. Native reconciliation can delete
    // those entries when the local catalog still has a readable desired definition, even though
    // no source database exists from which to rebuild their contents.
    for (var entry :
        java.util.stream.Stream.concat(
                journal.getLiveEntries().stream(), journal.getStagedEntries().stream())
            .toList()) {
      if (entry.isQuarantined() && catalog.owns(entry.getGenerationId().indexId)) {
        throw new IOException(
            "Unreadable local index journal entry "
                + entry.getGenerationId()
                + "; restore compatible journal metadata before restarting. Local data was"
                + " retained");
      }
    }
    journal = restoreSchemas(root, catalog, journal);
    for (var generation :
        java.util.stream.Stream.concat(
                journal.getLiveIndexes().stream(), journal.getStagedIndexes().stream())
            .toList()) {
      if (catalog.owns(generation.getIndexId()) && !generation.hasCurrentFormatVersion()) {
        throw new IOException(
            "Local index requires its current format; automatic empty upgrade refused");
      }
    }
    for (var migration : catalog.migrations().values()) {
      if (migration.state() == State.CANCELLED) {
        continue;
      }
      var live =
          journal.getLiveIndexes().stream()
              .filter(index -> index.getIndexId().equals(migration.target().getIndexId()))
              .findFirst()
              .orElseThrow(() -> new IOException("Missing or quarantined preservation source"));
      if (live.equals(migration.target())) {
        catalog.setMigration(migration.withState(State.COMPLETE, sourceFailure(root, live)));
      } else if (live.equals(migration.source()) && migration.state() != State.COMPLETE) {
        String sourceFailure = sourceFailure(root, live);
        var staged =
            journal.getStagedEntries().stream()
                .filter(
                    index ->
                        index.getGenerationId().indexId.equals(migration.target().getIndexId()))
                .findFirst();
        if (staged.isPresent()) {
          if (!journal.getStagedIndexes().contains(migration.target())) {
            throw new IOException("Unexpected or quarantined preserved target generation");
          }
          recoverSeed(root, catalog, migration, State.STAGED);
        } else if (journal.getDeletedEntries().stream()
            .anyMatch(
                entry -> entry.getGenerationId().equals(migration.target().getGenerationId()))) {
          catalog.setMigration(
              new IndexMigration(
                  migration.source(),
                  migration.target().incrementAttempt(),
                  sourceFailure.isEmpty() ? State.ACCEPTED : State.FAILED,
                  sourceFailure));
        } else if (Files.exists(IndexSeed.path(root, migration.target()))) {
          recoverSeed(root, catalog, migration, State.PREPARED);
        } else if (!sourceFailure.isEmpty()) {
          catalog.setMigration(migration.withState(State.FAILED, sourceFailure));
        } else if (migration.state() == State.PREPARED || migration.state() == State.STAGED) {
          catalog.setMigration(migration.withState(State.FAILED, "Missing prepared index seed"));
        }
      } else {
        throw new IOException("Preserved index does not match its live journal generation");
      }
    }
  }

  private static ConfigJournalV1 restoreSchemas(
      Path root, LocalIndexCatalog catalog, ConfigJournalV1 journal) throws IOException {
    var live = new java.util.ArrayList<>(journal.getLiveEntries());
    @com.google.errorprone.annotations.Var boolean changed = false;
    for (var schema : catalog.schemas().values()) {
      if (!schema.target().hasCurrentFormatVersion()) {
        throw new IOException("Local schema binding requires its current physical format");
      }
      var existing =
          live.stream()
              .filter(entry -> entry.getGenerationId().indexId.equals(schema.target().getIndexId()))
              .findFirst()
              .orElseThrow(() -> new IOException("Missing physical index for durable schema"));
      var definition =
          existing
              .getDefinition()
              .orElseThrow(() -> new IOException("Unreadable local schema owner"));
      if ((!definition.equals(schema.source()) && !definition.equals(schema.target()))
          || journal.getStagedEntries().stream()
              .anyMatch(
                  entry -> entry.getGenerationId().indexId.equals(schema.target().getIndexId()))) {
        throw new IOException("Schema binding conflicts with the physical index journal");
      }
      if (definition.equals(schema.source())) {
        live.set(
            live.indexOf(existing),
            com.xgen.mongot.config.backup.JournalEntry.parsed(schema.target()));
        changed = true;
      }
    }
    if (!changed) {
      return journal;
    }
    var restored =
        new ConfigJournalV1(journal.getStagedEntries(), live, journal.getDeletedEntries());
    FileUtils.atomicallyReplace(root.resolve("configJournal.json"), restored.toBson().toJson());
    return restored;
  }

  private static String sourceFailure(
      Path root, com.xgen.mongot.index.definition.IndexDefinitionGeneration generation) {
    try {
      IndexSeed.verifyExisting(root, generation);
      return "";
    } catch (IOException error) {
      return "Local index committed data is unavailable: " + error.getMessage();
    }
  }

  private static void recoverSeed(
      Path root, LocalIndexCatalog catalog, IndexMigration migration, State state) {
    try {
      IndexSeed.verifyPrepared(root, migration);
      catalog.setMigration(migration.withState(state, ""));
    } catch (IOException error) {
      // Keep journaled targets staged: the strict initializer will fail them without opening
      // their files, so the ordinary planner cannot replace them with an empty generation.
      catalog.setMigration(
          migration.withState(
              state == State.STAGED ? State.STAGED : State.FAILED,
              "Preserved index seed recovery failed: " + error.getMessage()));
    }
  }

  /** Installs ownership and durable recovery safeguards before lifecycle startup. */
  public static void configureFactory(
      LuceneIndexFactory factory, LocalIndexCatalog catalog, Path root) {
    factory.requireExistingDataFor(catalog::requiresExistingData);
    factory.allowSchemaUpdatesFor(id -> catalog.owns(id.indexId));
    factory.commitNewIndexesFor(catalog::allowsEmptyInitialization);
    factory.reserveGenerationsWith(catalog::bindInitialization);
    factory.prohibitEmptyRecoveryFor(id -> catalog.owns(id.indexId));
    factory.completeNewIndexesWith(
        generation -> {
          var initialization =
              catalog
                  .initialization(generation.getGenerationId())
                  .filter(
                      value ->
                          value.state() == IndexInitialization.State.INITIALIZING
                              && value.generation().orElseThrow().equals(generation))
                  .orElseThrow(
                      () -> new IOException("Empty initialization authorization was" + " revoked"));
          IndexSeed.verifyExisting(root, generation);
          IndexSeed.verifyBootstrap(root, generation);
          catalog.completeInitialization(initialization);
        });
    factory.validateInitializationsWith(
        generation -> {
          if (!catalog.owns(generation.getIndexId())) {
            return;
          }
          var initialization = catalog.initialization(generation.getGenerationId());
          if (initialization
              .filter(
                  value ->
                      value.state() == IndexInitialization.State.INITIALIZING
                          && value.generation().orElseThrow().equals(generation))
              .isPresent()) {
            IndexSeed.verifyBootstrap(root, generation);
            return;
          }
          var migration = catalog.migration(generation.getIndexId().toHexString());
          if (migration.isPresent()
              && migration.get().target().equals(generation)
              && migration.get().state() != State.COMPLETE
              && migration.get().state() != State.CANCELLED) {
            IndexSeed.verifyPrepared(root, migration.get());
          } else {
            IndexSeed.verifyExisting(root, generation);
          }
        });
    factory.preparedGenerationsFrom(
        producer ->
            catalog
                .migration(producer.getIndexDefinition().getIndexId().toHexString())
                .filter(
                    migration ->
                        migration.state() == State.PREPARED || migration.state() == State.STAGED)
                .filter(
                    migration ->
                        migration
                            .target()
                            .getIndexDefinition()
                            .equals(producer.getIndexDefinition()))
                .<com.xgen.mongot.index.definition.IndexDefinitionGeneration>map(
                    IndexMigration::target)
                .or(() -> catalog.reservedInitialization(producer.getIndexDefinition())));
  }

  IndexDescription update(UpdateIndexRequest request) {
    boolean inPlace;
    synchronized (this.manager) {
      observe();
      var plan = this.catalog.planUpdate(request);
      var pendingSchema = this.catalog.schemas().get(request.getIndexId());
      inPlace =
          plan.compatibility().kind() == IndexUpdateCompatibility.Kind.PRESERVE_CONTENT
              || (pendingSchema != null
                  && plan.compatibility().kind() == IndexUpdateCompatibility.Kind.NO_CHANGE
                  && this.indexes
                      .getIndexById(pendingSchema.target().getIndexId())
                      .filter(
                          index -> index.getDefinitionGeneration().equals(pendingSchema.source()))
                      .isPresent());
    }
    return inPlace ? updateSchema(request) : updateLegacyOrRebuild(request);
  }

  private IndexDescription updateSchema(UpdateIndexRequest request) {
    long started = System.nanoTime();
    com.xgen.mongot.index.IndexGeneration source;
    IndexSchemaUpdate schema;
    ApiIngestionManager.IndexLease pin;
    LuceneIndexFactory factory;
    boolean accepted;
    synchronized (this.manager) {
      var plan = this.catalog.planUpdate(request);
      source =
          this.indexes
              .getIndexById(new ObjectId(request.getIndexId()))
              .filter(index -> this.ingestion.isGenerationReady(index.getGenerationId()))
              .orElseThrow(
                  () ->
                      Status.UNAVAILABLE
                          .withDescription("Schema publication requires a ready index")
                          .asRuntimeException());
      if (plan.compatibility().kind() == IndexUpdateCompatibility.Kind.NO_CHANGE
          && source.getDefinition().equals(plan.candidate())
          && this.manager.configState.staged.getIndex(source.getGenerationId().indexId).isEmpty()) {
        // A background resume may have completed between dispatch and source capture.
        return schemaDescription(source.getDefinitionGeneration());
      }
      var pending = this.catalog.schemas().get(request.getIndexId());
      accepted =
          pending != null
              && pending.source().equals(source.getDefinitionGeneration())
              && pending.target().getIndexDefinition().equals(plan.candidate());
      if (accepted) {
        schema = pending;
      } else {
        if (source.getDefinition().getDefinitionVersion().orElseThrow()
                != request.getExpectedRevision()
            || plan.compatibility().kind() != IndexUpdateCompatibility.Kind.PRESERVE_CONTENT
            || this.manager
                .configState
                .staged
                .getIndex(source.getGenerationId().indexId)
                .isPresent()) {
          throw Status.UNAVAILABLE
              .withDescription("An index lifecycle transition is pending")
              .asRuntimeException();
        }
        schema =
            new IndexSchemaUpdate(
                source.getDefinitionGeneration().asSearch(),
                new SearchIndexDefinitionGeneration(
                    AnalyzerBoundSearchIndexDefinition.create(
                        plan.candidate().asSearchDefinition(), source.getOverriddenAnalyzers()),
                    source.getGenerationId().generation));
      }
      pin = this.ingestion.acquireQueryLease(source.getGenerationId());
      factory = (LuceneIndexFactory) this.manager.configState.indexFactory;
    }
    try (pin) {
      Runnable publish;
      try {
        publish = factory.prepareSchemaUpdate(pin.index(), schema.target().getIndexDefinition());
      } catch (com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException error) {
        throw Status.INVALID_ARGUMENT
            .withDescription(error.getMessage())
            .withCause(error)
            .asRuntimeException();
      }
      this.checkpoint.accept(Checkpoint.SCHEMA_PREPARED);
      return this.ingestion.publishSchema(
          pin,
          () -> {
            synchronized (this.manager) {
              boolean staged =
                  this.manager
                      .configState
                      .staged
                      .getIndex(source.getGenerationId().indexId)
                      .isPresent();
              return this.ingestion.coordinate(
                  () -> {
                    this.catalog.planUpdate(request);
                    if (accepted
                        && !pin.retired()
                        && this.indexes
                            .getIndexById(source.getGenerationId().indexId)
                            .filter(
                                current ->
                                    current.getDefinitionGeneration().equals(schema.target()))
                            .isPresent()
                        && !staged) {
                      // Concurrent retries of the same accepted target are idempotent.
                      return schemaDescription(schema.target());
                    }
                    if (this.indexes
                            .getIndexById(source.getGenerationId().indexId)
                            .filter(
                                current ->
                                    current
                                        .getDefinitionGeneration()
                                        .equals(source.getDefinitionGeneration()))
                            .isEmpty()
                        || pin.retired()
                        || staged) {
                      throw Status.ABORTED
                          .withDescription("Physical index or schema changed during preparation")
                          .asRuntimeException();
                    }
                    if (!accepted) {
                      this.catalog.acceptSchemaUpdate(schema);
                    }
                    this.checkpoint.accept(Checkpoint.ACCEPTED);
                    try {
                      this.manager.publishLocalSchema(
                          schema.source(),
                          schema.target(),
                          () -> {
                            this.checkpoint.accept(Checkpoint.SCHEMA_JOURNALED);
                            publish.run();
                          });
                    } catch (IOException
                        | com.xgen.mongot.config.util.Invariants.InvariantException error) {
                      throw Status.UNAVAILABLE
                          .withDescription("Schema is durable; publication will resume")
                          .withCause(error)
                          .asRuntimeException();
                    }
                    this.checkpoint.accept(Checkpoint.PUBLISHED);
                    this.metrics.count("indexUpdates", 1, "kind", "PRESERVE_CONTENT");
                    this.metrics.elapsed("indexSchemaPublication", started);
                    return schemaDescription(schema.target());
                  });
            }
          });
    }
  }

  private IndexDescription schemaDescription(
      com.xgen.mongot.index.definition.IndexDefinitionGeneration schema) {
    return this.catalog.getIndex(schema.getIndexId().toHexString()).toBuilder()
        .setGenerationId(schema.getGenerationId().uniqueString())
        .setActiveRevision(schema.getIndexDefinition().getDefinitionVersion().orElseThrow())
        .setLifecycleState("READY")
        .clearFailureMessage()
        .build();
  }

  /**
   * Compatibility recovery for already accepted v2/v3 copies; new schemas never enter this path.
   */
  private IndexDescription updateLegacyOrRebuild(UpdateIndexRequest request) {
    ApiIngestionManager.IndexLease lease;
    synchronized (this.manager) {
      observe();
      var plan = this.catalog.planUpdate(request);
      boolean retryCopy =
          plan.retry()
              && this.catalog
                  .migration(request.getIndexId())
                  .filter(
                      value ->
                          value.state() == State.FAILED
                              || (value.state() == State.STAGED && !value.failure().isEmpty()))
                  .isPresent();
      if (plan.compatibility().kind() != IndexUpdateCompatibility.Kind.PRESERVE_CONTENT
          && !retryCopy) {
        return applyUpdate(request);
      }
      var source =
          this.indexes
              .getIndexById(new ObjectId(request.getIndexId()))
              .filter(value -> this.ingestion.isGenerationReady(value.getGenerationId()))
              .orElseThrow(
                  () ->
                      (retryCopy ? Status.FAILED_PRECONDITION : Status.UNAVAILABLE)
                          .withDescription(
                              "Preservation needs a ready, durable"
                                  + " source; restore it or"
                                  + " explicitly rebuild/drop")
                          .asRuntimeException());
      if (!source.getDefinitionGeneration().hasCurrentFormatVersion()) {
        throw Status.FAILED_PRECONDITION
            .withDescription("Source format is not current")
            .asRuntimeException();
      }
      lease = this.ingestion.acquireQueryLease(source.getGenerationId());
    }
    try (lease) {
      try {
        this.ingestion.validateCommittedData(lease);
      } catch (java.io.InterruptedIOException error) {
        throw Status.CANCELLED.withCause(error).asRuntimeException();
      } catch (IOException error) {
        lease
            .index()
            .setStatus(
                IndexStatus.failed(
                    "Source committed data validation failed: " + error.getMessage()));
        throw Status.FAILED_PRECONDITION
            .withDescription("Source lacks valid commits in every partition")
            .withCause(error)
            .asRuntimeException();
      }
      this.checkpoint.accept(Checkpoint.SOURCE_VALIDATED);
      synchronized (this.manager) {
        this.catalog.planUpdate(request);
        if (this.indexes
            .getIndexById(new ObjectId(request.getIndexId()))
            .filter(value -> value.getGenerationId().equals(lease.index().getGenerationId()))
            .isEmpty()) {
          throw Status.ABORTED
              .withDescription("Source generation changed during validation")
              .asRuntimeException();
        }
        if (lease.retired()) {
          throw Status.UNAVAILABLE
              .withDescription("Source is retiring or unavailable")
              .asRuntimeException();
        }
        return applyUpdate(request);
      }
    }
  }

  private IndexDescription applyUpdate(UpdateIndexRequest request) {
    synchronized (this.manager) {
      observe();
      var plan = this.catalog.planUpdate(request);
      this.metrics.count("indexUpdates", 1, "kind", plan.compatibility().kind().name());
      if (!plan.retry() && plan.compatibility().kind() == IndexUpdateCompatibility.Kind.NO_CHANGE) {
        rejectFailedBootstrapNoOp(plan.candidate());
      }
      if (plan.retry()) {
        var pending = this.catalog.migration(request.getIndexId()).orElseThrow();
        if (pending.state() == State.STAGED && !pending.failure().isEmpty()) {
          if (this.manager
              .configState
              .staged
              .getIndex(pending.target().getIndexId())
              .filter(
                  index ->
                      index.getIndex().getStatus().getStatusCode() == IndexStatus.StatusCode.FAILED)
              .isEmpty()) {
            throw Status.UNAVAILABLE
                .withDescription("Preserved target initialization has not finished")
                .asRuntimeException();
          }
          try {
            this.manager.retireFailedPreparedIndex(pending.target().getGenerationId());
            this.catalog.replaceMigration(
                pending,
                new IndexMigration(
                    pending.source(), pending.target().incrementAttempt(), State.ACCEPTED, ""));
          } catch (IOException | com.xgen.mongot.config.util.Invariants.InvariantException e) {
            throw Status.UNAVAILABLE
                .withDescription("Cannot retry preserved index update")
                .withCause(e)
                .asRuntimeException();
          }
        }
        return this.ingestion.coordinate(() -> this.catalog.updateIndex(request));
      }
      if (plan.compatibility().kind() != IndexUpdateCompatibility.Kind.PRESERVE_CONTENT) {
        var source = this.indexes.getIndexById(new ObjectId(request.getIndexId()));
        var result = this.ingestion.coordinate(() -> this.catalog.updateIndex(request));
        return source
            .map(
                index ->
                    result.toBuilder()
                        .setGenerationId(index.getGenerationId().uniqueString())
                        .setActiveRevision(index.getDefinition().getDefinitionVersion().orElse(1L))
                        .build())
            .orElse(result);
      }
      var source =
          this.indexes
              .getIndexById(new ObjectId(request.getIndexId()))
              .filter(index -> this.ingestion.isGenerationReady(index.getGenerationId()))
              .orElseThrow(
                  () ->
                      Status.UNAVAILABLE
                          .withDescription("Preservation requires a ready source" + " generation")
                          .asRuntimeException());
      if (!source.getDefinitionGeneration().hasCurrentFormatVersion()
          || source.getDefinition().getDefinitionVersion().orElseThrow()
              != request.getExpectedRevision()
          || this.manager
              .configState
              .staged
              .getIndex(new ObjectId(request.getIndexId()))
              .isPresent()) {
        throw Status.FAILED_PRECONDITION
            .withDescription("Source generation has a pending lifecycle transition")
            .asRuntimeException();
      }
      var definition = plan.candidate().asSearchDefinition();
      var version = this.manager.configState.getNewUserIndexVersion(definition.getIndexId());
      var target =
          new SearchIndexDefinitionGeneration(
              AnalyzerBoundSearchIndexDefinition.create(
                  definition, source.getOverriddenAnalyzers()),
              definition.newGeneration(version));
      var migration =
          new IndexMigration(
              source.getDefinitionGeneration().asSearch(), target, State.ACCEPTED, "");
      var accepted = this.ingestion.coordinate(() -> this.catalog.acceptMigration(migration));
      this.checkpoint.accept(Checkpoint.ACCEPTED);
      return accepted.toBuilder()
          .setGenerationId(source.getGenerationId().uniqueString())
          .setActiveRevision(source.getDefinition().getDefinitionVersion().orElse(1L))
          .build();
    }
  }

  /** A no-op must not acknowledge progress on a failed bootstrap or grant new empty permission. */
  @com.google.errorprone.annotations.concurrent.GuardedBy("this.manager")
  private void rejectFailedBootstrapNoOp(com.xgen.mongot.index.definition.IndexDefinition desired) {
    var generation =
        this.manager
            .configState
            .staged
            .getIndex(desired.getIndexId())
            .or(() -> this.indexes.getIndexById(desired.getIndexId()))
            .filter(index -> index.getDefinition().equals(desired));
    if (generation.isEmpty()) {
      return;
    }
    var index = generation.orElseThrow();
    var status = index.getIndex().getStatus();
    if (status.getStatusCode() == IndexStatus.StatusCode.FAILED
        && status
            .getReason()
            .filter(reason -> reason == IndexStatus.Reason.INITIALIZATION_FAILED)
            .isPresent()
        && this.catalog.allowsEmptyInitialization(index.getGenerationId())) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              status.getMessage().orElse("Index initialization failed")
                  + ". Restart to resume this authorized initialization, or use"
                  + " INDEX_UPDATE_MODE_REBUILD_EMPTY to start a fresh empty generation")
          .asRuntimeException();
    }
  }

  /**
   * Observes publication before accepting another update or passing desired state to the planner.
   */
  void reconcile() {
    java.util.List<String> notifications;
    synchronized (this.manager) {
      observe();
      this.tasks.entrySet().removeIf(entry -> entry.getValue().isDone());
      for (var schema : this.catalog.schemas().values()) {
        String key = "schema:" + schema.indexId();
        if (!this.stopping
            && !this.tasks.containsKey(key)
            && this.indexes
                .getIndexById(schema.source().getIndexId())
                .filter(index -> index.getDefinitionGeneration().equals(schema.source()))
                .filter(index -> this.ingestion.isGenerationReady(index.getGenerationId()))
                .isPresent()) {
          this.tasks.put(
              key,
              CompletableFuture.runAsync(
                  () -> {
                    try {
                      updateSchema(
                          UpdateIndexRequest.newBuilder()
                              .setIndexId(schema.indexId())
                              .setExpectedRevision(
                                  schema
                                      .target()
                                      .getIndexDefinition()
                                      .getDefinitionVersion()
                                      .orElseThrow())
                              .setDefinitionBson(
                                  com.google.protobuf.ByteString.copyFrom(
                                      com.xgen.mongot.standalone.catalog
                                          .StandaloneDefinitionValidator.normalize(
                                          schema.target().getIndexDefinition())))
                              .build());
                    } catch (io.grpc.StatusRuntimeException ignored) {
                      // Durable schema and unchanged data remain available for retry/restart.
                    }
                  },
                  this.worker));
        }
      }
      for (var migration : this.catalog.migrations().values()) {
        if (this.stopping || this.tasks.containsKey(migration.indexId())) {
          continue;
        }
        if (migration.state() == State.ACCEPTED
            && this.ingestion.isGenerationReady(migration.source().getGenerationId())) {
          this.tasks.put(
              migration.indexId(),
              CompletableFuture.runAsync(() -> migrate(migration), this.worker));
        } else if (migration.state() == State.CANCELLED) {
          this.tasks.put(
              migration.indexId(),
              CompletableFuture.runAsync(() -> cleanup(migration), this.worker));
        }
      }
      for (var initialization : this.catalog.initializations().values()) {
        String task = "initialization:" + initialization.key();
        boolean obsolete =
            initialization.state() == IndexInitialization.State.CANCELLED
                || (initialization.state() == IndexInitialization.State.READY
                    && this.catalog.definitions().stream()
                        .noneMatch(initialization.definition()::equals));
        if (!this.stopping
            && obsolete
            && !this.tasks.containsKey(task)
            && initialization
                .generation()
                .map(
                    value ->
                        !referenced(this.manager.getJournalSnapshot(), value.getGenerationId()))
                .orElse(true)) {
          this.tasks.put(
              task,
              CompletableFuture.runAsync(() -> cleanupInitialization(initialization), this.worker));
        }
      }
      notifications = java.util.List.copyOf(this.published);
      this.published.clear();
    }
    notifications.forEach(this.publicationListener);
  }

  private void observe() {
    synchronized (this.manager) {
      for (var migration : this.catalog.migrations().values()) {
        if (migration.state() == State.CANCELLED) {
          continue;
        }
        if (migration.state() == State.COMPLETE) {
          if (!migration.failure().isEmpty()
              && this.ingestion.isGenerationReady(migration.target().getGenerationId())) {
            this.catalog.replaceMigration(migration, migration.withState(State.COMPLETE, ""));
          }
          continue;
        }
        var live = this.indexes.getIndexById(migration.target().getIndexId());
        if (live.filter(
                index -> index.getGenerationId().equals(migration.target().getGenerationId()))
            .isPresent()) {
          this.checkpoint.accept(Checkpoint.PUBLISHED);
          if (this.catalog.replaceMigration(migration, migration.withState(State.COMPLETE, ""))) {
            this.published.add(migration.source().getGenerationId().uniqueString());
          }
          continue;
        }
        var staged = this.manager.configState.staged.getIndex(migration.target().getIndexId());
        if (staged.isEmpty()
            && this.manager.getJournalSnapshot().getDeletedEntries().stream()
                .anyMatch(
                    entry ->
                        entry.getGenerationId().equals(migration.target().getGenerationId()))) {
          this.catalog.replaceMigration(
              migration,
              new IndexMigration(
                  migration.source(), migration.target().incrementAttempt(), State.ACCEPTED, ""));
          continue;
        }
        if (staged
            .filter(index -> index.getGenerationId().equals(migration.target().getGenerationId()))
            .isPresent()) {
          var status = staged.orElseThrow().getIndex().getStatus();
          String failure =
              status.getStatusCode() == IndexStatus.StatusCode.FAILED
                  ? status.getMessage().orElse("Preserved target initialization failed")
                  : migration.failure();
          if (migration.state() != State.STAGED || !migration.failure().equals(failure)) {
            this.catalog.replaceMigration(migration, migration.withState(State.STAGED, failure));
          }
        }
      }
    }
  }

  private void checkCurrent(IndexMigration migration) throws IOException {
    if (this.stopping
        || Thread.currentThread().isInterrupted()
        || this.catalog
            .migration(migration.indexId())
            .filter(
                current ->
                    current.target().equals(migration.target())
                        && current.state() == State.ACCEPTED)
            .isEmpty()) {
      throw new IOException("Index update was cancelled or superseded");
    }
  }

  private void migrate(IndexMigration migration) {
    long started = System.nanoTime();
    Path temporary = IndexSeed.temporary(this.config.dataPath(), migration);
    try {
      synchronized (this.manager) {
        checkCurrent(migration);
      }
      if (Files.exists(IndexSeed.path(this.config.dataPath(), migration.target()))) {
        try {
          IndexSeed.verifyPrepared(this.config.dataPath(), migration);
          synchronized (this.manager) {
            checkCurrent(migration);
            this.catalog.replaceMigration(migration, migration.withState(State.PREPARED, ""));
          }
          return;
        } catch (IOException invalidSeed) {
          // An explicit retry may replace an invalid, unjournaled copy from an
          // interrupted
          // preparation. The source remains fenced and will be committed again below.
        }
      }
      long drainStarted = System.nanoTime();
      this.ingestion.quiesce(migration.source().getGenerationId()).get(30, TimeUnit.SECONDS);
      this.metrics.elapsed("indexUpdateDrain", drainStarted);
      try (var lease = this.ingestion.acquireUpdateLease(migration.source().getGenerationId())) {
        synchronized (this.manager) {
          checkCurrent(migration);
          if (referenced(this.manager.getJournalSnapshot(), migration)) {
            throw new IOException("Cannot replace a journaled index seed");
          }
        }
        IndexSeed.remove(IndexSeed.path(this.config.dataPath(), migration.target()));
        IndexSeed.remove(temporary);
        this.ingestion.commitForUpdate(lease);
        this.checkpoint.accept(Checkpoint.COMMITTED);
        long estimate = IndexSeed.size(IndexSeed.path(this.config.dataPath(), migration.source()));
        if (Files.getFileStore(this.config.dataPath()).getUsableSpace() - estimate
            < this.config.minFreeDiskBytes()) {
          throw new IOException("Insufficient disk headroom to preserve the index");
        }
        this.ingestion.copyCommittedData(lease, temporary);
        this.metrics.count("indexUpdateCopiedBytes", IndexSeed.size(temporary));
        IndexSeed.prepare(temporary, migration);
        synchronized (this.manager) {
          checkCurrent(migration);
          Path target = IndexSeed.path(this.config.dataPath(), migration.target());
          if (Files.exists(target)) {
            throw new IOException("Reserved target directory already exists");
          }
          FileUtils.atomicallyRename(temporary, target);
        }
        this.checkpoint.accept(Checkpoint.INSTALLED);
        synchronized (this.manager) {
          checkCurrent(migration);
          this.catalog.replaceMigration(migration, migration.withState(State.PREPARED, ""));
        }
        this.checkpoint.accept(Checkpoint.PREPARED);
        this.metrics.elapsed("indexUpdatePreparation", started, "outcome", "prepared");
      }
    } catch (Exception error) {
      this.metrics.elapsed("indexUpdatePreparation", started, "outcome", "failed");
      synchronized (this.manager) {
        var current = this.catalog.migration(migration.indexId());
        if (current
            .filter(
                value ->
                    value.target().equals(migration.target()) && value.state() == State.ACCEPTED)
            .isPresent()) {
          this.catalog.replaceMigration(
              current.orElseThrow(),
              migration.withState(
                  State.FAILED, "Preserving index update failed: " + error.getMessage()));
        }
      }
    } finally {
      try {
        IndexSeed.remove(temporary);
      } catch (IOException ignored) {
        // The durable operation identifies this temporary directory for retry/startup
        // cleanup.
      }
    }
  }

  private void cleanup(IndexMigration migration) {
    synchronized (this.manager) {
      if (referenced(this.manager.getJournalSnapshot(), migration)) {
        return;
      }
    }
    try {
      IndexSeed.remove(IndexSeed.temporary(this.config.dataPath(), migration));
      IndexSeed.remove(IndexSeed.path(this.config.dataPath(), migration.target()));
      this.catalog.removeCancelledMigration(migration.indexId());
    } catch (IOException ignored) {
      // Retain the cancellation tombstone and retry on the next reconciliation.
    }
  }

  private void cleanupInitialization(IndexInitialization initialization) {
    synchronized (this.manager) {
      if (!initialization.equals(this.catalog.initializations().get(initialization.key()))
          || initialization
              .generation()
              .filter(
                  value -> referenced(this.manager.getJournalSnapshot(), value.getGenerationId()))
              .isPresent()) {
        return;
      }
    }
    try {
      if (initialization.generation().isPresent()) {
        var generation = initialization.generation().orElseThrow();
        IndexSeed.remove(IndexSeed.path(this.config.dataPath(), generation));
        IndexSeed.remove(
            this.config
                .dataPath()
                .resolve("indexMetadata")
                .resolve(
                    com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper
                        .getNameForGeneration(generation)));
      }
      this.catalog.removeInitialization(initialization);
    } catch (IOException ignored) {
      // Keep the owner record until physical cleanup succeeds.
    }
  }

  private static boolean referenced(ConfigJournalV1 journal, IndexMigration migration) {
    return referenced(journal, migration.target().getGenerationId());
  }

  private static boolean referenced(
      ConfigJournalV1 journal, com.xgen.mongot.index.version.GenerationId generation) {
    return java.util.stream.Stream.of(
            journal.getLiveEntries(), journal.getStagedEntries(), journal.getDeletedEntries())
        .flatMap(java.util.Collection::stream)
        .anyMatch(entry -> entry.getGenerationId().equals(generation));
  }

  @Override
  public void close() throws IOException {
    this.stopping = true;
    this.worker.shutdownNow();
    try {
      if (!this.worker.awaitTermination(30, TimeUnit.SECONDS)) {
        throw new IOException("Preserving index updates did not drain");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }
}
