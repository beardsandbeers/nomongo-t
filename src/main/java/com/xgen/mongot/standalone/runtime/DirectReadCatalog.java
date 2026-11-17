package com.xgen.mongot.standalone.runtime;

import com.google.protobuf.ByteString;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.config.manager.GenerationReadLeases;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.server.command.management.util.IndexMapper;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.grpc.IndexBuild;
import com.xgen.mongot.standalone.grpc.IndexDescription;
import com.xgen.mongot.standalone.grpc.IndexNamespace;
import com.xgen.mongot.standalone.grpc.IndexOwnership;
import com.xgen.mongot.standalone.grpc.IndexVersion;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/**
 * Read-only projection of both authorities over the shared engine. Never admits document writes.
 */
public final class DirectReadCatalog {
  /** Accepted remote metadata, with independent enumeration and single-ID lookup. */
  public interface RemoteDefinitions {
    List<IndexDefinition> snapshot();

    Optional<IndexDefinition> get(ObjectId id);
  }

  private final LocalIndexCatalog local;
  private final DefaultConfigManager manager;
  private final IndexCatalog live;
  private final InitializedIndexCatalog initialized;
  private final RemoteDefinitions remote;
  private final Object metadataMonitor;
  private com.xgen.mongot.standalone.metrics.StandaloneMetrics metrics =
      com.xgen.mongot.standalone.metrics.StandaloneMetrics.disabled();

  public DirectReadCatalog(
      LocalIndexCatalog local,
      DefaultConfigManager manager,
      IndexCatalog live,
      InitializedIndexCatalog initialized) {
    this(
        local,
        manager,
        live,
        initialized,
        new RemoteDefinitions() {
          @Override
          public List<IndexDefinition> snapshot() {
            return List.of();
          }

          @Override
          public Optional<IndexDefinition> get(ObjectId id) {
            return Optional.empty();
          }
        },
        new Object());
  }

  public DirectReadCatalog(
      LocalIndexCatalog local,
      DefaultConfigManager manager,
      IndexCatalog live,
      InitializedIndexCatalog initialized,
      RemoteDefinitions remote,
      Object metadataMonitor) {
    this.metadataMonitor = metadataMonitor;
    this.local = local;
    this.manager = manager;
    this.live = live;
    this.initialized = initialized;
    this.remote = remote;
  }

  public void observe(com.xgen.mongot.standalone.metrics.StandaloneMetrics metrics) {
    this.metrics = metrics;
    GenerationReadLeases leases;
    synchronized (this.manager) {
      leases = this.manager.configState.readLeases;
    }
    metrics.gauge("reads.active", leases, GenerationReadLeases::activeCount);
    metrics.gauge("reads.invalidations", leases, GenerationReadLeases::invalidationCount);
    metrics.gauge("reads.drains", leases, GenerationReadLeases::drainCount);
    metrics.gauge("reads.drain.total_nanos", leases, GenerationReadLeases::drainNanos);
  }

  public IndexCatalog liveIndexes() {
    return this.live;
  }

  /** Lock order matches reconciliation: coordinator, config manager, then local metadata. */
  public List<IndexDescription> snapshot() {
    synchronized (this.metadataMonitor) {
      var definitions = new LinkedHashMap<ObjectId, IndexDefinition>();
      this.remote.snapshot().forEach(value -> definitions.put(value.getIndexId(), value));
      synchronized (this.manager) {
        synchronized (this.local) {
          this.local.definitions().forEach(value -> definitions.put(value.getIndexId(), value));
          var ids = new LinkedHashSet<>(definitions.keySet());
          // Unreadable desired definitions are omitted by the remote provider, but native
          // reconciliation retains their live/staged generations. Actual deletions detach these
          // entries under the same coordinator/manager locks; never include phasing-out indexes.
          java.util.stream.Stream.concat(
                  this.live.getIndexes().stream(),
                  this.manager.configState.staged.getIndexes().stream())
              .map(generation -> generation.getGenerationId().indexId)
              .filter(id -> !this.local.owns(id))
              .forEach(ids::add);
          var quarantine = this.manager.configState.incompatibleIndexes;
          java.util.stream.Stream.concat(
                  quarantine.getLiveIndexes().stream(), quarantine.getStagedIndexes().stream())
              .map(value -> value.indexId())
              .forEach(ids::add);
          var result = new ArrayList<IndexDescription>();
          for (var id : ids) {
            result.add(describeIndex(id, Optional.ofNullable(definitions.get(id))));
          }
          return result.stream()
              .sorted(Comparator.comparing(IndexDescription::getIndexId))
              .toList();
        }
      }
    }
  }

  public IndexDescription describe(String id) {
    LocalIndexCatalog.validateIndexId(id);
    var key = new ObjectId(id);
    synchronized (this.metadataMonitor) {
      var remoteDefinition = this.remote.get(key);
      synchronized (this.manager) {
        synchronized (this.local) {
          var desired =
              this.local.owns(key) ? Optional.of(this.local.getDefinition(id)) : remoteDefinition;
          return describeIndex(key, desired);
        }
      }
    }
  }

  @com.google.errorprone.annotations.concurrent.GuardedBy("this.manager")
  private IndexDescription describeIndex(ObjectId id, Optional<IndexDefinition> desired) {
    var quarantine = this.manager.configState.incompatibleIndexes;
    var liveQuarantine = quarantine.getLiveIndex(id);
    var stagedQuarantine = quarantine.getStagedIndex(id);
    var staged = this.manager.configState.staged.getIndex(id);
    var known =
        desired
            .or(() -> this.live.getIndexById(id).map(IndexGeneration::getDefinition))
            .or(() -> staged.map(IndexGeneration::getDefinition));
    var result =
        known
            .map(this::describeDefinition)
            .map(IndexDescription::toBuilder)
            .orElseGet(
                () -> {
                  var entry =
                      liveQuarantine
                          .or(() -> stagedQuarantine)
                          .orElseThrow(
                              () ->
                                  Status.NOT_FOUND
                                      .withDescription("Index does not exist")
                                      .asRuntimeException());
                  return IndexDescription.newBuilder()
                      .setIndexId(id.toHexString())
                      .setName(entry.name())
                      .setOwnership(owner(id))
                      .setNamespace(
                          IndexNamespace.newBuilder()
                              .setDatabase(entry.database())
                              .setCollectionUuid(entry.collectionUuid().toString()))
                      .setLifecycleState("QUARANTINED")
                      .setFailureMessage("Index definition is incompatible with this server");
                });
    if (desired.isEmpty() && known.isPresent()) {
      // Retained definitions describe the active/build resources, not the unreadable desired state.
      result
          .clearDesiredDefinitionBson()
          .clearDesiredRevision()
          .clearDesiredVersion()
          .setLifecycleState("FAILED")
          .setFailureMessage("Desired index definition is incompatible with this server");
    }
    if (liveQuarantine.isPresent() && result.getGenerationId().isEmpty()) {
      result
          .setLifecycleState("QUARANTINED")
          .setFailureMessage("Index definition is incompatible with this server");
    }
    if (stagedQuarantine.isPresent() && !result.hasBuild()) {
      result
          .setBuild(
              IndexBuild.newBuilder()
                  .setGenerationId(stagedQuarantine.get().generationId().uniqueString())
                  .setState("QUARANTINED"))
          .setLifecycleState(result.getGenerationId().isEmpty() ? "QUARANTINED" : "FAILED")
          .setFailureMessage("Replacement index definition is incompatible with this server");
    }
    return result.build();
  }

  @com.google.errorprone.annotations.concurrent.GuardedBy("this.manager")
  private IndexDescription describeDefinition(IndexDefinition desired) {
    var id = desired.getIndexId();
    boolean standalone = this.local.owns(id);
    var result =
        standalone
            ? this.local.getIndex(id.toHexString()).toBuilder()
            : IndexDescription.newBuilder().setIndexId(id.toHexString()).setName(desired.getName());
    result
        .setOwnership(owner(id))
        .setType(
            desired instanceof SearchIndexDefinition
                ? com.xgen.mongot.standalone.grpc.IndexType.INDEX_TYPE_SEARCH
                : com.xgen.mongot.standalone.grpc.IndexType.INDEX_TYPE_VECTOR_SEARCH)
        .setDesiredDefinitionBson(encodeDefinition(desired))
        .setDesiredRevision(revision(desired))
        .setDesiredVersion(version(desired))
        .setLifecycleState("PENDING");
    if (!standalone) {
      var namespace =
          IndexNamespace.newBuilder()
              .setDatabase(desired.getDatabase())
              .setCollectionUuid(desired.getCollectionUuid().toString())
              .setCollectionName(desired.getLastObservedCollectionName());
      desired.getView().ifPresent(value -> namespace.setViewName(value.getName()));
      result.setNamespace(namespace);
    }
    if (desired instanceof SearchIndexDefinition) {
      result.addAllReadCapabilities(List.of("search", "search_meta", "search_mappings", "joins"));
    } else {
      result.addAllReadCapabilities(List.of("vector_search", "vector_fields"));
    }
    result.addAllReadCapabilities(List.of("index_statistics", "field_statistics"));
    this.live
        .getIndexById(id)
        .ifPresent(
            generation -> {
              var definition = generation.getDefinition();
              var status = generation.getIndex().getStatus();
              boolean ready =
                  (standalone
                          ? status.getStatusCode() == IndexStatus.StatusCode.STEADY
                          : status.canServiceQueries())
                      && this.initialized.getIndex(generation.getGenerationId()).isPresent();
              result
                  .setGenerationId(generation.getGenerationId().uniqueString())
                  .setActiveRevision(revision(definition))
                  .setActiveVersion(version(definition))
                  .setActiveDefinitionBson(encodeDefinition(definition))
                  .setQueryable(ready)
                  .setSourceStatus(status.getStatusCode().name())
                  .setFailureMessage(status.getMessage().orElse(""))
                  .setLifecycleState(
                      switch (status.getStatusCode()) {
                        case STEADY -> "READY";
                        case STALE -> "STALE";
                        case RECOVERING_TRANSIENT, RECOVERING_NON_TRANSIENT -> "RECOVERING";
                        case FAILED -> "FAILED";
                        default -> "BUILDING";
                      });
              if (standalone && revision(definition) != revision(desired)) {
                result.setLifecycleState("BUILDING").setQueryable(false);
              }
            });
    this.manager
        .configState
        .staged
        .getIndex(id)
        .ifPresent(
            staged -> {
              var status = staged.getIndex().getStatus();
              result
                  .setLifecycleState(
                      status.getStatusCode() == IndexStatus.StatusCode.FAILED
                          ? "FAILED"
                          : "BUILDING")
                  .setBuild(
                      IndexBuild.newBuilder()
                          .setGenerationId(staged.getGenerationId().uniqueString())
                          .setVersion(version(staged.getDefinition()))
                          .setDefinitionBson(encodeDefinition(staged.getDefinition()))
                          .setState(status.getStatusCode().name()));
              if (status.getStatusCode() == IndexStatus.StatusCode.FAILED) {
                result.setFailureMessage(status.getMessage().orElse("Index initialization failed"));
              }
            });
    if (standalone) {
      this.local
          .migration(id.toHexString())
          .filter(migration -> !migration.failure().isEmpty())
          .ifPresent(
              migration ->
                  result.setLifecycleState("FAILED").setFailureMessage(migration.failure()));
    }
    return result.build();
  }

  /** Resolves a query-ready generation, enforcing an optional physical generation fence. */
  public IndexGeneration resolve(String id, String expected) {
    LocalIndexCatalog.validateIndexId(id);
    var key = new ObjectId(id);
    synchronized (this.manager) {
      if (this.live.getIndexById(key).isPresent()) {
        return resolveLive(id, expected);
      }
    }
    // Only a missing active generation needs desired/quarantine metadata to distinguish
    // pending, incompatible, and unknown IDs. Never acquire the coordinator under the manager.
    synchronized (this.metadataMonitor) {
      var remoteDefinition = this.remote.get(key);
      synchronized (this.manager) {
        if (this.live.getIndexById(key).isPresent()) {
          return resolveLive(id, expected);
        }
        var quarantine = this.manager.configState.incompatibleIndexes;
        if (quarantine.getLiveIndex(key).isPresent()
            || quarantine.getStagedIndex(key).isPresent()) {
          throw Status.FAILED_PRECONDITION
              .withDescription("Index definition is incompatible with this server")
              .asRuntimeException();
        }
        if (this.local.owns(key)) {
          this.local.getDefinition(id); // A dropped local ID remains owned, but is not readable.
        } else if (remoteDefinition.isEmpty()) {
          throw Status.NOT_FOUND.withDescription("Index does not exist").asRuntimeException();
        }
        return resolveLive(id, expected);
      }
    }
  }

  private IndexGeneration resolveLive(String id, String expected) {
    var generation =
        this.live
            .getIndexById(new ObjectId(id))
            .orElseThrow(
                () ->
                    Status.UNAVAILABLE
                        .withDescription("Index has no active generation")
                        .asRuntimeException());
    if (!expected.isEmpty() && !generation.getGenerationId().uniqueString().equals(expected)) {
      throw Status.FAILED_PRECONDITION
          .withDescription("Requested generation is no longer live")
          .asRuntimeException();
    }
    if (this.local.owns(generation.getGenerationId().indexId)) {
      var desired = this.local.getDefinition(id);
      if (revision(desired) != revision(generation.getDefinition())
          || generation.getIndex().getStatus().getStatusCode() != IndexStatus.StatusCode.STEADY) {
        throw Status.UNAVAILABLE.withDescription("Index schema is changing").asRuntimeException();
      }
    }
    if (!generation.getIndex().getStatus().canServiceQueries()
        || this.initialized.getIndex(generation.getGenerationId()).isEmpty()) {
      throw Status.UNAVAILABLE.withDescription("Index is not query-ready").asRuntimeException();
    }
    return generation;
  }

  /** Pins the current definition and logical reader while retaining its physical resources. */
  public GenerationReadLeases.Lease acquire(String id, String expected) {
    // Snapshot providers may take the coordinator lock. Never call them under the manager lock.
    IndexGeneration resolved;
    try {
      resolved = resolve(id, expected);
    } catch (io.grpc.StatusRuntimeException error) {
      this.metrics.count("reads.resolution", 1, "status", error.getStatus().getCode().name());
      throw error;
    }
    this.metrics.count("reads.resolution", 1, "status", "OK");
    synchronized (this.manager) {
      var generation = resolveLive(id, resolved.getGenerationId().uniqueString());
      try {
        return this.manager.configState.readLeases.acquire(
            generation, this.initialized.getIndex(generation.getGenerationId()).orElseThrow());
      } catch (IllegalStateException error) {
        throw Status.UNAVAILABLE
            .withDescription("Read generation is retiring")
            .withCause(error)
            .asRuntimeException();
      }
    }
  }

  public GenerationReadLeases.Lease acquire(GenerationId generation) {
    return acquire(generation.indexId.toHexString(), generation.uniqueString());
  }

  /** Validates an existing lease; compatible logical revisions do not retire its readers. */
  public boolean isGenerationReady(GenerationId generation) {
    var liveGeneration = this.live.getIndexById(generation.indexId);
    if (liveGeneration.isEmpty()
        || !liveGeneration.get().getGenerationId().equals(generation)
        || !liveGeneration.get().getIndex().getStatus().canServiceQueries()
        || this.initialized.getIndex(generation).isEmpty()) {
      return false;
    }
    if (this.local.owns(generation.indexId)) {
      return liveGeneration.get().getIndex().getStatus().getStatusCode()
              == IndexStatus.StatusCode.STEADY
          && this.local.permitsReadContinuation(
              generation, revision(liveGeneration.get().getDefinition()));
    }
    return true;
  }

  public void invalidate(String id) {
    synchronized (this.manager) {
      this.live
          .getIndexById(new ObjectId(id))
          .ifPresent(
              generation ->
                  this.manager.configState.readLeases.invalidate(generation.getGenerationId()));
    }
  }

  public IndexOwnership owner(ObjectId id) {
    return this.local.owns(id)
        ? IndexOwnership.INDEX_OWNERSHIP_STANDALONE
        : IndexOwnership.INDEX_OWNERSHIP_REPLICATED;
  }

  public long revision(IndexDefinition definition) {
    return definition
        .getDefinitionVersion()
        .orElse(this.local.owns(definition.getIndexId()) ? 1L : 0L);
  }

  public IndexVersion version(IndexDefinition definition) {
    var version =
        IndexVersion.newBuilder()
            .setDomain(
                this.local.owns(definition.getIndexId())
                    ? IndexVersion.Domain.DOMAIN_STANDALONE_REVISION
                    : IndexVersion.Domain.DOMAIN_NATIVE_DEFINITION_VERSION);
    definition.getDefinitionVersion().ifPresent(version::setDefinitionVersion);
    return version.build();
  }

  public BsonDocument publicDefinition(IndexDefinition definition) {
    var result = new BsonDocument();
    result.putAll(
        this.local.owns(definition.getIndexId())
            ? new org.bson.RawBsonDocument(StandaloneDefinitionValidator.normalize(definition))
            : IndexMapper.toExternal(definition).toBson());
    return result;
  }

  private ByteString encodeDefinition(IndexDefinition definition) {
    return ByteString.copyFrom(
        this.local.owns(definition.getIndexId())
            ? StandaloneDefinitionValidator.normalize(definition)
            : StandaloneDefinitionValidator.encode(publicDefinition(definition)));
  }
}
