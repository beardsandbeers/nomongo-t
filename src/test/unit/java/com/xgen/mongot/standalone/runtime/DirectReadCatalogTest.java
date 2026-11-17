package com.xgen.mongot.standalone.runtime;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.xgen.mongot.config.manager.GenerationReadLeases;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.IndexOwnership;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.testing.mongot.config.manager.DefaultConfigManagerMocks;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.SearchIndex;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DirectReadCatalogTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void describe_unreadableDesiredDefinition_preservesQueryableIdentity() throws Exception {
    assertUnreadableDiscovery(false);
  }

  @Test
  public void snapshot_unreadableDesiredDefinition_preservesQueryableIdentity() throws Exception {
    assertUnreadableDiscovery(true);
  }

  private void assertUnreadableDiscovery(boolean listing) throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      var coordinator =
          new com.xgen.mongot.standalone.coexistence.CoexistenceConfigManager(
              fixture.manager.configManager,
              fixture.local,
              mock(com.xgen.mongot.lifecycle.LifecycleManager.class),
              com.xgen.mongot.monitor.ReplicationStateMonitor.enabled());
      try {
        var reads =
            new DirectReadCatalog(
                fixture.local,
                fixture.manager.configManager,
                fixture.manager.indexCatalog,
                fixture.manager.mockedDependencies.initializedIndexCatalog,
                coordinator,
                coordinator);
        assertThat(reads.describe(fixture.id()).getQueryable()).isTrue();

        coordinator.update(
            List.of(),
            List.of(),
            List.of(),
            java.util.Set.of(),
            java.util.Set.of(fixture.generation.getDefinition().getIndexId()));

        assertThat(reads.resolve(fixture.id(), "")).isSameInstanceAs(fixture.generation);
        try (var lease = reads.acquire(fixture.id(), "")) {
          assertThat(lease.retired()).isFalse();
        }
        if (listing) {
          assertThat(reads.snapshot()).hasSize(1);
        } else {
          assertThat(reads.describe(fixture.id()).getQueryable()).isTrue();
        }
        var retained = reads.describe(fixture.id());
        assertThat(reads.snapshot()).containsExactly(retained);
        assertThat(retained.getOwnership()).isEqualTo(IndexOwnership.INDEX_OWNERSHIP_REPLICATED);
        assertThat(retained.getGenerationId())
            .isEqualTo(fixture.generation.getGenerationId().uniqueString());
        assertThat(retained.getActiveDefinitionBson()).isNotEmpty();
        assertThat(retained.hasActiveVersion()).isTrue();
        assertThat(retained.getDesiredDefinitionBson()).isEmpty();
        assertThat(retained.hasDesiredVersion()).isFalse();
        assertThat(retained.getDesiredRevision()).isEqualTo(0);
        assertThat(retained.getLifecycleState()).isEqualTo("FAILED");
        assertThat(retained.getSourceStatus()).isEqualTo("STEADY");
        assertThat(retained.getFailureMessage())
            .contains("Desired index definition is incompatible");

        coordinator.update(
            List.of(),
            List.of(fixture.generation.getDefinition().asSearchDefinition()),
            List.of(),
            java.util.Set.of(),
            java.util.Set.of());
        var recovered = reads.describe(fixture.id());
        assertThat(recovered.getQueryable()).isTrue();
        assertThat(recovered.getDesiredDefinitionBson()).isNotEmpty();
        assertThat(recovered.hasDesiredVersion()).isTrue();
        assertThat(recovered.getLifecycleState()).isEqualTo("READY");
        assertThat(recovered.getFailureMessage()).isEmpty();

        coordinator.update(List.of(), List.of(), List.of(), java.util.Set.of(), java.util.Set.of());
        assertThat(reads.snapshot()).isEmpty();
        assertThat(
                assertThrows(StatusRuntimeException.class, () -> reads.describe(fixture.id()))
                    .getStatus()
                    .getCode())
            .isEqualTo(Status.Code.NOT_FOUND);
        assertThat(
                assertThrows(StatusRuntimeException.class, () -> reads.acquire(fixture.id(), ""))
                    .getStatus()
                    .getCode())
            .isEqualTo(Status.Code.NOT_FOUND);
      } finally {
        coordinator.stopSourceUpdates();
      }
    }
  }

  @Test
  public void snapshot_bothOwnersWithSameName_preservesIdentityAndNativeVersionZero()
      throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      var local =
          fixture.local.createIndex(
              CreateIndexRequest.newBuilder()
                  .setName("default")
                  .setType(IndexType.INDEX_TYPE_SEARCH)
                  .setDefinitionBson(
                      com.google.protobuf.ByteString.copyFrom(
                          com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator.encode(
                              org.bson.BsonDocument.parse("{mappings:{dynamic:true}}"))))
                  .build());

      var descriptions = fixture.reads.snapshot();
      var remote = fixture.reads.describe(fixture.id());

      assertThat(descriptions).hasSize(2);
      assertThat(remote.getOwnership()).isEqualTo(IndexOwnership.INDEX_OWNERSHIP_REPLICATED);
      assertThat(remote.getActiveVersion().hasDefinitionVersion()).isTrue();
      assertThat(remote.getActiveVersion().getDefinitionVersion()).isEqualTo(0);
      assertThat(remote.getNamespace().getDatabase()).isEqualTo("database");
      assertThat(remote.getQueryable()).isTrue();
      assertThat(fixture.reads.describe(local.getIndexId()).getOwnership())
          .isEqualTo(IndexOwnership.INDEX_OWNERSHIP_STANDALONE);
      assertThat(
              assertThrows(StatusRuntimeException.class, () -> fixture.local.getIndex(fixture.id()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
    }
  }

  @Test
  public void acquire_recoveringNativeGeneration_remainsReadableWithoutApiIngestion()
      throws Exception {
    try (var fixture =
            new Fixture(IndexStatus.recoveringNonTransient(new org.bson.BsonTimestamp(1, 0)));
        var lease = fixture.reads.acquire(fixture.id(), "")) {
      assertThat(lease.retired()).isFalse();
      assertThat(fixture.reads.describe(fixture.id()).getSourceStatus())
          .isEqualTo("RECOVERING_NON_TRANSIENT");
    }
  }

  @Test
  public void acquire_initialSync_isDiscoverableButUnavailable() throws Exception {
    try (var fixture = new Fixture(IndexStatus.initialSync())) {
      assertThat(fixture.reads.describe(fixture.id()).getQueryable()).isFalse();
      assertThat(
              assertThrows(
                      StatusRuntimeException.class, () -> fixture.reads.acquire(fixture.id(), ""))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.UNAVAILABLE);
    }
  }

  @Test
  public void acquire_generationFence_doesNotSelectAnotherGeneration() throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () -> fixture.reads.acquire(fixture.id(), "old-generation"))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
    }
  }

  @Test
  public void invalidate_activeRead_defersPhysicalRetirementUntilLeaseCloses() throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      var lease = fixture.reads.acquire(fixture.id(), "");
      var notifications = new AtomicInteger();
      var notified = new java.util.concurrent.CountDownLatch(1);
      lease.onInvalidated(
          () -> {
            notifications.incrementAndGet();
            notified.countDown();
          });

      var drained = fixture.registry().invalidate(fixture.generation.getGenerationId());
      assertThat(notified.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(lease.retired()).isTrue();
      assertThat(drained.isDone()).isFalse();
      lease.close();
      lease.close();
      drained.get(5, TimeUnit.SECONDS);

      assertThat(notifications.get()).isEqualTo(1);
    }
  }

  @Test
  public void describe_replacementBuilding_keepsActiveDefinitionAndQueryability() throws Exception {
    assertReplacementBuilding(false);
  }

  @Test
  public void describe_unreadableDesiredWithReplacementBuilding_keepsActiveAndBuild()
      throws Exception {
    assertReplacementBuilding(true);
  }

  private void assertReplacementBuilding(boolean unreadableDesired) throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      var next =
          com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder.create(
              SearchIndexDefinitionBuilder.from(
                      fixture.generation.getDefinition().asSearchDefinition())
                  .definitionVersion(1L)
                  .build(),
              new com.xgen.mongot.index.version.Generation(
                  new com.xgen.mongot.index.version.UserIndexVersion(1),
                  com.xgen.mongot.index.version.IndexFormatVersion.CURRENT),
              List.of());
      var staged = com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration(next);
      var manager = fixture.manager.configManager;
      synchronized (manager) {
        manager.configState.staged.addIndex(staged);
      }
      if (unreadableDesired) {
        when(fixture.remote.get(fixture.generation.getDefinition().getIndexId()))
            .thenReturn(Optional.empty());
        when(fixture.remote.snapshot()).thenReturn(List.of());
      }

      var description = fixture.reads.describe(fixture.id());

      assertThat(description.getLifecycleState())
          .isEqualTo(unreadableDesired ? "FAILED" : "BUILDING");
      assertThat(description.getQueryable()).isTrue();
      assertThat(description.getActiveVersion().getDefinitionVersion()).isEqualTo(0);
      assertThat(description.getBuild().getVersion().getDefinitionVersion()).isEqualTo(1);
      assertThat(fixture.reads.snapshot()).containsExactly(description);
      assertThat(description.hasDesiredVersion()).isEqualTo(!unreadableDesired);
    }
  }

  @Test
  public void describe_onlyPhasingOutGeneration_doesNotResurrectDeletedIndex() throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      var id = fixture.generation.getGenerationId().indexId;
      var manager = fixture.manager.configManager;
      synchronized (manager) {
        fixture.manager.indexCatalog.removeIndex(id);
        manager.configState.phasingOut.addIndex(fixture.generation);
      }
      when(fixture.remote.get(id)).thenReturn(Optional.empty());
      when(fixture.remote.snapshot()).thenReturn(List.of());

      var error =
          assertThrows(StatusRuntimeException.class, () -> fixture.reads.describe(fixture.id()));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
      assertThat(fixture.reads.snapshot()).isEmpty();
      assertThat(
              fixture.manager.mockedDependencies.initializedIndexCatalog.getIndex(
                  fixture.generation.getGenerationId()))
          .isPresent();
    }
  }

  @Test
  public void close_inflightReader_drainsOutsideManagerMonitor() throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      var lease = fixture.reads.acquire(fixture.id(), "");
      var invalidated = new java.util.concurrent.CountDownLatch(1);
      lease.onInvalidated(invalidated::countDown);
      var closing =
          java.util.concurrent.CompletableFuture.runAsync(fixture.manager.configManager::close);
      try {
        assertThat(invalidated.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(closing.isDone()).isFalse();
        // A reader can finish dependency checks while shutdown waits for its lease.
        synchronized (fixture.manager.configManager) {
          assertThat(lease.retired()).isTrue();
        }
      } finally {
        lease.close();
      }
      closing.get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void acquire_closedRegistry_rejectsAdmissionWithoutLeaking() throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      fixture.registry().stop().get(5, TimeUnit.SECONDS);

      assertThat(
              assertThrows(
                      StatusRuntimeException.class, () -> fixture.reads.acquire(fixture.id(), ""))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.UNAVAILABLE);
    }
  }

  @Test
  public void resolve_activeGeneration_doesNotReadOrProjectRemoteMetadata() throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      var resolved = fixture.reads.resolve(fixture.id(), "");
      try (var lease = fixture.reads.acquire(fixture.id(), "")) {
        assertThat(resolved).isSameInstanceAs(fixture.generation);
        assertThat(lease.retired()).isFalse();
        assertThat(fixture.reads.isGenerationReady(resolved.getGenerationId())).isTrue();
      }

      verifyNoInteractions(fixture.remote);
    }
  }

  @Test
  public void describe_singleIndex_doesNotEnumerateRemoteDefinitions() throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      var description = fixture.reads.describe(fixture.id());

      assertThat(description.getIndexId()).isEqualTo(fixture.id());
      verify(fixture.remote).get(fixture.generation.getDefinition().getIndexId());
      verify(fixture.remote, never()).snapshot();
    }
  }

  @Test
  public void resolve_missingGeneration_distinguishesPendingAndUnknownIds() throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      fixture.manager.indexCatalog.removeIndex(fixture.generation.getDefinition().getIndexId());
      var unknown = new org.bson.types.ObjectId();

      var pending =
          assertThrows(StatusRuntimeException.class, () -> fixture.reads.resolve(fixture.id(), ""));
      var missing =
          assertThrows(
              StatusRuntimeException.class, () -> fixture.reads.resolve(unknown.toHexString(), ""));

      assertThat(pending.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
      assertThat(missing.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
      verify(fixture.remote, never()).snapshot();
    }
  }

  @Test
  public void resolve_generationAppearsDuringMetadataLookup_rechecksLiveCatalog() throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      var definition = fixture.generation.getDefinition();
      fixture.manager.indexCatalog.removeIndex(definition.getIndexId());
      var manager = fixture.manager.configManager;
      when(fixture.remote.get(definition.getIndexId()))
          .thenAnswer(
              unused -> {
                assertThat(Thread.holdsLock(manager)).isFalse();
                synchronized (manager) {
                  fixture.manager.indexCatalog.addIndex(fixture.generation);
                }
                return Optional.of(definition);
              });

      var resolved = fixture.reads.resolve(fixture.id(), "");

      assertThat(resolved).isSameInstanceAs(fixture.generation);
    }
  }

  @Test
  public void describe_quarantinedStagedGeneration_preservesBuildState() throws Exception {
    assertQuarantinedStagedGeneration(false);
  }

  @Test
  public void describe_unreadableDesiredWithQuarantinedReplacement_preservesActiveAndBuild()
      throws Exception {
    assertQuarantinedStagedGeneration(true);
  }

  private void assertQuarantinedStagedGeneration(boolean unreadableDesired) throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      var definition = fixture.generation.getDefinition();
      var identity =
          new com.xgen.mongot.config.backup.JournalEntryIdentity(
              definition.getIndexId(),
              definition.getDatabase(),
              definition.getCollectionUuid(),
              definition.getName(),
              new com.xgen.mongot.index.version.Generation(
                  new com.xgen.mongot.index.version.UserIndexVersion(1),
                  com.xgen.mongot.index.version.IndexFormatVersion.CURRENT));
      var quarantined =
          new com.xgen.mongot.config.manager.QuarantinedIndex(
              identity,
              com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type.SEARCH,
              "Unsupported mapping",
              new org.bson.BsonDocument());
      var manager = fixture.manager.configManager;
      synchronized (manager) {
        manager.configState.incompatibleIndexes.addStagedIndex(quarantined);
      }
      if (unreadableDesired) {
        when(fixture.remote.get(definition.getIndexId())).thenReturn(Optional.empty());
        when(fixture.remote.snapshot()).thenReturn(List.of());
      }

      var description = fixture.reads.describe(fixture.id());

      assertThat(description.getQueryable()).isTrue();
      assertThat(description.hasBuild()).isTrue();
      assertThat(description.getBuild().getState()).isEqualTo("QUARANTINED");
      assertThat(description.getBuild().getGenerationId())
          .isEqualTo(identity.generationId().uniqueString());
      assertThat(description.getBuild().hasVersion()).isFalse();
      assertThat(description.getBuild().getDefinitionBson().isEmpty()).isTrue();
      assertThat(description.getLifecycleState()).isEqualTo("FAILED");
      assertThat(description.getSourceStatus()).isEqualTo("STEADY");
      assertThat(fixture.reads.snapshot()).containsExactly(description);
      try (var lease = fixture.reads.acquire(fixture.id(), "")) {
        assertThat(lease.retired()).isFalse();
      }
    }
  }

  @Test
  public void resolve_quarantinedActiveGeneration_reportsIncompatibilityWithoutLeasing()
      throws Exception {
    try (var fixture = new Fixture(IndexStatus.steady())) {
      var definition = fixture.generation.getDefinition();
      var identity =
          new com.xgen.mongot.config.backup.JournalEntryIdentity(
              definition.getIndexId(),
              definition.getDatabase(),
              definition.getCollectionUuid(),
              definition.getName(),
              fixture.generation.getGenerationId().generation);
      var quarantined =
          new com.xgen.mongot.config.manager.QuarantinedIndex(
              identity,
              com.xgen.mongot.index.definition.IndexDefinitionGeneration.Type.SEARCH,
              "private parser details",
              new org.bson.BsonDocument());
      var manager = fixture.manager.configManager;
      synchronized (manager) {
        fixture.manager.indexCatalog.removeIndex(definition.getIndexId());
        manager.configState.incompatibleIndexes.addLiveIndex(quarantined);
      }
      when(fixture.remote.get(definition.getIndexId())).thenReturn(Optional.empty());
      when(fixture.remote.snapshot()).thenReturn(List.of());

      var description = fixture.reads.describe(fixture.id());
      var error =
          assertThrows(StatusRuntimeException.class, () -> fixture.reads.acquire(fixture.id(), ""));

      assertThat(description.getLifecycleState()).isEqualTo("QUARANTINED");
      assertThat(description.getQueryable()).isFalse();
      assertThat(description.hasBuild()).isFalse();
      assertThat(description.getFailureMessage()).doesNotContain("private parser details");
      assertThat(fixture.reads.snapshot()).containsExactly(description);
      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(fixture.registry().activeCount()).isEqualTo(0);
    }
  }

  private final class Fixture implements AutoCloseable {
    private final LocalIndexCatalog local;
    private final DefaultConfigManagerMocks manager;
    private final IndexGeneration generation;
    private final DirectReadCatalog reads;
    private final DirectReadCatalog.RemoteDefinitions remote =
        mock(DirectReadCatalog.RemoteDefinitions.class);

    Fixture(IndexStatus status) throws Exception {
      this.local = new LocalIndexCatalog(DirectReadCatalogTest.this.folder.newFolder().toPath());
      this.manager = DefaultConfigManagerMocks.create();
      var definition =
          SearchIndexDefinitionBuilder.builder()
              .defaultMetadata()
              .dynamicMapping()
              .definitionVersion(0L)
              .build();
      this.generation =
          com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration(definition);
      when(this.generation.getIndex().getStatus()).thenReturn(status);
      var initialized = SearchIndex.mockInitializedIndex(this.generation);
      when(initialized.getStatus()).thenReturn(status);
      this.manager.indexCatalog.addIndex(this.generation);
      this.manager.mockedDependencies.initializedIndexCatalog.addIndex(initialized);
      when(this.remote.snapshot()).thenReturn(List.of(definition));
      when(this.remote.get(definition.getIndexId())).thenReturn(Optional.of(definition));
      this.reads =
          new DirectReadCatalog(
              this.local,
              this.manager.configManager,
              this.manager.indexCatalog,
              this.manager.mockedDependencies.initializedIndexCatalog,
              this.remote,
              new Object());
    }

    String id() {
      return this.generation.getGenerationId().indexId.toHexString();
    }

    GenerationReadLeases registry() {
      synchronized (this.manager.configManager) {
        return this.manager.configManager.configState.readLeases;
      }
    }

    @Override
    public void close() throws Exception {
      this.manager.configManager.close();
      this.local.close();
    }
  }
}
