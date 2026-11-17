package com.xgen.mongot.standalone.coexistence;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.xgen.mongot.lifecycle.LifecycleManager;
import com.xgen.mongot.monitor.ReplicationStateMonitor;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.testing.mongot.config.manager.DefaultConfigManagerMocks;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import java.util.List;
import java.util.Set;
import org.bson.types.ObjectId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CoexistenceConfigManagerTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void get_recoveredDefinition_preservesAcceptedMetadataAcrossMutation() throws Exception {
    var manager = DefaultConfigManagerMocks.create();
    try (var local = new LocalIndexCatalog(this.folder.newFolder().toPath())) {
      var definition =
          SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();
      manager.indexCatalog.addIndex(
          com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration(definition));
      var coordinator =
          new CoexistenceConfigManager(
              manager.configManager,
              local,
              mock(LifecycleManager.class),
              ReplicationStateMonitor.enabled());
      try {
        definition.setLastObservedCollectionName("changed-by-replication");
        var first = coordinator.get(definition.getIndexId()).orElseThrow();
        first.setLastObservedCollectionName("changed-by-caller");
        coordinator.snapshot().getFirst().setLastObservedCollectionName("changed-in-snapshot");

        var selected = coordinator.get(definition.getIndexId()).orElseThrow();

        assertThat(selected.getLastObservedCollectionName()).isEqualTo("collection");
        assertThat(coordinator.get(new ObjectId())).isEmpty();
      } finally {
        coordinator.stopSourceUpdates();
      }
    } finally {
      manager.configManager.close();
    }
  }

  @Test
  public void get_acceptedCatalogDeletion_removesPreviousIdentity() throws Exception {
    var manager = DefaultConfigManagerMocks.create();
    try (var local = new LocalIndexCatalog(this.folder.newFolder().toPath())) {
      var definition =
          SearchIndexDefinitionBuilder.builder().defaultMetadata().dynamicMapping().build();
      manager.indexCatalog.addIndex(
          com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration(definition));
      var coordinator =
          new CoexistenceConfigManager(
              manager.configManager,
              local,
              mock(LifecycleManager.class),
              ReplicationStateMonitor.enabled());
      try {
        assertThat(coordinator.get(definition.getIndexId())).isPresent();

        coordinator.update(List.of(), List.of(), List.of(), Set.of(), Set.of());

        assertThat(coordinator.get(definition.getIndexId())).isEmpty();
        assertThat(coordinator.snapshot()).isEmpty();
      } finally {
        coordinator.stopSourceUpdates();
      }
    } finally {
      manager.configManager.close();
    }
  }
}
