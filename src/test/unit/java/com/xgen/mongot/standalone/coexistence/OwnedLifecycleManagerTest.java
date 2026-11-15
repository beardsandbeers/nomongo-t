package com.xgen.mongot.standalone.coexistence;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.lifecycle.LifecycleManager;
import java.util.concurrent.CompletableFuture;
import org.bson.types.ObjectId;
import org.junit.Test;

public class OwnedLifecycleManagerTest {
  @Test
  public void shutdownReplication_sourceRestart_doesNotStopLocalIngestion() {
    var remote = mock(LifecycleManager.class);
    var local = mock(LifecycleManager.class);
    var draining = new CompletableFuture<Void>();
    when(remote.shutdownReplication()).thenReturn(draining);
    var lifecycle = new OwnedLifecycleManager(remote, local, ignored -> true);

    var result = lifecycle.shutdownReplication();
    lifecycle.restartReplication();

    assertThat(result).isSameInstanceAs(draining);
    verify(remote).shutdownReplication();
    verify(remote).restartReplication();
    verifyNoInteractions(local);
  }

  @Test
  public void dropIndex_localGeneration_preservesRemoteLifecycle() {
    var remote = mock(LifecycleManager.class);
    var local = mock(LifecycleManager.class);
    var generation = new GenerationId(new ObjectId(), Generation.FIRST);
    var draining = new CompletableFuture<Void>();
    when(local.dropIndex(generation)).thenReturn(draining);
    var lifecycle = new OwnedLifecycleManager(remote, local, generation.indexId::equals);

    var result = lifecycle.dropIndex(generation);

    assertThat(result).isSameInstanceAs(draining);
    verify(local).dropIndex(generation);
    verifyNoInteractions(remote);
  }

  @Test
  public void shutdown_bothProducers_waitsForBothDrains() {
    var remote = mock(LifecycleManager.class);
    var local = mock(LifecycleManager.class);
    var remoteDrain = new CompletableFuture<Void>();
    var localDrain = new CompletableFuture<Void>();
    when(remote.shutdown()).thenReturn(remoteDrain);
    when(local.shutdown()).thenReturn(localDrain);
    var lifecycle = new OwnedLifecycleManager(remote, local, ignored -> false);

    var result = lifecycle.shutdown();
    remoteDrain.complete(null);

    assertThat(result.isDone()).isFalse();
    localDrain.complete(null);
    assertThat(result.isDone()).isTrue();
  }
}
