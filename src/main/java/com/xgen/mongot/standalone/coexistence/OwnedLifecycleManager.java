package com.xgen.mongot.standalone.coexistence;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.lifecycle.LifecycleManager;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.bson.types.ObjectId;

/** Routes physical generations without coupling local writers to source-manager replacement. */
public final class OwnedLifecycleManager implements LifecycleManager {
  private final LifecycleManager remote;
  private final LifecycleManager local;
  private final Predicate<ObjectId> localOwner;

  public OwnedLifecycleManager(
      LifecycleManager remote, LifecycleManager local, Predicate<ObjectId> localOwner) {
    this.remote = remote;
    this.local = local;
    this.localOwner = localOwner;
  }

  private LifecycleManager owner(GenerationId id) {
    return this.localOwner.test(id.indexId) ? this.local : this.remote;
  }

  @Override
  public void add(IndexGeneration generation) {
    owner(generation.getGenerationId()).add(generation);
  }

  @Override
  public boolean contains(GenerationId id) {
    return owner(id).contains(id);
  }

  @Override
  public CompletableFuture<Void> dropIndex(GenerationId id) {
    return owner(id).dropIndex(id);
  }

  @Override
  public void restartReplication() {
    this.remote.restartReplication();
  }

  @Override
  public CompletableFuture<Void> shutdownReplication() {
    return this.remote.shutdownReplication();
  }

  @Override
  public void updateSyncSource(SyncSourceConfig config) {
    this.remote.updateSyncSource(config);
  }

  @Override
  public Optional<SyncSourceConfig> getSyncSourceConfig() {
    return this.remote.getSyncSourceConfig();
  }

  @Override
  public boolean isReplicationSupported() {
    return this.remote.isReplicationSupported();
  }

  @Override
  public boolean isAutoEmbeddingReplicationSupported() {
    return this.remote.isAutoEmbeddingReplicationSupported();
  }

  @Override
  public boolean hasAutoEmbeddingReplication() {
    return this.remote.hasAutoEmbeddingReplication();
  }

  @Override
  public void restartAutoEmbeddingReplication(SyncSourceConfig config) {
    this.remote.restartAutoEmbeddingReplication(config);
  }

  @Override
  public CompletableFuture<Void> shutdownAutoEmbeddingReplication() {
    return this.remote.shutdownAutoEmbeddingReplication();
  }

  @Override
  public ReplicationManager getReplicationManager() {
    return this.remote.getReplicationManager();
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    return CompletableFuture.allOf(this.remote.shutdown(), this.local.shutdown());
  }
}
