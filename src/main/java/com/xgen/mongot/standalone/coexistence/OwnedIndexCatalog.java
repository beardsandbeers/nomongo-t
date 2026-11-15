package com.xgen.mongot.standalone.coexistence;

import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.index.IndexGeneration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import org.bson.types.ObjectId;

/** An authority-scoped view over the engine's catalog. */
public final class OwnedIndexCatalog implements IndexCatalog {
  private final IndexCatalog delegate;
  private final Predicate<ObjectId> owns;

  public OwnedIndexCatalog(IndexCatalog delegate, Predicate<ObjectId> owns) {
    this.delegate = delegate;
    this.owns = owns;
  }

  @Override
  public void addIndex(IndexGeneration index) {
    if (!this.owns.test(index.getDefinition().getIndexId())) {
      throw new IllegalArgumentException("Index is outside this catalog authority");
    }
    this.delegate.addIndex(index);
  }

  @Override
  public Optional<IndexGeneration> getIndex(
      String db, UUID uuid, Optional<String> view, String name) {
    return this.delegate
        .getIndex(db, uuid, view, name)
        .filter(i -> this.owns.test(i.getDefinition().getIndexId()));
  }

  @Override
  public Optional<IndexGeneration> getIndexById(ObjectId id) {
    return this.owns.test(id) ? this.delegate.getIndexById(id) : Optional.empty();
  }

  @Override
  public List<IndexGeneration> getIndexes() {
    return this.delegate.getIndexes().stream()
        .filter(i -> this.owns.test(i.getDefinition().getIndexId()))
        .toList();
  }

  @Override
  public int getSize() {
    return getIndexes().size();
  }

  @Override
  public Optional<IndexGeneration> removeIndex(ObjectId id) {
    return this.owns.test(id) ? this.delegate.removeIndex(id) : Optional.empty();
  }
}
