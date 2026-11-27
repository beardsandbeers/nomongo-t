package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.FieldPath;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Learned BSON vector dimensions shared by the partitions and readers of one index generation. */
public final class VectorDimensions {

  public record SourcePaths(Set<String> spellings, Set<String> legacyPaths) {
    public static final SourcePaths EMPTY = new SourcePaths(Set.of(), Set.of());
  }

  /** Earlier commits used a plain vector field name for every dimension. */
  public record Binding(int dimensions, boolean dimensionInFieldName) {}

  private final ConcurrentHashMap<String, Binding> bindings = new ConcurrentHashMap<>();

  public Optional<Binding> binding(String path) {
    return Optional.ofNullable(this.bindings.get(path));
  }

  public Optional<Integer> get(FieldPath path) {
    return binding(path.toString()).map(Binding::dimensions);
  }

  /** Called under the generation's discovery lock after a successful write or during recovery. */
  public synchronized void record(String path, Binding binding) {
    var previous = this.bindings.putIfAbsent(path, binding);
    if (previous != null && previous.dimensions() != binding.dimensions()) {
      throw new IllegalStateException("Conflicting vector bindings for " + path);
    }
    if (previous != null && !binding.dimensionInFieldName()) {
      this.bindings.put(path, binding);
    }
  }
}
