package com.xgen.mongot.standalone.catalog;

import com.google.protobuf.ByteString;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DropIndexRequest;
import com.xgen.mongot.standalone.grpc.IndexDescription;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.IndexUpdateMode;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import io.grpc.Status;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.types.ObjectId;

/** Persists index definitions only. Content is stored exclusively by Lucene. */
public final class LocalIndexCatalog implements Closeable {
  private final Path path;
  private final Path ownershipPath;
  private final Set<String> ownedIds = new HashSet<>();
  private final FileChannel lockChannel;
  private final FileLock lock;
  private final LinkedHashMap<String, IndexDefinition> indexes = new LinkedHashMap<>();

  private final LinkedHashMap<String, IndexMigration> migrations = new LinkedHashMap<>();
  private final LinkedHashMap<String, IndexInitialization> initializations = new LinkedHashMap<>();
  private final LinkedHashMap<String, IndexSchemaUpdate> schemas = new LinkedHashMap<>();
  private boolean versioned;

  public record UpdatePlan(
      IndexDefinition candidate, IndexUpdateCompatibility.Result compatibility, boolean retry) {}

  /** Acquires exclusive ownership and loads only index definitions from disk. */
  public LocalIndexCatalog(Path directory) throws IOException {
    Files.createDirectories(directory);
    this.path = directory.resolve("indexes.json");
    this.ownershipPath = directory.resolve("standalone-owned-ids.json");
    this.lockChannel =
        FileChannel.open(
            directory.resolve("standalone.lock"),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE);
    try {
      this.lock = this.lockChannel.tryLock();
    } catch (OverlappingFileLockException e) {
      this.lockChannel.close();
      throw new IOException("Standalone data directory is already in use", e);
    }
    if (this.lock == null) {
      this.lockChannel.close();
      throw new IOException("Standalone data directory is already in use");
    }
    try {
      if (Files.exists(directory.resolve("coexistence.json"))
          && !Files.exists(this.ownershipPath)) {
        throw new IOException("Coexistence ownership metadata is missing");
      }
      if (Files.exists(this.ownershipPath)) {
        for (var id : BsonDocument.parse(Files.readString(this.ownershipPath)).getArray("ids")) {
          validateIndexId(id.asString().getValue());
          this.ownedIds.add(id.asString().getValue());
        }
      }
      if (Files.exists(directory.resolve("standalone.sqlite"))) {
        throw new IOException(
            "Legacy document-store state is unsupported. Use a new dataPath and"
                + " reingest content.");
      }
      if (Files.exists(this.path)) {
        var document = BsonDocument.parse(Files.readString(this.path));
        this.versioned = document.containsKey("catalogVersion");
        if (this.versioned) {
          int version = document.getInt32("catalogVersion").getValue();
          if (version != 2 && version != 3 && version != 4) {
            throw new IOException("Unsupported standalone catalog version");
          }
          if (version >= 3) {
            for (var value : document.getArray("initializations")) {
              var initialization = IndexInitialization.fromBson(value.asDocument());
              if (this.initializations.put(initialization.key(), initialization) != null) {
                throw new IOException("Duplicate initialization authorization");
              }
            }
          }
          if (version == 4) {
            for (var value : document.getArray("schemas")) {
              var schema = IndexSchemaUpdate.fromBson(value.asDocument());
              if (this.schemas.put(schema.indexId(), schema) != null) {
                throw new IOException("Duplicate local schema binding");
              }
            }
          }
          for (var value : document.getArray("updates")) {
            var migration = IndexMigration.fromBson(value.asDocument());
            if (this.migrations.put(migration.indexId(), migration) != null) {
              throw new IOException("Duplicate index migration");
            }
          }
        }
        var definitions = document.getArray(this.versioned ? "entries" : "indexes");
        for (var value : definitions) {
          var definition =
              StandaloneDefinitionValidator.restore(
                  StandaloneDefinitionValidator.encode(value.asDocument()));
          if (this.indexes.put(definition.getIndexId().toHexString(), definition) != null) {
            throw new IOException("Duplicate index identity in catalog");
          }
        }
        StandaloneDefinitionValidator.validateCatalog(definitions());
        if (Files.exists(directory.resolve("coexistence.json"))
            && !this.ownedIds.containsAll(this.indexes.keySet())) {
          throw new IOException("Local catalog conflicts with coexistence ownership metadata");
        }
        this.ownedIds.addAll(this.indexes.keySet());
      }
      for (var migration : this.migrations.values()) {
        var desired = this.indexes.get(migration.indexId());
        if (!this.ownedIds.contains(migration.indexId())
            || (migration.state() != IndexMigration.State.CANCELLED
                && (desired == null || !desired.equals(migration.target().getIndexDefinition())))) {
          throw new IOException("Migration does not match the desired catalog or ownership");
        }
      }
      var bound = new HashSet<GenerationId>();
      for (var schema : this.schemas.values()) {
        if (!this.ownedIds.contains(schema.indexId())
            || !schema.target().getIndexDefinition().equals(this.indexes.get(schema.indexId()))
            || this.migrations.containsKey(schema.indexId())) {
          throw new IOException("Schema binding does not match the local catalog or ownership");
        }
      }
      for (var initialization : this.initializations.values()) {
        if (!this.ownedIds.contains(initialization.indexId())
            || (initialization.active() && this.schemas.containsKey(initialization.indexId()))
            || (initialization.active()
                && !initialization.definition().equals(this.indexes.get(initialization.indexId())))
            || initialization
                .generation()
                .filter(value -> !bound.add(value.getGenerationId()))
                .isPresent()) {
          throw new IOException(
              "Initialization conflicts with catalog, ownership, or generation" + " binding");
        }
      }
      persistOwnership();
    } catch (BsonParseException e) {
      close();
      throw new IOException("Unreadable index migration", e);
    } catch (IOException | RuntimeException e) {
      close();
      throw e;
    }
  }

  /** Includes dropped IDs while their physical generations may still be retiring. */
  public synchronized boolean owns(ObjectId id) {
    return this.ownedIds.contains(id.toHexString());
  }

  private void persistOwnership() throws IOException {
    var ids = new BsonArray(this.ownedIds.stream().sorted().map(BsonString::new).toList());
    Path temporary = this.ownershipPath.resolveSibling("standalone-owned-ids.json.tmp");
    try (var output =
        FileChannel.open(
            temporary,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
      var buffer =
          ByteBuffer.wrap(new BsonDocument("ids", ids).toJson().getBytes(StandardCharsets.UTF_8));
      while (buffer.hasRemaining()) {
        output.write(buffer);
      }
      output.force(true);
    }
    Files.move(
        temporary,
        this.ownershipPath,
        StandardCopyOption.ATOMIC_MOVE,
        StandardCopyOption.REPLACE_EXISTING);
    try (var directory = FileChannel.open(this.path.getParent(), StandardOpenOption.READ)) {
      directory.force(true);
    }
  }

  public synchronized List<IndexDefinition> definitions() {
    return List.copyOf(this.indexes.values());
  }

  /** Creates an empty index or resolves an equivalent existing definition by name. */
  public synchronized IndexDescription createIndex(CreateIndexRequest request) {
    var candidate =
        StandaloneDefinitionValidator.parse(
            new ObjectId().toHexString(),
            request.getName(),
            request.getType(),
            1,
            request.getDefinitionBson().toByteArray());
    for (var existing : this.indexes.values()) {
      if (existing.getName().equals(request.getName())) {
        if (Arrays.equals(
                StandaloneDefinitionValidator.normalize(existing),
                candidate.normalizedUserDefinition())
            && existing.getType() == candidate.internal().getType()) {
          return describe(existing);
        }
        throw Status.ALREADY_EXISTS
            .withDescription("Index name has a different definition")
            .asRuntimeException();
      }
    }
    if (this.indexes.size() >= 64) {
      throw Status.RESOURCE_EXHAUSTED
          .withDescription("Standalone supports 64 indexes")
          .asRuntimeException();
    }
    this.ownedIds.add(candidate.internal().getIndexId().toHexString());
    try {
      persistOwnership();
    } catch (IOException e) {
      throw Status.UNAVAILABLE
          .withDescription("Cannot persist index ownership")
          .withCause(e)
          .asRuntimeException();
    }
    var updated = new LinkedHashMap<>(this.indexes);
    updated.put(candidate.internal().getIndexId().toHexString(), candidate.internal());
    var initializations = new LinkedHashMap<>(this.initializations);
    var initialization =
        IndexInitialization.authorize(
            candidate.internal(), IndexInitialization.Purpose.CREATE_EMPTY);
    initializations.put(initialization.key(), initialization);
    save(updated, new LinkedHashMap<>(this.migrations), initializations);
    return describe(candidate.internal());
  }

  /** Classifies a complete replacement, with optimistic concurrency checked even for no-ops. */
  public synchronized UpdatePlan planUpdate(UpdateIndexRequest request) {
    var existing = require(request.getIndexId());
    checkRevision(existing, request.getExpectedRevision());
    if (request.getUpdateMode() == IndexUpdateMode.UNRECOGNIZED) {
      throw new IllegalArgumentException("Unknown index update mode");
    }
    var parsed =
        StandaloneDefinitionValidator.parse(
            request.getIndexId(),
            existing.getName(),
            type(existing),
            request.getExpectedRevision(),
            request.getDefinitionBson().toByteArray());
    @com.google.errorprone.annotations.Var
    var compatibility = IndexUpdateCompatibility.classify(existing, parsed.internal());
    var pending = this.migrations.get(request.getIndexId());
    boolean rebuild = request.getUpdateMode() == IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY;
    if (pending != null && pending.state() != IndexMigration.State.COMPLETE) {
      if (!rebuild
          && pending.state() != IndexMigration.State.CANCELLED
          && compatibility.kind() == IndexUpdateCompatibility.Kind.NO_CHANGE) {
        return new UpdatePlan(existing, compatibility, true);
      }
      if (!rebuild) {
        throw Status.FAILED_PRECONDITION
            .withDescription("An index update is already pending")
            .asRuntimeException();
      }
    }
    if (!rebuild && compatibility.kind() == IndexUpdateCompatibility.Kind.NO_CHANGE) {
      return new UpdatePlan(existing, compatibility, false);
    }
    if (rebuild) {
      compatibility =
          new IndexUpdateCompatibility.Result(
              IndexUpdateCompatibility.Kind.REBUILD_REQUIRED, List.of("Explicit empty rebuild"));
    } else if (request.getUpdateMode() == IndexUpdateMode.INDEX_UPDATE_MODE_REQUIRE_PRESERVE
        && compatibility.kind() == IndexUpdateCompatibility.Kind.REBUILD_REQUIRED) {
      throw Status.FAILED_PRECONDITION
          .withDescription(String.join("; ", compatibility.reasons()))
          .asRuntimeException();
    }
    var candidate =
        StandaloneDefinitionValidator.parse(
                request.getIndexId(),
                existing.getName(),
                type(existing),
                request.getExpectedRevision() + 1,
                request.getDefinitionBson().toByteArray())
            .internal();
    return new UpdatePlan(candidate, compatibility, false);
  }

  /** Equivalent updates and explicit/required rebuilds need no source generation. */
  public synchronized IndexDescription updateIndex(UpdateIndexRequest request) {
    var plan = planUpdate(request);
    if (plan.retry()) {
      var current = this.migrations.get(request.getIndexId());
      if (current.state() == IndexMigration.State.FAILED) {
        setMigration(current.withState(IndexMigration.State.ACCEPTED, ""));
      }
      return describe(plan.candidate());
    }
    if (plan.compatibility().kind() == IndexUpdateCompatibility.Kind.NO_CHANGE) {
      return describe(plan.candidate());
    }
    if (plan.compatibility().kind() == IndexUpdateCompatibility.Kind.PRESERVE_CONTENT) {
      throw Status.FAILED_PRECONDITION
          .withDescription("Preservation requires a ready source generation")
          .asRuntimeException();
    }
    var updated = new LinkedHashMap<>(this.indexes);
    updated.put(request.getIndexId(), plan.candidate());
    var changes = new LinkedHashMap<>(this.migrations);
    cancelPending(changes, request.getIndexId());
    var initializations = cancelledInitializations(request.getIndexId());
    var initialization =
        IndexInitialization.authorize(plan.candidate(), IndexInitialization.Purpose.REBUILD_EMPTY);
    initializations.put(initialization.key(), initialization);
    save(updated, changes, initializations);
    return describe(plan.candidate());
  }

  public synchronized IndexDescription acceptMigration(IndexMigration migration) {
    var existing = require(migration.indexId());
    if (this.initializations.values().stream()
        .anyMatch(value -> value.indexId().equals(migration.indexId()) && value.active())) {
      throw Status.FAILED_PRECONDITION
          .withDescription("Source initialization is not durable yet")
          .asRuntimeException();
    }
    if (!existing.equals(migration.source().getIndexDefinition())) {
      throw Status.ABORTED.withDescription("Source definition changed").asRuntimeException();
    }
    var updated = new LinkedHashMap<>(this.indexes);
    updated.put(migration.indexId(), migration.target().getIndexDefinition());
    var changes = new LinkedHashMap<>(this.migrations);
    changes.put(migration.indexId(), migration);
    save(updated, changes);
    return describe(migration.target().getIndexDefinition());
  }

  public synchronized Map<String, IndexMigration> migrations() {
    return Map.copyOf(this.migrations);
  }

  public synchronized Map<String, IndexSchemaUpdate> schemas() {
    return Map.copyOf(this.schemas);
  }

  /**
   * Publishes only a definition and its existing physical identity, in one durable catalog write.
   */
  public synchronized void acceptSchemaUpdate(IndexSchemaUpdate schema) {
    var existing = require(schema.indexId());
    if (!existing.equals(schema.source().getIndexDefinition())) {
      throw Status.ABORTED.withDescription("Source schema changed").asRuntimeException();
    }
    if (this.initializations.values().stream()
        .anyMatch(value -> value.indexId().equals(schema.indexId()) && value.active())) {
      throw Status.FAILED_PRECONDITION
          .withDescription("Source initialization is not durable yet")
          .asRuntimeException();
    }
    var migration = this.migrations.get(schema.indexId());
    if (migration != null && migration.state() != IndexMigration.State.COMPLETE) {
      throw Status.FAILED_PRECONDITION
          .withDescription("A legacy index migration is pending")
          .asRuntimeException();
    }
    var updated = new LinkedHashMap<>(this.indexes);
    updated.put(schema.indexId(), schema.target().getIndexDefinition());
    var migrations = new LinkedHashMap<>(this.migrations);
    migrations.remove(schema.indexId());
    var schemas = new LinkedHashMap<>(this.schemas);
    schemas.put(schema.indexId(), schema);
    save(updated, migrations, new LinkedHashMap<>(this.initializations), schemas);
  }

  public synchronized Optional<IndexMigration> migration(String id) {
    return Optional.ofNullable(this.migrations.get(id));
  }

  public synchronized void setMigration(IndexMigration migration) {
    var changes = new LinkedHashMap<>(this.migrations);
    changes.put(migration.indexId(), migration);
    save(new LinkedHashMap<>(this.indexes), changes);
  }

  /** A background completion cannot overwrite a concurrent drop, rebuild, or retry. */
  public synchronized boolean replaceMigration(
      IndexMigration expected, IndexMigration replacement) {
    if (!expected.equals(this.migrations.get(expected.indexId()))) {
      return false;
    }
    setMigration(replacement);
    return true;
  }

  public synchronized void removeCancelledMigration(String id) {
    var current = this.migrations.get(id);
    if (current != null && current.state() == IndexMigration.State.CANCELLED) {
      var changes = new LinkedHashMap<>(this.migrations);
      changes.remove(id);
      save(new LinkedHashMap<>(this.indexes), changes);
    }
  }

  /** Hides unprepared replacements from the ordinary empty-generation planner. */
  public synchronized List<IndexDefinition> reconciliationDefinitions() {
    return reconciliationDefinitions(Map.of());
  }

  /** The shared generation planner follows the active schema while its local owner publishes it. */
  public synchronized List<IndexDefinition> reconciliationDefinitions(
      Map<String, IndexDefinition> active) {
    return this.indexes.values().stream()
        .map(
            definition -> {
              String id = definition.getIndexId().toHexString();
              if (this.schemas.containsKey(id)) {
                return active.getOrDefault(id, this.schemas.get(id).target().getIndexDefinition());
              }
              var migration = this.migrations.get(definition.getIndexId().toHexString());
              return migration != null
                      && (migration.state() == IndexMigration.State.ACCEPTED
                          || migration.state() == IndexMigration.State.FAILED)
                  ? migration.source().getIndexDefinition()
                  : definition;
            })
        .toList();
  }

  public synchronized Map<String, IndexInitialization> initializations() {
    return Map.copyOf(this.initializations);
  }

  public synchronized Optional<IndexInitialization> initialization(GenerationId generation) {
    return this.initializations.values().stream()
        .filter(
            value ->
                value
                    .generation()
                    .filter(bound -> bound.getGenerationId().equals(generation))
                    .isPresent())
        .findFirst();
  }

  public synchronized Optional<com.xgen.mongot.index.definition.IndexDefinitionGeneration>
      reservedInitialization(IndexDefinition definition) {
    return this.initializations.values().stream()
        .filter(
            value ->
                value.state() == IndexInitialization.State.INITIALIZING
                    && value.definition().equals(definition))
        .flatMap(value -> value.generation().stream())
        .findFirst();
  }

  /** Called under the config manager's reservation lock, before the generation is journaled. */
  public synchronized void bindInitialization(
      com.xgen.mongot.index.definition.IndexDefinitionGeneration generation) {
    var authorization =
        this.initializations.values().stream()
            .filter(
                value ->
                    value.state() == IndexInitialization.State.AUTHORIZED
                        && value.definition().equals(generation.getIndexDefinition()))
            .findFirst();
    if (authorization.isEmpty()) {
      return;
    }
    var changes = new LinkedHashMap<>(this.initializations);
    changes.put(authorization.get().key(), authorization.get().bind(generation));
    save(new LinkedHashMap<>(this.indexes), new LinkedHashMap<>(this.migrations), changes);
  }

  /** Revokes empty creation durably before ingestion can advertise STEADY. */
  public synchronized void completeInitialization(IndexInitialization expected) {
    if (!expected.equals(this.initializations.get(expected.key()))
        || !expected.definition().equals(this.indexes.get(expected.indexId()))) {
      throw Status.ABORTED
          .withDescription("Initialization was cancelled or superseded")
          .asRuntimeException();
    }
    var changes = new LinkedHashMap<>(this.initializations);
    changes.put(expected.key(), expected.withState(IndexInitialization.State.READY));
    save(new LinkedHashMap<>(this.indexes), new LinkedHashMap<>(this.migrations), changes);
  }

  public synchronized void removeInitialization(IndexInitialization expected) {
    if (!expected.equals(this.initializations.get(expected.key())) || expected.active()) {
      return;
    }
    var changes = new LinkedHashMap<>(this.initializations);
    changes.remove(expected.key());
    save(new LinkedHashMap<>(this.indexes), new LinkedHashMap<>(this.migrations), changes);
  }

  public synchronized boolean allowsEmptyInitialization(GenerationId generation) {
    return initialization(generation)
        .filter(value -> value.state() == IndexInitialization.State.INITIALIZING)
        .isPresent();
  }

  public synchronized boolean requiresExistingData(GenerationId generation) {
    return owns(generation.indexId) && !allowsEmptyInitialization(generation);
  }

  private LinkedHashMap<String, IndexInitialization> cancelledInitializations(String id) {
    var changes = new LinkedHashMap<>(this.initializations);
    changes.replaceAll(
        (key, value) ->
            value.indexId().equals(id)
                ? value.withState(IndexInitialization.State.CANCELLED)
                : value);
    return changes;
  }

  private static void cancelPending(Map<String, IndexMigration> changes, String id) {
    var previous = changes.get(id);
    if (previous != null && previous.state() != IndexMigration.State.COMPLETE) {
      changes.put(id, previous.withState(IndexMigration.State.CANCELLED, ""));
    } else {
      changes.remove(id);
    }
  }

  public synchronized IndexDescription dropIndex(DropIndexRequest request) {
    var existing = require(request.getIndexId());
    checkRevision(existing, request.getExpectedRevision());
    var updated = new LinkedHashMap<>(this.indexes);
    updated.remove(request.getIndexId());
    var changes = new LinkedHashMap<>(this.migrations);
    cancelPending(changes, request.getIndexId());
    save(updated, changes, cancelledInitializations(request.getIndexId()));
    return describe(existing).toBuilder().setLifecycleState("DROPPED").build();
  }

  public synchronized IndexDescription getIndex(String id) {
    return describe(require(id));
  }

  /** Resolves an instance-wide name once; callers retain the returned stable identity. */
  public synchronized IndexDescription getIndexByName(String name) {
    return this.indexes.values().stream()
        .filter(index -> index.getName().equals(name))
        .findFirst()
        .map(LocalIndexCatalog::describe)
        .orElseThrow(
            () -> Status.NOT_FOUND.withDescription("Index does not exist").asRuntimeException());
  }

  private IndexDefinition require(String id) {
    validateIndexId(id);
    var definition = this.indexes.get(id);
    if (definition == null) {
      throw Status.NOT_FOUND.withDescription("Index does not exist").asRuntimeException();
    }
    return definition;
  }

  public static void validateIndexId(String id) {
    if (!ObjectId.isValid(id) || !new ObjectId(id).toHexString().equals(id)) {
      throw new IllegalArgumentException("index_id must be a canonical ObjectId hex string");
    }
  }

  private static void checkRevision(IndexDefinition definition, long expected) {
    if (expected < 1 || definition.getDefinitionVersion().orElse(1L) != expected) {
      throw Status.ABORTED.withDescription("Index revision changed").asRuntimeException();
    }
  }

  private void save(
      LinkedHashMap<String, IndexDefinition> updated,
      LinkedHashMap<String, IndexMigration> changes) {
    save(updated, changes, new LinkedHashMap<>(this.initializations));
  }

  private void save(
      LinkedHashMap<String, IndexDefinition> updated,
      LinkedHashMap<String, IndexMigration> changes,
      LinkedHashMap<String, IndexInitialization> initializations) {
    var schemas = new LinkedHashMap<>(this.schemas);
    // Rebuild/drop removes the old binding, and therefore cannot reuse the old physical index.
    schemas
        .entrySet()
        .removeIf(
            entry ->
                !entry
                    .getValue()
                    .target()
                    .getIndexDefinition()
                    .equals(updated.get(entry.getKey())));
    save(updated, changes, initializations, schemas);
  }

  private void save(
      LinkedHashMap<String, IndexDefinition> updated,
      LinkedHashMap<String, IndexMigration> changes,
      LinkedHashMap<String, IndexInitialization> initializations,
      LinkedHashMap<String, IndexSchemaUpdate> schemas) {
    StandaloneDefinitionValidator.validateCatalog(new ArrayList<>(updated.values()));
    boolean useVersioned =
        this.versioned || !changes.isEmpty() || !initializations.isEmpty() || !schemas.isEmpty();
    var content =
        new BsonDocument(
            useVersioned ? "entries" : "indexes",
            new BsonArray(updated.values().stream().map(IndexDefinition::toBson).toList()));
    if (useVersioned) {
      content.append("catalogVersion", new BsonInt32(4));
      content.append(
          "schemas",
          new BsonArray(schemas.values().stream().map(IndexSchemaUpdate::toBson).toList()));
      content.append(
          "initializations",
          new BsonArray(
              initializations.values().stream().map(IndexInitialization::toBson).toList()));
      content.append(
          "updates", new BsonArray(changes.values().stream().map(IndexMigration::toBson).toList()));
    }
    byte[] bytes = content.toJson().getBytes(StandardCharsets.UTF_8);
    Path temporary = this.path.resolveSibling("indexes.json.tmp");
    try {
      try (var output =
          FileChannel.open(
              temporary,
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING,
              StandardOpenOption.WRITE)) {
        var buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
          output.write(buffer);
        }
        output.force(true);
      }
      Files.move(
          temporary,
          this.path,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      try (var directory = FileChannel.open(this.path.getParent(), StandardOpenOption.READ)) {
        directory.force(true);
      }
      this.indexes.clear();
      this.indexes.putAll(updated);
      this.migrations.clear();
      this.migrations.putAll(changes);
      this.initializations.clear();
      this.initializations.putAll(initializations);
      this.schemas.clear();
      this.schemas.putAll(schemas);
      this.versioned = useVersioned;
    } catch (IOException e) {
      throw Status.UNAVAILABLE
          .withDescription("Cannot persist index definitions")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private static IndexType type(IndexDefinition definition) {
    return definition.getType() == IndexDefinition.Type.SEARCH
        ? IndexType.INDEX_TYPE_SEARCH
        : IndexType.INDEX_TYPE_VECTOR_SEARCH;
  }

  private static IndexDescription describe(IndexDefinition definition) {
    return IndexDescription.newBuilder()
        .setIndexId(definition.getIndexId().toHexString())
        .setName(definition.getName())
        .setType(type(definition))
        .setDesiredRevision(definition.getDefinitionVersion().orElse(1L))
        .setDesiredDefinitionBson(
            ByteString.copyFrom(StandaloneDefinitionValidator.normalize(definition)))
        .setLifecycleState("PENDING")
        .build();
  }

  public static byte[] digest(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      this.lock.release();
    } finally {
      this.lockChannel.close();
    }
  }
}
