package com.xgen.mongot.standalone.runtime;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.lucene.directory.IndexDirectoryHelper;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.standalone.catalog.IndexMigration;
import com.xgen.mongot.util.FileUtils;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;

/** An independently durable, verified copy of all partitions of a quiescent generation. */
final class IndexSeed {
  private static final String MANIFEST = ".standalone-seed.json";

  private IndexSeed() {}

  static Path path(Path root, IndexDefinitionGeneration generation) {
    return generation.generation().equals(Generation.FIRST)
        ? root.resolve(generation.getIndexId().toHexString())
        : root.resolve(IndexDirectoryHelper.getNameForGeneration(generation));
  }

  static Path temporary(Path root, IndexMigration migration) {
    return root.resolve(
        "." + IndexDirectoryHelper.getNameForGeneration(migration.target()) + ".seed-tmp");
  }

  static long size(Path root) throws IOException {
    try (var paths = Files.walk(root)) {
      @Var long size = 0;
      for (Path path : paths.filter(Files::isRegularFile).toList()) {
        size = Math.addExact(size, Files.size(path));
      }
      return size;
    }
  }

  /** Verifies the exact commit files, including deletion files, in every partition. */
  private static BsonArray inspect(Path root, int partitions, boolean checksums)
      throws IOException {
    var result = new BsonArray();
    for (@Var int partition = 0; partition < partitions; partition++) {
      Path path = partitions == 1 ? root : root.resolve(Integer.toHexString(partition));
      if (!Files.isDirectory(path)) {
        throw new IOException("Missing preserved index partition: " + path);
      }
      try (var directory = FSDirectory.open(path)) {
        var commit = DirectoryReader.listCommits(directory).getLast();
        String internal = commit.getUserData().get("indexWriterData");
        if (internal != null && BsonDocument.parse(internal).getBoolean("isCleared").getValue()) {
          throw new IOException("Preserved index contains an interrupted-clear marker");
        }
        var files = new BsonDocument();
        for (String file : commit.getFileNames().stream().sorted().toList()) {
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Index snapshot cancelled");
          }
          var metadata = new BsonDocument("length", new BsonInt64(directory.fileLength(file)));
          if (checksums) {
            try (var input = directory.openInput(file, IOContext.READONCE)) {
              metadata.append("checksum", new BsonInt64(CodecUtil.checksumEntireFile(input)));
            }
          }
          files.append(file, metadata);
        }
        result.add(
            new BsonDocument("commit", new BsonString(commit.getSegmentsFileName()))
                .append("files", files));
      }
    }
    return result;
  }

  static void verifyExisting(Path root, IndexDefinitionGeneration definition) throws IOException {
    if (!definition.hasCurrentFormatVersion()) {
      throw new IOException(
          "Preserved index requires a supported current format; automatic empty upgrade refused");
    }
    try {
      inspect(path(root, definition), definition.getIndexDefinition().getNumPartitions(), false);
    } catch (org.bson.BsonInvalidOperationException | org.bson.json.JsonParseException e) {
      throw new IOException("Unreadable index commit metadata", e);
    }
  }

  /** Authorization is checked by the caller; existing contents must still be demonstrably empty. */
  static void verifyBootstrap(Path root, IndexDefinitionGeneration definition) throws IOException {
    int partitions = definition.getIndexDefinition().getNumPartitions();
    Path directory = path(root, definition);
    for (@Var int partition = 0; partition < partitions; partition++) {
      Path part = partitions == 1 ? directory : directory.resolve(Integer.toHexString(partition));
      if (!Files.exists(part)) {
        continue;
      }
      try (var files = Files.list(part)) {
        if (files.anyMatch(
            file -> {
              String name = file.getFileName().toString();
              return !name.equals("write.lock")
                  && !name.startsWith("segments_")
                  && !name.startsWith("pending_segments_");
            })) {
          throw new IOException("Unexpected data in empty initialization: " + part);
        }
      }
      try (var storage = FSDirectory.open(part)) {
        if (DirectoryReader.indexExists(storage)) {
          try (var reader = DirectoryReader.open(storage)) {
            if (reader.maxDoc() != 0) {
              throw new IOException("Empty initialization contains indexed documents");
            }
          }
          inspect(part, 1, false);
        }
      }
    }
  }

  static void prepare(Path temporary, IndexMigration migration) throws IOException {
    var manifest =
        new BsonDocument("source", migration.source().toBson())
            .append("target", migration.target().toBson())
            .append(
                "partitions",
                inspect(
                    temporary, migration.target().getIndexDefinition().getNumPartitions(), true));
    FileUtils.atomicallyReplace(
        temporary.resolve(MANIFEST),
        manifest.toJson(
            org.bson.json.JsonWriterSettings.builder()
                .outputMode(org.bson.json.JsonMode.EXTENDED)
                .build()));
  }

  static void verifyPrepared(Path root, IndexMigration migration) throws IOException {
    try {
      verifyManifest(root, migration);
    } catch (org.bson.BsonInvalidOperationException | org.bson.json.JsonParseException e) {
      throw new IOException("Unreadable preserved index seed manifest", e);
    }
  }

  private static void verifyManifest(Path root, IndexMigration migration) throws IOException {
    Path directory = path(root, migration.target());
    var manifest = BsonDocument.parse(Files.readString(directory.resolve(MANIFEST)));
    if (!manifest.getDocument("source").equals(migration.source().toBson())
        || !manifest.getDocument("target").equals(migration.target().toBson())
        || !manifest
            .getArray("partitions")
            .equals(
                inspect(
                    directory, migration.target().getIndexDefinition().getNumPartitions(), true))) {
      throw new IOException("Preserved index seed does not match its migration manifest");
    }
  }

  static void remove(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      return;
    }
    try (var paths = Files.walk(directory)) {
      for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
        Files.delete(path);
      }
    }
  }
}
