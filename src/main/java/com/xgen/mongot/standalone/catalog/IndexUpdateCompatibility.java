package com.xgen.mongot.standalone.catalog;

import com.xgen.mongot.index.definition.IndexDefinition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/** Conservative compatibility rules over parsed, normalized public index definitions. */
public final class IndexUpdateCompatibility {
  private static final Set<String> ADDITIVE_TYPES =
      Set.of("string", "token", "boolean", "date", "number", "objectId", "uuid");

  public enum Kind {
    NO_CHANGE,
    PRESERVE_CONTENT,
    REBUILD_REQUIRED
  }

  public record Result(Kind kind, List<String> reasons) {}

  private IndexUpdateCompatibility() {}

  /** Classifies validated definitions; unrecognized indexing changes require an empty rebuild. */
  public static Result classify(IndexDefinition before, IndexDefinition after) {
    byte[] oldBytes = StandaloneDefinitionValidator.normalize(before);
    byte[] newBytes = StandaloneDefinitionValidator.normalize(after);
    if (before.getType() == IndexDefinition.Type.SEARCH
        && after.getType() == IndexDefinition.Type.SEARCH) {
      var oldMappings = before.asSearchDefinition().getMappings().fieldTypeToBson();
      var newMappings = after.asSearchDefinition().getMappings().fieldTypeToBson();
      // Literal dotted keys can alias ordinary nested paths in the flattened resolver. Compare
      // their parsed mappings before treating a change as an additive path.
      if ((hasDottedNames(oldMappings) || hasDottedNames(newMappings))
          && !Arrays.equals(
              StandaloneDefinitionValidator.encode(oldMappings),
              StandaloneDefinitionValidator.encode(newMappings))) {
        return new Result(
            Kind.REBUILD_REQUIRED,
            List.of("Mapping changes involving dotted field names require rebuilding"));
      }
    }
    if (before.getType() == after.getType() && Arrays.equals(oldBytes, newBytes)) {
      return new Result(Kind.NO_CHANGE, List.of());
    }
    var reasons = new ArrayList<String>();
    if (before.getType() != IndexDefinition.Type.SEARCH
        || after.getType() != IndexDefinition.Type.SEARCH) {
      return new Result(Kind.REBUILD_REQUIRED, List.of("vector index changes require rebuilding"));
    }
    BsonDocument oldDefinition =
        new RawBsonDocument(oldBytes).decode(new org.bson.codecs.BsonDocumentCodec());
    BsonDocument newDefinition =
        new RawBsonDocument(newBytes).decode(new org.bson.codecs.BsonDocumentCodec());
    compareDocument(
        oldDefinition.getDocument("mappings"),
        newDefinition.getDocument("mappings"),
        "mappings",
        reasons);
    compareAnalyzers(oldDefinition, newDefinition, reasons);
    // These are the only global properties whose differences have explicit preservation rules.
    for (String key : List.of("mappings", "analyzers", "searchAnalyzer")) {
      oldDefinition.remove(key);
      newDefinition.remove(key);
    }
    // Byte comparison retains index-sort priority. BsonDocument.equals does not retain key order.
    if (!Arrays.equals(
        StandaloneDefinitionValidator.encode(oldDefinition),
        StandaloneDefinitionValidator.encode(newDefinition))) {
      reasons.add(
          "global indexing options changed (including analyzer, sort, storedSource or partitions)");
    }
    return new Result(
        reasons.isEmpty() ? Kind.PRESERVE_CONTENT : Kind.REBUILD_REQUIRED, List.copyOf(reasons));
  }

  private static void compareAnalyzers(
      BsonDocument before, BsonDocument after, List<String> reasons) {
    Map<String, BsonDocument> oldAnalyzers = analyzers(before);
    Map<String, BsonDocument> newAnalyzers = analyzers(after);
    oldAnalyzers.forEach(
        (name, definition) -> {
          if (!definition.equals(newAnalyzers.get(name))) {
            reasons.add("analyzers." + name + " changed or was removed");
          }
        });
  }

  private static Map<String, BsonDocument> analyzers(BsonDocument definition) {
    var result = new LinkedHashMap<String, BsonDocument>();
    if (definition.containsKey("analyzers")) {
      definition
          .getArray("analyzers")
          .forEach(
              value ->
                  result.put(value.asDocument().getString("name").getValue(), value.asDocument()));
    }
    return result;
  }

  private static void compareDocument(
      BsonDocument before, BsonDocument after, String path, List<String> reasons) {
    BsonDocument oldOptions = before.clone();
    BsonDocument newOptions = after.clone();
    oldOptions.remove("fields");
    newOptions.remove("fields");
    if (!oldOptions.equals(newOptions)) {
      reasons.add(path + " document options changed");
    }
    BsonDocument oldFields = before.getDocument("fields", new BsonDocument());
    BsonDocument newFields = after.getDocument("fields", new BsonDocument());
    oldFields.forEach(
        (name, oldField) -> {
          String fieldPath = path + ".fields." + name;
          if (!newFields.containsKey(name)) {
            reasons.add(fieldPath + " was removed");
          } else {
            compareField(oldField, newFields.get(name), fieldPath, reasons);
          }
        });
    newFields.forEach(
        (name, field) -> {
          if (!oldFields.containsKey(name)
              && (!before.get("dynamic", BsonBoolean.FALSE).equals(BsonBoolean.FALSE)
                  || !isAdditiveField(field))) {
            reasons.add(
                path
                    + ".fields."
                    + name
                    + " replaces a dynamic mapping or adds an unsupported type");
          }
        });
  }

  private static Map<String, BsonDocument> types(BsonValue field) {
    var result = new LinkedHashMap<String, BsonDocument>();
    List<BsonValue> values = field.isArray() ? field.asArray().getValues() : List.of(field);
    values.forEach(
        value -> result.put(value.asDocument().getString("type").getValue(), value.asDocument()));
    return result;
  }

  private static boolean hasDottedNames(BsonDocument mapping) {
    return mapping.getDocument("fields", new BsonDocument()).entrySet().stream()
        .anyMatch(
            entry ->
                entry.getKey().contains(".")
                    || types(entry.getValue()).values().stream()
                        .anyMatch(IndexUpdateCompatibility::hasDottedNames));
  }

  private static boolean isAdditiveField(BsonValue field) {
    return types(field).entrySet().stream()
        .allMatch(
            entry ->
                ADDITIVE_TYPES.contains(entry.getKey())
                    || (entry.getKey().equals("document")
                        && entry
                            .getValue()
                            .get("dynamic", BsonBoolean.FALSE)
                            .equals(BsonBoolean.FALSE)
                        && entry
                            .getValue()
                            .getDocument("fields", new BsonDocument())
                            .values()
                            .stream()
                            .allMatch(IndexUpdateCompatibility::isAdditiveField)));
  }

  private static void compareField(
      BsonValue before, BsonValue after, String path, List<String> reasons) {
    Map<String, BsonDocument> oldTypes = types(before);
    Map<String, BsonDocument> newTypes = types(after);
    if (!oldTypes.keySet().equals(newTypes.keySet())) {
      reasons.add(path + " mapping types changed");
      return;
    }
    oldTypes.forEach(
        (type, oldField) -> {
          BsonDocument newField = newTypes.get(type);
          switch (type) {
            case "document" -> compareDocument(oldField, newField, path, reasons);
            case "string" -> compareString(oldField, newField, path, reasons);
            default -> {
              if (!oldField.equals(newField)) {
                reasons.add(path + " indexing options changed");
              }
            }
          }
        });
  }

  private static void compareString(
      BsonDocument before, BsonDocument after, String path, List<String> reasons) {
    BsonDocument oldOptions = before.clone();
    BsonDocument newOptions = after.clone();
    oldOptions.remove("searchAnalyzer");
    newOptions.remove("searchAnalyzer");
    oldOptions.remove("multi");
    newOptions.remove("multi");
    if (!oldOptions.equals(newOptions)) {
      reasons.add(path + " string indexing options changed");
    }
    BsonDocument oldMulti = before.getDocument("multi", new BsonDocument());
    BsonDocument newMulti = after.getDocument("multi", new BsonDocument());
    oldMulti.forEach(
        (name, definition) -> {
          if (!newMulti.containsKey(name)) {
            reasons.add(path + ".multi." + name + " was removed");
          } else {
            compareString(
                definition.asDocument(),
                newMulti.getDocument(name),
                path + ".multi." + name,
                reasons);
          }
        });
  }
}
