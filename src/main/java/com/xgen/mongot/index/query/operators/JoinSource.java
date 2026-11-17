package com.xgen.mongot.index.query.operators;

import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

/** A logical source; architecture adapters enforce ownership and resolve its stable identity. */
public sealed interface JoinSource extends Encodable {
  record StandaloneIndex(String name) implements JoinSource {
    @Override
    public BsonValue toBson() {
      return new BsonString(this.name);
    }
  }

  record CollectionIndex(String collection, String index) implements JoinSource {
    @Override
    public BsonValue toBson() {
      return new BsonDocument("collection", new BsonString(this.collection))
          .append("index", new BsonString(this.index));
    }
  }

  static JoinSource fromBson(BsonParseContext context, BsonValue value) throws BsonParseException {
    if (value.isString()) {
      return new StandaloneIndex(name(context, value, true));
    }
    if (!value.isDocument()) {
      return context.handleUnexpectedType(
          "string or collection source object", value.getBsonType());
    }
    var doc = value.asDocument();
    if (doc.keySet().stream().anyMatch(k -> !k.equals("collection") && !k.equals("index"))) {
      return context.handleSemanticError(
          "only collection and index are supported in a join source");
    }
    String collection = name(context.child("collection"), doc.get("collection"), false);
    if (collection.startsWith(".") || collection.endsWith(".") || collection.contains("$")) {
      return context.handleSemanticError("invalid collection name");
    }
    String index =
        doc.containsKey("index")
            ? name(context.child("index"), doc.get("index"), false)
            : "default";
    return new CollectionIndex(collection, index);
  }

  private static String name(BsonParseContext context, BsonValue value, boolean index)
      throws BsonParseException {
    if (value == null || !value.isString()) {
      return context.handleSemanticError("must be a nonblank string");
    }
    String name = value.asString().getValue();
    if (name.isBlank() || name.indexOf('\0') >= 0 || (index && name.length() > 256)) {
      return context.handleSemanticError("invalid " + (index ? "index" : "collection") + " name");
    }
    return name;
  }
}
