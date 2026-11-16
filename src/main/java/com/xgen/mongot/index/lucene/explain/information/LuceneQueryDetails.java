package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

/** Lucene's diagnostic rendering of a compiled query before and after rewriting. */
public record LuceneQueryDetails(String analyzed, String rewritten) implements DocumentEncodable {
  private static class Fields {
    static final Field.Required<String> ANALYZED =
        Field.builder("analyzed").stringField().required();
    static final Field.Required<String> REWRITTEN =
        Field.builder("rewritten").stringField().required();
  }

  public static LuceneQueryDetails fromBson(DocumentParser parser) throws BsonParseException {
    return new LuceneQueryDetails(
        parser.getField(Fields.ANALYZED).unwrap(), parser.getField(Fields.REWRITTEN).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.ANALYZED, this.analyzed)
        .field(Fields.REWRITTEN, this.rewritten)
        .build();
  }
}
