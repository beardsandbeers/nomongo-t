package com.xgen.mongot.standalone.catalog;

import static com.google.common.truth.Truth.assertThat;

import com.xgen.mongot.standalone.grpc.IndexType;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class IndexUpdateCompatibilityTest {
  private final String before;
  private final String after;
  private final IndexUpdateCompatibility.Kind expected;

  public IndexUpdateCompatibilityTest(String name, String before, String after, String expected) {
    this.before = before;
    this.after = after;
    this.expected = IndexUpdateCompatibility.Kind.valueOf(expected);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Object[][] cases() {
    return new Object[][] {
      {
        "dotted path collision",
        mapping("'a.b':{type:'string'}"),
        mapping("'a.b':{type:'string'},a:{type:'document',fields:{b:{type:'token'}}}"),
        "REBUILD_REQUIRED"
      },
      {
        "dotted alias reordering",
        mapping("'a.b':{type:'string'},a:{type:'document',fields:{b:{type:'token'}}}"),
        mapping("a:{type:'document',fields:{b:{type:'token'}}},'a.b':{type:'string'}"),
        "NO_CHANGE"
      },
      {
        "defaults",
        "{mappings:{dynamic:true}}",
        "{mappings:{dynamic:true},numPartitions:1,analyzer:'lucene.standard'}",
        "NO_CHANGE"
      },
      {
        "add field",
        mapping("title:{type:'string'}"),
        mapping("title:{type:'string'},category:{type:'token'}"),
        "PRESERVE_CONTENT"
      },
      {
        "add nested field",
        mapping("doc:{type:'document',fields:{title:{type:'string'}}}"),
        mapping("doc:{type:'document',fields:{title:{type:'string'},count:{type:'number'}}}"),
        "PRESERVE_CONTENT"
      },
      {
        "static subtree under dynamic root",
        "{mappings:{dynamic:true,fields:{doc:{type:'document',dynamic:false}}}}",
        "{mappings:{dynamic:true,fields:{doc:{type:'document',dynamic:false,"
            + "fields:{x:{type:'string'}}}}}}",
        "PRESERVE_CONTENT"
      },
      {
        "dynamic replacement",
        "{mappings:{dynamic:true}}",
        "{mappings:{dynamic:true,fields:{title:{type:'token'}}}}",
        "REBUILD_REQUIRED"
      },
      {"remove field", mapping("title:{type:'string'}"), mapping(""), "REBUILD_REQUIRED"},
      {
        "index analyzer",
        mapping("title:{type:'string'}"),
        mapping("title:{type:'string',analyzer:'lucene.keyword'}"),
        "REBUILD_REQUIRED"
      },
      {
        "query analyzer",
        mapping("title:{type:'string'}"),
        mapping("title:{type:'string',searchAnalyzer:'lucene.keyword'}"),
        "PRESERVE_CONTENT"
      },
      {
        "global query analyzer",
        "{mappings:{dynamic:true}}",
        "{mappings:{dynamic:true},searchAnalyzer:'lucene.keyword'}",
        "PRESERVE_CONTENT"
      },
      {
        "new multi",
        mapping("title:{type:'string'}"),
        mapping("title:{type:'string',multi:{exact:{type:'string',analyzer:'lucene.keyword'}}}"),
        "PRESERVE_CONTENT"
      },
      {
        "normalizer",
        mapping("tag:{type:'token'}"),
        mapping("tag:{type:'token',normalizer:'lowercase'}"),
        "REBUILD_REQUIRED"
      },
      {
        "mixed change",
        mapping("title:{type:'string'}"),
        mapping("title:{type:'string',norms:'omit'},category:{type:'token'}"),
        "REBUILD_REQUIRED"
      },
      {
        "sort order",
        "{mappings:{fields:{a:{type:'number'},b:{type:'number'}}},sort:{a:1,b:1}}",
        "{mappings:{fields:{a:{type:'number'},b:{type:'number'}}},sort:{b:1,a:1}}",
        "REBUILD_REQUIRED"
      },
      {
        "stored source",
        "{mappings:{dynamic:true}}",
        "{mappings:{dynamic:true},storedSource:true}",
        "REBUILD_REQUIRED"
      },
      {
        "partitions",
        "{mappings:{dynamic:true}}",
        "{mappings:{dynamic:true},numPartitions:2}",
        "REBUILD_REQUIRED"
      },
      {
        "embedded addition",
        mapping("title:{type:'string'}"),
        mapping("title:{type:'string'},items:{type:'embeddedDocuments',dynamic:true}"),
        "REBUILD_REQUIRED"
      },
      {
        "dynamic change",
        "{mappings:{dynamic:true}}",
        "{mappings:{dynamic:false}}",
        "REBUILD_REQUIRED"
      },
      {
        "add analyzer",
        mapping("title:{type:'string'}"),
        "{mappings:{dynamic:false,fields:{title:{type:'string'}}},"
            + "analyzers:[{name:'extra',tokenizer:{type:'keyword'}}]}",
        "PRESERVE_CONTENT"
      },
    };
  }

  private static String mapping(String fields) {
    return "{mappings:{dynamic:false,fields:{" + fields + "}}}";
  }

  private static com.xgen.mongot.index.definition.IndexDefinition parse(String definition) {
    return StandaloneDefinitionValidator.parse(
            "0123456789abcdef01234567",
            "test",
            IndexType.INDEX_TYPE_SEARCH,
            1,
            StandaloneDefinitionValidator.encode(BsonDocument.parse(definition)))
        .internal();
  }

  @Test
  public void classify_definitionChange_appliesExplicitCompatibilityRules() {
    var oldDefinition = parse(this.before);
    var newDefinition = parse(this.after);

    var result = IndexUpdateCompatibility.classify(oldDefinition, newDefinition);

    assertThat(result.kind()).isEqualTo(this.expected);
  }
}
