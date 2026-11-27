package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.TypeSetDefinition;
import com.xgen.mongot.index.definition.TypeSetVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorDimensions.SourcePaths;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.bson.BsonDocument;
import org.junit.Test;

public class LuceneIndexMappingExporterTest {
  @Test
  public void export_partitionUnion_collapsesPhysicalTypesAndExcludesInternalFields() {
    var definition = SearchIndexDefinitionBuilder.VALID_INDEX;

    var result =
        LuceneIndexMappingExporter.export(
            definition,
            IndexFormatVersion.CURRENT,
            List.of(
                infos("$type:string/mixed", "$type:double/mixed", "$type:date/date"),
                infos(
                    "$type:string/mixed",
                    "$type:int64Multiple/mixed",
                    "$type:doubleV2/mixed",
                    "$type:dateV2/date",
                    "$type:boolean/other",
                    "$meta/_id",
                    "$storedSource",
                    "$multi/mixed.keyword",
                    "$type:null/nullOnly")));

    assertThat(result.getBoolean("dynamic").getValue()).isFalse();
    var fields = result.getDocument("fields");
    assertThat(fields.keySet()).containsExactly("mixed", "date", "other");
    assertThat(
            fields.getArray("mixed").stream()
                .map(value -> value.asDocument().getString("type").getValue())
                .toList())
        .containsExactly("number", "string");
    assertThat(fields.getDocument("date").getString("type").getValue()).isEqualTo("date");
  }

  @Test
  public void export_dynamicTypeSet_preservesOptionsAndOnlyObservedTypes() throws Exception {
    TypeSetDefinition typeSet;
    try (var parser =
        BsonDocumentParser.fromRoot(
                BsonDocument.parse(
                    """
                    {name: 'custom', types: [
                      {type:'string', analyzer:'lucene.keyword',
                        multi:{simple:{type:'string', analyzer:'lucene.simple'}}},
                      {type:'token', normalizer:'lowercase'},
                      {type:'number', representation:'int64'}]}
                    """))
            .build()) {
      typeSet = TypeSetDefinition.fromBson(parser);
    }
    var definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(mappings("{dynamic:{typeSet:'custom'}}"))
            .typeSets(typeSet)
            .build();

    var result =
        LuceneIndexMappingExporter.export(
            definition,
            IndexFormatVersion.CURRENT,
            List.of(infos("$type:string/object.value", "$type:token/object.value")));

    var object = result.getDocument("fields").getDocument("object");
    assertThat(object.getBoolean("dynamic").getValue()).isFalse();
    var value = object.getDocument("fields").getArray("value");
    assertThat(value).hasSize(2);
    assertThat(value.get(0).asDocument().getString("analyzer").getValue())
        .isEqualTo("lucene.keyword");
    assertThat(value.get(0).asDocument().getDocument("multi").containsKey("simple")).isTrue();
    assertThat(value.get(1).asDocument().getString("normalizer").getValue()).isEqualTo("lowercase");
  }

  @Test
  public void export_embeddedAndDocumentAtSamePath_keepsScopesSeparate() throws Exception {
    var definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(
                mappings(
                    """
                    {dynamic:false, fields:{items:[
                      {type:'document', dynamic:true},
                      {type:'embeddedDocuments', dynamic:true, fields:{unused:{type:'number'}}}
                    ], explicit:{type:'string', analyzer:'lucene.keyword'}}}
                    """))
            .build();

    var result =
        LuceneIndexMappingExporter.export(
            definition,
            IndexFormatVersion.CURRENT,
            List.of(
                infos(
                    "$type:string/items.rootOnly",
                    "$embedded:5/items/$type:boolean/items.embeddedOnly")));

    var items = result.getDocument("fields").getArray("items");
    assertThat(items.get(0).asDocument().getDocument("fields").keySet())
        .containsExactly("rootOnly");
    assertThat(items.get(1).asDocument().getDocument("fields").keySet())
        .containsExactly("embeddedOnly", "unused");
    assertThat(items.get(0).asDocument().getBoolean("dynamic").getValue()).isFalse();
    assertThat(items.get(1).asDocument().getBoolean("dynamic").getValue()).isFalse();
    assertThat(
            result.getDocument("fields").getDocument("explicit").getString("analyzer").getValue())
        .isEqualTo("lucene.keyword");
  }

  @Test
  public void export_legacyVectorWithMultipleBsonSpellings_reportsAmbiguousPath() throws Exception {
    var definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(mappings("{dynamic:{typeSet:'v'}}"))
            .typeSets(vectorTypeSet())
            .build();

    // Newer writes established one spelling. The legacy segment could contain the other.
    var paths = new SourcePaths(Set.of("a\0b"), Set.of("a.b"));
    assertThrows(
        LuceneIndexMappingExporter.AmbiguousVectorPathException.class,
        () ->
            LuceneIndexMappingExporter.export(
                definition, IndexFormatVersion.CURRENT, List.of(vectorInfo("a.b")), paths));
  }

  @Test
  public void export_legacyVectorUnderExplicitDottedParent_recoversBsonSpelling() throws Exception {
    var definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(
                mappings("{dynamic:false,fields:{'a.b':{type:'document',dynamic:{typeSet:'v'}}}}"))
            .typeSets(vectorTypeSet())
            .build();

    var result =
        LuceneIndexMappingExporter.export(
            definition, IndexFormatVersion.CURRENT, List.of(vectorInfo("a.b.v")));

    var vector =
        result.getDocument("fields").getDocument("a.b").getDocument("fields").getDocument("v");
    assertThat(vector.getInt32("numDimensions").getValue()).isEqualTo(2);
    assertThat(vector.getBoolean("bsonVectorOnly").getValue()).isTrue();
  }

  private static TypeSetDefinition vectorTypeSet() {
    return new TypeSetDefinition(
        "v",
        List.of(
            new TypeSetVectorFieldDefinition(
                Optional.empty(), VectorSimilarity.COSINE, VectorQuantization.NONE)));
  }

  private static FieldInfos vectorInfo(String path) {
    var field =
        new FieldInfo(
            "$type:knnByte/" + path,
            0,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1L,
            Map.of(),
            0,
            0,
            0,
            2,
            VectorEncoding.BYTE,
            VectorSimilarityFunction.COSINE,
            false,
            false);
    return new FieldInfos(new FieldInfo[] {field});
  }

  private static DocumentFieldDefinition mappings(String json) throws Exception {
    try (var parser = BsonDocumentParser.fromRoot(BsonDocument.parse(json)).build()) {
      return DocumentFieldDefinition.fromBson(parser);
    }
  }

  private static FieldInfos infos(String... names) {
    return new FieldInfos(
        IntStream.range(0, names.length)
            .mapToObj(
                i ->
                    new FieldInfo(
                        names[i],
                        i,
                        false,
                        false,
                        false,
                        IndexOptions.NONE,
                        DocValuesType.NONE,
                        DocValuesSkipIndexType.NONE,
                        -1L,
                        Map.of(),
                        0,
                        0,
                        0,
                        0,
                        VectorEncoding.BYTE,
                        VectorSimilarityFunction.COSINE,
                        false,
                        false))
            .toArray(FieldInfo[]::new));
  }
}
