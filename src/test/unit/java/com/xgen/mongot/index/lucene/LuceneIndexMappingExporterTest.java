package com.xgen.mongot.index.lucene;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.TypeSetDefinition;
import com.xgen.mongot.index.definition.TypeSetVectorFieldDefinition;
import com.xgen.mongot.index.definition.VectorDimensions.SourcePaths;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.index.definition.quantization.VectorQuantization;
import com.xgen.mongot.index.lucene.field.MappingObservations;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.bson.BsonDocument;
import org.junit.Test;

public class LuceneIndexMappingExporterTest {
  @Test
  public void exportSnapshot_indexedObservations_readsNoStoredDocumentsAcrossDeletesAndReopen()
      throws Exception {
    var definition = SearchIndexDefinitionBuilder.VALID_INDEX;
    String longKey = "猫".repeat(11_000);
    try (var directory = new ByteBuffersDirectory();
        var writer =
            new IndexWriter(
                directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      writer.addDocument(observedString("literal", FieldPath.newRoot("a.b")));
      writer.addDocument(observedString("nested", FieldPath.parse("a.b")));
      writer.addDocument(observedString("long", FieldPath.newRoot(longKey)));
      writer.commit();
      try (var before = guardDocumentReads(DirectoryReader.open(directory))) {
        writer.deleteDocuments(new Term("id", "literal"));
        writer.commit();
        try (var after = guardDocumentReads(DirectoryReader.open(directory))) {
          var current =
              LuceneIndexMappingExporter.exportSnapshot(
                      definition, IndexFormatVersion.CURRENT, List.of(after))
                  .getDocument("fields");
          assertThat(current.keySet()).containsExactly("a", longKey);
          assertThat(current.getDocument("a").getDocument("fields").containsKey("b")).isTrue();
        }
        var previous =
            LuceneIndexMappingExporter.exportSnapshot(
                    definition, IndexFormatVersion.CURRENT, List.of(before))
                .getDocument("fields");
        assertThat(previous.keySet()).containsExactly("a.b", "a", longKey);
      }
    }
  }

  @Test
  public void exportSnapshot_olderSegment_usesEvidenceFromBothPartitionsBeforeRequestingRebuild()
      throws Exception {
    var old = new Document();
    old.add(new StringField("$type:string/a.b", "old", Field.Store.YES));
    old.add(new StringField("$meta/fieldNames", "a.b", Field.Store.NO));
    try (var oldDir = new ByteBuffersDirectory();
        var oldWriter = new IndexWriter(oldDir, new IndexWriterConfig());
        var newDir = new ByteBuffersDirectory();
        var writer =
            new IndexWriter(
                newDir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      oldWriter.addDocument(old);
      writer.addDocument(observedString("literal", FieldPath.newRoot("a.b")));
      writer.addDocument(observedString("nested", FieldPath.parse("a.b")));
      try (var oldReader = guardDocumentReads(DirectoryReader.open(oldWriter));
          var current = guardDocumentReads(DirectoryReader.open(writer))) {
        var expected = snapshot(List.of(current));
        assertThat(snapshot(List.of(oldReader, current))).isEqualTo(expected);
        assertThat(snapshot(List.of(current, oldReader))).isEqualTo(expected);
        writer.deleteDocuments(new Term("id", "literal"));
        try (var deleted = DirectoryReader.openIfChanged(current)) {
          assertThrows(
              LuceneIndexMappingExporter.AmbiguousVectorPathException.class,
              () -> snapshot(List.of(deleted, oldReader)));
        }
        assertThat(snapshot(List.of(oldReader, current))).isEqualTo(expected);
      }
    }
  }

  @Test
  public void exportSnapshot_mixedSegment_doesNotReadStoredFieldsOrTermValuesAfterDeletes()
      throws Exception {
    var old = new Document();
    old.add(new StringField("$type:string/title", "old", Field.Store.YES));
    try (var dir = new ByteBuffersDirectory();
        var writer =
            new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      writer.addDocument(old);
      writer.addDocument(observedString("title", FieldPath.newRoot("title")));
      for (int i = 0; i < 4000; i++) {
        var doc = observedString(Integer.toString(i), FieldPath.newRoot("item"));
        doc.add(new StringField("$type:string/item", "value" + i, Field.Store.NO));
        writer.addDocument(doc);
      }
      try (var first = guardDocumentReads(DirectoryReader.open(writer))) {
        var expected = snapshot(List.of(first));
        writer.deleteDocuments(new Term("id", "1"));
        try (var second = DirectoryReader.openIfChanged(first)) {
          assertThat(snapshot(List.of(second))).isEqualTo(expected);
          writer.deleteDocuments(new Term("id", "2"));
          try (var third = DirectoryReader.openIfChanged(second)) {
            assertThat(snapshot(List.of(third))).isEqualTo(expected);
          }
        }
      }
    }
  }

  private static BsonDocument snapshot(List<DirectoryReader> readers) throws IOException {
    return LuceneIndexMappingExporter.exportSnapshot(
        SearchIndexDefinitionBuilder.VALID_INDEX, IndexFormatVersion.CURRENT, readers);
  }

  @Test
  public void exportSnapshot_preObservationDocument_retainsBothExplicitSpellings()
      throws Exception {
    var definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(
                mappings(
                    """
                    {dynamic:false,fields:{'a.b':{type:'string'},a:{type:'document',fields:{b:{type:'string'}}},
                     emb:{type:'document',dynamic:{typeSet:'v'}}}}
                    """))
            .typeSets(vectorTypeSet())
            .build();
    var old = new Document();
    old.add(new StringField("$type:string/a.b", "snow", Field.Store.YES));
    try (var directory = new ByteBuffersDirectory();
        var writer = new IndexWriter(directory, new IndexWriterConfig())) {
      writer.addDocument(old);
      writer.commit();
      try (var reader = DirectoryReader.open(directory)) {
        var fields =
            LuceneIndexMappingExporter.exportSnapshot(
                    definition, IndexFormatVersion.CURRENT, List.of(reader))
                .getDocument("fields");
        assertThat(fields.getDocument("a.b"))
            .isEqualTo(fields.getDocument("a").getDocument("fields").getDocument("b"));
      }
    }
  }

  @Test
  public void exportSnapshot_oldEmbeddedScope_requiresRebuildOnlyWhileOlderDocumentIsLive()
      throws Exception {
    var definition =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .mappings(
                mappings(
                    """
                    {dynamic:false,fields:{'a.b':{type:'embeddedDocuments',dynamic:true},a:{type:'document',fields:{b:{type:'embeddedDocuments',dynamic:true}}}}}
                    """))
            .build();
    var old = new Document();
    old.add(new StringField("id", "old", Field.Store.NO));
    old.add(new StringField("$embedded:3/a.b/$type:string/a.b.left", "snow", Field.Store.NO));
    old.add(new StringField("$meta/mappingObservationsV2", "", Field.Store.NO));
    try (var dir = new ByteBuffersDirectory();
        var writer =
            new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      writer.addDocument(old);
      writer.addDocument(observedString("new", FieldPath.newRoot("extra")));
      try (var first = guardDocumentReads(DirectoryReader.open(writer))) {
        writer.deleteDocuments(new Term("id", "old"));
        try (var second = DirectoryReader.openIfChanged(first)) {
          LuceneIndexMappingExporter.exportSnapshot(
              definition, IndexFormatVersion.CURRENT, List.of(second));
        }
        assertThrows(
            LuceneIndexMappingExporter.AmbiguousVectorPathException.class,
            () ->
                LuceneIndexMappingExporter.exportSnapshot(
                    definition, IndexFormatVersion.CURRENT, List.of(first)));
      }
    }
  }

  private static Document observedString(String id, FieldPath path) throws IOException {
    var document = new Document();
    var observations = new MappingObservations();
    document.add(observations);
    document.add(new StringField("id", id, Field.Store.NO));
    int start = document.getFields().size();
    document.add(new StringField("$type:string/" + path, "value", Field.Store.NO));
    observations.recordScalarFields(document, start, path);
    observations.finish(document);
    return document;
  }

  private static DirectoryReader guardDocumentReads(DirectoryReader reader) throws IOException {
    return new FilterDirectoryReader(
        reader,
        new FilterDirectoryReader.SubReaderWrapper() {
          @Override
          public LeafReader wrap(LeafReader leaf) {
            return new FilterLeafReader(leaf) {
              @Override
              public StoredFields storedFields() throws IOException {
                throw new AssertionError("opening stored documents during mapping export");
              }

              @Override
              public Terms terms(String field) throws IOException {
                if (field.startsWith("$type:"))
                  throw new AssertionError("opening value terms during mapping export: " + field);
                var terms = super.terms(field);
                if (terms == null || !field.equals(MappingObservations.NAME)) {
                  return terms;
                }
                return new FilterTerms(terms) {
                  @Override
                  public TermsEnum iterator() throws IOException {
                    return new FilterTermsEnum(super.iterator()) {
                      @Override
                      public PostingsEnum postings(PostingsEnum reuse, int flags)
                          throws IOException {
                        if (term().length != 0 && docFreq() > leaf.numDeletedDocs()) {
                          throw new AssertionError(
                              "document frequency already proves this spelling is live");
                        }
                        return super.postings(reuse, flags);
                      }
                    };
                  }
                };
              }

              @Override
              public CacheHelper getCoreCacheHelper() {
                return in.getCoreCacheHelper();
              }

              @Override
              public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
              }
            };
          }
        }) {
      @Override
      protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return guardDocumentReads(in);
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
      }
    };
  }

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
