package com.xgen.mongot.standalone.server;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.catalog.IndexCatalog;
import com.xgen.mongot.config.backup.JournalEntryIdentity;
import com.xgen.mongot.config.manager.DefaultConfigManager;
import com.xgen.mongot.config.manager.QuarantinedIndex;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagConfig;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlagRegistry;
import com.xgen.mongot.featureflag.dynamic.DynamicFeatureFlags;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.IndexMetricValuesSupplier;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.SearchIndexReader.QuerySnapshot;
import com.xgen.mongot.index.SearchIndexReader.SearchProducerAndMetaResults;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.vector.Similarity;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.CatalogServiceGrpc;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DeleteByQueryRequest;
import com.xgen.mongot.standalone.grpc.DeleteRequest;
import com.xgen.mongot.standalone.grpc.DocumentServiceGrpc;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.DropIndexRequest;
import com.xgen.mongot.standalone.grpc.ExplainVerbosity;
import com.xgen.mongot.standalone.grpc.GetIndexFieldStatsRequest;
import com.xgen.mongot.standalone.grpc.GetIndexMappingsRequest;
import com.xgen.mongot.standalone.grpc.GetIndexMappingsResponse;
import com.xgen.mongot.standalone.grpc.GetIndexRequest;
import com.xgen.mongot.standalone.grpc.GetIndexStatsRequest;
import com.xgen.mongot.standalone.grpc.GetMoreRequest;
import com.xgen.mongot.standalone.grpc.IndexDescription;
import com.xgen.mongot.standalone.grpc.IndexFieldStatsResponse;
import com.xgen.mongot.standalone.grpc.IndexStats;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.KillCursorRequest;
import com.xgen.mongot.standalone.grpc.ListIndexesRequest;
import com.xgen.mongot.standalone.grpc.QueryRequest;
import com.xgen.mongot.standalone.grpc.QueryServiceGrpc;
import com.xgen.mongot.standalone.grpc.SearchMetaRequest;
import com.xgen.mongot.standalone.grpc.StatsCollectionState;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import com.xgen.mongot.standalone.grpc.UpsertRequest;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import com.xgen.mongot.util.bson.BsonVectorParser;
import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.Vector;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.lucene.search.IndexSearcher;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class StandaloneGrpcServerTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static void awaitFieldTypeMetrics(
      IndexMetricValuesSupplier supplier, Map<FieldName.TypeField, Double> expected)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!supplier.getNumFieldsPerDatatype().equals(expected) && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertThat(supplier.getNumFieldsPerDatatype()).isEqualTo(expected);
  }

  @Test
  public void mappingUpdate_delayedPublication_preservesNewGenerationCursorAndStatisticsPages()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delayed-publication", "{mappings:{fields:{title:{type:'string'}}}}");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");
      var request =
          QueryRequest.newBuilder()
              .setIndexId(id)
              .setBatchSize(1)
              .setQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
              .build();
      var oldCursor = f.queries.search(request);

      var accepted =
          f.catalog.updateIndex(
              UpdateIndexRequest.newBuilder()
                  .setIndexId(id)
                  .setExpectedRevision(1)
                  .setUpdateMode(
                      com.xgen.mongot.standalone.grpc.IndexUpdateMode
                          .INDEX_UPDATE_MODE_REBUILD_EMPTY)
                  .setDefinitionBson(
                      bson("{mappings:{fields:{title:{type:'string'},category:{type:'token'}}}}"))
                  .build());
      var ready = f.ready(id, 2);
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");
      var currentCursor = f.queries.search(request);
      var fields = f.fieldStats(id, 1);
      String retired = oldCursor.getGenerationId();

      assertThat(retired).isEqualTo(oldCursor.getGenerationId());
      assertThat(accepted.getGenerationId()).isEqualTo(retired);
      assertThat(fields.getNextPageToken()).isNotEmpty();
      f.server.generationRetired(retired);

      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.queries.getMore(
                              GetMoreRequest.newBuilder()
                                  .setCursorId(oldCursor.getCursorId())
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
      var more =
          f.queries.getMore(
              GetMoreRequest.newBuilder().setCursorId(currentCursor.getCursorId()).build());
      assertThat(more.getGenerationId()).isEqualTo(ready.getGenerationId());
      assertThat(new RawBsonDocument(more.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
      assertThat(
              f.catalog
                  .getIndexFieldStats(
                      GetIndexFieldStatsRequest.newBuilder()
                          .setIndexId(id)
                          .setPageSize(1)
                          .setPageToken(fields.getNextPageToken())
                          .build())
                  .getIndexStats()
                  .getSnapshotId())
          .isEqualTo(fields.getIndexStats().getSnapshotId());
    }
  }

  @Test
  public void mappingUpdate_inPlace_keepsWriterFilesCursorsAndPublishesNewAnalyzer()
      throws Exception {
    String id;
    String physical;
    Path root = data();
    try (var f = new Fixture(root)) {
      id = f.create("in-place", "{numPartitions:2,mappings:{fields:{title:{type:'string'}}}}");
      physical = f.ready(id, 1).getGenerationId();
      f.upsert(id, "{_id:'a',title:'coffee',label:'CAPS Word'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");
      var cursor =
          f.queries.search(
              QueryRequest.newBuilder()
                  .setIndexId(id)
                  .setBatchSize(1)
                  .setQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
                  .build());
      assertThat(f.stats(id).getActiveRevision()).isEqualTo(1);
      var generation =
          f.runtime.liveIndexes().getIndexById(new org.bson.types.ObjectId(id)).orElseThrow();
      try (var pin = f.runtime.ingestion().acquireQueryLease(generation.getGenerationId());
          var direct = f.runtime.reads().acquire(id, physical)) {
        var originalReader = direct.reader();
        var writer = pin.index().getWriter();
        var updated =
            f.catalog.updateIndex(
                UpdateIndexRequest.newBuilder()
                    .setIndexId(id)
                    .setExpectedRevision(1)
                    .setDefinitionBson(
                        bson(
                            "{numPartitions:2,mappings:{fields:{title:{type:'string'},"
                                + "label:{type:'string',analyzer:'lucene.keyword'},"
                                + "tag:{type:'token',normalizer:'lowercase'}}}}"))
                    .build());

        assertThat(updated.getLifecycleState()).isEqualTo("READY");
        assertThat(updated.getGenerationId()).isEqualTo(physical);
        assertThat(updated.getActiveRevision()).isEqualTo(2);
        assertThat(pin.index().getWriter()).isSameInstanceAs(writer);
        assertThat(pin.retired()).isFalse();
        assertThat(direct.retired()).isFalse();
        assertThat(direct.definition().getDefinitionVersion()).hasValue(1L);
        assertThat(direct.reader()).isSameInstanceAs(originalReader);
        try (var current = f.runtime.reads().acquire(id, physical)) {
          assertThat(current.definition().getDefinitionVersion()).hasValue(2L);
          assertThat(current.reader()).isNotSameInstanceAs(originalReader);
        }
        try (var oldSnapshot = f.runtime.acquireQuerySnapshot(direct, false)) {
          assertThrows(
              com.xgen.mongot.index.query.InvalidQueryException.class,
              () -> oldSnapshot.joinFields(com.xgen.mongot.util.FieldPath.parse("tag")));
        }

        assertThat(f.query(id, "{text:{path:'label',query:'CAPS Word'}}").getArray("results"))
            .isEmpty();
        f.upsert(id, "{_id:'c',title:'coffee',label:'CAPS Word',tag:'UPPER'}");
        assertThat(f.query(id, "{text:{path:'label',query:'CAPS Word'}}").getArray("results"))
            .hasSize(1);
        assertThat(f.query(id, "{equals:{path:'tag',value:'upper'}}").getArray("results"))
            .hasSize(1);
        assertThat(
                new RawBsonDocument(
                        f.queries
                            .getMore(
                                GetMoreRequest.newBuilder()
                                    .setCursorId(cursor.getCursorId())
                                    .build())
                            .getBatchBson()
                            .toByteArray())
                    .getArray("results"))
            .hasSize(1);
        assertThat(
                f.catalog
                    .getIndexStats(GetIndexStatsRequest.newBuilder().setIndexId(id).build())
                    .getActiveRevision())
            .isEqualTo(2);
        assertThat(f.stats(id).getDocumentCount()).isEqualTo(3);
        try (var files = Files.list(root)) {
          assertThat(
                  files.filter(path -> path.getFileName().toString().startsWith(id + "_")).toList())
              .hasSize(1);
        }
        assertThat(f.runtime.catalog().migrations()).isEmpty();
      }
    }
    try (var f = new Fixture(root)) {
      assertThat(f.ready(id, 2).getGenerationId()).isEqualTo(physical);
      assertThat(f.query(id, "{text:{path:'label',query:'CAPS Word'}}").getArray("results"))
          .hasSize(1);
      f.upsert(id, "{_id:'d',title:'coffee',label:'CAPS Word'}");
      assertThat(f.query(id, "{text:{path:'label',query:'CAPS Word'}}").getArray("results"))
          .hasSize(2);
    }
  }

  @Test
  public void mappingUpdate_customAnalyzerAndMulti_preservesSortedEmbeddedDataAndMatchesFreshIndex()
      throws Exception {
    String before =
        "{storedSource:true,sort:{rank:1},mappings:{fields:{"
            + "rank:{type:'number'},title:{type:'string'},"
            + "items:{type:'embeddedDocuments',fields:{name:{type:'string'}}}}}}";
    String after =
        "{storedSource:true,sort:{rank:1},analyzers:[{name:'exact',tokenizer:{type:'keyword'}}],"
            + "mappings:{fields:{rank:{type:'number'},title:{type:'string',"
            + "multi:{exact:{type:'string',analyzer:'exact'}}},"
            + "label:{type:'string',analyzer:'exact',similarity:{type:'boolean'}},"
            + "items:{type:'embeddedDocuments',fields:{name:{type:'string'}}}}}}";
    try (var f =
        new Fixture(
            StandaloneConfig.development(data(), 0),
            new io.grpc.Metadata(),
            FeatureFlags.withDefaults().enable(Feature.SORTED_INDEX).build(),
            DynamicFeatureFlagRegistry.communityDefault())) {
      String id = f.create("mutable-sorted", before);
      String fresh = f.create("fresh-sorted", after);
      String physical = f.ready(id, 1).getGenerationId();
      String old = "{_id:'a',rank:2,title:'Old',items:[{name:'retained'}]}";
      f.upsert(id, old);
      f.upsert(fresh, old);

      var updated =
          f.catalog.updateIndex(
              UpdateIndexRequest.newBuilder()
                  .setIndexId(id)
                  .setExpectedRevision(1)
                  .setDefinitionBson(bson(after))
                  .build());
      String added = "{_id:'b',rank:1,title:'CAPS Word',label:'CAPS Word',items:[{name:'added'}]}";
      f.upsert(id, added);
      f.upsert(fresh, added);

      assertThat(updated.getGenerationId()).isEqualTo(physical);
      String labelQuery = "{text:{path:'label',query:'CAPS Word'},returnStoredSource:true}";
      assertThat(f.query(id, labelQuery).getArray("results"))
          .isEqualTo(f.query(fresh, labelQuery).getArray("results"));
      assertThat(f.query(id, "{text:{path:'label',query:'CAPS'}}").getArray("results")).isEmpty();
      assertThat(
              f.query(id, "{text:{path:{value:'title',multi:'exact'},query:'CAPS Word'}}")
                  .getArray("results"))
          .hasSize(1);
      assertThat(
              f.query(
                      id,
                      "{embeddedDocument:{path:'items',operator:{"
                          + "text:{path:'items.name',query:'retained'}}}}")
                  .getArray("results"))
          .hasSize(1);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(2);
    }
  }

  @Test
  public void mappingUpdate_queryAnalyzer_refreshesFieldTypeMetricsWithoutRestart()
      throws Exception {
    var flags = DynamicFeatureFlagRegistry.communityDefault();
    try (var f =
        new Fixture(
            StandaloneConfig.development(data(), 0),
            new io.grpc.Metadata(),
            FeatureFlags.communityDefaults(),
            flags)) {
      String id = f.create("field-counts", "{mappings:{fields:{title:{type:'string'}}}}");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      // Populate the old-schema cache after visibility, rather than sampling the empty index.
      flags.updateDynamicFeatureFlags(
          List.of(enabledFlag(DynamicFeatureFlags.NUM_FIELDS_PER_DATATYPE_METRIC)));
      var generation =
          f.runtime.liveIndexes().getIndexById(new org.bson.types.ObjectId(id)).orElseThrow();
      try (var pin = f.runtime.ingestion().acquireQueryLease(generation.getGenerationId())) {
        var supplier = pin.index().getMetricsUpdater().getIndexMetricValuesSupplier();
        awaitFieldTypeMetrics(supplier, Map.of(FieldName.TypeField.STRING, 1.0));

        var updated =
            f.catalog.updateIndex(
                UpdateIndexRequest.newBuilder()
                    .setIndexId(id)
                    .setExpectedRevision(1)
                    .setDefinitionBson(
                        bson(
                            "{analyzers:[{name:'edge',tokenizer:{type:'edgeGram',"
                                + "minGram:2,maxGram:15}}],mappings:{fields:{"
                                + "title:{type:'string',searchAnalyzer:'edge'}}}}"))
                    .build());

        assertThat(updated.getGenerationId())
            .isEqualTo(generation.getGenerationId().uniqueString());
        assertThat(updated.getActiveRevision()).isEqualTo(2);
        awaitFieldTypeMetrics(supplier, Map.of(FieldName.TypeField.AUTOCOMPLETE, 1.0));
      }
    }
  }

  @Test
  public void vectorSearch_autoNumCandidatesDisabled_requiresExplicitCandidates() throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      String id = f.createVector();
      f.upsert(id, "{_id:'near',v:[1.0,0.0,0.0]}");
      var request =
          QueryRequest.newBuilder()
              .setIndexId(id)
              .setQueryBson(bson("{path:'v',queryVector:[1.0,0.0,0.0],limit:1}"))
              .build();

      var error = assertThrows(StatusRuntimeException.class, () -> f.queries.vectorSearch(request));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(error.getStatus().getDescription())
          .isEqualTo("numCandidates is required for approximate vector search");
      assertThat(f.hasOpenCursors(id)).isFalse();
      var response =
          f.queries.vectorSearch(
              request.toBuilder()
                  .setQueryBson(
                      bson("{path:'v',queryVector:[1.0,0.0,0.0],numCandidates:10,limit:1}"))
                  .build());
      assertThat(new RawBsonDocument(response.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
    }
  }

  @Test
  public void vectorSearch_autoNumCandidatesEnabled_acceptsOmittedCandidates() throws Exception {
    Similarity.load();
    var flags =
        FeatureFlags.withDefaults()
            .enable(Feature.INDEX_FEATURE_VERSION_FOUR)
            .enable(Feature.AUTO_NUM_CANDIDATES)
            .build();
    try (var f =
        new Fixture(
            StandaloneConfig.development(data(), 0),
            new io.grpc.Metadata(),
            flags,
            DynamicFeatureFlagRegistry.communityDefault())) {
      String id = f.createVector();
      f.upsert(id, "{_id:'near',v:[1.0,0.0,0.0]}");

      var response =
          f.queries.vectorSearch(
              QueryRequest.newBuilder()
                  .setIndexId(id)
                  .setQueryBson(bson("{path:'v',queryVector:[1.0,0.0,0.0],limit:1}"))
                  .build());

      assertThat(new RawBsonDocument(response.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_quarantinedExplicitTarget_reportsIncompatibility() throws Exception {
    try (var f = new Fixture(data())) {
      var manager = f.stopReconciliation();
      var id = new org.bson.types.ObjectId();
      var identity =
          new JournalEntryIdentity(
              id, "database", java.util.UUID.randomUUID(), "unreadable", Generation.FIRST);
      var quarantine =
          new QuarantinedIndex(
              identity,
              IndexDefinitionGeneration.Type.SEARCH,
              "Unsupported mapping",
              new BsonDocument());
      synchronized (manager) {
        manager.configState.incompatibleIndexes.addLiveIndex(quarantine);
      }
      assertThat(
              f.catalog
                  .getIndex(GetIndexRequest.newBuilder().setIndexId(id.toHexString()).build())
                  .getLifecycleState())
          .isEqualTo("QUARANTINED");
      var query = bson("{exists:{path:'title'},count:{type:'total'}}");
      var search =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.search(
                      QueryRequest.newBuilder()
                          .setIndexId(id.toHexString())
                          .setQueryBson(query)
                          .build()));

      var meta =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.searchMeta(
                      SearchMetaRequest.newBuilder()
                          .setTargetIndexId(id.toHexString())
                          .setQueryBson(query)
                          .build()));

      assertThat(search.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(meta.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(meta.getStatus().getDescription()).isEqualTo(search.getStatus().getDescription());
      synchronized (manager) {
        assertThat(manager.configState.readLeases.activeCount()).isEqualTo(0);
      }
    }
  }

  @Test
  public void searchMeta_quarantinedReplacement_queriesHealthyActiveGeneration() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("healthy");
      f.upsert(id, "{_id:1,title:'coffee'}");
      var active = f.runtime.reads().resolve(id, "");
      var definition = active.getDefinition();
      var manager = f.stopReconciliation();
      var identity =
          new JournalEntryIdentity(
              definition.getIndexId(),
              definition.getDatabase(),
              definition.getCollectionUuid(),
              definition.getName(),
              active.getGenerationId().generation.incrementUser());
      var quarantine =
          new QuarantinedIndex(
              identity,
              IndexDefinitionGeneration.Type.SEARCH,
              "Unsupported replacement",
              new BsonDocument());
      synchronized (manager) {
        manager.configState.incompatibleIndexes.addStagedIndex(quarantine);
      }
      var description = f.catalog.getIndex(GetIndexRequest.newBuilder().setIndexId(id).build());
      assertThat(description.getLifecycleState()).isEqualTo("FAILED");
      assertThat(description.getQueryable()).isTrue();

      var response =
          f.queries.searchMeta(
              SearchMetaRequest.newBuilder()
                  .setTargetIndexId(id)
                  .setQueryBson(bson("{exists:{path:'title'},count:{type:'total'}}"))
                  .build());

      assertThat(response.getGenerationId()).isEqualTo(active.getGenerationId().uniqueString());
      assertThat(metadata(response).getDocument("count").getInt64("total").getValue()).isEqualTo(1);
      synchronized (manager) {
        assertThat(manager.configState.readLeases.activeCount()).isEqualTo(0);
      }
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_vectorIndex_rejectsTypeAndReleasesLease() throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("vector")
                      .setType(IndexType.INDEX_TYPE_VECTOR_SEARCH)
                      .setDefinitionBson(
                          bson(
                              "{fields:[{type:'vector',path:'v',numDimensions:3,"
                                  + "similarity:'cosine'}]}"))
                      .build())
              .getIndexId();
      f.ready(id, 1);
      var manager = f.stopReconciliation();

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.searchMeta(
                      SearchMetaRequest.newBuilder()
                          .setTargetIndexId(id)
                          .setQueryBson(bson("{exists:{path:'title'},count:{type:'total'}}"))
                          .build()));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(error.getStatus().getDescription())
          .isEqualTo("SearchMeta requires a search index");
      synchronized (manager) {
        assertThat(manager.configState.readLeases.activeCount()).isEqualTo(0);
      }
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void cursors_sameGeneration_isolatesNativeOriginAndDirectInvalidation() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("shared");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");
      var direct =
          f.queries.search(
              QueryRequest.newBuilder()
                  .setIndexId(id)
                  .setBatchSize(1)
                  .setQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
                  .build());
      var nativeManager =
          new com.xgen.mongot.standalone.coexistence.NativeCursorManager(f.runtime.cursors());
      var generation = f.runtime.reads().resolve(id, "");
      var definition = generation.getDefinition();
      var nativeCursor =
          nativeManager.newCursor(
              definition.getDatabase(),
              definition.getLastObservedCollectionName(),
              definition.getCollectionUuid(),
              Optional.empty(),
              new com.xgen.mongot.cursor.CursorQuery.Search(
                  com.xgen.mongot.index.query.SearchQuery.fromBson(
                      BsonDocument.parse("{index:'shared',text:{path:'title',query:'coffee'}}"),
                      com.xgen.mongot.index.query.QueryOptimizationFlags.DEFAULT_OPTIONS)),
              com.xgen.mongot.cursor.batch.QueryCursorOptions.empty(),
              com.xgen.mongot.index.query.QueryOptimizationFlags.DEFAULT_OPTIONS,
              Optional.empty());

      assertThrows(
          com.xgen.mongot.cursor.MongotCursorNotFoundException.class,
          () ->
              nativeManager.getNextBatch(
                  direct.getCursorId(),
                  com.xgen.mongot.util.Bytes.ofMebi(1),
                  com.xgen.mongot.cursor.batch.QueryCursorOptions.empty()));
      nativeManager.killCursor(direct.getCursorId());
      assertThat(f.runtime.cursors().isDirectCursor(direct.getCursorId())).isTrue();
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.queries.getMore(
                              GetMoreRequest.newBuilder()
                                  .setCursorId(nativeCursor.cursorId)
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
      f.runtime.reads().invalidate(id);
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.queries.getMore(
                              GetMoreRequest.newBuilder()
                                  .setCursorId(direct.getCursorId())
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
      var nativeBatch =
          nativeManager.getNextBatch(
              nativeCursor.cursorId,
              com.xgen.mongot.util.Bytes.ofMebi(1),
              com.xgen.mongot.cursor.batch.QueryCursorOptions.empty());

      assertThat(nativeBatch.batch.asArray().size()).isEqualTo(2);
      nativeManager.killCursor(nativeCursor.cursorId);
    }
  }

  @Test
  public void searchMeta_explicitTarget_selectsNamedIndexAndPreservesLegacyDefault()
      throws Exception {
    try (var f = new Fixture(data())) {
      String local = f.create("default");
      String selected = f.create("selected");
      f.upsert(local, "{_id:'a',title:'coffee'}");
      f.upsert(selected, "{_id:'a',title:'coffee'}");
      f.upsert(selected, "{_id:'b',title:'coffee'}");
      var query = metaRequest("{text:{path:'title',query:'coffee'},count:{type:'total'}}");

      var legacy = f.queries.searchMeta(query);
      var explicit = f.queries.searchMeta(query.toBuilder().setTargetIndexId(selected).build());

      assertThat(metadata(legacy).getDocument("count").getInt64("total").getValue()).isEqualTo(1);
      assertThat(metadata(explicit).getDocument("count").getInt64("total").getValue()).isEqualTo(2);
      assertThat(f.hasOpenCursors(selected)).isFalse();
    }
  }

  @Test
  public void listIndexes_paginationAndOwnershipFilter_returnsCompleteDistinctPages()
      throws Exception {
    try (var f = new Fixture(data())) {
      String first = f.create("one");
      String second = f.create("two");
      var request =
          com.xgen.mongot.standalone.grpc.ListIndexesRequest.newBuilder().setPageSize(1).build();

      var page = f.catalog.listIndexes(request);
      var next =
          f.catalog.listIndexes(request.toBuilder().setPageToken(page.getNextPageToken()).build());
      var remote =
          f.catalog.listIndexes(
              request.toBuilder()
                  .setOwnership(
                      com.xgen.mongot.standalone.grpc.IndexOwnership.INDEX_OWNERSHIP_REPLICATED)
                  .build());

      assertThat(java.util.Set.of(page.getIndexes(0).getIndexId(), next.getIndexes(0).getIndexId()))
          .containsExactly(first, second);
      assertThat(next.getNextPageToken()).isEmpty();
      assertThat(remote.getIndexesList()).isEmpty();
      assertThat(page.getIndexes(0).getOwnership())
          .isEqualTo(com.xgen.mongot.standalone.grpc.IndexOwnership.INDEX_OWNERSHIP_STANDALONE);
    }
  }

  @Test
  public void searchMeta_unchangedDefaultIndexBody_returnsDefaultCountWithoutCursor()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("default");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");

      var response = f.queries.searchMeta(metaRequest("{text:{path:'title',query:'coffee'}}"));

      assertThat(response.hasMetadataBson()).isTrue();
      assertThat(metadata(response))
          .isEqualTo(BsonDocument.parse("{count:{lowerBound:{$numberLong:'2'}}}"));
      assertThat(response.getGenerationId()).isNotEmpty();
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_facetsAcrossPartitions_matchesSearchMetadataAndReleasesCursor()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.create(
              "meta",
              "{numPartitions:2,mappings:{dynamic:false,"
                  + "fields:{brand:{type:'token'},price:{type:'number'}}}}");
      f.upsert(id, "{_id:'a',brand:'Acme',price:2}");
      f.upsert(id, "{_id:'b',brand:'Acme',price:3}");
      f.upsert(id, "{_id:'c',brand:'Brew',price:4}");
      String body =
          "{index:'meta',facet:{facets:{brands:{type:'string',"
              + "path:'brand',numBuckets:1},prices:{type:'number',"
              + "path:'price',boundaries:[0,3,10]}}},count:{type:'total'}}";
      var expected =
          f.queries.search(
              QueryRequest.newBuilder().setIndexId(id).setQueryBson(bson(body)).build());

      var response = f.queries.searchMeta(metaRequest(body));

      assertThat(metadata(response))
          .isEqualTo(new RawBsonDocument(expected.getMetadataBson().toByteArray()));
      assertThat(metadata(response).getDocument("count").getInt64("total").getValue()).isEqualTo(3);
      assertThat(
              metadata(response)
                  .getDocument("facet")
                  .getDocument("brands")
                  .getArray("buckets")
                  .getFirst()
                  .asDocument()
                  .getInt64("count")
                  .getValue())
          .isEqualTo(2);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_sortedAggregationAcrossPartitions_preservesFirstLastAndSum()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("meta", "{numPartitions:2,mappings:{dynamic:true}}");
      f.upsert(id, "{_id:'a',price:2}");
      f.upsert(id, "{_id:'b',price:3}");
      f.upsert(id, "{_id:'c',price:4}");
      String body =
          "{index:'meta',sort:{price:-1},"
              + "facet:{facets:{prices:{type:'number',path:'price',"
              + "boundaries:[0,10],aggregations:{total:{type:'sum',"
              + "path:'price'},first:{type:'first',path:'price'},"
              + "last:{type:'last',path:'price'}}}}}}";

      var response = f.queries.searchMeta(metaRequest(body));

      var bucket =
          metadata(response)
              .getDocument("facet")
              .getDocument("prices")
              .getArray("buckets")
              .getFirst()
              .asDocument();
      assertThat(bucket.getNumber("total").doubleValue()).isEqualTo(9);
      assertThat(bucket.getNumber("first").doubleValue()).isEqualTo(4);
      assertThat(bucket.getNumber("last").doubleValue()).isEqualTo(2);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_missingIndex_usesCoupledEmptyIndexMetadata() throws Exception {
    try (var f = new Fixture(data())) {
      var response = f.queries.searchMeta(metaRequest("{text:{path:'title',query:'coffee'}}"));

      assertThat(metadata(response))
          .isEqualTo(BsonDocument.parse("{count:{total:{$numberLong:'0'}}}"));
      assertThat(response.getGenerationId()).isEmpty();
    }
  }

  @Test
  public void searchMeta_missingIndexQueryPlanner_returnsExplainWithoutMetadata() throws Exception {
    try (var f = new Fixture(data())) {
      var response =
          f.queries.searchMeta(
              metaRequest("{index:'absent',text:{path:'title',query:'coffee'}}").toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_QUERY_PLANNER)
                  .build());

      assertThat(response.getExplainBson()).isNotEmpty();
      var explain = new RawBsonDocument(response.getExplainBson().toByteArray());
      assertThat(explain.getDocument("metadata").getString("indexName").getValue())
          .isEqualTo("absent");
      assertThat(response.hasMetadataBson()).isFalse();
      assertThat(response.getGenerationId()).isEmpty();
    }
  }

  @Test
  public void searchMeta_missingIndexExecutionStats_returnsEmptyMetadataAndExplain()
      throws Exception {
    try (var f = new Fixture(data())) {
      var response =
          f.queries.searchMeta(
              metaRequest("{text:{path:'title',query:'coffee'}}").toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());

      assertThat(response.getExplainBson()).isNotEmpty();
      var explain = new RawBsonDocument(response.getExplainBson().toByteArray());
      assertThat(explain.getDocument("metadata").getString("indexName").getValue())
          .isEqualTo("default");
      assertThat(metadata(response))
          .isEqualTo(BsonDocument.parse("{count:{total:{$numberLong:'0'}}}"));
      assertThat(response.getGenerationId()).isEmpty();
    }
  }

  @Test
  public void searchMeta_zeroMatches_returnsZeroWithRequestedCountType() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("default");
      f.upsert(id, "{_id:'a',title:'tea'}");

      var response = f.queries.searchMeta(metaRequest("{text:{path:'title',query:'coffee'}}"));

      assertThat(metadata(response))
          .isEqualTo(BsonDocument.parse("{count:{lowerBound:{$numberLong:'0'}}}"));
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_invalidQueryOnMissingIndex_validatesBeforeLookup() throws Exception {
    try (var f = new Fixture(data())) {
      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.searchMeta(
                      metaRequest(
                          "{index:'absent',text:{path:'title',query:'coffee'},unknown:true}")));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }
  }

  @Test
  public void searchMeta_identityAndGenerationFences_areEnforced() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("default");
      String other = f.create("other");
      var request = metaRequest("{text:{path:'title',query:'coffee'}}");
      var success = f.queries.searchMeta(request.toBuilder().setIndexId(id).build());

      var identityError =
          assertThrows(
              StatusRuntimeException.class,
              () -> f.queries.searchMeta(request.toBuilder().setIndexId(other).build()));
      var generationError =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.searchMeta(
                      request.toBuilder().setGenerationId(other + "-f1-u0-a0").build()));
      var malformed =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.searchMeta(
                      request.toBuilder().setGenerationId("bad-generation").build()));
      var fenced =
          f.queries.searchMeta(
              request.toBuilder().setGenerationId(success.getGenerationId()).build());

      assertThat(identityError.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(generationError.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(malformed.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(fenced.getGenerationId()).isEqualTo(success.getGenerationId());
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_missingFencedIndex_failsPrecondition() throws Exception {
    try (var f = new Fixture(data())) {
      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.searchMeta(
                      metaRequest("{text:{path:'title',query:'coffee'}}").toBuilder()
                          .setIndexId("000000000000000000000001")
                          .build()));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    }
  }

  @Test
  public void searchMeta_queryPlanner_returnsExplainWithoutMetadata() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("default");
      f.upsert(id, "{_id:'a',title:'coffee'}");

      var response =
          f.queries.searchMeta(
              metaRequest("{text:{path:'title',query:'coffee'}}").toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_QUERY_PLANNER)
                  .build());

      assertThat(response.hasMetadataBson()).isFalse();
      assertThat(
              new RawBsonDocument(response.getExplainBson().toByteArray())
                  .getDocument("query")
                  .getString("type")
                  .getValue())
          .isEqualTo("ConstantScoreQuery");
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_executionStats_returnsMetadataAndExplain() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("default");
      f.upsert(id, "{_id:'a',title:'coffee'}");

      var response =
          f.queries.searchMeta(
              metaRequest("{text:{path:'title',query:'coffee'}}").toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());

      assertThat(metadata(response).getDocument("count").getInt64("lowerBound").getValue())
          .isEqualTo(1);
      assertThat(response.getExplainBson()).isNotEmpty();
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_liveDynamicFlags_respectsChangesWithoutRestart() throws Exception {
    var flags = DynamicFeatureFlagRegistry.communityDefault();
    try (var f =
        new Fixture(
            StandaloneConfig.development(data(), 0),
            new io.grpc.Metadata(),
            FeatureFlags.communityDefaults(),
            flags)) {
      String id = f.create("default", "{mappings:{dynamic:false,fields:{brand:{type:'token'}}}}");
      f.upsert(id, "{_id:'a',brand:'Acme'}");
      var request =
          metaRequest("{facet:{facets:{brands:{type:'string',path:'brand',numBuckets:2000}}}}");
      var disabled =
          assertThrows(StatusRuntimeException.class, () -> f.queries.searchMeta(request));

      flags.updateDynamicFeatureFlags(
          List.of(enabledFlag(DynamicFeatureFlags.ENABLE_10K_BUCKET_LIMIT)));
      var response = f.queries.searchMeta(request);
      flags.updateDynamicFeatureFlags(List.of());
      var reset = assertThrows(StatusRuntimeException.class, () -> f.queries.searchMeta(request));

      assertThat(disabled.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(reset.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(metadata(response).getDocument("facet").getDocument("brands").getArray("buckets"))
          .hasSize(1);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_aggregationDisabled_rejectsMetricSyntax() throws Exception {
    var flags = FeatureFlags.withDefaults().disable(Feature.SEARCH_AGGREGATION).build();
    try (var f =
        new Fixture(
            StandaloneConfig.development(data(), 0),
            new io.grpc.Metadata(),
            flags,
            DynamicFeatureFlagRegistry.communityDefault())) {
      String id = f.create("default");

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.searchMeta(
                      SearchMetaRequest.newBuilder()
                          .setQueryBson(aggregationRequest(id).getQueryBson())
                          .build()));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void search_autoNumCandidatesDisabled_matchesSearchMetaValidation() throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      String id =
          f.create(
              "default",
              "{mappings:{dynamic:false,fields:{embedding:"
                  + "{type:'vector',numDimensions:3,similarity:'cosine'}}}}");
      f.upsert(id, "{_id:'near',embedding:[1.0,0.0,0.0]}");
      var body = bson("{vectorSearch:{path:'embedding',queryVector:[1.0,0.0,0.0],limit:1}}");

      var metaError =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.searchMeta(SearchMetaRequest.newBuilder().setQueryBson(body).build()));
      var searchError =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.search(
                      QueryRequest.newBuilder().setIndexId(id).setQueryBson(body).build()));

      assertThat(metaError.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(searchError.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(searchError.getStatus().getDescription())
          .isEqualTo(metaError.getStatus().getDescription());
      assertThat(searchError).hasMessageThat().contains("numCandidates is required");
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_autoNumCandidatesDisabled_validatesBeforeIndexLookup() throws Exception {
    try (var f = new Fixture(data())) {
      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.searchMeta(
                      metaRequest(
                          "{vectorSearch:{path:'embedding',queryVector:[1.0,0.0,0.0],limit:1}}")));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(error).hasMessageThat().contains("numCandidates is required");
    }
  }

  @Test
  public void searchMeta_exactVectorSearchAcrossPartitions_returnsCountAndExplainWithoutCursor()
      throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      String id =
          f.create(
              "default",
              "{numPartitions:2,mappings:{dynamic:false,fields:{embedding:"
                  + "{type:'vector',numDimensions:3,similarity:'cosine'}}}}");
      f.upsert(id, "{_id:'near',embedding:[1.0,0.0,0.0]}");
      f.upsert(id, "{_id:'far',embedding:[0.0,1.0,0.0]}");
      f.upsert(id, "{_id:'missing'}");
      String body =
          "{vectorSearch:{path:'embedding',queryVector:[1.0,0.0,0.0],exact:true,limit:2},"
              + "count:{type:'total'}}";
      var expected =
          f.queries.search(
              QueryRequest.newBuilder().setIndexId(id).setQueryBson(bson(body)).build());

      var response =
          f.queries.searchMeta(
              metaRequest(body).toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());

      assertThat(metadata(response))
          .isEqualTo(new RawBsonDocument(expected.getMetadataBson().toByteArray()));
      assertThat(metadata(response).getDocument("count").getInt64("total").getValue()).isEqualTo(2);
      assertThat(response.getExplainBson()).isNotEmpty();
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_autoNumCandidatesEnabled_acceptsOmittedNumCandidates() throws Exception {
    Similarity.load();
    var flags =
        FeatureFlags.withDefaults()
            .enable(Feature.INDEX_FEATURE_VERSION_FOUR)
            .enable(Feature.AUTO_NUM_CANDIDATES)
            .build();
    try (var f =
        new Fixture(
            StandaloneConfig.development(data(), 0),
            new io.grpc.Metadata(),
            flags,
            DynamicFeatureFlagRegistry.communityDefault())) {
      String id =
          f.create(
              "default",
              "{mappings:{dynamic:false,fields:{embedding:"
                  + "{type:'vector',numDimensions:3,similarity:'cosine'}}}}");
      f.upsert(id, "{_id:'near',embedding:[1.0,0.0,0.0]}");

      var response =
          f.queries.searchMeta(
              metaRequest("{vectorSearch:{path:'embedding',queryVector:[1.0,0.0,0.0],limit:1}}"));

      assertThat(metadata(response))
          .isEqualTo(BsonDocument.parse("{count:{lowerBound:{$numberLong:'1'}}}"));
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_retainedCursorCapacityFull_stillSucceeds() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("default");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      var field = StandaloneGrpcServer.class.getDeclaredField("queries");
      field.setAccessible(true);
      var capacityField = StandaloneQueryService.class.getDeclaredField("capacity");
      capacityField.setAccessible(true);
      var capacity = (java.util.concurrent.Semaphore) capacityField.get(field.get(f.server));
      assertThat(capacity.drainPermits()).isEqualTo(1024);

      var response = f.queries.searchMeta(metaRequest("{text:{path:'title',query:'coffee'}}"));

      assertThat(metadata(response).getDocument("count").getInt64("lowerBound").getValue())
          .isEqualTo(1);
      assertThat(capacity.availablePermits()).isEqualTo(0);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_combinedResponseLimit_rejectsEnvelopeOverflowAndReleasesCursor()
      throws Exception {
    int responseSize;
    try (var f = new Fixture(data())) {
      f.create("default");
      responseSize =
          f.queries
              .searchMeta(metaRequest("{text:{path:'title',query:'coffee'}}"))
              .getSerializedSize();
    }
    var base = StandaloneConfig.development(data(), 0);
    var config =
        new StandaloneConfig(
            base.dataPath(),
            base.address(),
            new StandaloneConfig.Limits(32, 100, 1000, responseSize - 1),
            base.maxInFlightRequests(),
            base.maxInFlightBytes(),
            base.minFreeDiskBytes(),
            base.commitInterval(),
            base.refreshInterval(),
            base.maxRpcDuration(),
            base.tls(),
            base.principals());
    try (var f = new Fixture(config)) {
      String id = f.create("default");

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () -> f.queries.searchMeta(metaRequest("{text:{path:'title',query:'coffee'}}")));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      assertThat(f.hasOpenCursors(id)).isFalse();
      assertThat(
              f.queries
                  .searchMeta(metaRequest("{index:'absent',text:{path:'title',query:'coffee'}}"))
                  .hasMetadataBson())
          .isTrue();
    }
  }

  @Test
  public void searchMeta_returnScopeWithoutReturnStoredSourceOption_usesMetadataOnlyValidation()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.create(
              "default",
              "{mappings:{dynamic:false,"
                  + "fields:{variants:{type:'embeddedDocuments',dynamic:true,"
                  + "storedSource:true}}}}");
      f.upsert(id, "{_id:'a',variants:[{price:2},{price:3}]}");

      var response =
          f.queries.searchMeta(
              metaRequest(
                  "{exists:{path:'variants.price'},"
                      + "returnScope:{path:'variants'},count:{type:'total'}}"));

      assertThat(metadata(response).getDocument("count").getInt64("total").getValue()).isEqualTo(2);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_optionalHitFeatures_areAcceptedWithoutStoredSource() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("default");
      f.upsert(id, "{_id:'a',title:'coffee'}");

      var response =
          f.queries.searchMeta(
              metaRequest(
                  "{text:{path:'title',query:'coffee'},"
                      + "highlight:{path:'title'},scoreDetails:true,sort:{title:1},"
                      + "returnStoredSource:false}"));

      assertThat(metadata(response).getDocument("count").getInt64("lowerBound").getValue())
          .isEqualTo(1);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_unknownExplainAndMalformedBson_areInvalidArguments() throws Exception {
    try (var f = new Fixture(data())) {
      var badExplain =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.searchMeta(
                      metaRequest("{text:{path:'title',query:'coffee'}}").toBuilder()
                          .setExplainVerbosityValue(99)
                          .build()));
      var badBson =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.searchMeta(
                      SearchMetaRequest.newBuilder()
                          .setQueryBson(ByteString.copyFromUtf8("bad bson"))
                          .build()));

      assertThat(badExplain.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(badBson.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }
  }

  @Test
  public void searchMeta_dateFacets_preserveBsonDateKeys() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("default", "{mappings:{dynamic:false,fields:{created:{type:'date'}}}}");
      f.upsert(id, "{_id:'a',created:{$date:'2025-06-01T00:00:00Z'}}");

      var response =
          f.queries.searchMeta(
              metaRequest(
                  "{facet:{facets:{dates:{type:'date',path:'created',"
                      + "boundaries:[{$date:'2025-01-01T00:00:00Z'},"
                      + "{$date:'2026-01-01T00:00:00Z'}]}}}}"));

      var bucket =
          metadata(response)
              .getDocument("facet")
              .getDocument("dates")
              .getArray("buckets")
              .getFirst()
              .asDocument();
      assertThat(bucket.get("_id").isDateTime()).isTrue();
      assertThat(bucket.getDateTime("_id").getValue())
          .isEqualTo(java.time.Instant.parse("2025-01-01T00:00:00Z").toEpochMilli());
      assertThat(bucket.getInt64("count").getValue()).isEqualTo(1);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void searchMeta_legacySortAndCountThreshold_doNotLimitFacetPopulation() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("default", "{mappings:{dynamic:false,fields:{brand:{type:'token'}}}}");
      f.upsert(id, "{_id:'a',brand:'Acme'}");
      f.upsert(id, "{_id:'b',brand:'Acme'}");
      f.upsert(id, "{_id:'c',brand:'Acme'}");

      var response =
          f.queries.searchMeta(
              metaRequest(
                  "{facet:{facets:{brands:{type:'string',path:'brand'}}},"
                      + "sort:{missing:1},count:{type:'lowerBound',threshold:1}}"));

      assertThat(metadata(response).getDocument("count").getInt64("lowerBound").getValue())
          .isEqualTo(3);
      assertThat(
              metadata(response)
                  .getDocument("facet")
                  .getDocument("brands")
                  .getArray("buckets")
                  .getFirst()
                  .asDocument()
                  .getInt64("count")
                  .getValue())
          .isEqualTo(3);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  private static SearchMetaRequest metaRequest(String body) {
    return SearchMetaRequest.newBuilder().setQueryBson(bson(body)).build();
  }

  private static BsonDocument metadata(
      com.xgen.mongot.standalone.grpc.SearchMetaResponse response) {
    return new RawBsonDocument(response.getMetadataBson().toByteArray());
  }

  @Test
  public void search_defaultAggregationConfiguration_returnsFacetMetrics() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("aggregation");
      f.upsert(id, "{_id:'a',price:2}");
      f.upsert(id, "{_id:'b',price:3}");

      var response = f.queries.search(aggregationRequest(id));

      var bucket =
          new RawBsonDocument(response.getMetadataBson().toByteArray())
              .getDocument("facet")
              .getDocument("prices")
              .getArray("buckets")
              .getFirst()
              .asDocument();
      assertThat(bucket.getNumber("total").doubleValue()).isEqualTo(5.0);
      assertThat(response.getCursorId()).isEqualTo(0);
    }
  }

  @Test
  public void search_aggregationDisabled_rejectsFacetMetrics() throws Exception {
    var flags = FeatureFlags.withDefaults().disable(Feature.SEARCH_AGGREGATION).build();
    try (var f =
        new Fixture(
            StandaloneConfig.development(data(), 0),
            new io.grpc.Metadata(),
            flags,
            DynamicFeatureFlagRegistry.communityDefault())) {
      String id = f.create("aggregation-disabled");

      var error =
          assertThrows(
              StatusRuntimeException.class, () -> f.queries.search(aggregationRequest(id)));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(error).hasMessageThat().contains("aggregations");
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void search_bucketLimitFlagChanges_updatesParsingWithoutRestart() throws Exception {
    var dynamicFlags = DynamicFeatureFlagRegistry.communityDefault();
    try (var f =
        new Fixture(
            StandaloneConfig.development(data(), 0),
            new io.grpc.Metadata(),
            FeatureFlags.communityDefaults(),
            dynamicFlags)) {
      String id =
          f.create("large-facet", "{mappings:{dynamic:false,fields:{brand:{type:'token'}}}}");
      f.upsert(id, "{_id:'a',brand:'Acme'}");
      var request =
          QueryRequest.newBuilder()
              .setIndexId(id)
              .setQueryBson(
                  bson("{facet:{facets:{brands:{type:'string',path:'brand',numBuckets:2000}}}}"))
              .build();

      var disabled = assertThrows(StatusRuntimeException.class, () -> f.queries.search(request));
      assertThat(disabled.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);

      dynamicFlags.updateDynamicFeatureFlags(
          List.of(enabledFlag(DynamicFeatureFlags.ENABLE_10K_BUCKET_LIMIT)));
      var response = f.queries.search(request);
      var buckets =
          new RawBsonDocument(response.getMetadataBson().toByteArray())
              .getDocument("facet")
              .getDocument("brands")
              .getArray("buckets");
      assertThat(buckets).hasSize(1);
      assertThat(buckets.get(0).asDocument().getString("_id").getValue()).isEqualTo("Acme");
      assertThat(buckets.get(0).asDocument().getInt64("count").getValue()).isEqualTo(1);

      dynamicFlags.updateDynamicFeatureFlags(List.of());
      var reset = assertThrows(StatusRuntimeException.class, () -> f.queries.search(request));
      assertThat(reset.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void search_scopedSortFlagChanges_updatesParsingAndExecution() throws Exception {
    var dynamicFlags = DynamicFeatureFlagRegistry.communityDefault();
    try (var f =
        new Fixture(
            StandaloneConfig.development(data(), 0),
            new io.grpc.Metadata(),
            FeatureFlags.communityDefaults(),
            dynamicFlags)) {
      String id =
          f.create(
              "scoped-sort",
              "{mappings:{dynamic:false,fields:{title:{type:'string'},"
                  + "variants:{type:'embeddedDocuments',dynamic:true}}}}");
      f.upsert(
          id, "{_id:'a',title:'coffee',variants:[{price:1,inStock:false},{price:9,inStock:true}]}");
      f.upsert(id, "{_id:'b',title:'coffee',variants:[{price:4,inStock:true}]}");
      var request =
          QueryRequest.newBuilder()
              .setIndexId(id)
              .setQueryBson(
                  bson(
                      """
                      {text:{path:'title',query:'coffee'},sort:{'variants.price':{
                        order:1,embeddedDocument:{path:'variants',
                          operator:{equals:{path:'variants.inStock',value:true}}}}}}
                      """))
              .build();

      var disabled = assertThrows(StatusRuntimeException.class, () -> f.queries.search(request));
      assertThat(disabled.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);

      dynamicFlags.updateDynamicFeatureFlags(
          List.of(enabledFlag(DynamicFeatureFlags.ENABLE_EMBEDDED_SCOPED_SORT)));
      var response = f.queries.search(request);
      var results = new RawBsonDocument(response.getBatchBson().toByteArray()).getArray("results");
      assertThat(results).hasSize(2);
      assertThat(results.get(0).asDocument().getString("_id").getValue()).isEqualTo("b");
      assertThat(results.get(1).asDocument().getString("_id").getValue()).isEqualTo("a");

      dynamicFlags.updateDynamicFeatureFlags(List.of());
      var reset = assertThrows(StatusRuntimeException.class, () -> f.queries.search(request));
      assertThat(reset.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  private static QueryRequest aggregationRequest(String id) {
    return QueryRequest.newBuilder()
        .setIndexId(id)
        .setQueryBson(
            bson(
                """
                {facet:{facets:{prices:{type:'number',path:'price',boundaries:[0,10],
                  aggregations:{total:{type:'sum',path:'price'}}}}}}
                """))
        .build();
  }

  private static DynamicFeatureFlagConfig enabledFlag(DynamicFeatureFlags flag) {
    return new DynamicFeatureFlagConfig(
        flag.getName(),
        DynamicFeatureFlagConfig.Phase.ENABLED,
        List.of(),
        List.of(),
        100,
        DynamicFeatureFlagConfig.Scope.MONGOT_CLUSTER);
  }

  @Test
  public void mappingUpdate_additiveFields_preservesContentAndDeletionsAcrossRestart()
      throws Exception {
    Path directory = data();
    String id;
    String next =
        "{numPartitions:2,storedSource:true,mappings:{dynamic:false,fields:"
            + "{title:{type:'string'},category:{type:'token'}}}}";
    try (var f = new Fixture(directory)) {
      id =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("preserved")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(
                          bson(
                              "{numPartitions:2,storedSource:true,mappings:{dynamic:false,fields:"
                                  + "{title:{type:'string'}}}}"))
                      .build())
              .getIndexId();
      String previous = f.ready(id, 1).getGenerationId();
      f.upsert(id, "{_id:'a',title:'coffee',category:'books'}");
      f.upsert(id, "{_id:'b',title:'coffee',category:'books'}");
      f.docs.delete(
          DeleteRequest.newBuilder()
              .setIndexId(id)
              .addIdsBson(bson("{_id:'b'}"))
              .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
              .build());

      f.catalog.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setIndexId(id)
              .setExpectedRevision(1)
              .setUpdateMode(
                  com.xgen.mongot.standalone.grpc.IndexUpdateMode
                      .INDEX_UPDATE_MODE_REQUIRE_PRESERVE)
              .setDefinitionBson(bson(next))
              .build());
      var changed = f.ready(id, 2);

      assertThat(changed.getGenerationId()).isEqualTo(previous);
      assertThat(f.results(id, "coffee").getArray("results")).hasSize(1);
      assertThat(f.query(id, "{equals:{path:'category',value:'books'}}").getArray("results"))
          .isEmpty();
      assertThat(
              f.query(id, "{text:{path:'title',query:'coffee'},returnStoredSource:true}")
                  .getArray("results")
                  .getFirst()
                  .asDocument()
                  .getDocument("storedSource")
                  .getString("category")
                  .getValue())
          .isEqualTo("books");
      f.docs.upsert(
          UpsertRequest.newBuilder()
              .setIndexId(id)
              .setGenerationId(previous)
              .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
              .addDocumentsBson(bson("{_id:'a',title:'coffee',category:'books'}"))
              .build());
      f.upsert(id, "{_id:'a',title:'coffee',category:'books'}");
      assertThat(f.query(id, "{equals:{path:'category',value:'books'}}").getArray("results"))
          .hasSize(1);
    }

    try (var f = new Fixture(directory)) {
      f.ready(id, 2);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
      assertThat(f.results(id, "coffee").getArray("results")).hasSize(1);
      assertThat(f.query(id, "{equals:{path:'category',value:'books'}}").getArray("results"))
          .hasSize(1);
      f.upsert(id, "{_id:'c',title:'coffee',category:'books'}");
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(2);
    }
  }

  @Test
  public void mappingUpdate_identicalDefinition_retainsRevisionGenerationAndCursor()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("no-op");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");
      var original = f.ready(id, 1);
      var cursor =
          f.queries.search(
              QueryRequest.newBuilder()
                  .setIndexId(id)
                  .setBatchSize(1)
                  .setQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
                  .build());

      var result =
          f.catalog.updateIndex(
              UpdateIndexRequest.newBuilder()
                  .setIndexId(id)
                  .setExpectedRevision(1)
                  .setDefinitionBson(original.getDesiredDefinitionBson())
                  .build());

      assertThat(result.getDesiredRevision()).isEqualTo(1);
      assertThat(result.getGenerationId()).isEqualTo(original.getGenerationId());
      assertThat(
              new RawBsonDocument(
                      f.queries
                          .getMore(
                              GetMoreRequest.newBuilder().setCursorId(cursor.getCursorId()).build())
                          .getBatchBson()
                          .toByteArray())
                  .getArray("results"))
          .hasSize(1);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(2);
    }
  }

  @Test
  public void mappingUpdate_queryAnalyzer_changesMatchingWithoutReindexing() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("query-analysis");
      f.upsert(id, "{_id:'a',title:'Coffee'}");
      assertThat(f.results(id, "COFFEE").getArray("results")).hasSize(1);

      f.catalog.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setIndexId(id)
              .setExpectedRevision(1)
              .setDefinitionBson(bson("{mappings:{dynamic:true},searchAnalyzer:'lucene.keyword'}"))
              .build());
      f.ready(id, 2);

      assertThat(f.results(id, "COFFEE").getArray("results")).isEmpty();
      assertThat(f.results(id, "coffee").getArray("results")).hasSize(1);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
    }
  }

  @Test
  public void mappingUpdate_requirePreserve_rejectsIncompatibleDefinitionWithoutMutation()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("reject-change");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      var original = f.ready(id, 1);

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.catalog.updateIndex(
                      UpdateIndexRequest.newBuilder()
                          .setIndexId(id)
                          .setExpectedRevision(1)
                          .setUpdateMode(
                              com.xgen.mongot.standalone.grpc.IndexUpdateMode
                                  .INDEX_UPDATE_MODE_REQUIRE_PRESERVE)
                          .setDefinitionBson(
                              bson("{mappings:{dynamic:true},analyzer:'lucene.keyword'}"))
                          .build()));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(f.ready(id, 1).getGenerationId()).isEqualTo(original.getGenerationId());
      assertThat(f.results(id, "coffee").getArray("results")).hasSize(1);
    }
  }

  @Test
  public void search_queryPlanner_returnsPlanWithoutHitsOrCursor() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("explain-plan");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");

      var response =
          f.queries.search(
              QueryRequest.newBuilder()
                  .setIndexId(id)
                  .setQueryBson(bson("{text:{path:'title',query:'COFFEE'}}"))
                  .setBatchSize(1)
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_QUERY_PLANNER)
                  .build());

      var explain = new RawBsonDocument(response.getExplainBson().toByteArray());
      assertThat(explain.getDocument("query").getString("type").getValue()).isEqualTo("TermQuery");
      assertThat(explain.getDocument("query").containsKey("stats")).isFalse();
      assertThat(explain.containsKey("joins")).isFalse();
      assertThat(explain.getDocument("luceneQuery").getString("analyzed").getValue())
          .contains("title:coffee");
      assertThat(explain.getDocument("luceneQuery").getString("rewritten").getValue())
          .contains("title:coffee");
      assertThat(explain.getDocument("metadata").getString("indexName").getValue())
          .isEqualTo("explain-plan");
      assertThat(new RawBsonDocument(response.getBatchBson().toByteArray()).getArray("results"))
          .isEmpty();
      assertThat(response.getCursorId()).isEqualTo(0);
      assertThat(response.getGenerationId()).isNotEmpty();
      assertThat(f.hasOpenCursors(id)).isFalse();
    }
  }

  @Test
  public void search_fuzzyExplain_preservesAnalyzedTermsWhenRewriteMatchesNothing()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("explain-fuzzy", "{analyzer:'lucene.english',mappings:{dynamic:true}}");
      f.upsert(id, "{_id:'a',title:'fox'}");

      var response =
          f.queries.search(
              QueryRequest.newBuilder()
                  .setIndexId(id)
                  .setQueryBson(
                      bson("{text:{path:'title',query:'THE RUNNING',fuzzy:{maxEdits:1}}}"))
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());

      var details =
          new RawBsonDocument(response.getExplainBson().toByteArray()).getDocument("luceneQuery");
      assertThat(details.getString("analyzed").getValue()).contains("title:run~1");
      assertThat(details.getString("analyzed").getValue()).doesNotContain("THE");
      assertThat(details.getString("analyzed").getValue()).doesNotContain("title:the");
      assertThat(details.getString("rewritten").getValue()).contains("MatchNoDocsQuery");
      assertThat(new RawBsonDocument(response.getBatchBson().toByteArray()).getArray("results"))
          .isEmpty();
    }
  }

  @Test
  public void search_phraseExplain_showsStemmedTermsAndPhraseStructure() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("explain-phrase", "{analyzer:'lucene.english',mappings:{dynamic:true}}");
      f.upsert(id, "{_id:'a',title:'running foxes'}");

      var response =
          f.queries.search(
              QueryRequest.newBuilder()
                  .setIndexId(id)
                  .setQueryBson(bson("{phrase:{path:'title',query:'RUNNING FOXES'}}"))
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_ALL_PLANS_EXECUTION)
                  .build());

      var details =
          new RawBsonDocument(response.getExplainBson().toByteArray()).getDocument("luceneQuery");
      assertThat(details.getString("analyzed").getValue()).contains("title:\"run fox\"");
      assertThat(details.getString("rewritten").getValue()).contains("title:\"run fox\"");
      assertThat(new RawBsonDocument(response.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
    }
  }

  @Test
  public void search_oversizedExplain_releasesCursorAndDoesNotLeakTracing() throws Exception {
    var base = StandaloneConfig.development(data(), 0);
    var config =
        new StandaloneConfig(
            base.dataPath(),
            base.address(),
            new StandaloneConfig.Limits(128, 100, 1000, 128),
            1,
            base.maxInFlightBytes(),
            base.minFreeDiskBytes(),
            base.commitInterval(),
            base.refreshInterval(),
            base.maxRpcDuration(),
            base.tls(),
            base.principals());
    try (var f = new Fixture(config)) {
      String id = f.create("explain-size-limit");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");
      var request =
          QueryRequest.newBuilder()
              .setIndexId(id)
              .setQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
              .setBatchSize(1)
              .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
              .build();

      var error = assertThrows(StatusRuntimeException.class, () -> f.queries.search(request));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      assertThat(f.hasOpenCursors(id)).isFalse();
      f.awaitRpcDelivery();
      var ordinary = f.queries.search(request.toBuilder().clearExplainVerbosity().build());
      assertThat(ordinary.getExplainBson()).isEmpty();
      assertThat(new RawBsonDocument(ordinary.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
      f.awaitRpcDelivery();
      f.queries.killCursor(
          KillCursorRequest.newBuilder().setCursorId(ordinary.getCursorId()).build());
    }
  }

  @Test
  public void search_executionStats_restoresTracingAcrossBatchesAndClosesExhaustedCursor()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("explain-execution");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");
      var request =
          QueryRequest.newBuilder()
              .setIndexId(id)
              .setQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
              .setBatchSize(1)
              .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
              .build();

      var first = f.queries.search(request);
      var next =
          f.queries.getMore(
              GetMoreRequest.newBuilder()
                  .setCursorId(first.getCursorId())
                  .setBatchSize(100)
                  .build());

      var initialExplain = new RawBsonDocument(first.getExplainBson().toByteArray());
      var nextExplain = new RawBsonDocument(next.getExplainBson().toByteArray());
      assertThat(initialExplain.getDocument("query").containsKey("stats")).isTrue();
      assertThat(nextExplain.getDocument("query").containsKey("stats")).isTrue();
      assertThat(nextExplain.getDocument("luceneQuery"))
          .isEqualTo(initialExplain.getDocument("luceneQuery"));
      assertThat(initialExplain.getDocument("resourceUsage").getNumber("numBatches").intValue())
          .isEqualTo(1);
      assertThat(nextExplain.getDocument("resourceUsage").getNumber("numBatches").intValue())
          .isEqualTo(2);
      assertThat(first.getCursorId()).isNotEqualTo(0);
      assertThat(new RawBsonDocument(first.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
      assertThat(new RawBsonDocument(next.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
      assertThat(next.getCursorId()).isEqualTo(0);
      assertThat(next.getGenerationId()).isEqualTo(first.getGenerationId());
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.queries.getMore(
                              GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
      assertThat(
              f.queries
                  .search(request.toBuilder().clearExplainVerbosity().build())
                  .getExplainBson())
          .isEmpty();
    }
  }

  @Test
  public void search_allPlansExecution_returnsStatsAndSupportsKillingCursor() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("explain-all");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");

      var response =
          f.queries.search(
              QueryRequest.newBuilder()
                  .setIndexId(id)
                  .setQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
                  .setBatchSize(1)
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_ALL_PLANS_EXECUTION)
                  .build());
      f.queries.killCursor(
          KillCursorRequest.newBuilder().setCursorId(response.getCursorId()).build());

      assertThat(
              new RawBsonDocument(response.getExplainBson().toByteArray())
                  .getDocument("query")
                  .containsKey("stats"))
          .isTrue();
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.queries.getMore(
                              GetMoreRequest.newBuilder()
                                  .setCursorId(response.getCursorId())
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
    }
  }

  @Test
  public void search_unknownExplainVerbosity_rejectsRequest() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("explain-invalid");

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.search(
                      QueryRequest.newBuilder()
                          .setIndexId(id)
                          .setQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
                          .setExplainVerbosityValue(99)
                          .build()));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(f.results(id, "coffee").getArray("results")).isEmpty();
    }
  }

  @Test
  public void deleteByQuery_singleOperatorCompound_matchesSearchSemantics() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-single-clause");
      f.upsert(id, "{_id:'remove',title:'coffee'}");
      f.upsert(id, "{_id:'keep',title:'tea'}");
      String predicate = "{compound:{filter:{text:{path:'title',query:'coffee'}}}}";
      assertThat(f.query(id, predicate).getArray("results")).hasSize(1);

      var result =
          f.docs.deleteByQuery(
              DeleteByQueryRequest.newBuilder()
                  .setIndexId(id)
                  .setSearchQueryBson(bson(predicate))
                  .build());

      assertThat(result.getProcessedCount()).isEqualTo(1);
      assertThat(f.results(id, "tea").getArray("results")).hasSize(1);
    }
  }

  @Test
  public void deleteByQuery_singleOperatorCompound_rejectsNestedUnsupportedOperator()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-single-unsupported");
      f.upsert(id, "{_id:'keep',title:'coffee'}");

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.docs.deleteByQuery(
                      DeleteByQueryRequest.newBuilder()
                          .setIndexId(id)
                          .setSearchQueryBson(
                              bson(
                                  "{compound:{filter:{compound:{must:{"
                                      + "knnBeta:{path:'v',vector:[1,0,0],k:1}}}}}}"))
                          .build()));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
    }
  }

  @Test
  public void deleteByQuery_vectorClauseLimit_returnsResourceExhaustedWithoutDeleting()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("delete-clause-limit")
                      .setType(IndexType.INDEX_TYPE_VECTOR_SEARCH)
                      .setDefinitionBson(
                          bson(
                              "{fields:[{type:'vector',path:'v',numDimensions:3,"
                                  + "similarity:'cosine'},"
                                  + "{type:'filter',path:'value'}]}"))
                      .build())
              .getIndexId();
      f.ready(id, 1);
      f.upsert(id, "{_id:'keep',value:1,v:[1,0,0]}");
      var clauses =
          java.util.stream.IntStream.range(0, 2000)
              .mapToObj(
                  i -> BsonDocument.parse("{value:{$gt:" + (i * 2) + ",$lt:" + (i * 2 + 2) + "}}"))
              .collect(java.util.stream.Collectors.toCollection(org.bson.BsonArray::new));
      var predicate = new BsonDocument("$or", clauses);

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.docs.deleteByQuery(
                      DeleteByQueryRequest.newBuilder()
                          .setIndexId(id)
                          .setVectorFilterBson(
                              ByteString.copyFrom(StandaloneDefinitionValidator.encode(predicate)))
                          .build()));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
    }
  }

  @Test
  public void deleteByQuery_smallBatchByteLimit_splitsInternalDeletes() throws Exception {
    var base = StandaloneConfig.development(data(), 0);
    var config =
        new StandaloneConfig(
            base.dataPath(),
            base.address(),
            new StandaloneConfig.Limits(64, 100, 1000, 64),
            base.maxInFlightRequests(),
            base.maxInFlightBytes(),
            base.minFreeDiskBytes(),
            base.commitInterval(),
            base.refreshInterval(),
            base.maxRpcDuration(),
            base.tls(),
            base.principals());
    try (var f = new Fixture(config, new io.grpc.Metadata())) {
      String id = f.create("delete-batches");
      f.upsert(id, "{_id:'document-one',title:'coffee'}");
      f.upsert(id, "{_id:'document-two',title:'coffee'}");
      f.upsert(id, "{_id:'document-three',title:'coffee'}");
      f.upsert(id, "{_id:'document-four',title:'coffee'}");

      var result =
          f.docs.deleteByQuery(
              DeleteByQueryRequest.newBuilder().setIndexId(id).setMatchAll(true).build());

      assertThat(result.getProcessedCount()).isEqualTo(4);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(0);
    }
  }

  @Test
  public void deleteByQuery_moreThanTenThousandMatches_streamsWholeEmbeddedBlocks()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.create(
              "delete-large",
              "{numPartitions:2,mappings:{dynamic:true,fields:{"
                  + "items:{type:'embeddedDocuments',dynamic:true}}}}");
      f.upsertMany(id, 10001, i -> "{_id:" + i + ",items:[{value:'remove'},{value:'remove'}]}");
      f.upsert(id, "{_id:'keep',title:'tea'}");
      var request =
          DeleteByQueryRequest.newBuilder()
              .setIndexId(id)
              .setSearchQueryBson(
                  bson(
                      "{embeddedDocument:{path:'items',operator:{"
                          + "text:{path:'items.value',query:'remove'}}}}"));

      var preview = f.docs.deleteByQuery(request.clone().setDryRun(true).build());
      assertThat(preview.getMatchedCount()).isEqualTo(10001);
      assertThat(preview.getProcessedCount()).isEqualTo(0);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(10002);

      var deleted = f.docs.deleteByQuery(request.build());

      assertThat(deleted.getMatchedCount()).isEqualTo(10001);
      assertThat(deleted.getProcessedCount()).isEqualTo(10001);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
      assertThat(f.stats(id).getLuceneDocumentCount()).isEqualTo(1);
      assertThat(f.results(id, "tea").getArray("results")).hasSize(1);
    }
  }

  @Test
  public void deleteByQuery_moreThanSixteenMiBOfIds_streamsAcrossByteBudget() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-large-ids", "{mappings:{dynamic:false}}");
      String prefix = "x".repeat(20000);
      f.upsertMany(id, 1000, i -> "{_id:'" + prefix + i + "'}");

      var deleted =
          f.docs.deleteByQuery(
              DeleteByQueryRequest.newBuilder().setIndexId(id).setMatchAll(true).build());

      assertThat(deleted.getMatchedCount()).isEqualTo(1000);
      assertThat(deleted.getProcessedCount()).isEqualTo(1000);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(0);
    }
  }

  @Test
  public void deleteByQuery_retiredLimitField_isIgnoredAndDeletesAllMatches() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-retired-limit");
      f.upsertMany(id, 201, i -> "{_id:" + i + ",title:'coffee'}");
      var oldField =
          com.google.protobuf.UnknownFieldSet.newBuilder()
              .addField(
                  7, com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build())
              .build();
      var request =
          DeleteByQueryRequest.newBuilder()
              .setIndexId(id)
              .setMatchAll(true)
              .setUnknownFields(oldField);

      var preview = f.docs.deleteByQuery(request.clone().setDryRun(true).build());
      var deleted = f.docs.deleteByQuery(request.build());

      assertThat(preview.getMatchedCount()).isEqualTo(201);
      assertThat(preview.getProcessedCount()).isEqualTo(0);
      assertThat(deleted.getMatchedCount()).isEqualTo(201);
      assertThat(deleted.getProcessedCount()).isEqualTo(201);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(0);
    }
  }

  @Test
  public void collectDocumentIds_cancellationBeforeScan_releasesReadersForLaterDeletion()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-cancel");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      var generation =
          f.runtime.liveIndexes().getIndexById(new org.bson.types.ObjectId(id)).orElseThrow();
      try (var lease = f.runtime.ingestion().acquireStatisticsLease(generation.getGenerationId())) {
        var error =
            assertThrows(
                StatusRuntimeException.class,
                () ->
                    lease
                        .index()
                        .getReader()
                        .collectDocumentIds(
                            new BsonDocument(),
                            false,
                            true,
                            ignored -> {},
                            () -> {
                              throw Status.CANCELLED.asRuntimeException();
                            }));

        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
      }
      assertThat(
              f.docs
                  .deleteByQuery(
                      DeleteByQueryRequest.newBuilder().setIndexId(id).setMatchAll(true).build())
                  .getProcessedCount())
          .isEqualTo(1);
    }
  }

  @Test
  public void collectDocumentIds_duringSelection_allowsConcurrentIngestion() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-concurrent");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      var generation =
          f.runtime.liveIndexes().getIndexById(new org.bson.types.ObjectId(id)).orElseThrow();
      var selected = new java.util.ArrayList<ByteString>();
      try (var lease = f.runtime.ingestion().acquireStatisticsLease(generation.getGenerationId())) {
        lease
            .index()
            .getReader()
            .collectDocumentIds(
                new BsonDocument(),
                false,
                true,
                value -> {
                  selected.add(ByteString.copyFrom(value));
                  // The RPC runs on a different worker, so holding ingestion coordination here
                  // would time out.
                  f.docs
                      .withDeadlineAfter(5, TimeUnit.SECONDS)
                      .upsert(
                          UpsertRequest.newBuilder()
                              .setIndexId(id)
                              .addDocumentsBson(bson("{_id:'later',title:'coffee'}"))
                              .build());
                },
                () -> {});
      }

      assertThat(selected).hasSize(1);
      assertThat(
              f.docs
                  .deleteByQuery(
                      DeleteByQueryRequest.newBuilder().setIndexId(id).setMatchAll(true).build())
                  .getProcessedCount())
          .isEqualTo(2);
    }
  }

  @Test
  public void deleteByQuery_rebuiltGeneration_rejectsOldFenceAndPreservesNewDocuments()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-rebuild");
      String old = f.ready(id, 1).getGenerationId();
      f.catalog.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setUpdateMode(
                  com.xgen.mongot.standalone.grpc.IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
              .setIndexId(id)
              .setExpectedRevision(1)
              .setDefinitionBson(bson("{mappings:{dynamic:true}}"))
              .build());
      f.ready(id, 2);
      f.upsert(id, "{_id:'new',title:'coffee'}");

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.docs.deleteByQuery(
                      DeleteByQueryRequest.newBuilder()
                          .setIndexId(id)
                          .setGenerationId(old)
                          .setMatchAll(true)
                          .build()));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
    }
  }

  @Test
  public void deleteByQuery_authorization_requiresBothRolesAndIndexGrantForDryRun()
      throws Exception {
    var base = StandaloneConfig.development(data(), 0);
    var admin =
        new StandaloneConfig.Principal(
            "admin",
            com.xgen.mongot.standalone.catalog.LocalIndexCatalog.digest(
                "admin-token-012345678901234567890123456789"
                    .getBytes(java.nio.charset.StandardCharsets.US_ASCII)),
            java.util.Set.of(StandaloneConfig.Role.values()),
            java.util.Set.of("*"));
    var writer =
        new StandaloneConfig.Principal(
            "writer",
            com.xgen.mongot.standalone.catalog.LocalIndexCatalog.digest(
                "writer-token-012345678901234567890123456789"
                    .getBytes(java.nio.charset.StandardCharsets.US_ASCII)),
            java.util.Set.of(StandaloneConfig.Role.WRITE),
            java.util.Set.of("*"));
    var restricted =
        new StandaloneConfig.Principal(
            "restricted",
            com.xgen.mongot.standalone.catalog.LocalIndexCatalog.digest(
                "restricted-token-012345678901234567890123456789"
                    .getBytes(java.nio.charset.StandardCharsets.US_ASCII)),
            java.util.Set.of(StandaloneConfig.Role.WRITE, StandaloneConfig.Role.QUERY),
            java.util.Set.of("507f1f77bcf86cd799439011"));
    var config =
        new StandaloneConfig(
            base.dataPath(),
            base.address(),
            base.documentLimits(),
            base.maxInFlightRequests(),
            base.maxInFlightBytes(),
            base.minFreeDiskBytes(),
            base.commitInterval(),
            base.refreshInterval(),
            base.maxRpcDuration(),
            base.tls(),
            java.util.List.of(admin, writer, restricted));
    try (var f = new Fixture(config, bearer("admin-token-012345678901234567890123456789"))) {
      String id = f.create("delete-auth");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      var request =
          DeleteByQueryRequest.newBuilder()
              .setIndexId(id)
              .setMatchAll(true)
              .setDryRun(true)
              .build();
      var writerStub =
          DocumentServiceGrpc.newBlockingStub(f.channel)
              .withInterceptors(
                  io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(
                      bearer("writer-token-012345678901234567890123456789")));
      var restrictedStub =
          DocumentServiceGrpc.newBlockingStub(f.channel)
              .withInterceptors(
                  io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(
                      bearer("restricted-token-012345678901234567890123456789")));

      var metaWriter =
          QueryServiceGrpc.newBlockingStub(f.channel)
              .withInterceptors(
                  io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(
                      bearer("writer-token-012345678901234567890123456789")));
      var metaRestricted =
          QueryServiceGrpc.newBlockingStub(f.channel)
              .withInterceptors(
                  io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(
                      bearer("restricted-token-012345678901234567890123456789")));
      var metaRequest = metaRequest("{index:'delete-auth',text:{path:'title',query:'coffee'}}");
      assertThat(
              assertThrows(StatusRuntimeException.class, () -> metaWriter.searchMeta(metaRequest))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.PERMISSION_DENIED);
      assertThat(
              assertThrows(
                      StatusRuntimeException.class, () -> metaRestricted.searchMeta(metaRequest))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.PERMISSION_DENIED);

      var missingRole =
          assertThrows(StatusRuntimeException.class, () -> writerStub.deleteByQuery(request));
      var missingGrant =
          assertThrows(StatusRuntimeException.class, () -> restrictedStub.deleteByQuery(request));

      assertThat(missingRole.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
      assertThat(missingGrant.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
      assertThat(f.docs.deleteByQuery(request).getMatchedCount()).isEqualTo(1);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
    }
  }

  private static io.grpc.Metadata bearer(String token) {
    var headers = new io.grpc.Metadata();
    headers.put(
        io.grpc.Metadata.Key.of("authorization", io.grpc.Metadata.ASCII_STRING_MARSHALLER),
        "Bearer " + token);
    return headers;
  }

  @Test
  public void deleteByQuery_partitionedEmbedded_preservesTypedIdsAndDeletesWholeBlocks()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("delete-blocks")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(
                          bson(
                              "{numPartitions:2,mappings:{dynamic:true,fields:{"
                                  + "items:{type:'embeddedDocuments',dynamic:true}}}}"))
                      .build())
              .getIndexId();
      f.ready(id, 1);
      f.upsert(id, "{_id:1,items:[{value:'remove'},{value:'remove'}]}");
      f.upsert(id, "{_id:{$numberLong:'1'},items:[{value:'keep'}]}");
      f.upsert(id, "{_id:'1',title:'keep'}");
      f.upsert(id, "{_id:{$oid:'507f1f77bcf86cd799439011'},title:'keep'}");

      var removed =
          f.docs.deleteByQuery(
              DeleteByQueryRequest.newBuilder()
                  .setIndexId(id)
                  .setSearchQueryBson(
                      bson(
                          "{embeddedDocument:{path:'items',operator:{"
                              + "text:{path:'items.value',query:'remove'}}}}"))
                  .build());

      assertThat(removed.getMatchedCount()).isEqualTo(1);
      assertThat(removed.getProcessedCount()).isEqualTo(1);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(3);
      assertThat(f.stats(id).getLuceneDocumentCount()).isEqualTo(4);
      assertThat(
              f.docs
                  .deleteByQuery(
                      DeleteByQueryRequest.newBuilder().setIndexId(id).setMatchAll(true).build())
                  .getMatchedCount())
          .isEqualTo(3);
      assertThat(f.stats(id).getLuceneDocumentCount()).isEqualTo(0);
    }
  }

  @Test
  public void deleteByQuery_committed_survivesRestart() throws Exception {
    String id;
    try (var f = new Fixture(data())) {
      id = f.create("delete-commit");
      f.upsert(id, "{_id:'gone',title:'coffee'}");
      f.upsert(id, "{_id:'keep',title:'tea'}");

      var result =
          f.docs.deleteByQuery(
              DeleteByQueryRequest.newBuilder()
                  .setIndexId(id)
                  .setSearchQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
                  .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED)
                  .build());

      assertThat(result.getProcessedCount()).isEqualTo(1);
    }
    try (var reopened = new Fixture(data())) {
      reopened.ready(id, 1);
      assertThat(reopened.results(id, "coffee").getArray("results")).isEmpty();
      assertThat(reopened.results(id, "tea").getArray("results")).hasSize(1);
    }
  }

  @Test
  public void deleteByQuery_indexedWrite_refreshesBeforeSelection() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-indexed");
      f.docs.upsert(
          UpsertRequest.newBuilder()
              .setIndexId(id)
              .addDocumentsBson(bson("{_id:'a',title:'coffee'}"))
              .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_INDEXED)
              .build());

      var result =
          f.docs.deleteByQuery(
              DeleteByQueryRequest.newBuilder()
                  .setIndexId(id)
                  .setSearchQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
                  .build());

      assertThat(result.getMatchedCount()).isEqualTo(1);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(0);
    }
  }

  @Test
  public void deleteByQuery_nestedUnsupportedOperator_rejectsBeforeWriting() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-invalid");
      f.upsert(id, "{_id:'a',title:'coffee'}");

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.docs.deleteByQuery(
                      DeleteByQueryRequest.newBuilder()
                          .setIndexId(id)
                          .setSearchQueryBson(
                              bson(
                                  "{compound:{filter:[{join:{localField:'title',"
                                      + "foreignField:'title',"
                                      + "operator:{exists:{path:'title'}}}}]}}"))
                          .build()));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
      assertThat(
              f.docs
                  .deleteByQuery(
                      DeleteByQueryRequest.newBuilder().setIndexId(id).setMatchAll(true).build())
                  .getProcessedCount())
          .isEqualTo(1);
    }
  }

  @Test
  public void deleteByQuery_invalidSelectors_rejectsBeforeWriting() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-selector");
      f.upsert(id, "{_id:'a',title:'coffee'}");

      var empty =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.docs.deleteByQuery(
                      DeleteByQueryRequest.newBuilder()
                          .setIndexId(id)
                          .setSearchQueryBson(bson("{}"))
                          .build()));
      var falseAll =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.docs.deleteByQuery(
                      DeleteByQueryRequest.newBuilder().setIndexId(id).setMatchAll(false).build()));
      var incompatible =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.docs.deleteByQuery(
                      DeleteByQueryRequest.newBuilder()
                          .setIndexId(id)
                          .setVectorFilterBson(bson("{title:'coffee'}"))
                          .build()));

      assertThat(empty.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(falseAll.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(incompatible.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
    }
  }

  @Test
  public void deleteByQuery_vectorFilter_preservesNonmatchingDocuments() throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("delete-vector")
                      .setType(IndexType.INDEX_TYPE_VECTOR_SEARCH)
                      .setDefinitionBson(
                          bson(
                              "{fields:[{type:'vector',path:'v',numDimensions:3,"
                                  + "similarity:'cosine'},{type:'filter',path:'tenant'}]}"))
                      .build())
              .getIndexId();
      f.ready(id, 1);
      f.upsert(id, "{_id:'a',tenant:'gone',v:[1,0,0]}");
      f.upsert(id, "{_id:'b',tenant:'keep',v:[0,1,0]}");
      f.upsert(id, "{_id:'c',tenant:'gone'}");

      var result =
          f.docs.deleteByQuery(
              DeleteByQueryRequest.newBuilder()
                  .setIndexId(id)
                  .setVectorFilterBson(bson("{tenant:{$eq:'gone'}}"))
                  .build());

      assertThat(result.getMatchedCount()).isEqualTo(2);
      assertThat(result.getProcessedCount()).isEqualTo(2);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
    }
  }

  @Test
  public void deleteByQuery_staleGeneration_rejectsWithoutDeletion() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-generation");
      f.upsert(id, "{_id:'a',title:'coffee'}");

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.docs.deleteByQuery(
                      DeleteByQueryRequest.newBuilder()
                          .setIndexId(id)
                          .setMatchAll(true)
                          .setGenerationId("stale")
                          .build()));

      assertThat(error.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.FAILED_PRECONDITION);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
    }
  }

  @Test
  public void deleteByQuery_dryRunThenDelete_preservesNonmatches() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("delete-query");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'tea'}");
      var request =
          DeleteByQueryRequest.newBuilder()
              .setIndexId(id)
              .setSearchQueryBson(bson("{text:{path:'title',query:'coffee'}}"));

      var preview = f.docs.deleteByQuery(request.setDryRun(true).build());

      assertThat(preview.getMatchedCount()).isEqualTo(1);
      assertThat(preview.getProcessedCount()).isEqualTo(0);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(2);
      var deleted = f.docs.deleteByQuery(request.setDryRun(false).build());
      assertThat(deleted.getProcessedCount()).isEqualTo(1);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
      assertThat(f.docs.deleteByQuery(request.build()).getMatchedCount()).isEqualTo(0);
    }
  }

  @Test
  public void push_omittedConcern_returnsBeforeRefreshAndExplicitVisibleWaits() throws Exception {
    var defaults = StandaloneConfig.development(data(), 0);
    var config =
        new StandaloneConfig(
            defaults.dataPath(),
            defaults.address(),
            defaults.documentLimits(),
            defaults.maxInFlightRequests(),
            defaults.maxInFlightBytes(),
            defaults.minFreeDiskBytes(),
            Duration.ofHours(1),
            Duration.ofHours(1),
            defaults.maxRpcDuration(),
            defaults.tls(),
            defaults.principals());
    try (var f = new Fixture(config)) {
      String id = f.create("concerns");
      f.docs.upsert(
          UpsertRequest.newBuilder()
              .setIndexId(id)
              .addDocumentsBson(bson("{_id:'a',title:'coffee'}"))
              .build());

      assertThat(f.results(id, "coffee").getArray("results")).isEmpty();
      f.docs.upsert(
          UpsertRequest.newBuilder()
              .setIndexId(id)
              .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
              .addDocumentsBson(bson("{_id:'b',title:'coffee'}"))
              .build());
      assertThat(f.results(id, "coffee").getArray("results")).hasSize(2);
      f.docs.delete(
          DeleteRequest.newBuilder().setIndexId(id).addIdsBson(bson("{_id:'a'}")).build());
      assertThat(f.results(id, "coffee").getArray("results")).hasSize(2);
      f.docs.delete(
          DeleteRequest.newBuilder()
              .setIndexId(id)
              .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
              .addIdsBson(bson("{_id:'b'}"))
              .build());
      assertThat(f.results(id, "coffee").getArray("results")).isEmpty();
    }
  }

  @Test
  public void push_concurrentBatches_indexesEveryDocumentInPartitionedIndex() throws Exception {
    try (var f = new Fixture(data());
        var workers = Executors.newFixedThreadPool(8)) {
      String id =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("concurrent")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(bson("{numPartitions:2,mappings:{dynamic:true}}"))
                      .build())
              .getIndexId();
      f.ready(id, 1);
      var requests =
          IntStream.range(0, 16)
              .mapToObj(
                  batch ->
                      UpsertRequest.newBuilder()
                          .setIndexId(id)
                          .addAllDocumentsBson(
                              IntStream.range(0, 100)
                                  .mapToObj(
                                      document ->
                                          bson(
                                              "{_id:'"
                                                  + batch
                                                  + "-"
                                                  + document
                                                  + "',title:'coffee'}"))
                                  .toList())
                          .build())
              .toList();

      var results =
          workers.invokeAll(
              requests.stream()
                  .<java.util.concurrent.Callable<Integer>>map(
                      request -> () -> f.docs.upsert(request).getIndexedCount())
                  .toList());

      assertThat(
              results.stream()
                  .map(
                      result -> {
                        try {
                          return result.get(10, TimeUnit.SECONDS);
                        } catch (Exception e) {
                          throw new AssertionError(e);
                        }
                      })
                  .toList())
          .containsExactlyElementsIn(java.util.Collections.nCopies(16, 100));
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1600);
    }
  }

  @Test
  public void getIndexStats_emptyAndMutations_reportsLiveDocumentsAndOptionalAverages()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("stats");

      var empty = f.stats(id);
      assertThat(empty.getDocumentCount()).isEqualTo(0);
      assertThat(empty.hasAverageIndexBytesPerDocument()).isFalse();
      assertThat(empty.hasAverageDirectoryBytesPerDocument()).isFalse();
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'tea'}");
      f.upsert(id, "{_id:'a',title:'replacement'}");
      var two = f.stats(id);

      assertThat(two.getDocumentCount()).isEqualTo(2);
      assertThat(two.getLuceneDocumentCount()).isEqualTo(2);
      assertThat(two.getIndexSizeBytes()).isGreaterThan(0);
      assertThat(two.getAverageIndexBytesPerDocument()).isEqualTo(two.getIndexSizeBytes() / 2.0);
      assertThat(two.getDirectorySizeBytes().hasValue()).isTrue();
      assertThat(two.getMemory().getAccountedWriterBufferHeapBytes().hasValue()).isTrue();
      f.docs.delete(
          DeleteRequest.newBuilder()
              .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
              .setIndexId(id)
              .addIdsBson(bson("{_id:'b'}"))
              .build());
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(1);
    }
  }

  @Test
  public void getIndexFieldStats_partitionedEmbeddedMixedTypes_reconcilesLedgerAndPreservesPages()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("field-stats")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(
                          bson(
                              """
                              {numPartitions:2,mappings:{dynamic:true,fields:{
                                unused:{type:'number'},
                                items:{type:'embeddedDocuments',dynamic:true}
                              }}}
                              """))
                      .build())
              .getIndexId();
      f.ready(id, 1);
      f.upsert(id, "{_id:'a',mixed:17,items:[{value:'one'},{value:'two'}]}");
      f.upsert(id, "{_id:'b',mixed:'seventeen',items:[{value:'three'}]}");

      var full = f.fieldStats(id, 100);

      assertThat(full.getIndexStats().getDocumentCount()).isEqualTo(2);
      assertThat(full.getIndexStats().getLuceneDocumentCount()).isEqualTo(5);
      assertThat(
              full.getTotalAttributedDiskBytes()
                  + full.getSharedDiskBytes()
                  + full.getUnattributedDiskBytes())
          .isEqualTo(full.getIndexStats().getIndexSizeBytes());
      assertThat(
              full.getFieldsList().stream()
                  .mapToLong(field -> field.getAttributedDiskBytes().getValue())
                  .sum())
          .isEqualTo(full.getTotalAttributedDiskBytes());
      var mixed =
          full.getFieldsList().stream()
              .filter(field -> field.getPath().equals("mixed"))
              .findFirst()
              .orElseThrow();
      assertThat(mixed.getRepresentationsList().stream().map(rep -> rep.getMappingType()).toList())
          .containsAtLeast("number", "string");
      var unused =
          full.getFieldsList().stream()
              .filter(field -> field.getPath().equals("unused"))
              .findFirst()
              .orElseThrow();
      assertThat(unused.getConfigured()).isTrue();
      assertThat(unused.getObserved()).isFalse();
      assertThat(unused.getAttributedDiskBytes().getValue()).isEqualTo(0);
      var first = f.fieldStats(id, 1);
      assertThat(first.getFieldsCount()).isEqualTo(1);
      assertThat(first.getNextPageToken()).isNotEmpty();
      f.upsert(id, "{_id:'c',newField:'after snapshot'}");
      var second =
          f.catalog.getIndexFieldStats(
              GetIndexFieldStatsRequest.newBuilder()
                  .setIndexId(id)
                  .setPageSize(1)
                  .setPageToken(first.getNextPageToken())
                  .build());
      assertThat(second.getIndexStats().getSnapshotId())
          .isEqualTo(first.getIndexStats().getSnapshotId());
      assertThat(second.getIndexStats().getDocumentCount()).isEqualTo(2);
      assertThat(f.stats(id).getDocumentCount()).isEqualTo(3);
      var selected =
          f.catalog.getIndexFieldStats(
              GetIndexFieldStatsRequest.newBuilder().setIndexId(id).addPaths("mixed").build());
      assertThat(selected.getFieldsCount()).isEqualTo(1);
      assertThat(selected.getTotalAttributedDiskBytes())
          .isEqualTo(full.getTotalAttributedDiskBytes());
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.catalog.getIndexFieldStats(
                              GetIndexFieldStatsRequest.newBuilder()
                                  .setIndexId(id)
                                  .setPageSize(2)
                                  .setPageToken(first.getNextPageToken())
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }
  }

  @Test
  public void getIndexStats_nestedVectors_excludesChildDocumentsFromSourceCount() throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("nested-vector-stats")
                      .setType(IndexType.INDEX_TYPE_VECTOR_SEARCH)
                      .setDefinitionBson(
                          bson(
                              """
                              {numPartitions:2,nestedRoot:'sections',fields:[
                                {type:'vector',path:'sections.embedding',numDimensions:3,
                                 similarity:'euclidean'}]}
                              """))
                      .build())
              .getIndexId();
      f.ready(id, 1);
      f.upsert(id, "{_id:'a',sections:[{embedding:[1,0,0]},{embedding:[0,1,0]}]}");
      f.upsert(id, "{_id:'b',sections:[{embedding:[0,0,1]}]}");

      var fields = f.fieldStats(id, 100);

      assertThat(fields.getIndexStats().getDocumentCount()).isEqualTo(2);
      assertThat(fields.getIndexStats().getLuceneDocumentCount()).isEqualTo(5);
      assertThat(fields.getFieldsList().stream().map(field -> field.getPath()).toList())
          .contains("sections.embedding");
    }
  }

  @Test
  public void getIndexStats_invalidTargetsAndTokens_returnsGrpcErrors() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("stats-errors");

      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.catalog.getIndexStats(
                              GetIndexStatsRequest.newBuilder().setIndexId("bad").build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.catalog.getIndexStats(
                              GetIndexStatsRequest.newBuilder()
                                  .setIndexId(id)
                                  .setGenerationId("stale")
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.catalog.getIndexFieldStats(
                              GetIndexFieldStatsRequest.newBuilder()
                                  .setIndexId(id)
                                  .setPageSize(501)
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.catalog.getIndexFieldStats(
                              GetIndexFieldStatsRequest.newBuilder()
                                  .setIndexId(id)
                                  .setPageToken("invalid")
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.catalog.getIndexFieldStats(
                              GetIndexFieldStatsRequest.newBuilder()
                                  .setIndexId(id)
                                  .setPageToken("00000000-0000-0000-0000-000000000000")
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
    }
  }

  @Test
  public void getIndexMappings_mixedNestedFields_canCreateAndQueryStaticIndex() throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("dynamic")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(
                          bson(
                              """
                              {analyzer:'lucene.standard', numPartitions:2, storedSource:true,
                               mappings:{dynamic:true, fields:{
                                 title:{type:'string', analyzer:'lucene.keyword',
                                   multi:{simple:{type:'string', analyzer:'lucene.simple'}}},
                                 unused:{type:'number'},
                                 blocked:{type:'document', dynamic:false,
                                   fields:{kept:{type:'boolean'}}},
                                 items:{type:'embeddedDocuments', dynamic:true}
                               }}}
                              """))
                      .build())
              .getIndexId();
      var ready = f.ready(id, 1);
      f.upsert(
          id,
          """
          {_id:'a', title:'coffee', mixed:42, nested:{value:'tea'},
           items:[{score:1, detail:{text:'embedded'}}], blocked:{secret:'hidden', kept:true},
           numbers:[1, 2.5], date:{$date:'2026-01-01T00:00:00Z'},
           objectId:{$oid:'507f1f77bcf86cd799439011'},
           uuid:{$binary:{base64:'AAAAAAAAAAAAAAAAAAAAAA==', subType:'04'}}, flag:true}
          """);
      f.upsert(
          id,
          "{_id:'b', title:'tea', mixed:'forty two', nested:'also a scalar',"
              + " items:[{score:'high'}]}");

      var exported =
          f.catalog.getIndexMappings(
              GetIndexMappingsRequest.newBuilder()
                  .setIndexId(id)
                  .setGenerationId(ready.getGenerationId())
                  .build());

      assertThat(exported.getGenerationId()).isEqualTo(ready.getGenerationId());
      assertThat(exported.getActiveRevision()).isEqualTo(1);
      var mapping = BsonDocument.parse(exported.getMappingsJson());
      var fields = mapping.getDocument("fields");
      assertThat(mapping.getBoolean("dynamic").getValue()).isFalse();
      assertThat(
              fields.getArray("mixed").stream()
                  .map(v -> v.asDocument().getString("type").getValue())
                  .toList())
          .containsExactly("number", "string");
      assertThat(
              fields.getArray("nested").stream()
                  .map(v -> v.asDocument().getString("type").getValue())
                  .toList())
          .containsExactly("document", "string");
      assertThat(fields.getDocument("title").getString("analyzer").getValue())
          .isEqualTo("lucene.keyword");
      assertThat(fields.getDocument("title").getDocument("multi").containsKey("simple")).isTrue();
      assertThat(fields.getDocument("unused").getString("type").getValue()).isEqualTo("number");
      assertThat(fields.getDocument("blocked").getDocument("fields").keySet())
          .containsExactly("kept");
      assertThat(fields.getDocument("items").getBoolean("dynamic").getValue()).isFalse();
      assertThat(fields.getDocument("items").getDocument("fields").getArray("score")).hasSize(2);
      assertThat(fields.getDocument("date").getString("type").getValue()).isEqualTo("date");
      assertThat(fields.getDocument("objectId").getString("type").getValue()).isEqualTo("objectId");
      assertThat(fields.getDocument("uuid").getString("type").getValue()).isEqualTo("uuid");
      assertThat(fields.getDocument("flag").getString("type").getValue()).isEqualTo("boolean");
      var definition = BsonDocument.parse(exported.getDefinitionJson());
      assertThat(definition.getDocument("mappings")).isEqualTo(mapping);
      assertThat(definition.getBoolean("storedSource").getValue()).isTrue();
      String staticId =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("static")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(bson(exported.getDefinitionJson()))
                      .build())
              .getIndexId();
      f.ready(staticId, 1);
      f.upsert(
          staticId,
          "{_id:'a', title:'coffee', mixed:42, nested:{value:'tea'}, items:[{score:1}],"
              + " unexpected:'ignored'}");
      f.upsert(
          staticId,
          "{_id:'b', title:'tea', mixed:'forty two', nested:'also a scalar',"
              + " items:[{score:'high'}]}");

      assertThat(f.results(staticId, "coffee").getArray("results")).hasSize(1);
      assertThat(f.query(staticId, "{equals:{path:'mixed', value:42}}").getArray("results"))
          .hasSize(1);
      assertThat(f.query(staticId, "{text:{path:'mixed', query:'forty'}}").getArray("results"))
          .hasSize(1);
      assertThat(f.query(staticId, "{text:{path:'nested.value', query:'tea'}}").getArray("results"))
          .hasSize(1);
      assertThat(
              f.query(
                      staticId,
                      """
                      {embeddedDocument:{path:'items',
                        operator:{text:{path:'items.score',query:'high'}}}}
                      """)
                  .getArray("results"))
          .hasSize(1);
      assertThat(
              f.catalog
                  .getIndexMappings(
                      GetIndexMappingsRequest.newBuilder().setIndexId(staticId).build())
                  .getMappingsJson())
          .doesNotContain("unexpected");
    }
  }

  @Test
  public void getIndexMappings_customTypeSet_preservesDynamicOptionsInStaticIndex()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("type-set")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(
                          bson(
                              """
                              {analyzers:[{name:'casefold',tokenizer:{type:'keyword'},
                                tokenFilters:[{type:'lowercase'}]}],
                               typeSets:[{name:'sample',types:[
                                 {type:'string',analyzer:'casefold'},
                                 {type:'number',representation:'int64'},
                                 {type:'token',normalizer:'lowercase'},
                                 {type:'autocomplete',minGrams:2,maxGrams:6},
                                 {type:'geo',indexShapes:true}]}],
                               mappings:{dynamic:{typeSet:'sample'}}}
                              """))
                      .build())
              .getIndexId();
      f.ready(id, 1);
      f.upsert(
          id,
          "{_id:'one', object:{label:'COFFEE',amount:7},"
              + " location:{type:'Point',coordinates:[1.0,2.0]}}");

      var exported =
          f.catalog.getIndexMappings(GetIndexMappingsRequest.newBuilder().setIndexId(id).build());

      var definition = BsonDocument.parse(exported.getDefinitionJson());
      assertThat(definition.getArray("analyzers").get(0).asDocument().getString("name").getValue())
          .isEqualTo("casefold");
      var fields = definition.getDocument("mappings").getDocument("fields");
      var object = fields.getDocument("object");
      assertThat(object.getBoolean("dynamic").getValue()).isFalse();
      assertThat(
              object.getDocument("fields").getArray("label").stream()
                  .map(v -> v.asDocument().getString("type").getValue())
                  .toList())
          .containsExactly("autocomplete", "string", "token");
      assertThat(
              object
                  .getDocument("fields")
                  .getDocument("amount")
                  .getString("representation")
                  .getValue())
          .isEqualTo("int64");
      assertThat(
              fields.getArray("location").stream()
                  .map(v -> v.asDocument().getString("type").getValue())
                  .toList())
          .containsExactly("document", "geo");
      String staticId =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("static-type-set")
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(bson(exported.getDefinitionJson()))
                      .build())
              .getIndexId();
      f.ready(staticId, 1);
      f.upsert(
          staticId,
          "{_id:'one', object:{label:'COFFEE',amount:7},"
              + " location:{type:'Point',coordinates:[1.0,2.0]}}");

      assertThat(
              f.query(staticId, "{text:{path:'object.label',query:'coffee'}}").getArray("results"))
          .hasSize(1);
      assertThat(
              f.query(staticId, "{equals:{path:'object.label',value:'coffee'}}")
                  .getArray("results"))
          .hasSize(1);
      assertThat(
              f.query(staticId, "{autocomplete:{path:'object.label',query:'cof'}}")
                  .getArray("results"))
          .hasSize(1);
      assertThat(f.query(staticId, "{equals:{path:'object.amount',value:7}}").getArray("results"))
          .hasSize(1);
    }
  }

  @Test
  public void search_vectorTypeSet_bindsValidBinaryValuesPerPathAndRecoversAfterRestart()
      throws Exception {
    Similarity.load();
    Path directory = data();
    String id;
    String three = bsonFloatVector(1, 0, 0);
    String eight = bsonFloatVector(1, 0, 0, 0, 0, 0, 0, 0);
    String query = exactVectorQuery("nested.v", "[1,0,0]");
    String fixedQuery = exactVectorQuery("fixed.v", "[1,0,0,0,0,0,0,0]");
    String definition =
        """
        {numPartitions:2, mappings:{dynamic:{typeSet:'learn'},fields:{
           fixed:{type:'document',dynamic:{typeSet:'fixed'}},
           bits:{type:'document',dynamic:{typeSet:'bits'}},
           inside:{type:'embeddedDocuments',dynamic:{typeSet:'learn'},fields:{tag:{type:'token'}}},
           staticVector:{type:'vector',similarity:'cosine',numDimensions:3}}},
         typeSets:[{name:'learn',types:[{type:'vector',similarity:'cosine',quantization:'scalar'}]},
           {name:'fixed',types:[{type:'vector',similarity:'euclidean',quantization:'binary',numDimensions:8}]},
           {name:'bits',types:[{type:'vector',similarity:'euclidean'}]}]}
        """;
    try (var f = new Fixture(directory)) {
      id = f.create("discovery", definition);
      assertThat(f.query(id, fixedQuery).getArray("results")).isEmpty();
      // One complete float32 and one trailing byte must not bind the field to one dimension.
      f.upsert(id, "{_id:0,nested:{v:BinData(9,'JwAAAIA/eA==')}}");
      // A nonzero padding byte is invalid for float32 even when its three low bits are zero.
      f.upsert(id, "{_id:-1,nested:{v:BinData(9,'JwgAAIA/')}}");
      f.upsert(
          id, "{_id:1,nested:{v:[0.0,1.0]},other:BinData(0,'AAAA'),staticVector:[1.0,0.0,0.0]}");
      f.upsert(
          id,
          "{_id:2,nested:{v:"
              + bsonFloatVector(0, 0)
              + "},fixed:{v:"
              + three
              + "},'fixed.alias':"
              + three
              + ",inside:[{tag:'keep',v:"
              + three
              + "}]}");
      assertThat(f.query(id, query).getArray("results")).isEmpty();
      f.upsert(
          id,
          "{_id:3,nested:{v:"
              + three
              + "},other:"
              + eight
              + ",fixed:{v:"
              + eight
              + "},bits:{v:BinData(9,'EAD/')},ignored:["
              + three
              + "]}");
      f.upsert(id, "{_id:4,nested:{v:" + eight + "},fixed:{v:[1,0,0,0,0,0,0,0]}}");

      assertThat(f.query(id, query).getArray("results")).hasSize(1);
      assertThat(f.query(id, exactVectorQuery("staticVector", "[1,0,0]")).getArray("results"))
          .hasSize(1);
      assertThat(f.query(id, fixedQuery).getArray("results")).hasSize(1);
      var fields =
          BsonDocument.parse(
                  f.catalog
                      .getIndexMappings(GetIndexMappingsRequest.newBuilder().setIndexId(id).build())
                      .getMappingsJson())
              .getDocument("fields");
      var revision =
          f.catalog.updateIndex(
              UpdateIndexRequest.newBuilder()
                  .setIndexId(id)
                  .setExpectedRevision(1)
                  .setDefinitionBson(
                      bson(
                          definition.replace(
                              "{numPartitions:",
                              "{searchAnalyzer:'lucene.keyword',numPartitions:")))
                  .build());
      assertThat(revision.getActiveRevision()).isEqualTo(2);
      assertThat(f.query(id, query).getArray("results")).hasSize(1);
      assertThat(fields.getDocument("ignored").getString("type").getValue()).isEqualTo("document");
      assertThat(fields.getDocument("fixed").getDocument("fields").containsKey("alias")).isFalse();
      var inside = fields.getDocument("inside").getDocument("fields");
      assertThat(inside.keySet()).containsExactly("tag", "v");
      assertThat(inside.getDocument("v").getString("type").getValue()).isEqualTo("document");
      assertThat(
              fields
                  .getDocument("bits")
                  .getDocument("fields")
                  .getDocument("v")
                  .getInt32("numDimensions")
                  .getValue())
          .isEqualTo(8);
      assertThat(fields.getDocument("other").getInt32("numDimensions").getValue()).isEqualTo(8);
      assertThat(fields.getDocument("nested").getDocument("fields").getDocument("v"))
          .isEqualTo(
              BsonDocument.parse(
                  "{type:'vector',similarity:'cosine',quantization:'scalar',numDimensions:3,bsonVectorOnly:true}"));
    }

    try (var f = new Fixture(directory)) {
      f.ready(id, 2);
      f.upsert(id, "{_id:5,nested:{v:" + eight + "}}");
      assertThat(f.query(id, query).getArray("results")).hasSize(1);
      assertThat(
              assertThrows(
                  StatusRuntimeException.class,
                  () -> f.query(id, exactVectorQuery("nested.v", "[1,0]"))))
          .hasMessageThat()
          .contains("indexed with 3 dimensions");
    }
  }

  @Test
  public void upsert_nestedValueAliasingDottedStaticMapping_preservesConfiguredVectorSchema()
      throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      String id =
          f.create(
              "dotted-vector",
              """
              {mappings:{dynamic:{typeSet:'learn'},fields:{
                  'group.v':{type:'vector',numDimensions:3,similarity:'euclidean'}}},
               typeSets:[{name:'learn',types:[{type:'vector',similarity:'euclidean'}]}]}
              """);
      f.upsert(id, "{_id:1,group:{v:" + bsonFloatVector(1, 0) + "}}");

      f.upsert(id, "{_id:2,'group.v':" + bsonFloatVector(1, 0, 0) + "}");

      assertThat(f.query(id, exactVectorQuery("group.v", "[1,0,0]")).getArray("results"))
          .hasSize(1);
    }
  }

  @Test
  public void getIndexMappings_vectorTypeSet_exportedDefinitionReproducesSearchResults()
      throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      var dynamic =
          f.create(
              "discovered-vectors",
              """
              {mappings:{dynamic:{typeSet:'binary'},fields:{
                    scalar:{type:'document',dynamic:{typeSet:'scalar'}},
                    plain:{type:'document',dynamic:{typeSet:'plain'}}}},
               typeSets:[{name:'binary',types:[{type:'vector',similarity:'euclidean',quantization:'binary'}]},
                         {name:'scalar',types:[{type:'vector',similarity:'euclidean',quantization:'scalar'}]},
                         {name:'plain',types:[{type:'vector',similarity:'euclidean'}]}]}
              """);
      var floats = bsonFloatVector(1, 0, 0, 0, 0, 0, 0, 0);
      String packedBits = "BinData(9,'EAD/')";
      String document =
          "{_id:7,v:" + floats + ",scalar:{v:" + floats + "},plain:{v:" + packedBits + "}}";
      // A length of three cannot bind a binary-quantized field.
      f.upsert(dynamic, "{_id:1,v:" + bsonFloatVector(1, 2, 3) + "}");
      f.upsert(dynamic, document);
      var exported =
          f.catalog.getIndexMappings(
              GetIndexMappingsRequest.newBuilder().setIndexId(dynamic).build());

      var fixed = f.create("exported-vectors", exported.getDefinitionJson());
      f.upsert(fixed, document);

      String queryVector = "[1,0,0,0,0,0,0,0]";
      assertSameSingleHit(f, fixed, dynamic, exactVectorQuery("v", queryVector));
      assertSameSingleHit(
          f,
          fixed,
          dynamic,
          "{vectorSearch:{path:'v',queryVector:" + queryVector + ",numCandidates:16,limit:1}}");
      assertSameSingleHit(f, fixed, dynamic, exactVectorQuery("scalar.v", queryVector));
      assertSameSingleHit(f, fixed, dynamic, exactVectorQuery("plain.v", packedBits));
    }
  }

  @Test
  public void getIndexMappings_vectorUnderLiteralDottedDocument_retainsConfiguredParent()
      throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      String dynamic =
          f.create(
              "dotted-parent",
              """
              {mappings:{dynamic:false,fields:{'a.b':{type:'document',dynamic:{typeSet:'learn'}}}},
               typeSets:[{name:'learn',types:[{type:'vector',similarity:'euclidean'}]}]}
              """);
      String document = "{_id:1,'a.b':{v:" + bsonFloatVector(1, 0) + "}}";
      f.upsert(dynamic, document);
      var exported =
          f.catalog.getIndexMappings(
              GetIndexMappingsRequest.newBuilder().setIndexId(dynamic).build());
      var fixed = f.create("static-dotted-parent", exported.getDefinitionJson());
      f.upsert(fixed, document);

      assertSameSingleHit(f, fixed, dynamic, exactVectorQuery("a.b.v", "[1,0]"));
    }
  }

  @Test
  public void getIndexMappings_literalAndNestedVectors_preservesBothBsonPathsAfterRestart()
      throws Exception {
    Similarity.load();
    var directory = data();
    String definition =
        "{mappings:{dynamic:{typeSet:'v'}},typeSets:[{name:'v',types:[{type:'vector',similarity:'euclidean'}]}]}";
    String vector = bsonFloatVector(1, 0);
    String literal = "{_id:1,'a.b':" + vector + "}";
    String nested = "{_id:2,a:{b:" + vector + "}}";
    String numeric = "{_id:3,'a.b':[1,0]}";
    String id;
    try (var f = new Fixture(directory)) {
      id = f.create("shapes", definition);
      f.upsert(id, literal);
      var literalExport =
          f.catalog.getIndexMappings(GetIndexMappingsRequest.newBuilder().setIndexId(id).build());
      var fields = BsonDocument.parse(literalExport.getMappingsJson()).getDocument("fields");
      assertThat(fields.keySet()).containsExactly("_id", "a.b");
      f.upsert(id, nested);
      f.upsert(id, numeric);
    }
    try (var f = new Fixture(directory)) {
      f.ready(id, 1);
      var exported =
          f.catalog.getIndexMappings(GetIndexMappingsRequest.newBuilder().setIndexId(id).build());
      var fixed = f.create("static-shapes", exported.getDefinitionJson());
      f.upsert(fixed, literal);
      f.upsert(fixed, nested);
      f.upsert(fixed, numeric);
      String query = exactVectorQuery("a.b", "[1,0]");
      var hits = f.query(id, query).getArray("results");
      assertThat(hits).hasSize(2);
      assertThat(f.query(fixed, query).getArray("results")).containsExactlyElementsIn(hits);
      var ordinary =
          f.create(
              "ordinary",
              "{mappings:{dynamic:false,fields:{'a.b':{type:'vector',numDimensions:2,similarity:'euclidean'}}}}");
      f.upsert(ordinary, numeric);
      assertThat(f.query(ordinary, query).getArray("results")).hasSize(1);
    }
  }

  @Test
  public void upsert_firstVectorWithAnalyzedText_preservesAllTokensAndStoredValues()
      throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      String id =
          f.create(
              "with-text",
              "{mappings:{dynamic:{typeSet:'v'}},typeSets:[{name:'v',types:[{type:'string',store:true},{type:'vector',similarity:'euclidean'}]}]}");
      f.upsert(
          id,
          "{_id:1,v:"
              + bsonFloatVector(1, 0)
              + ",text:['winter snow','blue jay'],other:'sugar maple'}");
      assertThat(f.query(id, "{phrase:{path:'text',query:'blue jay'}}").getArray("results"))
          .hasSize(1);
      assertThat(f.query(id, "{phrase:{path:'other',query:'sugar maple'}}").getArray("results"))
          .hasSize(1);
      assertThat(f.query(id, exactVectorQuery("v", "[1,0]")).getArray("results")).hasSize(1);
    }
  }

  @Test
  public void getIndexMappings_literalTokenAndNestedText_preservesBothSearches() throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.create(
              "alias",
              "{mappings:{dynamic:false,fields:{a:{type:'document',dynamic:true},'a.b':{type:'token'}}}}");
      String document = "{_id:1,a:{b:'snow'},'a.b':'other'}";
      f.upsert(id, document);
      var exported =
          f.catalog.getIndexMappings(GetIndexMappingsRequest.newBuilder().setIndexId(id).build());
      var fixed = f.create("static-alias", exported.getDefinitionJson());
      f.upsert(fixed, document);
      assertSameSingleHit(f, fixed, id, "{text:{path:'a.b',query:'snow'}}");
      assertSameSingleHit(f, fixed, id, "{equals:{path:'a.b',value:'other'}}");
    }
  }

  @Test
  public void query_unboundVectorOnSparseShard_returnsEmptyUntilFirstWrite() throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      String definition =
          "{mappings:{dynamic:{typeSet:'v'}},typeSets:[{name:'v',types:[{type:'vector',similarity:'euclidean'}]}]}";
      String full = f.create("full", definition);
      String sparse = f.create("sparse", definition);
      f.upsert(full, "{_id:1,v:" + bsonFloatVector(1, 0) + "}");
      f.upsert(sparse, "{_id:2,other:" + bsonFloatVector(1, 0) + "}");
      String exact = exactVectorQuery("v", "[1,0]");
      String ann = "{vectorSearch:{path:'v',queryVector:[1,0],numCandidates:8,limit:2}}";
      assertThat(f.query(full, exact).getArray("results")).hasSize(1);
      assertThat(f.query(sparse, exact).getArray("results")).isEmpty();
      assertThat(f.query(full, ann).getArray("results")).hasSize(1);
      assertThat(f.query(sparse, ann).getArray("results")).isEmpty();
      f.upsert(sparse, "{_id:3,v:" + bsonFloatVector(1, 0, 0) + "}");
      assertThat(f.query(sparse, exactVectorQuery("v", "[1,0,0]")).getArray("results")).hasSize(1);
      assertThat(assertThrows(StatusRuntimeException.class, () -> f.query(sparse, exact)))
          .hasMessageThat()
          .contains("3 dimensions");
    }
  }

  private static final String MIXED_VECTOR_MAPPING =
      "{numPartitions:2,mappings:{dynamic:{typeSet:'v'}},typeSets:[{name:'v',types:["
          + "{type:'string'},{type:'vector',similarity:'euclidean'}]}]}";

  private static GetIndexMappingsResponse exportMappings(Fixture f, String id) {
    return f.catalog.getIndexMappings(GetIndexMappingsRequest.newBuilder().setIndexId(id).build());
  }

  @Test
  public void getIndexMappings_mixedLiteralKey_preservesTextAndVectorHits() throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      var id = f.create("mixed-literal", MIXED_VECTOR_MAPPING);
      var vector = "{_id:1,'a.b':" + bsonFloatVector(1, 0) + "}";
      var text = "{_id:2,'a.b':'snow'}";
      f.upsert(id, vector);
      f.upsert(id, text);
      var fixed = f.create("fixed-mixed", exportMappings(f, id).getDefinitionJson());
      f.upsert(fixed, vector);
      f.upsert(fixed, text);

      assertSameSingleHit(f, fixed, id, "{text:{path:'a.b',query:'snow'}}");
      assertSameSingleHit(f, fixed, id, exactVectorQuery("a.b", "[1,0]"));
    }
  }

  @Test
  public void getIndexMappings_scalarAndVectorOnDifferentSpellings_preservesBothQueries()
      throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      var id = f.create("typed-aliases", MIXED_VECTOR_MAPPING);
      String literal = "{_id:1,'a.b':'snow'}";
      String nested = "{_id:2,a:{b:" + bsonFloatVector(1, 0) + "}}";
      f.upsert(id, literal);
      f.upsert(id, nested);

      var fixed = f.create("typed-static", exportMappings(f, id).getDefinitionJson());
      f.upsert(fixed, literal);
      f.upsert(fixed, nested);

      assertSameSingleHit(f, fixed, id, exactVectorQuery("a.b", "[1,0]"));
      assertSameSingleHit(f, fixed, id, "{text:{path:'a.b',query:'snow'}}");
    }
  }

  @Test
  public void getIndexMappings_keywordAndVectorAliases_preserveAnalysisInEitherOrder()
      throws Exception {
    Similarity.load();
    String mapping =
        MIXED_VECTOR_MAPPING.replace("type:'string'", "type:'string',analyzer:'lucene.keyword'");
    try (var f = new Fixture(data())) {
      assertReplayQueries(
          f,
          "keyword-literal",
          mapping,
          List.of("{_id:1,'a.b':'Snow Day'}", "{_id:2,a:{b:" + bsonFloatVector(1, 0) + "}}"),
          Map.of(
              "{text:{path:'a.b',query:'snow'}}",
              0,
              "{text:{path:'a.b',query:'Snow Day'}}",
              1,
              exactVectorQuery("a.b", "[1,0]"),
              1));
      assertReplayQueries(
          f,
          "keyword-nested",
          mapping,
          List.of("{_id:1,'a.b':" + bsonFloatVector(1, 0) + "}", "{_id:2,a:{b:'Snow Day'}}"),
          Map.of(
              "{text:{path:'a.b',query:'snow'}}",
              0,
              "{text:{path:'a.b',query:'Snow Day'}}",
              1,
              exactVectorQuery("a.b", "[1,0]"),
              1));
    }
  }

  @Test
  public void getIndexMappings_conflictingDynamicAnalyzers_preservesEffectiveAnalyzer()
      throws Exception {
    try (var f = new Fixture(data())) {
      assertReplayQueries(
          f,
          "analyzer",
          """
          {mappings:{dynamic:{typeSet:'root'},fields:{a:{type:'document',dynamic:{typeSet:'nested'}}}},
           typeSets:[{name:'root',types:[{type:'string',analyzer:'lucene.keyword'},{type:'vector',similarity:'euclidean'}]},
                     {name:'nested',types:[{type:'string',analyzer:'lucene.standard',searchAnalyzer:'lucene.whitespace'}]}]}
          """,
          List.of("{_id:1,'a.b':'Snow Day'}", "{_id:2,a:{b:'Snow Day'}}"),
          Map.of("{text:{path:'a.b',query:'snow'}}", 2, "{text:{path:'a.b',query:'Snow Day'}}", 0));
    }
  }

  @Test
  public void getIndexMappings_aliasedEmbeddedAutocomplete_preservesBothQueries() throws Exception {
    var dir = data();
    String original;
    String replay;
    var queries =
        List.of(
            "{embeddedDocument:{path:'a.b',operator:{autocomplete:{path:'a.b.left',query:'sno'}}}}",
            "{embeddedDocument:{path:'a.b',operator:{autocomplete:{path:'a.b.right',query:'rai'}}}}");
    try (var f = new Fixture(dir)) {
      original =
          f.create(
              "embedded-autocomplete",
              """
              {mappings:{dynamic:false,fields:{'a.b':{type:'embeddedDocuments',dynamic:{typeSet:'c'}},
               a:{type:'document',fields:{b:{type:'embeddedDocuments',dynamic:{typeSet:'c'}}}}}},
               typeSets:[{name:'c',types:[{type:'autocomplete',minGrams:2,maxGrams:8}]}]}
              """);
      var docs = List.of("{_id:1,'a.b':[{left:'snow'}]}", "{_id:2,a:{b:[{right:'rain'}]}}");
      for (String doc : docs) f.upsert(original, doc);
      replay = f.create("replayed", exportMappings(f, original).getDefinitionJson());
      for (String doc : docs) f.upsert(replay, doc);
    }
    try (var f = new Fixture(dir)) {
      f.ready(original, 1);
      f.ready(replay, 1);
      for (String query : queries) assertSameSingleHit(f, replay, original, query);
    }
  }

  @Test
  public void getIndexMappings_skippedVectorAndNullAlias_preservesFieldPresence() throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      String v = bsonFloatVector(1, 0);
      assertReplayQueries(
          f,
          "duplicate",
          MIXED_VECTOR_MAPPING,
          List.of("{_id:1,'a.b':" + v + ",a:{b:" + v + "}}"),
          Map.of("{exists:{path:'a'}}", 1, exactVectorQuery("a.b", "[1,0]"), 1));
      assertReplayQueries(
          f,
          "null-alias",
          MIXED_VECTOR_MAPPING,
          List.of("{_id:1,'a.b':null}", "{_id:2,a:{b:" + v + "}}"),
          Map.of(
              "{exists:{path:'a.b'}}",
              2,
              "{equals:{path:'a.b',value:null}}",
              1,
              exactVectorQuery("a.b", "[1,0]"),
              1));
    }
  }

  private static final String ALIAS_TYPE_SETS =
      """
      {mappings:{dynamic:{typeSet:'d'},fields:{a:{type:'document',dynamic:{typeSet:'s'}}}},
       typeSets:[{name:'d',types:[{type:'vector',similarity:'euclidean'},%s]},
                {name:'s',types:[%s]}]}
      """;

  @Test
  public void getIndexMappings_literalVectorUnderClosedAncestor_preservesOnlyLiteralHit()
      throws Exception {
    Similarity.load();
    String template =
        "{mappings:{dynamic:{typeSet:'d'},fields:{a:%s}},"
            + "typeSets:[{name:'d',types:[{type:'vector',similarity:'euclidean'}]}]}";
    var docs =
        List.of(
            "{_id:1,'a.b':" + bsonFloatVector(1, 0) + "}",
            "{_id:2,a:{b:" + bsonFloatVector(1, 0) + "}}");
    var queries = Map.of(exactVectorQuery("a.b", "[1,0]"), 1);
    try (var f = new Fixture(data())) {
      assertReplayQueries(
          f, "closed", template.formatted("{type:'document',dynamic:false}"), docs, queries);
      assertReplayQueries(f, "scalar", template.formatted("{type:'string'}"), docs, queries);
    }
  }

  @Test
  public void getIndexMappings_dynamicGeoAlias_acceptsGeoQueryBeforeAndAfterReplay()
      throws Exception {
    try (var f = new Fixture(data())) {
      assertReplayQueries(
          f,
          "geo",
          ALIAS_TYPE_SETS.formatted("{type:'geo',indexShapes:true}", "{type:'string'}"),
          List.of("{_id:1,'a.b':{type:'Point',coordinates:[1,1]}}", "{_id:2,a:{b:'green'}}"),
          Map.of(
              "{geoWithin:{path:'a.b',circle:{center:{type:'Point',coordinates:[1,1]},radius:5}}}",
                  1,
              "{text:{path:'a.b',query:'green'}}", 1));
    }
  }

  @Test
  public void getIndexMappings_distinctStringMultis_preservesEachAlias() throws Exception {
    String multi = "{type:'string',multi:{%s:{type:'string',analyzer:'lucene.keyword'}}}";
    try (var f = new Fixture(data())) {
      assertReplayQueries(
          f,
          "multis",
          ALIAS_TYPE_SETS.formatted(multi.formatted("lit"), multi.formatted("nest")),
          List.of("{_id:1,'a.b':'red apple'}", "{_id:2,a:{b:'green pear'}}"),
          Map.of(
              "{text:{path:{value:'a.b',multi:'lit'},query:'red apple'}}", 1,
              "{text:{path:{value:'a.b',multi:'nest'},query:'green pear'}}", 1,
              "{text:{path:{value:'a.b',multi:'lit'},query:'green pear'}}", 0));
    }
  }

  @Test
  public void getIndexMappings_dynamicBooleanSimilarity_preservesScores() throws Exception {
    try (var f = new Fixture(data())) {
      assertReplayQueries(
          f,
          "ranking",
          ALIAS_TYPE_SETS.formatted(
              "{type:'string',similarity:{type:'boolean'}}", "{type:'string'}"),
          List.of("{_id:1,x:'snow day today'}", "{_id:2,x:'snow snow snow'}"),
          Map.of("{text:{path:'x',query:'snow'}}", 2));
    }
  }

  private static void assertReplayQueries(
      Fixture f,
      String name,
      String definition,
      List<String> documents,
      Map<String, Integer> expectedHits)
      throws Exception {
    String original = f.create(name, definition);
    for (String doc : documents) {
      f.upsert(original, doc);
    }
    String replay = f.create(name + "-replay", exportMappings(f, original).getDefinitionJson());
    for (String doc : documents) {
      f.upsert(replay, doc);
    }
    expectedHits.forEach(
        (query, count) -> {
          var results = f.query(original, query).getArray("results");
          assertWithMessage(query).that(results).hasSize(count);
          assertThat(f.query(replay, query).getArray("results")).isEqualTo(results);
        });
  }

  @Test
  public void upsert_wrongDimensionBeforeValidAlias_preservesValidVectorAcrossReplay()
      throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      var id = f.create("dimension-alias", MIXED_VECTOR_MAPPING);
      String valid = bsonFloatVector(1, 0);
      String first = "{_id:1,a:{b:" + valid + "}}";
      String second = "{_id:2,'a.b':" + bsonFloatVector(1, 0, 0) + ",a:{b:" + valid + "}}";
      f.upsert(id, first);
      f.upsert(id, second);

      var fixed = f.create("dimension-static", exportMappings(f, id).getDefinitionJson());
      f.upsert(fixed, first);
      f.upsert(fixed, second);
      String query = exactVectorQuery("a.b", "[1,0]");
      var original = f.query(id, query).getArray("results");

      assertThat(original).hasSize(2);
      assertThat(f.query(fixed, query).getArray("results")).containsExactlyElementsIn(original);
    }
  }

  @Test
  public void getIndexMappings_aliasedEmbeddedRoots_preservesDistinctChildrenAfterRestart()
      throws Exception {
    assertDistinctEmbeddedChildren(
        """
        {mappings:{dynamic:{typeSet:'v'},fields:{'a.b':{type:'embeddedDocuments',dynamic:true},
            a:{type:'document',fields:{b:{type:'embeddedDocuments',dynamic:true}}}}},
         typeSets:[{name:'v',types:[{type:'vector',similarity:'euclidean'}]}]}
        """);
  }

  @Test
  public void getIndexMappings_aliasedEmbeddedRootsWithoutVectors_preservesDistinctChildren()
      throws Exception {
    assertDistinctEmbeddedChildren(
        """
        {mappings:{dynamic:false,fields:{'a.b':{type:'embeddedDocuments',dynamic:true},
            a:{type:'document',fields:{b:{type:'embeddedDocuments',dynamic:true}}}}}}
        """);
  }

  private void assertDistinctEmbeddedChildren(String definition) throws Exception {
    var directory = data();
    String id;
    try (var f = new Fixture(directory)) {
      id = f.create("embedded-aliases", definition);
      f.upsert(id, "{_id:1,'a.b':[{leftOnly:'snow'}]}");
      f.upsert(id, "{_id:2,a:{b:[{rightOnly:'rain'}]}}");
    }
    try (var f = new Fixture(directory)) {
      f.ready(id, 1);
      var fields =
          BsonDocument.parse(exportMappings(f, id).getMappingsJson()).getDocument("fields");
      assertThat(fields.getDocument("a.b").getDocument("fields").keySet())
          .containsExactly("leftOnly");
      assertThat(
              fields
                  .getDocument("a")
                  .getDocument("fields")
                  .getDocument("b")
                  .getDocument("fields")
                  .keySet())
          .containsExactly("rightOnly");
    }
  }

  @Test
  public void upsert_binaryAliases_preservesCandidateOrderOnReplay() throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      var id =
          f.create(
              "binary-aliases",
              MIXED_VECTOR_MAPPING.replace(
                  "similarity:'euclidean'", "similarity:'euclidean',quantization:'binary'"));
      String valid = bsonFloatVector(1, 0, 0, 0, 0, 0, 0, 0);
      String invalid = bsonFloatVector(1, 0, 0);
      var docs =
          List.of(
              "{_id:1,'a.b':" + valid + "}",
              "{_id:2,a:{b:" + valid + "}}",
              "{_id:3,'a.b':" + invalid + ",a:{b:" + valid + "}}",
              "{_id:4,a:{b:" + valid + "},'a.b':" + invalid + "}",
              "{_id:5,'a.b':" + valid + ",a:{b:" + valid + "}}",
              "{_id:6,'a.b':" + bsonFloatVector(new float[16]) + ",a:{b:" + valid + "}}");
      for (var doc : docs) {
        f.upsert(id, doc);
      }
      var fixed = f.create("replayed-aliases", exportMappings(f, id).getDefinitionJson());
      for (var doc : docs) {
        f.upsert(fixed, doc);
      }

      var query = "{vectorSearch:{path:'a.b',exact:true,queryVector:[1,0,0,0,0,0,0,0],limit:10}}";
      var original = f.query(id, query).getArray("results");
      assertThat(original).hasSize(docs.size());
      assertThat(f.query(fixed, query).getArray("results")).containsExactlyElementsIn(original);
    }
  }

  @Test
  public void getIndexMappings_replacedLiteralVector_disappearsAcrossMergeAndRestart()
      throws Exception {
    Similarity.load();
    var directory = data();
    String id;
    try (var f = new Fixture(directory)) {
      id = f.create("retired-spelling", MIXED_VECTOR_MAPPING);
      var v = bsonFloatVector(1, 0);
      f.upsert(id, "{_id:1,'a.b':" + v + "}");
      f.upsert(id, "{_id:2,a:{b:" + v + "}}");
      f.upsert(id, "{_id:1}");
      assertOnlyNestedSpelling(f, id);
      try (var lease = f.runtime.reads().acquire(id, "")) {
        lease.index().getWriter().forceMerge(1);
      }
      assertOnlyNestedSpelling(f, id);
    }
    try (var f = new Fixture(directory)) {
      f.ready(id, 1);
      assertOnlyNestedSpelling(f, id);
    }
  }

  private static void assertOnlyNestedSpelling(Fixture f, String id) {
    var fields = BsonDocument.parse(exportMappings(f, id).getMappingsJson()).getDocument("fields");
    assertThat(fields.containsKey("a.b")).isFalse();
    assertThat(
            fields
                .getDocument("a")
                .getDocument("fields")
                .getDocument("b")
                .getString("type")
                .getValue())
        .isEqualTo("vector");
  }

  private static void assertSameSingleHit(Fixture f, String fixed, String dynamic, String query) {
    var results = f.query(dynamic, query).getArray("results");
    assertThat(results).hasSize(1);
    assertThat(f.query(fixed, query).getArray("results")).isEqualTo(results);
  }

  private static String exactVectorQuery(String path, String vector) {
    return "{vectorSearch:{path:'" + path + "',queryVector:" + vector + ",exact:true,limit:10}}";
  }

  private static String bsonFloatVector(float... values) {
    var binary = BsonVectorParser.encode(Vector.fromFloats(values, FloatVector.OriginalType.BSON));
    return "BinData(9,'" + java.util.Base64.getEncoder().encodeToString(binary.getData()) + "')";
  }

  @Test
  public void getIndexMappings_restart_recoversDynamicFieldsFromLucene() throws Exception {
    Path data = data();
    String id;
    String mappings;
    try (var f = new Fixture(data)) {
      id = f.create("persistent-mapping");
      f.upsert(id, "{_id:'a', nested:{value:17}}");
      mappings =
          f.catalog
              .getIndexMappings(GetIndexMappingsRequest.newBuilder().setIndexId(id).build())
              .getMappingsJson();
    }

    try (var f = new Fixture(data)) {
      f.ready(id, 1);

      assertThat(
              f.catalog
                  .getIndexMappings(GetIndexMappingsRequest.newBuilder().setIndexId(id).build())
                  .getMappingsJson())
          .isEqualTo(mappings);
    }
  }

  @Test
  public void getIndexMappings_invalidTargets_returnsGrpcErrors() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("mapping-errors");

      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.catalog.getIndexMappings(
                              GetIndexMappingsRequest.newBuilder().setIndexId("bad-id").build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.catalog.getIndexMappings(
                              GetIndexMappingsRequest.newBuilder()
                                  .setIndexId("507f1f77bcf86cd799439011")
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.catalog.getIndexMappings(
                              GetIndexMappingsRequest.newBuilder()
                                  .setIndexId(id)
                                  .setGenerationId("stale")
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
      var vector =
          f.catalog.createIndex(
              CreateIndexRequest.newBuilder()
                  .setName("vector-mapping")
                  .setType(IndexType.INDEX_TYPE_VECTOR_SEARCH)
                  .setDefinitionBson(
                      bson(
                          """
                          {fields:[{type:'vector',path:'v',numDimensions:3,similarity:'cosine'}]}
                          """))
                  .build());
      f.ready(vector.getIndexId(), 1);
      var vectorMappings =
          f.catalog.getIndexMappings(
              GetIndexMappingsRequest.newBuilder().setIndexId(vector.getIndexId()).build());
      assertThat(vectorMappings.getIndexType()).isEqualTo(IndexType.INDEX_TYPE_VECTOR_SEARCH);
      assertThat(
              BsonDocument.parse("{fields:" + vectorMappings.getVectorFieldsJson() + "}")
                  .getArray("fields")
                  .get(0)
                  .asDocument()
                  .getString("path")
                  .getValue())
          .isEqualTo("v");
      assertThat(vectorMappings.getMappingsJson()).isEmpty();
      f.catalog.dropIndex(
          DropIndexRequest.newBuilder().setIndexId(id).setExpectedRevision(1).build());
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.catalog.getIndexMappings(
                              GetIndexMappingsRequest.newBuilder().setIndexId(id).build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
    }
  }

  @Test
  public void push_upsertsAndDeletesOnlyTheSelectedIndex() throws Exception {
    try (var f = new Fixture(data())) {
      String first = f.create("first");
      String second = f.create("second");
      f.upsert(first, "{_id: 'a', title: 'coffee', privateField: 'not-stored'}");
      assertThat(f.results(first, "coffee").getArray("results")).hasSize(1);
      assertThat(f.results(second, "coffee").getArray("results")).isEmpty();
      f.upsert(first, "{_id: 'a', title: 'tea'}");
      assertThat(f.results(first, "coffee").getArray("results")).isEmpty();
      assertThat(f.results(first, "tea").getArray("results")).hasSize(1);
      var deleted =
          f.docs.delete(
              DeleteRequest.newBuilder()
                  .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
                  .setIndexId(first)
                  .addIdsBson(bson("{_id: 'a'}"))
                  .addIdsBson(bson("{_id: 'missing'}"))
                  .build());
      assertThat(deleted.getProcessedCount()).isEqualTo(2);
      assertThat(f.results(first, "tea").getArray("results")).isEmpty();
    }
  }

  @Test
  public void restart_preservesCommittedLuceneDataAndIndexDefinitionsOnly() throws Exception {
    Path data = data();
    String id;
    try (var f = new Fixture(data)) {
      id = f.create("persistent");
      f.docs.upsert(
          UpsertRequest.newBuilder()
              .setIndexId(id)
              .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED)
              .addDocumentsBson(
                  bson("{_id: 'a', title: 'coffee', privateField: 'secret-content-marker'}"))
              .build());
    }
    try (var f = new Fixture(data)) {
      f.ready(id, 1);
      var result = f.results(id, "coffee").getArray("results");
      assertThat(result).hasSize(1);
      assertThat(result.get(0).asDocument().containsKey("storedSource")).isFalse();
      assertThat(f.catalog.listIndexes(ListIndexesRequest.getDefaultInstance()).getIndexesCount())
          .isEqualTo(1);
    }
    assertThat(Files.exists(data.resolve("standalone.sqlite"))).isFalse();
    assertThat(Files.readString(data.resolve("indexes.json")))
        .doesNotContain("secret-content-marker");
    try (var paths = Files.list(data)) {
      assertThat(paths.filter(Files::isRegularFile).map(p -> p.getFileName().toString()).toList())
          .containsExactly(
              "standalone.lock",
              "indexes.json",
              "standalone-owned-ids.json",
              "configJournal.json",
              "dynamicFeatureFlags.json");
    }
  }

  @Test
  public void restart_missingCatalog_preservesCommittedGenerationForRestore() throws Exception {
    Path directory = data();
    String id;
    String generation;
    try (var f = new Fixture(directory)) {
      id = f.create("persistent");
      generation = f.ready(id, 1).getGenerationId();
      f.docs.upsert(
          UpsertRequest.newBuilder()
              .setIndexId(id)
              .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED)
              .addDocumentsBson(bson("{_id:'a',title:'coffee'}"))
              .build());
    }
    Path catalog = directory.resolve("indexes.json");
    Path backup = Files.move(catalog, this.folder.getRoot().toPath().resolve("backup.json"));
    var journal = Files.readString(directory.resolve("configJournal.json"));
    var ownership = Files.readString(directory.resolve("standalone-owned-ids.json"));

    var failure = assertThrows(IOException.class, () -> new Fixture(directory));

    assertThat(failure).hasMessageThat().contains("Missing indexes.json");
    assertThat(Files.readString(directory.resolve("configJournal.json"))).isEqualTo(journal);
    assertThat(Files.readString(directory.resolve("standalone-owned-ids.json")))
        .isEqualTo(ownership);
    Files.move(backup, catalog);
    try (var f = new Fixture(directory)) {
      assertThat(f.ready(id, 1).getGenerationId()).isEqualTo(generation);
      assertThat(f.results(id, "coffee").getArray("results")).hasSize(1);
    }
  }

  @Test
  public void standaloneLaunch_journalHasNonLocalGeneration_refusesBeforeRemovingData()
      throws Exception {
    Path directory = data();
    try (var fixture = new Fixture(directory)) {
      fixture.create("replicated-directory-fixture");
    }
    Files.move(directory.resolve("indexes.json"), directory.resolve("saved-indexes.json"));
    Files.move(
        directory.resolve("standalone-owned-ids.json"), directory.resolve("saved-ownership.json"));
    String journal = Files.readString(directory.resolve("configJournal.json"));

    var error =
        assertThrows(
            IOException.class,
            () -> new StandaloneRuntime(StandaloneConfig.development(directory, 0)));

    assertThat(error).hasMessageThat().contains("native mongot launcher");
    assertThat(Files.readString(directory.resolve("configJournal.json"))).isEqualTo(journal);
  }

  @Test
  public void sharedServices_nativeTls_doesNotLoadStandaloneCertificateFiles() throws Exception {
    var base = StandaloneConfig.development(data(), 0);
    var config =
        new StandaloneConfig(
            base.dataPath(),
            base.address(),
            base.documentLimits(),
            base.maxInFlightRequests(),
            base.maxInFlightBytes(),
            base.minFreeDiskBytes(),
            base.commitInterval(),
            base.refreshInterval(),
            base.maxRpcDuration(),
            Optional.of(
                new StandaloneConfig.Tls(
                    base.dataPath().resolve("unused.crt"), base.dataPath().resolve("unused.key"))),
            base.principals());

    try (var runtime = new StandaloneRuntime(config);
        var services = new StandaloneGrpcServer(runtime, true)) {
      assertThat(services.services()).hasSize(4);
    }
  }

  @Test
  public void mappingUpdate_startsEmptyAndRequiresReingestion() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("schema");
      f.upsert(id, "{_id: 'a', title: 'coffee'}");
      String oldGeneration = f.ready(id, 1).getGenerationId();
      f.catalog.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setUpdateMode(
                  com.xgen.mongot.standalone.grpc.IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
              .setIndexId(id)
              .setExpectedRevision(1)
              .setDefinitionBson(
                  bson("{mappings: {dynamic: false, fields: {title: {type: 'string'}}}}"))
              .build());
      var updated = f.ready(id, 2);
      assertThat(updated.getGenerationId()).isNotEqualTo(oldGeneration);
      assertThat(f.results(id, "coffee").getArray("results")).isEmpty();
      var stale =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.docs.upsert(
                      UpsertRequest.newBuilder()
                          .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
                          .setIndexId(id)
                          .setGenerationId(oldGeneration)
                          .addDocumentsBson(bson("{_id: 'a', title: 'coffee'}"))
                          .build()));
      assertThat(stale.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      f.upsert(id, "{_id: 'a', title: 'coffee'}");
      assertThat(f.results(id, "coffee").getArray("results")).hasSize(1);
    }
  }

  @Test
  public void newIndex_neverBackfillsContentFromAnotherIndex() throws Exception {
    try (var f = new Fixture(data())) {
      String first = f.create("first");
      f.upsert(first, "{_id: 'a', title: 'coffee'}");
      String second = f.create("second");
      assertThat(f.results(second, "coffee").getArray("results")).isEmpty();
      f.catalog.dropIndex(
          DropIndexRequest.newBuilder().setIndexId(first).setExpectedRevision(1).build());
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.docs.upsert(
                              UpsertRequest.newBuilder()
                                  .setWriteConcern(
                                      DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
                                  .setIndexId(first)
                                  .addDocumentsBson(bson("{_id: 'a'}"))
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
      String replacement = f.create("first");
      assertThat(replacement).isNotEqualTo(first);
      assertThat(f.results(replacement, "coffee").getArray("results")).isEmpty();
    }
  }

  @Test
  public void validation_rejectsMalformedBatchBeforeWriting() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("validation");
      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.docs.upsert(
                      UpsertRequest.newBuilder()
                          .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
                          .setIndexId(id)
                          .addDocumentsBson(bson("{_id:'a',title:'coffee'}"))
                          .addDocumentsBson(bson("{title:'missing id'}"))
                          .build()));
      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(f.results(id, "coffee").getArray("results")).isEmpty();
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.docs.delete(
                              DeleteRequest.newBuilder()
                                  .setWriteConcern(
                                      DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
                                  .setIndexId(id)
                                  .addIdsBson(bson("{_id:'a',other:1}"))
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }
  }

  @Test
  public void vectorSearch_explain_returnsPlanAndTracesPagedExecution() throws Exception {
    com.xgen.mongot.index.lucene.vector.Similarity.load();
    try (var f = new Fixture(data())) {
      String id =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("explain-vectors")
                      .setType(IndexType.INDEX_TYPE_VECTOR_SEARCH)
                      .setDefinitionBson(
                          bson(
                              "{fields:[{type:'vector',path:'embedding',"
                                  + "numDimensions:3,similarity:'cosine'}]}"))
                      .build())
              .getIndexId();
      f.ready(id, 1);
      f.upsert(id, "{_id:'a',embedding:[1.0,0.0,0.0]}");
      f.upsert(id, "{_id:'b',embedding:[0.9,0.1,0.0]}");
      var request =
          QueryRequest.newBuilder()
              .setIndexId(id)
              .setQueryBson(bson("{path:'embedding',queryVector:[1.0,0.0,0.0],exact:true,limit:2}"))
              .setBatchSize(1);

      var plan =
          f.queries.vectorSearch(
              request
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_QUERY_PLANNER)
                  .build());
      var first =
          f.queries.vectorSearch(
              request
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());
      var next =
          f.queries.getMore(
              GetMoreRequest.newBuilder()
                  .setCursorId(first.getCursorId())
                  .setBatchSize(100)
                  .build());

      assertThat(new RawBsonDocument(plan.getExplainBson().toByteArray()).containsKey("query"))
          .isTrue();
      assertThat(new RawBsonDocument(plan.getBatchBson().toByteArray()).getArray("results"))
          .isEmpty();
      assertThat(plan.getCursorId()).isEqualTo(0);
      assertThat(first.getCursorId()).isNotEqualTo(0);
      assertThat(new RawBsonDocument(first.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
      assertThat(new RawBsonDocument(next.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
      assertThat(
              new RawBsonDocument(next.getExplainBson().toByteArray())
                  .getDocument("resourceUsage")
                  .getNumber("numBatches")
                  .intValue())
          .isEqualTo(2);
      assertThat(next.getCursorId()).isEqualTo(0);
    }
  }

  @Test
  public void vectorPush_usesExistingVectorSchemaAndReader() throws Exception {
    Similarity.load();
    try (var f = new Fixture(data())) {
      var index =
          f.catalog.createIndex(
              CreateIndexRequest.newBuilder()
                  .setName("vectors")
                  .setType(IndexType.INDEX_TYPE_VECTOR_SEARCH)
                  .setDefinitionBson(
                      bson(
                          "{fields:[{type:'vector',path:'embedding',"
                              + "numDimensions:3,similarity:'cosine'}]}"))
                  .build());
      String id = index.getIndexId();
      f.ready(id, 1);
      f.upsert(id, "{_id:'near',embedding:[1.0,0.0,0.0]}");
      var result =
          f.queries.vectorSearch(
              QueryRequest.newBuilder()
                  .setIndexId(id)
                  .setQueryBson(
                      bson("{path:'embedding',queryVector:[1.0,0.0,0.0],exact:true,limit:1}"))
                  .build());
      assertThat(new RawBsonDocument(result.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
    }
  }

  @Test
  public void ownershipLock_rejectsSecondRuntime() throws Exception {
    Path data = data();
    try (var f = new Fixture(data)) {
      assertThrows(
          IOException.class, () -> new StandaloneRuntime(StandaloneConfig.development(data, 0)));
    }
  }

  @Test
  public void getMore_compatiblePublicationDuringReadinessCheck_preservesCursor() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("readiness-race", "{mappings:{fields:{title:{type:'string'}}}}");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");
      var cursor =
          f.queries.search(
              QueryRequest.newBuilder()
                  .setIndexId(id)
                  .setBatchSize(1)
                  .setQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
                  .build());
      assertThat(cursor.getCursorId()).isNotEqualTo(0);
      var live = f.runtime.liveIndexes();
      var interleaved = Mockito.mock(IndexCatalog.class, AdditionalAnswers.delegatesTo(live));
      var publish = new AtomicBoolean(true);
      Mockito.doAnswer(
              call -> {
                var captured = live.getIndexById(call.getArgument(0));
                if (publish.compareAndSet(true, false)) {
                  f.runtime.updateIndex(
                      UpdateIndexRequest.newBuilder()
                          .setIndexId(id)
                          .setExpectedRevision(1)
                          .setDefinitionBson(
                              bson(
                                  "{mappings:{fields:{title:{type:'string'},tag:{type:'token'}}}}"))
                          .build());
                  assertThat(f.runtime.describeIndex(id).getGenerationId())
                      .isEqualTo(cursor.getGenerationId());
                  assertThat(f.runtime.describeIndex(id).getActiveRevision()).isEqualTo(2);
                }
                return captured;
              })
          .when(interleaved)
          .getIndexById(ArgumentMatchers.any());
      var field = f.runtime.reads().getClass().getDeclaredField("live");
      field.setAccessible(true);
      field.set(f.runtime.reads(), interleaved);

      var more =
          f.queries.getMore(GetMoreRequest.newBuilder().setCursorId(cursor.getCursorId()).build());

      assertThat(new RawBsonDocument(more.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
    }
  }

  @Test
  public void search_serialSinglePartition_cachesOrdinaryQueriesOnly() throws Exception {
    assertQueryCaching(1, false);
  }

  @Test
  public void search_concurrentSinglePartition_cachesOrdinaryQueriesOnly() throws Exception {
    assertQueryCaching(1, true);
  }

  @Test
  public void search_serialMultiplePartitions_cachesOrdinaryQueriesOnly() throws Exception {
    assertQueryCaching(2, false);
  }

  @Test
  public void search_concurrentMultiplePartitions_cachesOrdinaryQueriesOnly() throws Exception {
    assertQueryCaching(2, true);
  }

  private void assertQueryCaching(int partitions, boolean concurrent) throws Exception {
    try (var f = new Fixture(data())) {
      String id =
          f.create(
              "query-cache",
              "{numPartitions:"
                  + partitions
                  + ",mappings:{fields:{title:{type:'string'},key:{type:'token'}}}}");
      f.upsert(id, "{_id:'a',title:'coffee',key:'a'}");
      var captured = captureSnapshot(f, id);
      var plain = bson("{text:{path:'title',query:'coffee'},concurrent:" + concurrent + "}");
      var join =
          bson(
              "{compound:{filter:[{join:{localField:'key',foreignField:'key',"
                  + "operator:{exists:{path:'title'}}}}]},concurrent:"
                  + concurrent
                  + "}");

      f.queries.search(QueryRequest.newBuilder().setIndexId(id).setQueryBson(plain).build());
      var caches =
          snapshotSearchers(captured.get().actual(), partitions).stream()
              .map(IndexSearcher::getQueryCache)
              .toList();
      assertThat(caches).hasSize(partitions);
      assertThat(caches).doesNotContain(null);

      f.queries.search(QueryRequest.newBuilder().setIndexId(id).setQueryBson(join).build());
      assertThat(
              snapshotSearchers(captured.get().actual(), partitions).stream()
                  .map(IndexSearcher::getQueryCache)
                  .toList())
          .containsExactlyElementsIn(Collections.nCopies(partitions, null));

      f.queries.searchMeta(
          SearchMetaRequest.newBuilder().setTargetIndexId(id).setQueryBson(plain).build());
      assertThat(
              snapshotSearchers(captured.get().actual(), partitions).stream()
                  .map(IndexSearcher::getQueryCache)
                  .toList())
          .containsExactlyElementsIn(caches)
          .inOrder();

      f.queries.searchMeta(
          SearchMetaRequest.newBuilder().setTargetIndexId(id).setQueryBson(join).build());
      assertThat(
              snapshotSearchers(captured.get().actual(), partitions).stream()
                  .map(IndexSearcher::getQueryCache)
                  .toList())
          .containsExactlyElementsIn(Collections.nCopies(partitions, null));
    }
  }

  private static List<IndexSearcher> snapshotSearchers(QuerySnapshot snapshot, int partitions)
      throws Exception {
    var wrapped = snapshot.getClass().getDeclaredField("snapshot");
    wrapped.setAccessible(true);
    var pinned = wrapped.get(snapshot);
    List<?> snapshots;
    if (partitions == 1) {
      snapshots = List.of(pinned);
    } else {
      var children = pinned.getClass().getDeclaredField("snapshots");
      children.setAccessible(true);
      snapshots = (List<?>) children.get(pinned);
    }
    var searchers = new ArrayList<IndexSearcher>();
    for (var child : snapshots) {
      var referenceField = child.getClass().getDeclaredField("reference");
      referenceField.setAccessible(true);
      var reference = referenceField.get(child);
      var getSearcher = reference.getClass().getDeclaredMethod("getIndexSearcher");
      getSearcher.setAccessible(true);
      searchers.add((IndexSearcher) getSearcher.invoke(reference));
    }
    return searchers;
  }

  @Test
  public void search_deadlineBeforeLuceneWork_abortsScan() throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("cancel-probe");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      var finished = new CountDownLatch(1);
      var completedUnderCancelledContext = new AtomicBoolean();
      captureSnapshot(
          f,
          id,
          (actual, observed) -> {
            Mockito.doAnswer(
                    call -> {
                      var context = Context.current();
                      var cancelled = new CountDownLatch(1);
                      context.addListener(unused -> cancelled.countDown(), Runnable::run);
                      try {
                        assertThat(cancelled.await(5, TimeUnit.SECONDS)).isTrue();
                        var result =
                            actual.query(
                                call.getArgument(0),
                                call.getArgument(1),
                                call.getArgument(2),
                                call.getArgument(3));
                        completedUnderCancelledContext.set(context.isCancelled());
                        return result;
                      } finally {
                        finished.countDown();
                      }
                    })
                .when(observed)
                .query(
                    ArgumentMatchers.any(),
                    ArgumentMatchers.any(),
                    ArgumentMatchers.any(),
                    ArgumentMatchers.any());
          });

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries
                      .withDeadlineAfter(1, TimeUnit.SECONDS)
                      .search(
                          QueryRequest.newBuilder()
                              .setIndexId(id)
                              .setQueryBson(
                                  bson("{text:{path:'title',query:'coffee'},count:{type:'total'}}"))
                              .build()));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
      assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(completedUnderCancelledContext.get()).isFalse();
    }
  }

  @Test
  public void getMore_completedSearchAndExpiredGetMore_useCurrentRpcCancellation()
      throws Exception {
    try (var f = new Fixture(data())) {
      String id = f.create("get-more-cancellation");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");
      f.upsert(id, "{_id:'c',title:'coffee'}");
      var searchFinished = new CountDownLatch(1);
      var scanFinished = new CountDownLatch(1);
      var expire = new AtomicBoolean();
      var scannedAfterCancellation = new AtomicBoolean();
      captureSnapshot(
          f,
          id,
          (actual, observed) -> {
            Mockito.doAnswer(
                    call -> {
                      Context.current()
                          .addListener(unused -> searchFinished.countDown(), Runnable::run);
                      var result =
                          actual.query(
                              call.getArgument(0),
                              call.getArgument(1),
                              call.getArgument(2),
                              call.getArgument(3));
                      var producer =
                          Mockito.mock(
                              BatchProducer.class,
                              AdditionalAnswers.delegatesTo(result.searchBatchProducer));
                      var searcher = snapshotSearchers(actual, 1).getFirst();
                      Mockito.doAnswer(
                              batch -> {
                                if (!expire.get()) {
                                  // Exercise the actual Lucene callback even when this small
                                  // fixture has buffered hits.
                                  searcher.getTimeout().shouldExit();
                                  result.searchBatchProducer.execute(
                                      batch.getArgument(0), batch.getArgument(1));
                                  return null;
                                }
                                var cancelled = new CountDownLatch(1);
                                Context.current()
                                    .addListener(unused -> cancelled.countDown(), Runnable::run);
                                try {
                                  assertThat(cancelled.await(5, TimeUnit.SECONDS)).isTrue();
                                  searcher.getTimeout().shouldExit();
                                  scannedAfterCancellation.set(true);
                                  result.searchBatchProducer.execute(
                                      batch.getArgument(0), batch.getArgument(1));
                                  return null;
                                } finally {
                                  scanFinished.countDown();
                                }
                              })
                          .when(producer)
                          .execute(ArgumentMatchers.any(), ArgumentMatchers.any());
                      return new SearchProducerAndMetaResults(producer, result.metaResults);
                    })
                .when(observed)
                .query(
                    ArgumentMatchers.any(),
                    ArgumentMatchers.any(),
                    ArgumentMatchers.any(),
                    ArgumentMatchers.any());
          });
      var first =
          f.queries.search(
              QueryRequest.newBuilder()
                  .setIndexId(id)
                  .setBatchSize(1)
                  .setQueryBson(bson("{text:{path:'title',query:'coffee'}}"))
                  .build());
      assertThat(searchFinished.await(5, TimeUnit.SECONDS)).isTrue();

      var request =
          GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).setBatchSize(1).build();
      var second = f.queries.getMore(request);
      assertThat(new RawBsonDocument(second.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
      assertThat(second.getCursorId()).isEqualTo(first.getCursorId());
      expire.set(true);
      var error =
          assertThrows(
              StatusRuntimeException.class,
              () -> f.queries.withDeadlineAfter(1, TimeUnit.SECONDS).getMore(request));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
      assertThat(scanFinished.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(scannedAfterCancellation.get()).isFalse();
      assertThat(
              assertThrows(StatusRuntimeException.class, () -> f.queries.getMore(request))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
    }
  }

  private interface SnapshotObserver {
    void accept(QuerySnapshot actual, QuerySnapshot observed) throws Exception;
  }

  private record CapturedSnapshot(QuerySnapshot actual, QuerySnapshot observed) {}

  private static AtomicReference<CapturedSnapshot> captureSnapshot(Fixture f, String id)
      throws Exception {
    return captureSnapshot(f, id, (actual, observed) -> {});
  }

  private static AtomicReference<CapturedSnapshot> captureSnapshot(
      Fixture f, String id, SnapshotObserver observer) throws Exception {
    var captured = new AtomicReference<CapturedSnapshot>();
    try (var lease = f.runtime.reads().acquire(id, "")) {
      var initialized = lease.index();
      var reader = (SearchIndexReader) lease.reader();
      var observedReader =
          Mockito.mock(SearchIndexReader.class, AdditionalAnswers.delegatesTo(reader));
      Mockito.doAnswer(
              call -> {
                var actual = reader.acquireQuerySnapshot(call.getArgument(0));
                var observed =
                    Mockito.mock(QuerySnapshot.class, AdditionalAnswers.delegatesTo(actual));
                observer.accept(actual, observed);
                captured.set(new CapturedSnapshot(actual, observed));
                return observed;
              })
          .when(observedReader)
          .acquireQuerySnapshot(ArgumentMatchers.anyBoolean());
      var readerField = initialized.getClass().getDeclaredField("reader");
      readerField.setAccessible(true);
      readerField.set(initialized, observedReader);
    }
    return captured;
  }

  private Path data() {
    return this.folder.getRoot().toPath().resolve("data");
  }

  private static ByteString bson(String json) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json)));
  }

  private static final class Fixture implements AutoCloseable {
    private final StandaloneRuntime runtime;
    private final StandaloneGrpcServer server;
    private final ManagedChannel channel;
    private final CatalogServiceGrpc.CatalogServiceBlockingStub catalog;
    private final DocumentServiceGrpc.DocumentServiceBlockingStub docs;
    private final QueryServiceGrpc.QueryServiceBlockingStub queries;

    private Fixture(Path data) throws Exception {
      this(StandaloneConfig.development(data, 0));
    }

    private Fixture(StandaloneConfig config) throws Exception {
      this(config, new io.grpc.Metadata());
    }

    private Fixture(StandaloneConfig config, io.grpc.Metadata headers) throws Exception {
      this(
          config,
          headers,
          FeatureFlags.communityDefaults(),
          DynamicFeatureFlagRegistry.communityDefault());
    }

    private Fixture(
        StandaloneConfig config,
        io.grpc.Metadata headers,
        FeatureFlags flags,
        DynamicFeatureFlagRegistry dynamicFlags)
        throws Exception {
      this.runtime = new StandaloneRuntime(config, flags, dynamicFlags);
      this.server = new StandaloneGrpcServer(this.runtime).start();
      // Method timeouts start afresh for every RPC; a deadline stored on a stub is absolute.
      this.channel =
          NettyChannelBuilder.forAddress("127.0.0.1", this.server.port())
              .usePlaintext()
              .defaultServiceConfig(
                  Map.of(
                      "methodConfig", List.of(Map.of("name", List.of(Map.of()), "timeout", "45s"))))
              .build();
      this.catalog =
          CatalogServiceGrpc.newBlockingStub(this.channel)
              .withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
      this.docs =
          DocumentServiceGrpc.newBlockingStub(this.channel)
              .withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
      this.queries =
          QueryServiceGrpc.newBlockingStub(this.channel)
              .withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private DefaultConfigManager stopReconciliation() throws Exception {
      var reconcilerField = StandaloneRuntime.class.getDeclaredField("reconciler");
      reconcilerField.setAccessible(true);
      var reconciler =
          (java.util.concurrent.ScheduledThreadPoolExecutor) reconcilerField.get(this.runtime);
      reconciler.shutdown();
      assertThat(reconciler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      var managerField = StandaloneRuntime.class.getDeclaredField("configManager");
      managerField.setAccessible(true);
      return (DefaultConfigManager) managerField.get(this.runtime);
    }

    private String create(String name) throws Exception {
      return create(name, "{mappings:{dynamic:true}}");
    }

    private String create(String name, String definition) throws Exception {
      String id =
          this.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName(name)
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(bson(definition))
                      .build())
              .getIndexId();
      ready(id, 1);
      return id;
    }

    private String createVector() throws Exception {
      String id =
          this.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("vectors")
                      .setType(IndexType.INDEX_TYPE_VECTOR_SEARCH)
                      .setDefinitionBson(
                          bson(
                              "{fields:[{type:'vector',path:'v',numDimensions:3,"
                                  + "similarity:'cosine'}]}"))
                      .build())
              .getIndexId();
      ready(id, 1);
      return id;
    }

    private IndexDescription ready(String id, long revision) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (System.nanoTime() < deadline) {
        awaitRpcDelivery();
        var index = this.catalog.getIndex(GetIndexRequest.newBuilder().setIndexId(id).build());
        if (index.getLifecycleState().equals("READY") && index.getActiveRevision() == revision) {
          return index;
        }
        Thread.sleep(20);
      }
      throw new AssertionError(
          "Index never became ready: "
              + this.catalog.getIndex(GetIndexRequest.newBuilder().setIndexId(id).build()));
    }

    private void upsert(String id, String json) throws Exception {
      awaitRpcDelivery();
      assertThat(
              this.docs
                  .upsert(
                      UpsertRequest.newBuilder()
                          .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
                          .setIndexId(id)
                          .addDocumentsBson(bson(json))
                          .build())
                  .getIndexedCount())
          .isEqualTo(1);
      awaitRpcDelivery();
    }

    /**
     * Completion can reach the client just before the dispatcher's delivery finally releases
     * admission.
     */
    private void awaitRpcDelivery() throws Exception {
      if (this.runtime.config().maxInFlightRequests() != 1) {
        return;
      }
      var dispatcherField = StandaloneGrpcServer.class.getDeclaredField("dispatcher");
      dispatcherField.setAccessible(true);
      var permitsField = RpcDispatcher.class.getDeclaredField("requests");
      permitsField.setAccessible(true);
      var permits =
          (java.util.concurrent.Semaphore) permitsField.get(dispatcherField.get(this.server));
      assertThat(permits.tryAcquire(5, TimeUnit.SECONDS)).isTrue();
      permits.release();
    }

    private void upsertMany(String id, int count, java.util.function.IntFunction<String> document) {
      IntStream.range(0, (count + 999) / 1000)
          .forEach(
              batch -> {
                var documents =
                    IntStream.range(batch * 1000, Math.min(count, (batch + 1) * 1000))
                        .mapToObj(i -> bson(document.apply(i)))
                        .toList();
                assertThat(
                        this.docs
                            .upsert(
                                UpsertRequest.newBuilder()
                                    .setIndexId(id)
                                    .addAllDocumentsBson(documents)
                                    .build())
                            .getIndexedCount())
                    .isEqualTo(documents.size());
              });
    }

    private IndexStats stats(String id) {
      return this.catalog.getIndexStats(
          GetIndexStatsRequest.newBuilder().setIndexId(id).setRefresh(true).build());
    }

    private boolean hasOpenCursors(String id) {
      var generation =
          this.runtime.liveIndexes().getIndexById(new org.bson.types.ObjectId(id)).orElseThrow();
      return this.runtime.cursors().hasOpenCursors(generation.getGenerationId());
    }

    private IndexFieldStatsResponse fieldStats(String id, int size) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      while (System.nanoTime() < deadline) {
        var result =
            this.catalog.getIndexFieldStats(
                GetIndexFieldStatsRequest.newBuilder().setIndexId(id).setPageSize(size).build());
        if (result.getState() == StatsCollectionState.STATS_COLLECTION_STATE_READY) {
          return result;
        }
        Thread.sleep(20);
      }
      throw new AssertionError("Field statistics did not finish");
    }

    private BsonDocument results(String id, String term) {
      return query(id, "{text:{path:'title',query:'" + term + "'}}");
    }

    private BsonDocument query(String id, String json) {
      return new RawBsonDocument(
          this.queries
              .search(QueryRequest.newBuilder().setIndexId(id).setQueryBson(bson(json)).build())
              .getBatchBson()
              .toByteArray());
    }

    @Override
    public void close() throws Exception {
      this.channel.shutdownNow();
      this.channel.awaitTermination(5, TimeUnit.SECONDS);
      try {
        this.server.close();
      } finally {
        this.runtime.close();
      }
    }
  }
}
