package com.xgen.mongot.standalone.server;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.index.lucene.vector.Similarity;
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
import com.xgen.mongot.standalone.grpc.StatsCollectionState;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import com.xgen.mongot.standalone.grpc.UpsertRequest;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StandaloneGrpcServerTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

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
      var ordinary = f.queries.search(request.toBuilder().clearExplainVerbosity().build());
      assertThat(ordinary.getExplainBson()).isEmpty();
      assertThat(new RawBsonDocument(ordinary.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
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
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.catalog.getIndexMappings(
                              GetIndexMappingsRequest.newBuilder()
                                  .setIndexId(vector.getIndexId())
                                  .build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
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
      this.runtime = new StandaloneRuntime(config);
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

    private IndexDescription ready(String id, long revision) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (System.nanoTime() < deadline) {
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

    private void upsert(String id, String json) {
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
