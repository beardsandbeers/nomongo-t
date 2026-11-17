package com.xgen.mongot.standalone.server;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.xgen.mongot.cursor.CursorQuery;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.extensions.join.JoinPlanner.MemoryLimits;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.lucene.explain.information.JoinExplainInformation;
import com.xgen.mongot.index.query.OperatorQuery;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.operators.CompoundClause;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.JoinField;
import com.xgen.mongot.index.query.operators.JoinKeySet;
import com.xgen.mongot.index.query.operators.JoinOperator;
import com.xgen.mongot.index.query.operators.MaterializedJoinOperator;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.config.StandaloneConfig;
import com.xgen.mongot.standalone.grpc.CatalogServiceGrpc;
import com.xgen.mongot.standalone.grpc.CreateIndexRequest;
import com.xgen.mongot.standalone.grpc.DeleteRequest;
import com.xgen.mongot.standalone.grpc.DocumentServiceGrpc;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import com.xgen.mongot.standalone.grpc.DropIndexRequest;
import com.xgen.mongot.standalone.grpc.ExplainVerbosity;
import com.xgen.mongot.standalone.grpc.GetIndexRequest;
import com.xgen.mongot.standalone.grpc.GetMoreRequest;
import com.xgen.mongot.standalone.grpc.IndexDescription;
import com.xgen.mongot.standalone.grpc.IndexType;
import com.xgen.mongot.standalone.grpc.KillCursorRequest;
import com.xgen.mongot.standalone.grpc.QueryRequest;
import com.xgen.mongot.standalone.grpc.QueryResponse;
import com.xgen.mongot.standalone.grpc.QueryServiceGrpc;
import com.xgen.mongot.standalone.grpc.SearchMetaRequest;
import com.xgen.mongot.standalone.grpc.UpdateIndexRequest;
import com.xgen.mongot.standalone.grpc.UpsertRequest;
import com.xgen.mongot.standalone.runtime.StandaloneRuntime;
import com.xgen.mongot.util.Bytes;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Exercises actual Lucene indexes and the public gRPC entry point, including cursor dependencies.
 */
public class StandaloneJoinsTest {
  private static final String MAPPING =
      """
      {numPartitions:2,storedSource:true,mappings:{dynamic:false,fields:{
        key:{type:'token'}, parent:{type:'token'}, office:{type:'token'},
        title:{type:'string'}
      }}}
      """;
  private static final String JOIN =
      """
      {compound:{filter:[{join:{from:'foreign',localField:'parent',foreignField:'key',
        operator:{equals:{path:'office',value:'Berlin'}}}}]},returnStoredSource:true,
        count:{type:'total'}}
      """;
  private static final String INTEGER_FIELD =
      "{type:'number',representation:'int64',indexDoubles:false}";
  private static final String INTEGER_MAPPING =
      MAPPING
          .replace("key:{type:'token'}", "key:" + INTEGER_FIELD)
          .replace("parent:{type:'token'}", "parent:" + INTEGER_FIELD);

  private static final String MIXED_FIELD =
      "[{type:'token'},{type:'objectId'}," + INTEGER_FIELD + "]";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void getMore_delayedForeignPublication_keepsCursorDependingOnNewGeneration()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      f.upsert(local, "{_id:'first',parent:'a'}");
      f.upsert(local, "{_id:'second',parent:'a'}");
      var oldCursor = f.search(local, JOIN, 1);
      String oldGeneration = f.ready(foreign, 1).getGenerationId();

      f.catalog.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setIndexId(foreign)
              .setExpectedRevision(1)
              .setUpdateMode(
                  com.xgen.mongot.standalone.grpc.IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
              .setDefinitionBson(
                  bson(
                      MAPPING.replace(
                          "title:{type:'string'}",
                          "title:{type:'string'},category:{type:'token'}")))
              .build());
      f.ready(foreign, 2);
      f.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      var current = f.search(local, JOIN, 1);
      String retired = oldGeneration;

      assertThat(retired).isEqualTo(oldGeneration);
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
      assertThat(
              ids(
                  f.queries.getMore(
                      GetMoreRequest.newBuilder().setCursorId(current.getCursorId()).build())))
          .hasSize(1);
    }
  }

  @Test
  public void search_inPlaceUpdateWithoutCopyHeadroom_keepsMetadataAndForeignJoinsReadable()
      throws Exception {
    String foreign;
    String local;
    try (var seed = new Fixture(data(), List.of())) {
      foreign = seed.create("foreign", MAPPING);
      local = seed.create("local", MAPPING);
      seed.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      seed.upsert(local, "{_id:'matched',parent:'a'}");
    }

    try (var f = new Fixture(data(), List.of(), Long.MAX_VALUE / 2)) {
      f.ready(foreign, 1);
      f.ready(local, 1);
      f.catalog.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setIndexId(foreign)
              .setExpectedRevision(1)
              .setDefinitionBson(
                  bson(MAPPING.replace("title:{", "category:{type:'token'},title:{")))
              .build());
      f.ready(foreign, 2);

      var meta =
          f.queries.searchMeta(
              SearchMetaRequest.newBuilder()
                  .setIndexId(foreign)
                  .setQueryBson(
                      bson(
                          "{index:'foreign',equals:{path:'office',value:'Berlin'},"
                              + "count:{type:'total'}}"))
                  .build());
      assertThat(
              new RawBsonDocument(meta.getMetadataBson().toByteArray())
                  .getDocument("count")
                  .getNumber("total")
                  .longValue())
          .isEqualTo(1);
      assertThat(
              ids(
                  f.search(
                      foreign,
                      "{equals:{path:'office',value:'Berlin'},returnStoredSource:true}",
                      100)))
          .containsExactly("p");
      assertThat(ids(f.search(local, JOIN, 100))).containsExactly("matched");
      assertThat(f.runtime.describeIndex(foreign).getActiveRevision()).isEqualTo(2);
      assertThat(f.runtime.describeIndex(foreign).getDesiredRevision()).isEqualTo(2);
      assertThrows(StatusRuntimeException.class, () -> f.upsert(foreign, "{_id:'new',key:'b'}"));
    }
  }

  @Test
  public void search_aliasesShareDependency_enforcesEveryGenerationFence() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String local = f.create("local", MAPPING);
      String foreign = f.create("foreign", MAPPING);
      String generation = f.runtime.reads().resolve(foreign, "").getGenerationId().uniqueString();
      var expression = BsonDocument.parse(JOIN.replace("from:'foreign'", "from:'first'"));
      var filters = expression.getDocument("compound").getArray("filter");
      var second = filters.getFirst().asDocument().clone();
      second.getDocument("join").put("from", new org.bson.BsonString("second"));
      filters.add(second);
      var request =
          QueryRequest.newBuilder()
              .setIndexId(local)
              .setQueryBson(bson(expression.toJson()))
              .addJoinIndexes(
                  com.xgen.mongot.standalone.grpc.JoinIndexBinding.newBuilder()
                      .setAlias("first")
                      .setIndexId(foreign)
                      .setGenerationId(generation))
              .addJoinIndexes(
                  com.xgen.mongot.standalone.grpc.JoinIndexBinding.newBuilder()
                      .setAlias("second")
                      .setIndexId(foreign)
                      .setGenerationId("retired-generation"))
              .build();

      var error = assertThrows(StatusRuntimeException.class, () -> f.queries.search(request));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_aliasSelectsRoot_enforcesGenerationFence() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String local = f.create("local", MAPPING);
      var request =
          QueryRequest.newBuilder()
              .setIndexId(local)
              .setQueryBson(bson(JOIN))
              .addJoinIndexes(
                  com.xgen.mongot.standalone.grpc.JoinIndexBinding.newBuilder()
                      .setAlias("foreign")
                      .setIndexId(local)
                      .setGenerationId("retired-generation"))
              .build();

      var error = assertThrows(StatusRuntimeException.class, () -> f.queries.search(request));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_invalidationDuringJoinCallbackRegistration_releasesCapacityOnce()
      throws Exception {
    assertCallbackRegistrationCleanup(false);
  }

  @Test
  public void search_callbackRegistrationFailsDuringInvalidation_releasesCapacityOnce()
      throws Exception {
    assertCallbackRegistrationCleanup(true);
  }

  @SuppressWarnings("unchecked")
  private void assertCallbackRegistrationCleanup(boolean failRegistration) throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'f',key:'one',office:'Berlin'}");
      f.upsert(local, "{_id:'a',parent:'one'}");
      f.upsert(local, "{_id:'b',parent:'one'}");
      var generation = f.runtime.reads().resolve(local, "").getGenerationId();
      var service = (StandaloneQueryService) fieldValue(f.server, "queries");
      var manager =
          (com.xgen.mongot.config.manager.DefaultConfigManager)
              fieldValue(f.runtime, "configManager");
      com.xgen.mongot.config.manager.GenerationReadLeases registry;
      synchronized (manager) {
        registry = manager.configState.readLeases;
      }
      var notifications =
          (java.util.concurrent.ExecutorService) fieldValue(registry, "notifications");
      var planner =
          org.mockito.Mockito.mock(
              StandaloneJoinPlanner.class,
              org.mockito.Mockito.withSettings()
                  .mockMaker(org.mockito.MockMakers.INLINE)
                  .spiedInstance(fieldValue(service, "joinPlanner"))
                  .defaultAnswer(org.mockito.Mockito.CALLS_REAL_METHODS));
      org.mockito.Mockito.doAnswer(
              invocation -> {
                var plan = (StandaloneJoinPlanner.Plan) invocation.callRealMethod();
                var environment = fieldValue(fieldValue(plan, "this$0"), "environment");
                var leases =
                    (java.util.Map<
                            com.xgen.mongot.index.version.GenerationId,
                            com.xgen.mongot.config.manager.GenerationReadLeases.Lease>)
                        fieldValue(environment, "leases");
                var firstId = leases.keySet().iterator().next();
                var first =
                    org.mockito.Mockito.mock(
                        com.xgen.mongot.config.manager.GenerationReadLeases.Lease.class,
                        org.mockito.Mockito.withSettings()
                            .mockMaker(org.mockito.MockMakers.INLINE)
                            .spiedInstance(leases.get(firstId))
                            .defaultAnswer(org.mockito.Mockito.CALLS_REAL_METHODS));
                org.mockito.Mockito.doAnswer(
                        register -> {
                          // Retire after registering the root callback, while visiting the first
                          // join dependency. Also exercise failure before publishing the cursor.
                          f.runtime.reads().invalidate(local);
                          if (failRegistration) {
                            throw new IllegalStateException("Callback registration failed");
                          }
                          return register.callRealMethod();
                        })
                    .when(first)
                    .onInvalidated(org.mockito.ArgumentMatchers.any(Runnable.class));
                leases.put(firstId, first);
                return plan;
              })
          .when(planner)
          .prepare(
              org.mockito.ArgumentMatchers.any(),
              org.mockito.ArgumentMatchers.any(),
              org.mockito.ArgumentMatchers.anyList());
      var plannerField = StandaloneQueryService.class.getDeclaredField("joinPlanner");
      plannerField.setAccessible(true);
      plannerField.set(service, planner);
      var capacity = (java.util.concurrent.Semaphore) fieldValue(service, "capacity");
      assertThat(capacity.availablePermits()).isEqualTo(1024);

      var error = assertThrows(StatusRuntimeException.class, () -> f.search(local, JOIN, 1));
      notifications.submit(() -> {}).get(5, TimeUnit.SECONDS);

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
      assertThat(capacity.availablePermits()).isEqualTo(1024);
      assertThat(registry.activeCount()).isEqualTo(0);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
      assertThat((java.util.Map<?, ?>) fieldValue(service, "leases")).isEmpty();
      assertThat(f.runtime.cursors().hasOpenCursors(generation)).isFalse();
    }
  }

  private static Object fieldValue(Object owner, String name) throws Exception {
    var field = owner.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(owner);
  }

  @Test
  public void expireIdle_busyCursor_allowsHealthPublicationAndRechecksIdleTime() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String id = f.create("busy-cursor", "{mappings:{dynamic:true}}");
      f.upsert(id, "{_id:'a',title:'coffee'}");
      f.upsert(id, "{_id:'b',title:'coffee'}");
      long cursor = f.search(id, "{text:{path:'title',query:'coffee'}}", 1).getCursorId();
      var queries = (StandaloneQueryService) fieldValue(f.server, "queries");
      var leases = (Map<?, ?>) fieldValue(queries, "leases");
      var lease = leases.get(cursor);
      assertThat(lease).isNotNull();
      var lastAccess = lease.getClass().getDeclaredField("lastAccess");
      lastAccess.setAccessible(true);
      var sweeper = (ExecutorService) fieldValue(f.server, "cursorSweeper");
      var started = new CountDownLatch(1);
      var aged = new CountDownLatch(1);
      var published = new CountDownLatch(2);
      Future<?> sweep;

      // Hold the same monitor as an active GetMore, then refresh its timestamp at batch completion.
      synchronized (lease) {
        sweep =
            sweeper.submit(
                () -> {
                  started.countDown();
                  assertThat(aged.await(5, TimeUnit.SECONDS)).isTrue();
                  queries.expireIdle();
                  return null;
                });
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        lastAccess.setLong(lease, System.nanoTime() - TimeUnit.MINUTES.toNanos(3));
        aged.countDown();
        f.server.publishHealth(
            (name, ready) -> {
              if (name.isEmpty() && ready) {
                published.countDown();
              }
            });
        assertThat(published.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(sweep.isDone()).isFalse();
        lastAccess.setLong(lease, System.nanoTime());
      }
      sweep.get(5, TimeUnit.SECONDS);
      assertThat(leases.containsKey(cursor)).isTrue();
    }
  }

  @Test
  public void searchMeta_partitionedJoin_returnsCountAndExplainAndReleasesDependencies()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'f1',key:'one',office:'Berlin'}");
      f.upsert(foreign, "{_id:'f2',key:'one',office:'Berlin'}");
      f.upsert(local, "{_id:'a',parent:'one'}");
      f.upsert(local, "{_id:'b',parent:'two'}");
      var body = BsonDocument.parse(JOIN).append("index", new org.bson.BsonString("local"));

      var response =
          f.queries.searchMeta(
              SearchMetaRequest.newBuilder()
                  .setQueryBson(ByteString.copyFrom(StandaloneDefinitionValidator.encode(body)))
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());

      assertThat(
              new RawBsonDocument(response.getMetadataBson().toByteArray())
                  .getDocument("count")
                  .getInt64("total")
                  .getValue())
          .isEqualTo(1);
      assertThat(response.getGenerationId()).isNotEmpty();
      var explain = new RawBsonDocument(response.getExplainBson().toByteArray());
      var preparation = explain.getDocument("joins").getDocument("preparation");
      assertThat(preparation.getInt32("keyCount").getValue()).isEqualTo(1);
      assertThat(preparation.getArray("sources")).hasSize(1);
      assertThat(
              preparation
                  .getArray("sources")
                  .getFirst()
                  .asDocument()
                  .getString("foreignIndexId")
                  .getValue())
          .isEqualTo(foreign);
      assertThat(explain.getArray("indexPartitionExplain")).hasSize(2);
      assertLocalJoinPlan(
          explain.getArray("indexPartitionExplain").get(0).asDocument(), "termInSet", 1);
      assertLocalJoinPlan(
          explain.getArray("indexPartitionExplain").get(1).asDocument(), "termInSet", 1);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_mixedIdsAndReferences_matchesSameTypesAndPreservesTypedDeletes()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign =
          f.create("foreign", MAPPING.replace("key:{type:'token'}", "_id:" + MIXED_FIELD));
      String local =
          f.create(
              "local",
              MAPPING
                  .replace("key:{type:'token'}", "_id:" + MIXED_FIELD)
                  .replace("parent:{type:'token'}", "parent:" + MIXED_FIELD));
      f.upsert(foreign, "{_id:42,office:'Berlin'}");
      f.upsert(foreign, "{_id:'42',office:'Paris'}");
      f.upsert(foreign, "{_id:{$oid:'507f1f77bcf86cd799439011'},office:'Berlin'}");
      f.upsert(foreign, "{_id:'507f1f77bcf86cd799439011',office:'Paris'}");
      f.upsert(foreign, "{_id:'account',office:'Berlin'}");
      f.upsert(local, "{_id:'integer-ref',parent:{$numberLong:'42'}}");
      f.upsert(local, "{_id:{$numberLong:'43'},parent:{$oid:'507f1f77bcf86cd799439011'}}");
      f.upsert(local, "{_id:{$oid:'507f1f77bcf86cd799439012'},parent:'account'}");
      f.upsert(
          local, "{_id:'mixed-array',parent:[42,'account',{$oid:'507f1f77bcf86cd799439011'}]}");
      f.upsert(local, "{_id:'string-number',parent:'42'}");
      f.upsert(local, "{_id:'hex-string',parent:'507f1f77bcf86cd799439011'}");
      f.upsert(local, "{_id:'double',parent:42.0}");
      String query = JOIN.replace("foreignField:'key'", "foreignField:'_id'");

      var response = f.search(local, query, 100);
      var excluded = f.search(local, query.replace("filter:", "mustNot:"), 100);
      f.docs.delete(
          DeleteRequest.newBuilder()
              .setIndexId(foreign)
              .addIdsBson(bson("{_id:42}"))
              .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
              .build());
      var afterDelete = f.search(local, query, 100);

      assertThat(typedIds(response))
          .containsExactly(
              new org.bson.BsonString("integer-ref"),
              new org.bson.BsonInt64(43),
              BsonDocument.parse("{id:{$oid:'507f1f77bcf86cd799439012'}}").get("id"),
              new org.bson.BsonString("mixed-array"));
      assertThat(ids(excluded)).containsExactly("string-number", "hex-string", "double");
      assertThat(typedIds(afterDelete))
          .containsExactly(
              new org.bson.BsonInt64(43),
              BsonDocument.parse("{id:{$oid:'507f1f77bcf86cd799439012'}}").get("id"),
              new org.bson.BsonString("mixed-array"));
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_mixedForeignMappingAndIntegerLocalMapping_usesTheirSharedDomain()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign =
          f.create("foreign", MAPPING.replace("key:{type:'token'}", "_id:" + MIXED_FIELD));
      String local = f.create("local", INTEGER_MAPPING);
      f.upsert(foreign, "{_id:42,office:'Berlin'}");
      f.upsert(foreign, "{_id:'43',office:'Berlin'}");
      f.upsert(local, "{_id:'match',parent:{$numberLong:'42'}}");
      f.upsert(local, "{_id:'wrong-type',parent:43}");

      var response = f.search(local, JOIN.replace("foreignField:'key'", "foreignField:'_id'"), 100);

      assertThat(ids(response)).containsExactly("match");
    }
  }

  private static List<org.bson.BsonValue> typedIds(QueryResponse response) {
    return new RawBsonDocument(response.getBatchBson().toByteArray())
        .getArray("results").stream()
            .map(value -> value.asDocument().getDocument("storedSource").get("_id"))
            .toList();
  }

  @Test
  public void search_explainedMixedJoin_reportsSeparateEqualityDomains() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign =
          f.create("foreign", MAPPING.replace("key:{type:'token'}", "key:" + MIXED_FIELD));
      String local =
          f.create("local", MAPPING.replace("parent:{type:'token'}", "parent:" + MIXED_FIELD));
      f.upsert(foreign, "{_id:'f',key:['a',42,{$oid:'507f1f77bcf86cd799439011'}],office:'Berlin'}");
      f.upsert(local, "{_id:'l',parent:['a',42,{$oid:'507f1f77bcf86cd799439011'}]}");

      var response =
          f.queries.search(
              request(local, JOIN, 100).toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());
      var preparation = preparation(response);
      var sources = preparation.getArray("sources");

      assertThat(ids(response)).containsExactly("l");
      assertThat(preparation.getInt32("keyCount").getValue()).isEqualTo(3);
      assertThat(preparation.getInt64("keyBytes").getValue()).isEqualTo(21);
      assertThat(sources).hasSize(3);
      assertThat(
              sources.stream()
                  .map(value -> value.asDocument().getString("keyType").getValue())
                  .toList())
          .containsExactly("TOKEN", "OBJECT_ID", "INT64");
      assertThat(
              sources.stream().map(value -> value.asDocument().getInt32("id").getValue()).toList())
          .containsExactly(0, 1, 2);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_explainedEmptyMembership_reportsPreparationAndEmptyStrategy()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING.replace("numPartitions:2", "numPartitions:1"));
      f.upsert(foreign, "{_id:'f',key:'account',office:'Paris'}");
      f.upsert(local, "{_id:'l',parent:'account'}");

      var response =
          f.queries.search(
              request(local, JOIN, 100).toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());
      var preparation = preparation(response);
      var execution =
          new RawBsonDocument(response.getExplainBson().toByteArray())
              .getDocument("joins")
              .getArray("execution")
              .getFirst()
              .asDocument();

      assertThat(ids(response)).isEmpty();
      assertThat(preparation.getInt32("keyCount").getValue()).isEqualTo(0);
      assertThat(preparation.getInt64("keyBytes").getValue()).isEqualTo(0);
      assertThat(execution.getString("strategy").getValue()).isEqualTo("empty");
      assertThat(execution.getString("selectionReason").getValue()).isEqualTo("noForeignKeys");
      assertThat(execution.getArray("segments")).isEmpty();
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_explainedJoin_preservesPartitionPlansAndCursorDependencies() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING.replace("numPartitions:2", "numPartitions:4"));
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'f',key:'account',office:'Berlin'}");
      f.upsert(local, "{_id:'a',parent:'account'}");
      f.upsert(local, "{_id:'b',parent:'account'}");
      f.upsert(local, "{_id:'c',parent:'account'}");

      var first =
          f.queries.search(
              request(local, JOIN, 1).toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());
      var next =
          f.queries.getMore(
              GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).setBatchSize(1).build());
      f.catalog.dropIndex(
          DropIndexRequest.newBuilder().setIndexId(foreign).setExpectedRevision(1).build());

      assertPartitionPlans(first);
      assertPartitionPlans(next);
      var preparation = preparation(first);
      assertThat(preparation(next)).isEqualTo(preparation);
      assertThat(preparation.getInt32("localPartitions").getValue()).isEqualTo(2);
      assertThat(preparation.getInt32("keyCount").getValue()).isEqualTo(1);
      assertThat(preparation.getInt64("keyBytes").getValue()).isEqualTo(7);
      assertThat(preparation.getInt64("reservedBytes").getValue()).isGreaterThan(7);
      assertThat(preparation.getDocument("timing").getDouble("millisElapsed").getValue())
          .isAtLeast(0.0);
      var source = preparation.getArray("sources").getFirst().asDocument();
      assertThat(source.getString("from").getValue()).isEqualTo("foreign");
      assertThat(source.getString("foreignIndexId").getValue()).isEqualTo(foreign);
      assertThat(source.getString("foreignGeneration").getValue()).startsWith(foreign);
      assertThat(source.getBoolean("selfJoin").getValue()).isFalse();
      assertThat(source.getInt32("foreignPartitions").getValue()).isEqualTo(4);
      assertThat(source.getInt64("foreignWorkUnits").getValue()).isGreaterThan(0);
      assertThat(
              source
                  .getDocument("collection")
                  .getDocument("invocationCounts")
                  .getInt64("collectForeignKeys")
                  .getValue())
          .isEqualTo(1);
      assertThat(
              new RawBsonDocument(next.getExplainBson().toByteArray())
                  .getDocument("resourceUsage")
                  .getNumber("numBatches")
                  .intValue())
          .isEqualTo(2);
      assertThat(new RawBsonDocument(next.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
      assertThat(
              assertThrows(
                      StatusRuntimeException.class,
                      () ->
                          f.queries.getMore(
                              GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).build()))
                  .getStatus()
                  .getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
    }
  }

  @Test
  public void search_explainedConcurrentSelfJoin_isolatesMembershipFromPartitionPlans()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String local = f.create("local", MAPPING);
      f.upsert(local, "{_id:'a',key:'account',parent:'account',office:'Berlin'}");
      f.upsert(local, "{_id:'b',key:'account',parent:'account',office:'Berlin'}");
      f.upsert(local, "{_id:'c',key:'account',parent:'account',office:'Berlin'}");
      var expression = BsonDocument.parse(JOIN.replace("from:'foreign',", ""));
      expression.put("concurrent", BsonBoolean.TRUE);

      var first =
          f.queries.search(
              request(local, expression.toJson(), 1).toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_ALL_PLANS_EXECUTION)
                  .build());
      var next =
          f.queries.getMore(
              GetMoreRequest.newBuilder()
                  .setCursorId(first.getCursorId())
                  .setBatchSize(100)
                  .build());

      assertPartitionPlans(first);
      assertPartitionPlans(next);
      assertThat(preparation(next)).isEqualTo(preparation(first));
      var source = preparation(first).getArray("sources").getFirst().asDocument();
      assertThat(source.getBoolean("selfJoin").getValue()).isTrue();
      assertThat(source.getString("foreignIndexId").getValue()).isEqualTo(local);
      assertThat(new RawBsonDocument(first.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(1);
      assertThat(new RawBsonDocument(next.getBatchBson().toByteArray()).getArray("results"))
          .hasSize(2);
      assertThat(next.getCursorId()).isEqualTo(0);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_explainedSinglePartitionJoin_returnsOnlyLocalPlannerQuery() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING.replace("numPartitions:2", "numPartitions:1"));
      f.upsert(foreign, "{_id:'f',key:'account',office:'Berlin'}");
      f.upsert(local, "{_id:'a',parent:'account'}");

      var response =
          f.queries.search(
              request(local, JOIN, 1).toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_QUERY_PLANNER)
                  .build());

      var explain = new RawBsonDocument(response.getExplainBson().toByteArray());
      assertThat(explain.getDocument("query").toJson()).contains("parent");
      assertThat(explain.getDocument("query").toJson()).doesNotContain("office");
      assertThat(explain.getDocument("query").containsKey("stats")).isFalse();
      var preparation = preparation(response);
      assertThat(preparation.containsKey("timing")).isFalse();
      assertThat(preparation.containsKey("foreignWorkUnits")).isFalse();
      assertThat(preparation.getArray("sources").getFirst().asDocument().containsKey("collection"))
          .isFalse();
      var execution = explain.getDocument("joins").getArray("execution").getFirst().asDocument();
      assertThat(execution.getString("strategy").getValue()).isEqualTo("termInSet");
      assertThat(execution.containsKey("segments")).isFalse();
      assertThat(new RawBsonDocument(response.getBatchBson().toByteArray()).getArray("results"))
          .isEmpty();
      assertThat(response.getCursorId()).isEqualTo(0);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  private static void assertPartitionPlans(QueryResponse response) {
    assertPartitionPlans(response, "termInSet", 1);
  }

  private static void assertPartitionPlans(QueryResponse response, String strategy, int keyCount) {
    var partitions =
        new RawBsonDocument(response.getExplainBson().toByteArray())
            .getArray("indexPartitionExplain");
    assertThat(partitions).hasSize(2);
    assertLocalPlan(partitions.get(0).asDocument(), strategy, keyCount);
    assertLocalPlan(partitions.get(1).asDocument(), strategy, keyCount);
  }

  private static BsonDocument preparation(QueryResponse response) throws Exception {
    var joins = new RawBsonDocument(response.getExplainBson().toByteArray()).getDocument("joins");
    try (var parser = BsonDocumentParser.fromRoot(joins).build()) {
      assertThat(JoinExplainInformation.fromBson(parser).toBson()).isEqualTo(joins);
    }
    return joins.getDocument("preparation");
  }

  private static void assertLocalPlan(BsonDocument explain, String strategy, int keyCount) {
    assertThat(explain.containsKey("collectors")).isTrue();
    assertLocalJoinPlan(explain, strategy, keyCount);
  }

  private static void assertLocalJoinPlan(BsonDocument explain, String strategy, int keyCount) {
    assertThat(explain.getDocument("luceneQuery").getString("analyzed").getValue())
        .contains("parent");
    assertThat(explain.getDocument("luceneQuery").getString("rewritten").getValue())
        .contains("parent");
    assertThat(explain.containsKey("query")).isTrue();
    assertThat(explain.get("query").toString()).contains("parent");
    assertThat(explain.get("query").toString()).doesNotContain("office");
    var joins = explain.getDocument("joins");
    assertThat(joins.containsKey("preparation")).isFalse();
    assertThat(joins.getArray("execution")).hasSize(1);
    var execution = joins.getArray("execution").getFirst().asDocument();
    assertThat(execution.getInt32("id").getValue()).isEqualTo(0);
    assertThat(execution.getString("strategy").getValue()).isEqualTo(strategy);
    assertThat(execution.getInt32("keyCount").getValue()).isEqualTo(keyCount);
  }

  @Test
  public void search_objectIdReferencesToMappedId_preservesBinaryKeysAcrossPartitionsAndPages()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign =
          f.create(
              "foreign",
              MAPPING.replace("key:{type:'token'}", "_id:[{type:'objectId'},{type:'number'}]"));
      String local =
          f.create(
              "local",
              MAPPING.replace(
                  "parent:{type:'token'}", "parent:[{type:'objectId'},{type:'number'}]"));
      f.upsert(foreign, "{_id:{$oid:'ff0000000000000000000001'},office:'Berlin'}");
      f.upsert(foreign, "{_id:{$oid:'ff0000000000000000000002'},office:'Paris'}");
      f.upsert(local, "{_id:'a',parent:{$oid:'ff0000000000000000000001'}}");
      f.upsert(
          local,
          "{_id:'b',parent:[{$oid:'ff0000000000000000000002'},{$oid:'ff0000000000000000000001'}]}");
      f.upsert(local, "{_id:'other',parent:{$oid:'ff0000000000000000000002'}}");
      f.upsert(local, "{_id:'string',parent:'ff0000000000000000000001'}");
      String query = JOIN.replace("foreignField:'key'", "foreignField:'_id'");

      var first = f.search(local, query, 1);
      var next =
          f.queries.getMore(GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).build());

      assertThat(ids(first)).hasSize(1);
      assertThat(ids(next)).hasSize(1);
      assertThat(ids(next)).containsNoneIn(ids(first));
      assertThat(ids(f.search(local, query, 100))).containsExactly("a", "b");
      assertThat(
              new RawBsonDocument(first.getMetadataBson().toByteArray())
                  .getDocument("count")
                  .getNumber("total")
                  .longValue())
          .isEqualTo(2);
    }
  }

  @Test
  public void search_integerReferencesToMappedId_preservesExactKeysAcrossPartitionsAndPages()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign =
          f.create(
              "foreign",
              INTEGER_MAPPING
                  .replace("numPartitions:2", "numPartitions:4")
                  .replace("key:" + INTEGER_FIELD, "_id:" + INTEGER_FIELD));
      String local = f.create("local", INTEGER_MAPPING);
      f.upsert(foreign, "{_id:{$numberLong:'-9223372036854775808'},office:'Berlin'}");
      f.upsert(foreign, "{_id:{$numberLong:'9223372036854775807'},office:'Berlin'}");
      f.upsert(foreign, "{_id:{$numberLong:'9007199254740993'},office:'Berlin'}");
      f.upsert(foreign, "{_id:42,office:'Berlin'}");
      f.upsert(foreign, "{_id:{$numberLong:'-7'},office:'Berlin'}");
      f.upsert(foreign, "{_id:0,office:'Paris'}");
      f.upsert(local, "{_id:'min',parent:{$numberLong:'-9223372036854775808'}}");
      f.upsert(local, "{_id:'max',parent:{$numberLong:'9223372036854775807'}}");
      f.upsert(local, "{_id:'large',parent:{$numberLong:'9007199254740993'}}");
      f.upsert(local, "{_id:'wide',parent:{$numberLong:'42'}}");
      f.upsert(local, "{_id:'narrow',parent:-7}");
      f.upsert(local, "{_id:'array',parent:[-7,{$numberLong:'9007199254740993'}]}");
      f.upsert(local, "{_id:'adjacent',parent:{$numberLong:'9007199254740992'}}");
      f.upsert(local, "{_id:'fraction',parent:-7.9}");
      f.upsert(local, "{_id:'double',parent:42.0}");
      f.upsert(local, "{_id:'string',parent:'42'}");
      f.upsert(local, "{_id:'unselected',parent:0}");
      f.upsert(local, "{_id:'missing'}");
      f.upsert(local, "{_id:'null',parent:null}");
      f.upsert(local, "{_id:'empty',parent:[]}");
      String query = JOIN.replace("foreignField:'key'", "foreignField:'_id'");

      var first = f.search(local, query, 1);
      var next =
          f.queries.getMore(
              GetMoreRequest.newBuilder()
                  .setCursorId(first.getCursorId())
                  .setBatchSize(100)
                  .build());

      assertThat(ids(first)).hasSize(1);
      assertThat(ids(next)).hasSize(5);
      assertThat(ids(next)).containsNoneIn(ids(first));
      assertThat(ids(f.search(local, query, 100)))
          .containsExactly("min", "max", "large", "wide", "narrow", "array");
      assertThat(
              new RawBsonDocument(first.getMetadataBson().toByteArray())
                  .getDocument("count")
                  .getNumber("total")
                  .longValue())
          .isEqualTo(6);
      assertThat(next.getCursorId()).isEqualTo(0);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_integerArraysAndExclusion_deduplicatesAndIgnoresNonIntegerValues()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", INTEGER_MAPPING);
      String local = f.create("local", INTEGER_MAPPING);
      f.upsert(foreign, "{_id:'a',key:[0,0,{$numberLong:'42'}],office:'Berlin'}");
      f.upsert(foreign, "{_id:'b',key:42,office:'Berlin'}");
      f.upsert(foreign, "{_id:'wrong',key:[7.9,'8',null],office:'Berlin'}");
      f.upsert(local, "{_id:'scalar',parent:{$numberLong:'0'}}");
      f.upsert(local, "{_id:'array',parent:[0,42,42]}");
      f.upsert(local, "{_id:'fraction',parent:7}");
      f.upsert(local, "{_id:'string',parent:8}");
      f.upsert(local, "{_id:'missing'}");

      var response = f.search(local, JOIN, 100);
      var exclusion = f.search(local, JOIN.replace("filter:", "mustNot:"), 100);
      var empty = f.search(local, JOIN.replace("value:'Berlin'", "value:'Paris'"), 100);

      assertThat(ids(response)).containsExactly("scalar", "array");
      assertThat(ids(exclusion)).containsExactly("fraction", "string", "missing");
      assertThat(ids(empty)).isEmpty();
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_integerConcurrentSelfJoin_preservesFacetsAndExplain() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String local = f.create("local", INTEGER_MAPPING);
      f.upsert(local, "{_id:'a',key:1,parent:2,office:'Berlin'}");
      f.upsert(local, "{_id:'b',key:2,parent:1,office:'Paris'}");
      f.upsert(local, "{_id:'c',key:3,parent:{$numberLong:'1'},office:'Berlin'}");
      var query =
          new BsonDocument(
                  "facet",
                  new BsonDocument(
                          "operator",
                          new BsonDocument(
                              "compound",
                              BsonDocument.parse(JOIN.replace("from:'foreign',", ""))
                                  .getDocument("compound")))
                      .append(
                          "facets", BsonDocument.parse("{offices:{type:'string',path:'office'}}")))
              .append("returnStoredSource", BsonBoolean.TRUE)
              .append("concurrent", BsonBoolean.TRUE);

      var response =
          f.queries.search(
              request(local, query.toJson(), 100).toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());

      assertThat(ids(response)).containsExactly("b", "c");
      assertThat(
              new RawBsonDocument(response.getMetadataBson().toByteArray())
                  .getDocument("facet").getDocument("offices").getArray("buckets").stream()
                      .map(value -> value.asDocument().getString("_id").getValue())
                      .toList())
          .containsExactly("Berlin", "Paris");
      assertPartitionPlans(response, "numeric", 2);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_integerUnsafeMappings_rejectsBothLocalAndForeignPaths() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      f.create("foreign", INTEGER_MAPPING);
      String defaultDouble =
          f.create("default-double", INTEGER_MAPPING.replace(INTEGER_FIELD, "{type:'number'}"));
      String acceptsDoubles =
          f.create(
              "accepts-doubles",
              INTEGER_MAPPING.replace("indexDoubles:false", "indexDoubles:true"));
      String ignoresIntegers =
          f.create(
              "ignores-integers",
              INTEGER_MAPPING.replace(
                  "indexDoubles:false", "indexDoubles:true,indexIntegers:false"));
      String local = f.create("local", INTEGER_MAPPING);

      var defaultError =
          assertThrows(StatusRuntimeException.class, () -> f.search(defaultDouble, JOIN, 100));
      var doubleError =
          assertThrows(StatusRuntimeException.class, () -> f.search(acceptsDoubles, JOIN, 100));
      var integerError =
          assertThrows(StatusRuntimeException.class, () -> f.search(ignoresIntegers, JOIN, 100));
      var foreignError =
          assertThrows(
              StatusRuntimeException.class,
              () -> f.search(local, JOIN.replace("from:'foreign'", "from:'default-double'"), 100));

      assertThat(defaultError.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(defaultError.getStatus().getDescription()).contains("representation:'int64'");
      assertThat(doubleError.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(doubleError.getStatus().getDescription()).contains("indexDoubles:false");
      assertThat(integerError.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(integerError.getStatus().getDescription()).contains("indexIntegers:true");
      assertThat(foreignError.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(foreignError.getStatus().getDescription()).contains("'key'");
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_integerAndTokenMappings_rejectsCrossTypeJoin() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      f.create("foreign", INTEGER_MAPPING);
      String local = f.create("local", MAPPING);

      var error = assertThrows(StatusRuntimeException.class, () -> f.search(local, JOIN, 100));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(error.getStatus().getDescription()).contains("same type");
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_integerMembershipMemoryLimit_releasesReservationsForFreshQueries()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", INTEGER_MAPPING);
      String local = f.create("local", INTEGER_MAPPING);
      var keys =
          java.util.stream.IntStream.range(0, 30_000)
              .mapToObj(i -> (org.bson.BsonValue) new org.bson.BsonInt32(i))
              .toList();
      f.upsert(
          foreign,
          BsonDocument.parse("{_id:'p',office:'Berlin'}")
              .append("key", new org.bson.BsonArray(keys))
              .toJson());
      f.upsert(local, "{_id:'match',parent:0}");

      var error = assertThrows(StatusRuntimeException.class, () -> f.search(local, JOIN, 100));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
      f.upsert(foreign, "{_id:'p',key:0,office:'Berlin'}");
      assertThat(ids(f.search(local, JOIN, 100))).containsExactly("match");
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void collectKeys_integerArrayCancellation_releasesSnapshotsForFreshQueries()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", INTEGER_MAPPING);
      String local = f.create("local", INTEGER_MAPPING);
      f.upsert(foreign, "{_id:'p',key:[0,1,2,3,4,5,6,7,8,9],office:'Berlin'}");
      f.upsert(local, "{_id:'match',parent:0}");
      var generation =
          f.runtime
              .liveIndexes()
              .getIndexById(new org.bson.types.ObjectId(foreign))
              .orElseThrow()
              .getGenerationId();
      var parsed = (OperatorQuery) SearchQuery.fromBson(BsonDocument.parse(JOIN));
      var join =
          (JoinOperator) ((CompoundOperator) parsed.operator()).filter().operators().getFirst();
      var work = new AtomicInteger();

      try (var lease = f.runtime.reads().acquire(generation);
          var snapshot = f.runtime.acquireQuerySnapshot(lease, false)) {
        assertThrows(
            CancellationException.class,
            () ->
                snapshot.collectKeys(
                    join.operator(),
                    join.foreignField(),
                    ignored -> {},
                    () -> cancelAfter(work, 4)));
      }

      assertThat(ids(f.search(local, JOIN, 100))).containsExactly("match");
    }
  }

  @Test
  public void search_tokenObjectIdAndNumberMappings_matchesAllLosslessTypes() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String mapping =
          MAPPING
              .replace(
                  "key:{type:'token'}",
                  "key:[{type:'token',normalizer:'lowercase'},{type:'objectId'},{type:'number'}]")
              .replace(
                  "parent:{type:'token'}",
                  "parent:[{type:'token',normalizer:'lowercase'},"
                      + "{type:'objectId'},{type:'number'}]");
      String foreign = f.create("foreign", mapping);
      String local = f.create("local", mapping);
      f.upsert(foreign, "{_id:'p',key:'ABC',office:'Berlin'}");
      f.upsert(foreign, "{_id:'oid-parent',key:{$oid:'ff0000000000000000000001'},office:'Berlin'}");
      f.upsert(local, "{_id:'string-match',parent:'abc'}");
      f.upsert(local, "{_id:'oid-child',parent:{$oid:'ff0000000000000000000001'}}");

      var response = f.search(local, JOIN, 100);

      assertThat(ids(response)).containsExactly("string-match", "oid-child");
    }
  }

  @Test
  public void search_dynamicTokenAndObjectIdTypeSet_preservesTokenJoins() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String mapping =
          """
          {numPartitions:2,storedSource:true,
           typeSets:[{name:'joinKeys',types:[
             {type:'token',normalizer:'lowercase'},{type:'objectId'}]}],
           mappings:{dynamic:{typeSet:'joinKeys'}}}
          """;
      String foreign = f.create("foreign", mapping);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p',key:'ABC',office:'BERLIN'}");
      f.upsert(local, "{_id:'match',parent:'abc'}");
      // The same normalization requirement still applies to a dynamically resolved token.
      String normalizedLocal =
          f.create(
              "normalized-local",
              MAPPING.replace(
                  "parent:{type:'token'}", "parent:{type:'token',normalizer:'lowercase'}"));
      f.upsert(normalizedLocal, "{_id:'match',parent:'abc'}");

      var response = f.search(normalizedLocal, JOIN, 100);
      var mismatch = assertThrows(StatusRuntimeException.class, () -> f.search(local, JOIN, 100));

      assertThat(ids(response)).containsExactly("match");
      assertThat(mismatch.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }
  }

  @Test
  public void search_objectIdAndTokenMappings_rejectsCrossTypeJoin() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      f.create("foreign", MAPPING.replace("key:{type:'token'}", "key:{type:'objectId'}"));
      String local = f.create("local", MAPPING);

      var error = assertThrows(StatusRuntimeException.class, () -> f.search(local, JOIN, 100));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(error.getStatus().getDescription()).contains("same type");
    }
  }

  @Test
  public void materializeJoin_countRequirements_preservesRequestHints() throws Exception {
    var totalQuery = SearchQuery.fromBson(BsonDocument.parse(JOIN));
    var boundedQuery =
        SearchQuery.fromBson(
            BsonDocument.parse(JOIN.replace("type:'total'", "type:'lowerBound',threshold:17")));
    var facetQuery =
        SearchQuery.fromBson(
            new BsonDocument(
                "facet",
                new BsonDocument(
                        "operator",
                        new BsonDocument(
                            "compound", BsonDocument.parse(JOIN).getDocument("compound")))
                    .append(
                        "facets", BsonDocument.parse("{offices:{type:'string',path:'office'}}"))));
    var join =
        (JoinOperator)
            ((CompoundOperator) StandaloneJoinPlanner.operator(totalQuery))
                .filter()
                .operators()
                .getFirst();
    var field = new JoinField(JoinField.Type.TOKEN, StockNormalizerName.NONE);
    var keys = new JoinKeySet.Builder(ignored -> {}).build();

    var total = StandaloneJoinPlanner.materializeJoin(join, field, keys, totalQuery);
    var bounded = StandaloneJoinPlanner.materializeJoin(join, field, keys, boundedQuery);
    var facet = StandaloneJoinPlanner.materializeJoin(join, field, keys, facetQuery);

    assertThat(total.exhaustive()).isTrue();
    assertThat(bounded.exhaustive()).isFalse();
    assertThat(bounded.countThreshold()).isEqualTo(17);
    assertThat(facet.exhaustive()).isTrue();
  }

  @Test
  public void search_guideExampleWithDifferentMappings_returnsOnlyMatchingArchitect()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String accounts =
          f.create(
              "accounts_search",
              """
              {numPartitions:2,mappings:{dynamic:false,fields:{
                accountKey:{type:'token'},region:{type:'token'},industry:{type:'token'}
              }}}
              """);
      String contacts =
          f.create(
              "contacts_search",
              """
              {numPartitions:2,storedSource:true,mappings:{dynamic:false,fields:{
                parent:{type:'token'},title:{type:'string'}
              }}}
              """);
      f.upsert(accounts, "{_id:'a1',accountKey:'a1',region:'bay area',industry:'aerospace'}");
      f.upsert(accounts, "{_id:'a2',accountKey:'a2',region:'new york',industry:'aerospace'}");
      f.upsert(accounts, "{_id:'a3',accountKey:'a3',region:'bay area',industry:'finance'}");
      f.upsert(contacts, "{_id:'c1',parent:'a1',title:'Principal Software Architect'}");
      f.upsert(contacts, "{_id:'c2',parent:'a2',title:'Solutions Architect'}");
      f.upsert(contacts, "{_id:'c3',parent:'a1',title:'Aerospace Engineer'}");
      f.upsert(contacts, "{_id:'c4',parent:'a3',title:'Cloud Architect'}");

      var response =
          f.search(
              contacts,
              """
              {compound:{must:[{text:{path:'title',query:'architect'}}],filter:[{
                join:{from:'accounts_search',localField:'parent',foreignField:'accountKey',
                  operator:{compound:{filter:[
                    {equals:{path:'region',value:'bay area'}},
                    {equals:{path:'industry',value:'aerospace'}}
                  ]}}}
              }]},returnStoredSource:true}
              """,
              100);

      assertThat(ids(response)).containsExactly("c1");
    }
  }

  @Test
  public void search_crossIndexArraysAndDuplicates_filtersBeforeCountWithoutDuplicatingHits()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p1',key:['a','b'],office:'Berlin'}");
      f.upsert(foreign, "{_id:'p2',key:'a',office:'Berlin'}");
      f.upsert(foreign, "{_id:'p3',key:'c',office:'Paris'}");
      f.upsert(local, "{_id:'a',parent:'a',title:'architect'}");
      f.upsert(local, "{_id:'b',parent:['b','a'],title:'architect'}");
      f.upsert(local, "{_id:'c',parent:'c',title:'architect'}");
      f.upsert(local, "{_id:'missing',title:'architect'}");
      f.upsert(local, "{_id:'null',parent:null,title:'architect'}");

      var response = f.search(local, JOIN, 100);

      assertThat(ids(response)).containsExactly("a", "b");
      assertThat(
              new RawBsonDocument(response.getMetadataBson().toByteArray())
                  .getDocument("count")
                  .getNumber("total")
                  .longValue())
          .isEqualTo(2);
      assertThat(response.getCursorId()).isEqualTo(0);
    }
  }

  @Test
  public void search_selfJoinOmittedOrExplicitFrom_reusesPartitionWideMembership()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String index = f.create("employees", MAPPING);
      f.upsert(index, "{_id:'manager',key:'m',office:'Berlin'}");
      f.upsert(index, "{_id:'employee',parent:'m'}");
      f.upsert(index, "{_id:'self',key:'s',parent:'s',office:'Berlin'}");
      f.upsert(index, "{_id:'orphan',parent:'absent'}");
      String query = JOIN.replace("from:'foreign',", "");

      var implicit = f.search(index, query, 100);
      var explicit = f.search(index, JOIN.replace("'foreign'", "'employees'"), 100);

      assertThat(ids(implicit)).containsExactly("employee", "self");
      assertThat(ids(explicit)).containsExactlyElementsIn(ids(implicit));
    }
  }

  @Test
  public void search_exclusion_preservesMissingAndOrphanKeys() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      f.upsert(local, "{_id:'matched',parent:'a'}");
      f.upsert(local, "{_id:'orphan',parent:'absent'}");
      f.upsert(local, "{_id:'missing'}");

      var response = f.search(local, JOIN.replace("filter:", "mustNot:"), 100);

      assertThat(ids(response)).containsExactly("orphan", "missing");
    }
  }

  @Test
  public void getMore_foreignDocumentChanges_keepsInitialMembership() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      f.upsert(local, "{_id:'first',parent:'a'}");
      f.upsert(local, "{_id:'second',parent:'a'}");
      var first = f.search(local, JOIN, 1);
      assertThat(first.getCursorId()).isNotEqualTo(0);

      f.docs.delete(
          DeleteRequest.newBuilder()
              .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
              .setIndexId(foreign)
              .addIdsBson(bson("{_id:'p'}"))
              .build());
      var second =
          f.queries.getMore(GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).build());
      var fresh = f.search(local, JOIN, 100);

      assertThat(ids(second)).hasSize(1);
      assertThat(ids(second)).containsNoneIn(ids(first));
      assertThat(ids(fresh)).isEmpty();
    }
  }

  @Test
  public void getMore_droppedForeignIndexAndReusedName_invalidatesCursor() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      f.upsert(local, "{_id:'first',parent:'a'}");
      f.upsert(local, "{_id:'second',parent:'a'}");
      var first = f.search(local, JOIN, 1);

      f.catalog.dropIndex(
          DropIndexRequest.newBuilder().setIndexId(foreign).setExpectedRevision(1).build());
      String replacement = f.create("foreign", MAPPING);
      var failure =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.getMore(
                      GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).build()));

      assertThat(replacement).isNotEqualTo(foreign);
      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
      assertThat(ids(f.search(local, JOIN, 100))).isEmpty();
    }
  }

  @Test
  public void getMore_foreignMappingUpdate_invalidatesCursorAndUsesReingestedContent()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      f.upsert(local, "{_id:'first',parent:'a'}");
      f.upsert(local, "{_id:'second',parent:'a'}");
      var first = f.search(local, JOIN, 1);

      f.catalog.updateIndex(
          UpdateIndexRequest.newBuilder()
              .setUpdateMode(
                  com.xgen.mongot.standalone.grpc.IndexUpdateMode.INDEX_UPDATE_MODE_REBUILD_EMPTY)
              .setIndexId(foreign)
              .setExpectedRevision(1)
              .setDefinitionBson(bson(MAPPING))
              .build());
      f.ready(foreign, 2);
      var failure =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.queries.getMore(
                      GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).build()));

      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
      assertThat(ids(f.search(local, JOIN, 100))).isEmpty();
      f.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      assertThat(ids(f.search(local, JOIN, 100))).containsExactly("first", "second");
    }
  }

  @Test
  public void search_foreignNameAndMappingValidation_returnsSpecificErrors() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);

      var missing =
          assertThrows(
              StatusRuntimeException.class,
              () -> f.search(local, JOIN.replace("'foreign'", "'Foreign'"), 100));
      var mapping =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.search(local, JOIN.replace("foreignField:'key'", "foreignField:'title'"), 100));
      var scoring =
          assertThrows(
              StatusRuntimeException.class,
              () -> f.search(local, JOIN.replace("filter:", "must:"), 100));

      assertThat(missing.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
      assertThat(mapping.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(scoring.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }
  }

  @Test
  public void search_localTextAndSort_preservesScoresAndFiltersBeforeTopK() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      f.upsert(local, "{_id:'a',key:'a',parent:'a',title:'architect'}");
      f.upsert(local, "{_id:'b',key:'b',parent:'a',title:'principal software architect'}");
      f.upsert(local, "{_id:'excluded',key:'z',parent:'missing',title:'architect'}");
      var baseline =
          f.search(local, "{text:{path:'title',query:'architect'},returnStoredSource:true}", 100);
      var query = BsonDocument.parse(JOIN);
      query
          .getDocument("compound")
          .put("must", org.bson.BsonArray.parse("[{text:{path:'title',query:'architect'}}]"));
      query.put("sort", BsonDocument.parse("{key:1}"));
      query.put("highlight", BsonDocument.parse("{path:'title'}"));
      query.put("scoreDetails", org.bson.BsonBoolean.TRUE);

      var filtered = f.search(local, query.toJson(), 1);
      var next =
          f.queries.getMore(
              GetMoreRequest.newBuilder().setCursorId(filtered.getCursorId()).build());

      assertThat(ids(filtered)).containsExactly("a");
      assertThat(ids(next)).containsExactly("b");
      assertThat(score(filtered, "a")).isEqualTo(score(baseline, "a"));
      assertThat(score(next, "b")).isEqualTo(score(baseline, "b"));
      assertThat(
              new RawBsonDocument(filtered.getBatchBson().toByteArray())
                  .getArray("results")
                  .getFirst()
                  .asDocument()
                  .containsKey("$searchHighlights"))
          .isTrue();
    }
  }

  @Test
  public void search_facetCollector_appliesJoinToHitsAndBucketsAcrossPartitions() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      f.upsert(local, "{_id:'first',parent:'a',office:'Berlin'}");
      f.upsert(local, "{_id:'second',parent:'a',office:'Paris'}");
      f.upsert(local, "{_id:'excluded',parent:'missing',office:'Rome'}");
      var query =
          new BsonDocument(
                  "facet",
                  new BsonDocument(
                          "operator",
                          new BsonDocument(
                              "compound", BsonDocument.parse(JOIN).getDocument("compound")))
                      .append(
                          "facets", BsonDocument.parse("{offices:{type:'string',path:'office'}}")))
              .append("returnStoredSource", org.bson.BsonBoolean.TRUE);

      var response = f.search(local, query.toJson(), 100);
      var metadata = new RawBsonDocument(response.getMetadataBson().toByteArray());
      query
          .getDocument("facet")
          .getDocument("operator")
          .getDocument("compound")
          .getArray("filter")
          .add(
              BsonDocument.parse(
                  "{equals:{path:'office',value:'Berlin',doesNotAffect:'offices'}}"));
      var drillSideways = f.search(local, query.toJson(), 100);
      var drillMetadata = new RawBsonDocument(drillSideways.getMetadataBson().toByteArray());

      assertThat(ids(response)).containsExactly("first", "second");
      assertThat(
              metadata.getDocument("facet").getDocument("offices").getArray("buckets").stream()
                  .map(value -> value.asDocument().getString("_id").getValue())
                  .toList())
          .containsExactly("Berlin", "Paris");
      assertThat(ids(drillSideways)).containsExactly("first");
      // Standalone defaults disable drill-sideways; preserve the existing facet behavior.
      assertThat(
              drillMetadata.getDocument("facet").getDocument("offices").getArray("buckets").stream()
                  .map(value -> value.asDocument().getString("_id").getValue())
                  .toList())
          .containsExactly("Berlin");
    }
  }

  @Test
  public void search_multipleJoinsAndBooleanFilterComposition_intersectsAndUnionsMembership()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String another = f.create("another", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'berlin',key:['a','b'],office:'Berlin'}");
      f.upsert(another, "{_id:'paris',key:'c',office:'Paris'}");
      f.upsert(local, "{_id:'both',parent:'a',key:'c'}");
      f.upsert(local, "{_id:'berlin-only',parent:'b',key:'a'}");
      f.upsert(local, "{_id:'paris-only',parent:'missing',key:'c'}");
      var intersection = BsonDocument.parse(JOIN);
      intersection
          .getDocument("compound")
          .getArray("filter")
          .add(
              BsonDocument.parse(
                  """
                  {join:{from:'another',localField:'key',foreignField:'key',
                            operator:{equals:{path:'office',value:'Paris'}}}}
                  """));
      var union = BsonDocument.parse(JOIN);
      var firstJoin =
          BsonDocument.parse(JOIN).getDocument("compound").getArray("filter").getFirst();
      union
          .getDocument("compound")
          .put(
              "filter",
              org.bson.BsonArray.parse(
                  """
                  [{compound:{should:[{equals:{path:'key',value:'c'}}],minimumShouldMatch:1}}]
                  """));
      union
          .getDocument("compound")
          .getArray("filter")
          .getFirst()
          .asDocument()
          .getDocument("compound")
          .getArray("should")
          .add(firstJoin);

      var both = f.search(local, intersection.toJson(), 100);
      var either = f.search(local, union.toJson(), 100);

      assertThat(ids(both)).containsExactly("both");
      assertThat(ids(either)).containsExactly("both", "berlin-only", "paris-only");
    }
  }

  @Test
  public void search_reverseJoin_returnsEachForeignDocumentOnce() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'a',key:'a'}");
      f.upsert(foreign, "{_id:'b',key:'b'}");
      f.upsert(local, "{_id:'one',parent:'a',title:'architect'}");
      f.upsert(local, "{_id:'two',parent:'a',title:'architect'}");
      f.upsert(local, "{_id:'three',parent:'b',title:'engineer'}");

      var response =
          f.search(
              foreign,
              """
              {compound:{filter:[{join:{from:'local',localField:'key',foreignField:'parent',
                operator:{text:{path:'title',query:'architect'}}}}]},returnStoredSource:true}
              """,
              100);

      assertThat(ids(response)).containsExactly("a");
    }
  }

  @Test
  public void search_emptyAndWrongTypeKeys_usesIndexedTokenEquality() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p',key:'',office:'Berlin'}");
      f.upsert(local, "{_id:'empty',parent:''}");
      f.upsert(local, "{_id:'array',parent:[]}");
      f.upsert(local, "{_id:'number',parent:0}");
      f.upsert(local, "{_id:'null',parent:null}");

      var response = f.search(local, JOIN, 100);

      assertThat(ids(response)).containsExactly("empty");
    }
  }

  @Test
  public void search_normalizers_comparesEffectiveNormalizerAndRejectsMismatch() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local =
          f.create(
              "local",
              MAPPING.replace("parent:{type:'token'}", "parent:{type:'token',normalizer:'none'}"));
      String mismatch =
          f.create(
              "mismatch",
              MAPPING.replace(
                  "parent:{type:'token'}", "parent:{type:'token',normalizer:'lowercase'}"));
      f.upsert(foreign, "{_id:'p',key:'ABC',office:'Berlin'}");
      f.upsert(local, "{_id:'matched',parent:'ABC'}");

      var response = f.search(local, JOIN, 100);
      var error = assertThrows(StatusRuntimeException.class, () -> f.search(mismatch, JOIN, 100));

      assertThat(ids(response)).containsExactly("matched");
      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }
  }

  @Test
  public void search_matchingLowercaseNormalizers_comparesNormalizedUnicodeTokens()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign =
          f.create(
              "foreign",
              MAPPING.replace("key:{type:'token'}", "key:{type:'token',normalizer:'lowercase'}"));
      String local =
          f.create(
              "local",
              MAPPING.replace(
                  "parent:{type:'token'}", "parent:{type:'token',normalizer:'lowercase'}"));
      f.upsert(foreign, "{_id:'p',key:'ÄBC',office:'Berlin'}");
      f.upsert(local, "{_id:'matched',parent:'äbc'}");

      var response = f.search(local, JOIN, 100);

      assertThat(ids(response)).containsExactly("matched");
    }
  }

  @Test
  public void search_nestedJoinAndJoinCountLimit_rejectsWithoutPartialResults() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      var root = BsonDocument.parse(JOIN);
      var join = root.getDocument("compound").getArray("filter").get(0).asDocument();
      var nested = BsonDocument.parse(JOIN);
      nested
          .getDocument("compound")
          .getArray("filter")
          .get(0)
          .asDocument()
          .getDocument("join")
          .put("operator", new BsonDocument("compound", root.getDocument("compound")));
      root.getDocument("compound")
          .put(
              "filter",
              new org.bson.BsonArray(
                  java.util.Collections.nCopies(StandaloneJoinPlanner.MAX_JOINS + 1, join)));

      var nestedError =
          assertThrows(StatusRuntimeException.class, () -> f.search(local, nested.toJson(), 100));
      var limitError =
          assertThrows(StatusRuntimeException.class, () -> f.search(local, root.toJson(), 100));

      assertThat(nestedError.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(limitError.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    }
  }

  @Test
  public void search_denseJoin_choosesOrdinalsForTotalAndHashForFirstPage() throws Exception {
    try (var f = new Fixture(data(), List.of(), new MemoryLimits(1L << 28, 1L << 29))) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, keyDocument(52_000, 1));
      f.upsert(local, "{_id:'first',parent:'x0'}");
      f.upsert(local, "{_id:'second',parent:'x51999'}");
      f.upsert(local, "{_id:'miss',parent:'absent'}");
      String total = JOIN.replace("equals:{path:'office',value:'Berlin'}", "exists:{path:'key'}");
      String firstPage = total.replace("type:'total'", "type:'lowerBound'");
      var totalRequest =
          request(local, total, 100).toBuilder()
              .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
              .build();

      var ordinal = f.queries.search(totalRequest);
      var hash =
          f.queries.search(
              totalRequest.toBuilder()
                  .setQueryBson(request(local, firstPage, 100).getQueryBson())
                  .build());

      assertThat(ids(ordinal)).containsExactly("first", "second");
      assertThat(
              new RawBsonDocument(ordinal.getMetadataBson().toByteArray())
                  .getDocument("count")
                  .getNumber("total")
                  .longValue())
          .isEqualTo(2);
      assertThat(ids(hash)).containsExactlyElementsIn(ids(ordinal));
      assertJoinStrategy(ordinal, "ordinalBitmap");
      assertJoinStrategy(hash, "adaptive");
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_emptyLocalDictionaryAfterHashLimit_matchesNoLocalKeys() throws Exception {
    try (var f = new Fixture(data(), List.of(), new MemoryLimits(200_000, 2_000_000))) {
      String mapping =
          "{storedSource:true,mappings:{dynamic:false,fields:{key:{type:'objectId'}}}}";
      String foreign = f.create("foreign", mapping);
      String local = f.create("local", mapping);
      var keys = new org.bson.BsonArray();
      java.util.stream.IntStream.range(0, 128).forEach(i -> keys.add(new org.bson.BsonObjectId()));
      f.upsert(
          foreign,
          new BsonDocument("_id", new org.bson.BsonString("f")).append("key", keys).toJson());
      f.upsert(local, "{_id:'missing'}");
      String query =
          """
          {compound:{filter:[{join:{from:'foreign',localField:'key',foreignField:'key',
            operator:{exists:{path:'key'}}}}]},returnStoredSource:true,count:{type:'total'}}
          """;

      var included =
          f.queries.search(
              request(local, query, 100).toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());
      var excluded = f.search(local, query.replace("filter:", "mustNot:"), 100);

      assertThat(ids(included)).isEmpty();
      assertThat(ids(excluded)).containsExactly("missing");
      var explain =
          new RawBsonDocument(included.getExplainBson().toByteArray()).getDocument("joins");
      var source = preparation(included).getArray("sources").getFirst().asDocument();
      assertThat(source.getInt64("reservedBytes").getValue()).isEqualTo(0);
      var execution = explain.getArray("execution").getFirst().asDocument();
      assertThat(execution.getString("selectionReason").getValue()).isEqualTo("noLocalKeys");
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_segmentedFirstPageAboveHashCap_retriesOrdinalsAndPreservesCursor()
      throws Exception {
    try (var f = new Fixture(data(), List.of(), new MemoryLimits(1L << 28, 1L << 29))) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, keyDocument(75_000, 1));
      var secondForeign = BsonDocument.parse(keyDocument(75_000, 2));
      secondForeign.put("_id", new org.bson.BsonString("q"));
      f.upsert(foreign, secondForeign.toJson());
      f.upsert(local, "{_id:'first',parent:'x0'}");
      f.upsert(local, "{_id:'second',parent:'xx74999'}");
      f.upsert(local, "{_id:'miss',parent:'absent'}");
      String firstPage =
          JOIN.replace("equals:{path:'office',value:'Berlin'}", "exists:{path:'key'}")
              .replace("type:'total'", "type:'lowerBound'");

      var first =
          f.queries.search(
              request(local, firstPage, 1).toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());

      assertJoinStrategy(first, "ordinalBitmap");
      var explain = new RawBsonDocument(first.getExplainBson().toByteArray());
      assertThat(
              explain
                  .getDocument("joins")
                  .getDocument("preparation")
                  .getInt32("keyCount")
                  .getValue())
          .isEqualTo(150_000);
      assertThat(
              explain
                  .getArray("indexPartitionExplain")
                  .getFirst()
                  .asDocument()
                  .getDocument("joins")
                  .getArray("execution")
                  .getFirst()
                  .asDocument()
                  .getString("selectionReason")
                  .getValue())
          .isEqualTo("hashAdmissionLimitExceeded");
      assertThat(first.getCursorId()).isNotEqualTo(0L);
      assertThat(reservedBytes(f)).isGreaterThan(0.0);
      assertThat(reservedBytes(f)).isLessThan(1024 * 1024.0);
      var next =
          f.queries.getMore(GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).build());
      var allIds = new java.util.ArrayList<>(ids(first));
      allIds.addAll(ids(next));
      assertThat(allIds).containsExactly("first", "second");
      assertThat(next.getCursorId()).isEqualTo(0L);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_denseJoinAboveHashCap_preservesSortedCursorAndReleasesMemory()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, keyDocument(100_001, 1));
      f.upsert(local, "{_id:'first',key:'a',parent:'x100000'}");
      f.upsert(local, "{_id:'second',key:'b',parent:'x0'}");
      f.upsert(local, "{_id:'miss',key:'0',parent:'absent'}");
      String sorted =
          JOIN.replace("equals:{path:'office',value:'Berlin'}", "exists:{path:'key'}")
              .replace("count:{type:'total'}", "count:{type:'lowerBound'},sort:{key:1}");

      var first = f.search(local, sorted, 1);

      assertThat(ids(first)).containsExactly("first");
      assertThat(first.getCursorId()).isNotEqualTo(0L);
      assertThat(reservedBytes(f)).isGreaterThan(0.0);
      assertThat(reservedBytes(f)).isLessThan(1024 * 1024.0);
      var next =
          f.queries.getMore(GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).build());
      assertThat(ids(next)).containsExactly("second");
      assertThat(next.getCursorId()).isEqualTo(0L);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  private static void assertJoinStrategy(QueryResponse response, String strategy) {
    var explain = new RawBsonDocument(response.getExplainBson().toByteArray());
    for (var partition : explain.getArray("indexPartitionExplain")) {
      assertThat(
              partition
                  .asDocument()
                  .getDocument("joins")
                  .getArray("execution")
                  .getFirst()
                  .asDocument()
                  .getString("strategy")
                  .getValue())
          .isEqualTo(strategy);
    }
  }

  @Test
  public void search_largerHeapBudget_acceptsMembershipAboveSmallerHeapLimit() throws Exception {
    try (var f = new Fixture(data(), List.of(), MemoryLimits.fromHeapBytes(4L << 30))) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, keyDocument(120, 7000));
      f.upsert(local, "{_id:'match',parent:'" + "x".repeat(7000) + "0'}");

      var response =
          f.queries.search(
              request(local, JOIN, 100).toBuilder()
                  .setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)
                  .build());

      assertThat(ids(response)).containsExactly("match");
      var preparation =
          new RawBsonDocument(response.getExplainBson().toByteArray())
              .getDocument("joins")
              .getDocument("preparation");
      var limits = preparation.getDocument("limits");
      assertThat(limits.getInt64("memoryBytes").getValue()).isEqualTo(67108864L);
      assertThat(limits.getInt64("concurrentMemoryBytes").getValue()).isEqualTo(536870912L);
      assertThat(preparation.getInt64("reservedBytes").getValue()).isGreaterThan(33554432L);
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_configuredConcurrentBudget_rejectsUntilRetainedCursorCloses()
      throws Exception {
    try (var f = new Fixture(data(), List.of(), new MemoryLimits(300000L, 300000L))) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      f.upsert(local, "{_id:'first',parent:'a'}");
      f.upsert(local, "{_id:'second',parent:'a'}");
      var first = f.search(local, JOIN, 1);
      double reserved = reservedBytes(f);

      var error = assertThrows(StatusRuntimeException.class, () -> f.search(local, JOIN, 100));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      assertThat(error.getStatus().getDescription())
          .contains("Concurrent join-memory limit exceeded (limit: 300000 bytes, reserved: ");
      assertThat(error.getStatus().getDescription()).contains("bytes, requested: ");
      assertThat(reservedBytes(f)).isEqualTo(reserved);

      f.queries.killCursor(KillCursorRequest.newBuilder().setCursorId(first.getCursorId()).build());

      assertThat(ids(f.search(local, JOIN, 100))).containsExactly("first", "second");
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  @Test
  public void search_repeatedMemoryBudgetFailures_releasesReservationsForLaterQueries()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, keyDocument(120, 7000));

      assertBudgetFailures(f, local);
      f.upsert(foreign, keyDocument(500, 64));
      f.upsert(local, "{_id:'match',parent:'" + "x".repeat(64) + "0'}");

      assertThat(ids(f.search(local, JOIN, 100))).containsExactly("match");
    }
  }

  private static String keyDocument(int count, int length) {
    var values =
        java.util.stream.IntStream.range(0, count)
            .mapToObj(i -> (org.bson.BsonValue) new org.bson.BsonString("x".repeat(length) + i))
            .toList();
    return new BsonDocument("_id", new org.bson.BsonString("p"))
        .append("office", new org.bson.BsonString("Berlin"))
        .append("key", new org.bson.BsonArray(values))
        .toJson();
  }

  private static void assertBudgetFailures(Fixture f, String local) {
    for (int attempt = 0; attempt < 10; attempt++) {
      var error = assertThrows(StatusRuntimeException.class, () -> f.search(local, JOIN, 100));
      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
      assertThat(error.getStatus().getDescription())
          .contains("Join query-memory limit exceeded (limit: 33554432 bytes, reserved: ");
      assertThat(error.getStatus().getDescription()).contains("bytes, requested: ");
    }
  }

  private static double score(QueryResponse response, String id) {
    return new RawBsonDocument(response.getBatchBson().toByteArray())
        .getArray("results").stream()
            .map(org.bson.BsonValue::asDocument)
            .filter(hit -> hit.getDocument("storedSource").getString("_id").getValue().equals(id))
            .findFirst()
            .orElseThrow()
            .getNumber("$searchScore")
            .doubleValue();
  }

  @Test
  public void killCursor_joinMembership_releasesRetainedMemory() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      f.upsert(local, "{_id:'first',parent:'a'}");
      f.upsert(local, "{_id:'second',parent:'a'}");
      var first = f.search(local, JOIN, 1);
      assertThat(reservedBytes(f)).isGreaterThan(0.0);

      f.queries.killCursor(KillCursorRequest.newBuilder().setCursorId(first.getCursorId()).build());

      assertThat(reservedBytes(f)).isEqualTo(0.0);
      assertThat(ids(f.search(local, JOIN, 100))).containsExactly("first", "second");
      assertThat(reservedBytes(f)).isEqualTo(0.0);
    }
  }

  private static double reservedBytes(Fixture f) throws Exception {
    return f.runtime.serviceStatus().getMetricsList().stream()
        .filter(metric -> metric.getName().equals("standalone.joins.reservedBytes"))
        .flatMap(metric -> metric.getMeasurementsMap().values().stream())
        .mapToDouble(Double::doubleValue)
        .sum();
  }

  @Test
  public void search_queryOnlyIndexGrants_authorizesForeignAndRechecksContinuation()
      throws Exception {
    String local;
    String foreign;
    try (var seed = new Fixture(data(), List.of())) {
      foreign = seed.create("foreign", MAPPING);
      local = seed.create("local", MAPPING);
      seed.upsert(foreign, "{_id:'p',key:'a',office:'Berlin'}");
      seed.upsert(local, "{_id:'first',parent:'a'}");
      seed.upsert(local, "{_id:'second',parent:'a'}");
    }
    var onlyLocal =
        new StandaloneConfig.Principal(
            "reader", digest("local"), Set.of(StandaloneConfig.Role.QUERY), Set.of(local));
    var both =
        new StandaloneConfig.Principal(
            "reader", digest("both"), Set.of(StandaloneConfig.Role.QUERY), Set.of(local, foreign));
    try (var f = new Fixture(data(), List.of(onlyLocal, both))) {
      f.readyInternally(local, 1);
      f.readyInternally(foreign, 1);
      var restricted = f.caller("local");
      var authorized = f.caller("both");
      var first = authorized.search(request(local, JOIN, 1));

      var deniedSearch =
          assertThrows(
              StatusRuntimeException.class, () -> restricted.search(request(local, JOIN, 100)));
      var deniedPage =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  restricted.getMore(
                      GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).build()));
      var closed =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  authorized.getMore(
                      GetMoreRequest.newBuilder().setCursorId(first.getCursorId()).build()));

      assertThat(ids(first)).hasSize(1);
      assertThat(deniedSearch.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
      assertThat(deniedPage.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
      assertThat(closed.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    }
  }

  @Test
  public void search_pinnedSelfJoinSnapshot_survivesRefreshBetweenKeyCollectionAndCursorCreation()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String index = f.create("employees", MAPPING);
      f.upsert(index, "{_id:'manager',key:'m',office:'Berlin'}");
      f.upsert(index, "{_id:'employee',parent:'m'}");
      var generation =
          f.runtime
              .liveIndexes()
              .getIndexById(new org.bson.types.ObjectId(index))
              .orElseThrow()
              .getGenerationId();
      var parsed =
          (OperatorQuery)
              SearchQuery.fromBson(
                  BsonDocument.parse(
                      JOIN.replace("from:'foreign',", "")
                          .replace(
                              "returnStoredSource:true",
                              "index:'employees',returnStoredSource:true")));
      var compound = (CompoundOperator) parsed.operator();
      var join = (JoinOperator) compound.filter().operators().getFirst();
      try (var lease = f.runtime.reads().acquire(generation);
          var snapshot = f.runtime.acquireQuerySnapshot(lease, false)) {
        var builder = new JoinKeySet.Builder(ignored -> {});
        snapshot.collectKeys(join.operator(), join.foreignField(), builder::add, () -> {});
        var keys = builder.build();
        f.upsert(index, "{_id:'manager',key:'m',office:'Paris'}");
        f.upsert(index, "{_id:'employee',parent:'absent'}");
        var materialized =
            new MaterializedJoinOperator(
                join, snapshot.joinField(join.localField()), keys, true, 1000);
        var planned =
            parsed.withOperator(
                new CompoundOperator(
                    compound.score(),
                    new CompoundClause(List.of(materialized)),
                    compound.must(),
                    compound.mustNot(),
                    compound.should(),
                    compound.minimumShouldMatch(),
                    compound.doesNotAffect()));

        var cursor =
            f.runtime
                .cursors()
                .newCursor(
                    generation,
                    new CursorQuery.Search(planned, Optional.of(snapshot)),
                    new QueryCursorOptions(Optional.empty(), Optional.of(100), false),
                    QueryOptimizationFlags.DEFAULT_OPTIONS);
        try {
          var batch =
              f.runtime
                  .cursors()
                  .getNextBatch(
                      cursor.cursorId,
                      Bytes.ofBytes(1024 * 1024),
                      new BatchCursorOptions(Optional.empty(), Optional.of(100)));

          assertThat(
                  keys.terms().stream().map(org.apache.lucene.util.BytesRef::utf8ToString).toList())
              .containsExactly("m");
          assertThat(batch.batch.asArray()).hasSize(1);
          assertThat(
                  batch
                      .batch
                      .asArray()
                      .getFirst()
                      .asDocument()
                      .getDocument("storedSource")
                      .getString("_id")
                      .getValue())
              .isEqualTo("employee");
          assertThat(ids(f.search(index, JOIN.replace("from:'foreign',", ""), 100))).isEmpty();
        } finally {
          f.runtime.cursors().killCursor(cursor.cursorId);
        }
      }
    }
  }

  @Test
  public void collectKeys_cancellationDuringCollection_releasesSnapshotsForFreshQueries()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      f.upsert(foreign, keyDocument(20, 8));
      f.upsert(foreign, "{_id:'second',key:'other',office:'Berlin'}");
      f.upsert(foreign, "{_id:'third',key:'another',office:'Berlin'}");
      f.upsert(local, "{_id:'match',parent:'xxxxxxxx0'}");
      var generation =
          f.runtime
              .liveIndexes()
              .getIndexById(new org.bson.types.ObjectId(foreign))
              .orElseThrow()
              .getGenerationId();
      var parsed = (OperatorQuery) SearchQuery.fromBson(BsonDocument.parse(JOIN));
      var join =
          (JoinOperator) ((CompoundOperator) parsed.operator()).filter().operators().getFirst();
      var work = new AtomicInteger();
      try (var lease = f.runtime.reads().acquire(generation);
          var snapshot = f.runtime.acquireQuerySnapshot(lease, false)) {
        assertThrows(
            CancellationException.class,
            () ->
                snapshot.collectKeys(
                    join.operator(),
                    join.foreignField(),
                    ignored -> {},
                    () -> cancelAfter(work, 4)));
      }

      assertThat(ids(f.search(local, JOIN, 100))).containsExactly("match");
    }
  }

  @Test
  public void search_overlongKeys_followsDocumentedIndexedTokenTruncation() throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      String foreign = f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      String prefix = "x".repeat(8191);
      f.upsert(foreign, "{_id:'p',key:'" + prefix + "a',office:'Berlin'}");
      f.upsert(local, "{_id:'match',parent:'" + prefix + "b'}");

      var response = f.search(local, JOIN, 100);

      assertThat(ids(response)).containsExactly("match");
    }
  }

  private static void cancelAfter(AtomicInteger work, int limit) {
    if (work.incrementAndGet() == limit) {
      throw new CancellationException("Cancelled during foreign collection");
    }
  }

  @Test
  public void search_vectorTargetAndEmbeddedForeignPredicate_rejectsUnsupportedForms()
      throws Exception {
    try (var f = new Fixture(data(), List.of())) {
      f.create("foreign", MAPPING);
      String local = f.create("local", MAPPING);
      String vector =
          f.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName("vectors")
                      .setType(IndexType.INDEX_TYPE_VECTOR_SEARCH)
                      .setDefinitionBson(
                          bson(
                              """
                              {fields:[{type:'vector',path:'embedding',numDimensions:3,
                                similarity:'cosine'}]}
                              """))
                      .build())
              .getIndexId();
      f.ready(vector, 1);
      var embedded = BsonDocument.parse(JOIN);
      embedded
          .getDocument("compound")
          .getArray("filter")
          .getFirst()
          .asDocument()
          .getDocument("join")
          .put(
              "operator",
              BsonDocument.parse(
                  """
                  {hasRoot:{operator:{equals:{path:'office',value:'Berlin'}}}}
                  """));

      var wrongType =
          assertThrows(
              StatusRuntimeException.class,
              () -> f.search(local, JOIN.replace("'foreign'", "'vectors'"), 100));
      var wrongScope =
          assertThrows(StatusRuntimeException.class, () -> f.search(local, embedded.toJson(), 100));

      assertThat(wrongType.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(wrongScope.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    }
  }

  @Test
  public void checkCancellation_expiredDeadline_preservesDeadlineStatus() throws Exception {
    var executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
    try (var context = io.grpc.Context.ROOT.withDeadlineAfter(-1, TimeUnit.SECONDS, executor)) {
      var error =
          assertThrows(
              StatusRuntimeException.class, () -> StandaloneJoinPlanner.checkCancellation(context));

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
    } finally {
      executor.shutdownNow();
    }
  }

  private Path data() {
    return this.folder.getRoot().toPath().resolve("data");
  }

  private static byte[] digest(String value) {
    return LocalIndexCatalog.digest(value.getBytes(StandardCharsets.US_ASCII));
  }

  private static ByteString bson(String json) {
    return ByteString.copyFrom(StandaloneDefinitionValidator.encode(BsonDocument.parse(json)));
  }

  private static QueryRequest request(String id, String query, int size) {
    return QueryRequest.newBuilder()
        .setIndexId(id)
        .setQueryBson(bson(query))
        .setBatchSize(size)
        .build();
  }

  private static List<String> ids(QueryResponse response) {
    return new RawBsonDocument(response.getBatchBson().toByteArray())
        .getArray("results").stream()
            .map(
                value -> value.asDocument().getDocument("storedSource").getString("_id").getValue())
            .toList();
  }

  private static final class Fixture implements AutoCloseable {
    private final StandaloneRuntime runtime;
    private final StandaloneGrpcServer server;
    private final ManagedChannel channel;
    private final CatalogServiceGrpc.CatalogServiceBlockingStub catalog;
    private final DocumentServiceGrpc.DocumentServiceBlockingStub docs;
    private final QueryServiceGrpc.QueryServiceBlockingStub queries;

    private Fixture(Path data, List<StandaloneConfig.Principal> principals) throws Exception {
      this(data, principals, StandaloneConfig.development(data, 0).minFreeDiskBytes());
    }

    private Fixture(Path data, List<StandaloneConfig.Principal> principals, long minFreeDiskBytes)
        throws Exception {
      // Keep admission tests independent of the JVM heap chosen by the test runner.
      this(data, principals, minFreeDiskBytes, MemoryLimits.fromHeapBytes(2L << 30));
    }

    private Fixture(
        Path data, List<StandaloneConfig.Principal> principals, MemoryLimits memoryLimits)
        throws Exception {
      this(data, principals, 0, memoryLimits);
    }

    private Fixture(
        Path data,
        List<StandaloneConfig.Principal> principals,
        long minFreeDiskBytes,
        MemoryLimits memoryLimits)
        throws Exception {
      var base = StandaloneConfig.development(data, 0);
      var config =
          new StandaloneConfig(
              base.dataPath(),
              base.address(),
              base.documentLimits(),
              base.maxInFlightRequests(),
              base.maxInFlightBytes(),
              minFreeDiskBytes,
              base.commitInterval(),
              base.refreshInterval(),
              base.maxRpcDuration(),
              base.tls(),
              principals,
              false,
              memoryLimits);
      this.runtime = new StandaloneRuntime(config);
      this.server = new StandaloneGrpcServer(this.runtime).start();
      this.channel =
          NettyChannelBuilder.forAddress("127.0.0.1", this.server.port()).usePlaintext().build();
      this.catalog = CatalogServiceGrpc.newBlockingStub(this.channel);
      this.docs = DocumentServiceGrpc.newBlockingStub(this.channel);
      this.queries = QueryServiceGrpc.newBlockingStub(this.channel);
    }

    private QueryServiceGrpc.QueryServiceBlockingStub caller(String token) {
      var metadata = new Metadata();
      metadata.put(
          Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
      return this.queries.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    private String create(String name, String mapping) throws Exception {
      String id =
          this.catalog
              .createIndex(
                  CreateIndexRequest.newBuilder()
                      .setName(name)
                      .setType(IndexType.INDEX_TYPE_SEARCH)
                      .setDefinitionBson(bson(mapping))
                      .build())
              .getIndexId();
      ready(id, 1);
      return id;
    }

    private IndexDescription ready(String id, long revision) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (System.nanoTime() < deadline) {
        var result = this.catalog.getIndex(GetIndexRequest.newBuilder().setIndexId(id).build());
        if (result.getLifecycleState().equals("READY") && result.getActiveRevision() == revision) {
          return result;
        }
        Thread.sleep(20);
      }
      throw new AssertionError("Index did not become ready");
    }

    private void readyInternally(String id, long revision) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (System.nanoTime() < deadline) {
        var result =
            this.runtime.serviceStatus().getIndexesList().stream()
                .filter(index -> index.getIndexId().equals(id))
                .findFirst()
                .orElseThrow();
        if (result.getLifecycleState().equals("READY") && result.getActiveRevision() == revision) {
          return;
        }
        Thread.sleep(20);
      }
      throw new AssertionError("Index did not become ready");
    }

    private void upsert(String id, String doc) {
      this.docs.upsert(
          UpsertRequest.newBuilder()
              .setWriteConcern(DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE)
              .setIndexId(id)
              .addDocumentsBson(bson(doc))
              .build());
    }

    private QueryResponse search(String id, String query, int size) {
      return this.queries.search(request(id, query, size));
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
