package com.xgen.mongot.standalone.coexistence;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.cursor.batch.ConstantBatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.extensions.join.JoinExecutionContext;
import com.xgen.mongot.extensions.join.JoinPlanner;
import com.xgen.mongot.index.BatchProducer;
import com.xgen.mongot.index.MetaResults;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.SearchIndexReader.QuerySnapshot;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.lucene.explain.information.JoinSourceInformation;
import com.xgen.mongot.index.lucene.explain.tracing.Explain;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.operators.JoinField;
import com.xgen.mongot.index.query.operators.JoinSource;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;

public class CoupledJoinServiceTest {
  @Test
  public void close_afterTransfer_releasesProducerAndReservationExactlyOnce() throws Exception {
    var fixture = new Fixture();
    var prepared = fixture.prepare();
    prepared.query(
        prepared.plan.query(),
        QueryCursorOptions.empty(),
        new ConstantBatchSizeStrategy(),
        QueryOptimizationFlags.DEFAULT_OPTIONS);
    var removed = new AtomicBoolean();
    prepared.registered(() -> removed.set(true));

    prepared.close();
    prepared.close();

    verify(fixture.producer, times(1)).close();
    verify(fixture.lease, times(1)).close();
    assertThat(removed.get()).isTrue();
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void registered_closeBeforeRegistration_removesMarkerImmediately() throws Exception {
    var fixture = new Fixture();
    var prepared = fixture.prepare();
    var removed = new AtomicBoolean();

    prepared.close();
    prepared.registered(() -> removed.set(true));

    assertThat(removed.get()).isTrue();
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void query_dependencyRetiresAfterTransfer_cleanupStillOwnsProducer() throws Exception {
    var fixture = new Fixture();
    var prepared = fixture.prepare();
    when(fixture.snapshot.query(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              fixture.retired.set(true);
              return new SearchIndexReader.SearchProducerAndMetaResults(
                  fixture.producer, MetaResults.EMPTY);
            });

    assertThrows(
        IllegalStateException.class,
        () ->
            prepared.query(
                prepared.plan.query(),
                QueryCursorOptions.empty(),
                new ConstantBatchSizeStrategy(),
                QueryOptimizationFlags.DEFAULT_OPTIONS));
    prepared.close();

    verify(fixture.producer).close();
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void query_cancelledInvocation_checksBeforeInitialLocalExecution() throws Exception {
    var fixture = new Fixture();
    try (var prepared = fixture.prepare()) {
      JoinExecutionContext.run(
          () -> {
            throw new CancellationException();
          },
          () -> {
            assertThrows(
                CancellationException.class,
                () ->
                    prepared.query(
                        prepared.plan.query(),
                        QueryCursorOptions.empty(),
                        new ConstantBatchSizeStrategy(),
                        QueryOptimizationFlags.DEFAULT_OPTIONS));
            return null;
          });
      verify(fixture.snapshot, never()).query(any(), any(), any(), any());
    }
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void prepare_explainedCollectionSource_preservesObjectSourceAndRoundTrips()
      throws Exception {
    var fixture = new Fixture();
    var query =
        SearchQuery.fromBson(
            BsonDocument.parse(
                """
                {compound:{filter:[{join:{from:{collection:'accounts'},
                  localField:'parent',foreignField:'key',
                  operator:{equals:{path:'office',value:'Berlin'}}}}]}}
                """));
    try (var scope = Explain.setup(Optional.of(Explain.Verbosity.EXECUTION_STATS), Optional.of(1));
        var plan = new JoinPlanner(fixture).prepare(query, fixture.generation)) {
      var source =
          Explain.collect()
              .orElseThrow()
              .toBson()
              .getDocument("joins")
              .getDocument("preparation")
              .getArray("sources")
              .getFirst()
              .asDocument();

      assertThat(source.getDocument("from"))
          .isEqualTo(BsonDocument.parse("{collection:'accounts',index:'default'}"));
      assertThat(source.getString("foreignIndexId").getValue())
          .isEqualTo(fixture.generation.indexId.toHexString());
      try (var parser = BsonDocumentParser.fromRoot(source).build()) {
        assertThat(JoinSourceInformation.fromBson(parser).toBson()).isEqualTo(source);
      }
    }
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  @Test
  public void prepare_mappingFailure_releasesGenerationLease() throws Exception {
    var fixture = new Fixture();
    when(fixture.snapshot.joinFields(any()))
        .thenThrow(new InvalidQueryException("invalid mapping"));

    assertThrows(InvalidQueryException.class, fixture::prepare);

    verify(fixture.lease).close();
    assertThat(JoinPlanner.retainedBytes()).isEqualTo(0.0);
  }

  private static class Fixture implements JoinPlanner.Environment {
    final Closeable lease = mock(Closeable.class);
    final QuerySnapshot snapshot = mock(QuerySnapshot.class);
    final BatchProducer producer = mock(BatchProducer.class);
    final AtomicBoolean retired = new AtomicBoolean();
    final GenerationId generation = new GenerationId(new ObjectId(), Generation.CURRENT);

    Fixture() throws Exception {
      when(this.snapshot.joinFields(any()))
          .thenReturn(List.of(new JoinField(JoinField.Type.TOKEN, StockNormalizerName.NONE)));
      when(this.snapshot.partitions()).thenReturn(1);
      when(this.snapshot.query(any(), any(), any(), any()))
          .thenReturn(
              new SearchIndexReader.SearchProducerAndMetaResults(this.producer, MetaResults.EMPTY));
    }

    CoupledJoinService.Prepared prepare() throws Exception {
      var query =
          SearchQuery.fromBson(
              BsonDocument.parse(
                  """
                  {compound:{filter:[{join:{localField:'parent',foreignField:'key',
                    operator:{equals:{path:'office',value:'Berlin'}}}}]}}
                  """));
      return new CoupledJoinService.Prepared(
          new JoinPlanner(this).prepare(query, this.generation), this.generation);
    }

    public String resolve(JoinSource source) {
      return this.generation.indexId.toHexString();
    }

    public GenerationId pin(String id, Optional<GenerationId> expected) {
      return this.generation;
    }

    public Closeable lease(GenerationId generation) {
      return this.lease;
    }

    public QuerySnapshot acquire(GenerationId generation, boolean concurrent) {
      return this.snapshot;
    }

    public void verify(String id, GenerationId generation) {
      if (this.retired.get()) {
        throw new IllegalStateException("retired");
      }
    }

    public Runnable cancellation() {
      return () -> {};
    }

    public RuntimeException invalid(String message) {
      return new IllegalArgumentException(message);
    }

    public RuntimeException exhausted(String message) {
      return new IllegalArgumentException(message);
    }
  }
}
