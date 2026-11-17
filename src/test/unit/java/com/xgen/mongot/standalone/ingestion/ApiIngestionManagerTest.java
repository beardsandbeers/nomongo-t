package com.xgen.mongot.standalone.ingestion;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.IndexGeneration.mockIndexGeneration;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.xgen.mongot.catalog.DefaultIndexCatalog;
import com.xgen.mongot.catalog.InitializedIndexCatalog;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.SearchIndexReader;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory;
import com.xgen.mongot.standalone.catalog.StandaloneDefinitionValidator;
import com.xgen.mongot.standalone.config.StandaloneConfig.Limits;
import com.xgen.mongot.standalone.grpc.DocumentWriteConcern;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ApiIngestionManagerTest {
  @Test
  public void publishSchema_newWritesQueueAndReadersRemainAvailable() throws Exception {
    try (var f = new Fixture();
        var pin = f.manager.acquireQueryLease(f.generation.getGenerationId());
        var workers = Executors.newSingleThreadExecutor()) {
      var publishing = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      var update =
          workers.submit(
              () ->
                  f.manager.publishSchema(
                      pin,
                      () -> {
                        publishing.countDown();
                        try {
                          await(release);
                        } catch (InterruptedException error) {
                          Thread.currentThread().interrupt();
                          throw new IllegalStateException(error);
                        }
                        return "published";
                      }));
      try {
        assertThat(publishing.await(5, TimeUnit.SECONDS)).isTrue();
        var write =
            f.manager.writeAsync(
                f.generation.getGenerationId().indexId.toHexString(),
                () -> f.generation.getDefinition().getDefinitionVersion().orElse(1L),
                "",
                List.of(Fixture.bson("queued")),
                false,
                DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);
        try (var reader = f.manager.acquireQueryLease(f.generation.getGenerationId())) {
          assertThat(reader.retired()).isFalse();
          assertThat(write.isDone()).isFalse();
          release.countDown();
          assertThat(update.get(5, TimeUnit.SECONDS)).isEqualTo("published");
          assertThat(write.get(5, TimeUnit.SECONDS))
              .isEqualTo(f.generation.getGenerationId().uniqueString());
          verify(f.writer).updateIndex(any());
          verify(f.writer).commit(EncodedUserData.EMPTY);
        }
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void publishSchema_waitsForExecutingMutationIncludingItsCommit() throws Exception {
    try (var f = new Fixture();
        var pin = f.manager.acquireQueryLease(f.generation.getGenerationId());
        var workers = Executors.newSingleThreadExecutor()) {
      var committing = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      doAnswer(
              invocation -> {
                committing.countDown();
                await(release);
                return null;
              })
          .when(f.writer)
          .commit(EncodedUserData.EMPTY);
      var write =
          f.manager.writeAsync(
              f.generation.getGenerationId().indexId.toHexString(),
              () -> f.generation.getDefinition().getDefinitionVersion().orElse(1L),
              "",
              List.of(Fixture.bson("active")),
              false,
              DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);
      try {
        assertThat(committing.await(5, TimeUnit.SECONDS)).isTrue();
        var update = workers.submit(() -> f.manager.publishSchema(pin, () -> "published"));
        assertThat(update.isDone()).isFalse();
        release.countDown();
        write.get(5, TimeUnit.SECONDS);
        assertThat(update.get(5, TimeUnit.SECONDS)).isEqualTo("published");
        assertThat(pin.retired()).isFalse();
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void quiesce_readersAndUpdatePin_doNotBlockMutationsDrainButDoBlockRetirement()
      throws Exception {
    try (var f = new Fixture()) {
      var generation = f.generation.getGenerationId();
      var first = f.manager.acquireQueryLease(generation);
      try (first) {
        var drained = f.manager.quiesce(generation);
        assertThat(drained.isDone()).isTrue();
        try (var late = f.manager.acquireQueryLease(generation)) {
          var update = f.manager.acquireUpdateLease(generation);
          try (update) {
            assertThrows(
                StatusRuntimeException.class, () -> f.manager.acquireUpdateLease(generation));
            assertThrows(
                StatusRuntimeException.class, () -> f.manager.acquireStatisticsLease(generation));
            assertThrows(StatusRuntimeException.class, () -> f.write("late"));
            var retirement = f.manager.dropIndex(generation);
            first.close();
            first.close();
            late.close();
            assertThat(retirement.isDone()).isFalse();
            update.close();
            assertThat(retirement.isDone()).isTrue();
          }
        }
      }
    }
  }

  @Test
  public void quiesce_secondMutationEpochWithReader_waitsForTheCurrentBatch() throws Exception {
    try (var f = new Fixture();
        var read = f.manager.acquireQueryLease(f.generation.getGenerationId());
        var workers = Executors.newSingleThreadExecutor()) {
      f.write("first");
      var entered = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      doAnswer(
              invocation -> {
                entered.countDown();
                await(release);
                return null;
              })
          .when(f.writer)
          .updateIndex(any());
      var second = workers.submit(() -> f.write("second"));
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        var drained = f.manager.quiesce(f.generation.getGenerationId());
        assertThat(drained.isDone()).isFalse();
        assertThat(f.manager.quiesce(f.generation.getGenerationId())).isSameInstanceAs(drained);
        release.countDown();
        second.get(5, TimeUnit.SECONDS);
        drained.get(5, TimeUnit.SECONDS);
        try (var update = f.manager.acquireUpdateLease(f.generation.getGenerationId())) {
          assertThat(read.retired()).isFalse();
          f.manager.commitForUpdate(update);
        }
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void shutdown_readAndUpdateLeases_waitsForBothKinds() throws Exception {
    try (var f = new Fixture()) {
      var generation = f.generation.getGenerationId();
      f.manager.quiesce(generation).get(5, TimeUnit.SECONDS);
      try (var read = f.manager.acquireQueryLease(generation);
          var update = f.manager.acquireUpdateLease(generation)) {
        var shutdown = f.manager.shutdown();
        assertThat(shutdown.isDone()).isFalse();
        assertThrows(StatusRuntimeException.class, () -> f.manager.acquireQueryLease(generation));
        read.close();
        assertThat(shutdown.isDone()).isFalse();
        update.close();
        shutdown.get(5, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  public void quiesce_admittedBatch_drainsBeforeSnapshotAndKeepsQueriesReady() throws Exception {
    try (var f = new Fixture()) {
      var entered = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      doAnswer(
              invocation -> {
                entered.countDown();
                await(release);
                return null;
              })
          .when(f.writer)
          .updateIndex(any());
      var response =
          f.manager.writeAsync(
              f.generation.getGenerationId().indexId.toHexString(),
              () -> f.generation.getDefinition().getDefinitionVersion().orElse(1L),
              "",
              List.of(Fixture.bson("a")),
              false,
              DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE);

      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        var drained = f.manager.quiesce(f.generation.getGenerationId());

        assertThat(drained.isDone()).isFalse();
        assertThat(f.manager.isGenerationReady(f.generation.getGenerationId())).isTrue();
        assertThrows(StatusRuntimeException.class, () -> f.write("late"));
        try (var read = f.manager.acquireQueryLease(f.generation.getGenerationId())) {
          assertThat(read.retired()).isFalse();
        }
        assertThrows(
            StatusRuntimeException.class,
            () -> f.manager.acquireUpdateLease(f.generation.getGenerationId()));
        release.countDown();
        response.get(5, TimeUnit.SECONDS);
        drained.get(5, TimeUnit.SECONDS);
        verify(f.reader).refresh();
        try (var lease = f.manager.acquireUpdateLease(f.generation.getGenerationId())) {
          f.manager.commitForUpdate(lease);
          verify(f.writer).commit(EncodedUserData.EMPTY);
        }
        f.manager.commitDirty();
        verify(f.writer).commit(EncodedUserData.EMPTY);
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void retired_scanCheck_doesNotAcquireIngestionMonitorAndObservesDrop() throws Exception {
    try (var f = new Fixture();
        var caller = Executors.newSingleThreadExecutor()) {
      java.util.concurrent.CompletableFuture<Void> retirement;
      try (var lease = f.manager.acquireStatisticsLease(f.generation.getGenerationId())) {
        var retired =
            f.manager.coordinate(
                () ->
                    java.util.concurrent.CompletableFuture.supplyAsync(lease::retired, caller)
                        .orTimeout(5, TimeUnit.SECONDS)
                        .join());
        retirement = f.manager.dropIndex(f.generation.getGenerationId());

        assertThat(retired).isFalse();
        assertThat(lease.retired()).isTrue();
        assertThat(retirement.isDone()).isFalse();
      }
      assertThat(retirement.isDone()).isTrue();
    }
  }

  @Test
  public void deleteDocuments_nativeFailure_preservesDirtyTrackingAndReleasesLease()
      throws Exception {
    try (var f = new Fixture()) {
      doAnswer(
              invocation -> {
                assertThat(Thread.holdsLock(f.manager)).isFalse();
                throw new java.io.IOException("injected native deletion failure");
              })
          .when(f.writer)
          .deleteDocuments(any(), any(), any());
      String id = f.generation.getGenerationId().indexId.toHexString();
      String generation = f.generation.getGenerationId().uniqueString();

      var error =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  f.manager.deleteDocuments(
                      id,
                      () -> f.generation.getDefinition().getDefinitionVersion().orElse(1L),
                      generation,
                      List.of(Fixture.bson("a").toByteArray()),
                      () -> {}));
      f.manager.completeWrite(
          id,
          () -> f.generation.getDefinition().getDefinitionVersion().orElse(1L),
          generation,
          DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);

      assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
      verify(f.writer).commit(EncodedUserData.EMPTY);
      assertThat(f.manager.dropIndex(f.generation.getGenerationId()).isDone()).isTrue();
    }
  }

  @Test
  public void writeAsync_cancelledResponse_retainsLeaseUntilWorkDrains() throws Exception {
    try (var f = new Fixture()) {
      var entered = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      doAnswer(
              invocation -> {
                entered.countDown();
                await(release);
                return null;
              })
          .when(f.writer)
          .updateIndex(any());

      var response =
          f.manager.writeAsync(
              f.generation.getGenerationId().indexId.toHexString(),
              () -> f.generation.getDefinition().getDefinitionVersion().orElse(1L),
              "",
              List.of(Fixture.bson("a")),
              false,
              DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_INDEXED);
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        response.cancel(false);
        var retirement = f.manager.dropIndex(f.generation.getGenerationId());
        assertThat(retirement.isDone()).isFalse();
        release.countDown();
        retirement.get(5, TimeUnit.SECONDS);
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void completeWrite_visibleAndCommitted_runOutsideCatalogMonitor() throws Exception {
    try (var f = new Fixture()) {
      f.write("pending");
      doAnswer(
              invocation -> {
                assertThat(Thread.holdsLock(f.manager)).isFalse();
                return null;
              })
          .when(f.reader)
          .refresh();
      doAnswer(
              invocation -> {
                assertThat(Thread.holdsLock(f.manager)).isFalse();
                return null;
              })
          .when(f.writer)
          .commit(any());
      String id = f.generation.getGenerationId().indexId.toHexString();
      String generation = f.generation.getGenerationId().uniqueString();

      f.manager.completeWrite(
          id,
          () -> f.generation.getDefinition().getDefinitionVersion().orElse(1L),
          generation,
          DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE);
      f.manager.completeWrite(
          id,
          () -> f.generation.getDefinition().getDefinitionVersion().orElse(1L),
          generation,
          DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);

      verify(f.reader).refresh();
      verify(f.writer).commit(EncodedUserData.EMPTY);
    }
  }

  @Test
  public void write_concurrentBatches_sameAndDifferentIndexUpdatesOverlap() throws Exception {
    try (var f = new Fixture();
        var workers = Executors.newFixedThreadPool(3)) {
      var other = f.registerIndex();
      var entered = new CountDownLatch(3);
      var release = new CountDownLatch(1);
      doAnswer(
              invocation -> {
                entered.countDown();
                await(release);
                return null;
              })
          .when(f.writer)
          .updateIndex(any());

      var first = workers.submit(() -> f.write("a"));
      var second = workers.submit(() -> f.write("b"));
      var third =
          workers.submit(
              () ->
                  f.manager.write(
                      other.getGenerationId().indexId.toHexString(),
                      () -> other.getDefinition().getDefinitionVersion().orElse(1L),
                      "",
                      List.of(Fixture.bson("c")),
                      false,
                      DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_INDEXED));
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
      } finally {
        release.countDown();
      }
      assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo(second.get(5, TimeUnit.SECONDS));
      assertThat(third.get(5, TimeUnit.SECONDS)).isEqualTo(other.getGenerationId().uniqueString());
      verify(f.reader, never()).refresh();
      verify(f.writer, never()).commit(any());
    }
  }

  @Test
  public void write_batchWithRepeatedIds_preservesDocumentOrder() throws Exception {
    try (var f = new Fixture()) {
      f.manager.write(
          f.generation.getGenerationId().indexId.toHexString(),
          () -> f.generation.getDefinition().getDefinitionVersion().orElse(1L),
          "",
          List.of(Fixture.bson("a"), Fixture.bson("b"), Fixture.bson("a")),
          false,
          DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_INDEXED);

      var events = ArgumentCaptor.forClass(DocumentEvent.class);
      verify(f.writer, times(3)).updateIndex(events.capture());
      assertThat(
              events.getAllValues().stream()
                  .map(event -> event.getDocumentId().asString().getValue())
                  .toList())
          .containsExactly("a", "b", "a")
          .inOrder();
    }
  }

  @Test
  public void write_partialFailure_keepsAppliedPrefixDirty() throws Exception {
    try (var f = new Fixture()) {
      doAnswer(
              invocation -> {
                DocumentEvent event = invocation.getArgument(0);
                if (event.getDocumentId().asString().getValue().equals("bad")) {
                  throw new IllegalArgumentException("mapping failure");
                }
                return null;
              })
          .when(f.writer)
          .updateIndex(any());

      assertThrows(
          IllegalArgumentException.class,
          () ->
              f.manager.write(
                  f.generation.getGenerationId().indexId.toHexString(),
                  () -> f.generation.getDefinition().getDefinitionVersion().orElse(1L),
                  "",
                  List.of(Fixture.bson("a"), Fixture.bson("bad")),
                  false,
                  DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_INDEXED));
      f.manager.commitDirty();

      verify(f.writer).commit(EncodedUserData.EMPTY);
    }
  }

  @Test
  public void commitDirty_writeRacesCommit_keepsLaterBatchDirtyWithoutBlockingIt()
      throws Exception {
    try (var f = new Fixture();
        var workers = Executors.newFixedThreadPool(2)) {
      f.write("before");
      var entered = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      doAnswer(
              invocation -> {
                entered.countDown();
                await(release);
                return null;
              })
          .when(f.writer)
          .commit(any());

      var commit = workers.submit(f.manager::commitDirty);
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        workers.submit(() -> f.write("during")).get(5, TimeUnit.SECONDS);
      } finally {
        release.countDown();
      }
      commit.get(5, TimeUnit.SECONDS);
      f.manager.commitDirty();
      f.manager.commitDirty();

      verify(f.writer, times(2)).commit(EncodedUserData.EMPTY);
    }
  }

  @Test
  public void dropIndex_activeWrite_waitsForLeaseAndRejectsNewWrites() throws Exception {
    try (var f = new Fixture();
        var workers = Executors.newFixedThreadPool(1)) {
      var entered = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      doAnswer(
              invocation -> {
                entered.countDown();
                await(release);
                return null;
              })
          .when(f.writer)
          .updateIndex(any());

      var write = workers.submit(() -> f.write("a"));
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        var dropped = f.manager.dropIndex(f.generation.getGenerationId());
        assertThat(dropped.isDone()).isFalse();
        assertThat(
                assertThrows(StatusRuntimeException.class, () -> f.write("b"))
                    .getStatus()
                    .getCode())
            .isEqualTo(Status.Code.UNAVAILABLE);
        release.countDown();
        write.get(5, TimeUnit.SECONDS);
        dropped.get(5, TimeUnit.SECONDS);
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void shutdown_activeWrite_drainsAndCommitsBeforeRetirementCompletes() throws Exception {
    try (var f = new Fixture();
        var workers = Executors.newFixedThreadPool(1)) {
      var entered = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      doAnswer(
              invocation -> {
                entered.countDown();
                await(release);
                return null;
              })
          .when(f.writer)
          .updateIndex(any());

      var write = workers.submit(() -> f.write("a"));
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        var shutdown = f.manager.shutdown();
        var dropped = f.manager.dropIndex(f.generation.getGenerationId());
        assertThat(shutdown.isDone()).isFalse();
        assertThat(dropped.isDone()).isFalse();
        verify(f.writer, never()).commit(any());
        release.countDown();
        write.get(5, TimeUnit.SECONDS);
        shutdown.get(5, TimeUnit.SECONDS);
        dropped.get(5, TimeUnit.SECONDS);
        verify(f.writer).commit(EncodedUserData.EMPTY);
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void write_visibleRefresh_doesNotBlockIndexedBatch() throws Exception {
    try (var f = new Fixture();
        var workers = Executors.newFixedThreadPool(2)) {
      var entered = new CountDownLatch(1);
      var release = new CountDownLatch(1);
      doAnswer(
              invocation -> {
                entered.countDown();
                await(release);
                return null;
              })
          .when(f.reader)
          .refresh();

      var visible =
          workers.submit(
              () -> f.write("visible", DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE));
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        workers.submit(() -> f.write("indexed")).get(5, TimeUnit.SECONDS);
        assertThat(visible.isDone()).isFalse();
      } finally {
        release.countDown();
      }
      visible.get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void write_explicitConcerns_refreshesOrCommitsOnlyWhenRequested() throws Exception {
    try (var f = new Fixture()) {
      f.write("indexed");
      verify(f.reader, never()).refresh();
      verify(f.writer, never()).commit(any());

      f.write("visible", DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_VISIBLE);
      verify(f.reader).refresh();
      verify(f.writer, never()).commit(any());

      f.write("committed", DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_COMMITTED);
      verify(f.reader).refresh();
      verify(f.writer).commit(EncodedUserData.EMPTY);
      f.manager.commitDirty();
      verify(f.writer).commit(EncodedUserData.EMPTY);
    }
  }

  private static void await(CountDownLatch latch) throws InterruptedException {
    if (!latch.await(5, TimeUnit.SECONDS)) {
      throw new AssertionError("Timed out waiting for concurrent operation");
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final DefaultIndexCatalog live = new DefaultIndexCatalog();
    private final InitializedIndexCatalog initialized = new InitializedIndexCatalog();
    private final IndexGeneration generation;
    private final IndexWriter writer = mock(IndexWriter.class);
    private final SearchIndexReader reader = mock(SearchIndexReader.class);
    private final ApiIngestionManager manager;
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final IndexingWorkSchedulerFactory schedulers =
        IndexingWorkSchedulerFactory.createWithoutEmbeddingStrategy(4, this.metrics, false);

    private Fixture() throws Exception {
      when(this.writer.prepareBatch(any())).thenAnswer(invocation -> invocation.getArgument(0));
      this.manager =
          new ApiIngestionManager(
              this.live,
              this.initialized,
              Limits.defaults(),
              Duration.ofHours(1),
              this.schedulers
                  .getIndexingWorkSchedulers()
                  .get(IndexingWorkSchedulerFactory.IndexingStrategy.DEFAULT));
      this.generation = registerIndex();
    }

    private IndexGeneration registerIndex() {
      var generation = mockIndexGeneration(new ObjectId());
      this.live.addIndex(generation);
      var index = mock(InitializedSearchIndex.class);
      when(index.getGenerationId()).thenReturn(generation.getGenerationId());
      when(index.getStatus()).thenReturn(IndexStatus.steady());
      when(index.getWriter()).thenReturn(this.writer);
      when(index.getReader()).thenReturn(this.reader);
      this.initialized.addIndex(index);
      this.manager.add(generation);
      return generation;
    }

    private static ByteString bson(String id) {
      return ByteString.copyFrom(
          StandaloneDefinitionValidator.encode(BsonDocument.parse("{_id:'" + id + "'}")));
    }

    private String write(String id) {
      return write(id, DocumentWriteConcern.DOCUMENT_WRITE_CONCERN_INDEXED);
    }

    private String write(String id, DocumentWriteConcern concern) {
      return this.manager.write(
          this.generation.getGenerationId().indexId.toHexString(),
          () -> this.generation.getDefinition().getDefinitionVersion().orElse(1L),
          "",
          List.of(bson(id)),
          false,
          concern);
    }

    @Override
    public void close() throws Exception {
      this.manager.shutdown().get(5, TimeUnit.SECONDS);
      for (var scheduler :
          java.util.Set.copyOf(this.schedulers.getIndexingWorkSchedulers().values())) {
        scheduler.shutdown().get(5, TimeUnit.SECONDS);
      }
      this.metrics.close();
    }
  }
}
