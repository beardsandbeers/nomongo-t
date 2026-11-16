package com.xgen.mongot.standalone.coexistence;

import com.google.errorprone.annotations.Var;
import com.xgen.mongot.cursor.CursorQuery;
import com.xgen.mongot.cursor.IntermediateSearchCursorInfo;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.cursor.MongotCursorManagerImpl;
import com.xgen.mongot.cursor.MongotCursorNotFoundException;
import com.xgen.mongot.cursor.MongotCursorResultInfo;
import com.xgen.mongot.cursor.QueryBatchTimerRecorder;
import com.xgen.mongot.cursor.SearchCursorInfo;
import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.ReaderClosedException;
import com.xgen.mongot.index.lucene.explain.tracing.ExplainQueryState;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.util.Bytes;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/** Keeps native cursor IDs compatible while denying transport access to direct-API cursors. */
public final class NativeCursorManager implements MongotCursorManager {
  private final MongotCursorManagerImpl delegate;
  private final Optional<CoupledJoinService> joins;
  private final java.util.Set<Long> joinCursors =
      java.util.concurrent.ConcurrentHashMap.newKeySet();

  public NativeCursorManager(MongotCursorManagerImpl delegate) {
    this.delegate = delegate;
    this.joins = Optional.empty();
  }

  public NativeCursorManager(MongotCursorManagerImpl delegate, CoupledJoinService joins) {
    this.delegate = delegate;
    this.joins = Optional.of(joins);
  }

  private void requireNative(long id) throws MongotCursorNotFoundException {
    if (this.delegate.isDirectCursor(id)) {
      throw new MongotCursorNotFoundException(id);
    }
  }

  @Override
  public SearchCursorInfo newCursor(
      String db,
      String collection,
      UUID uuid,
      Optional<String> view,
      CursorQuery query,
      QueryCursorOptions options,
      QueryOptimizationFlags flags,
      Optional<SearchEnvoyMetadata> metadata)
      throws IOException,
          InvalidQueryException,
          IndexUnavailableException,
          InterruptedException,
          ReaderClosedException {
    if (this.joins.isEmpty()
        || !(query instanceof CursorQuery.Search search)
        || !(search.query() instanceof SearchQuery logical)
        || !CoupledJoinService.contains(logical)
        || metadata.map(m -> m.getRoutedFromAnotherShard()).orElse(false)) {
      return this.delegate.newCursor(db, collection, uuid, view, query, options, flags, metadata);
    }
    @Var CoupledJoinService.Prepared prepared = null;
    @Var Long cursor = null;
    try {
      var plan = this.joins.get().prepare(db, uuid, view, logical);
      if (plan.isEmpty()) {
        return this.delegate.newCursor(db, collection, uuid, view, query, options, flags, metadata);
      }
      prepared = plan.get();
      var info =
          this.delegate.newCursor(
              db,
              collection,
              uuid,
              view,
              new CursorQuery.Search(
                  prepared.plan.query(), Optional.of(prepared), Optional.of(prepared.generation)),
              options,
              flags,
              metadata);
      cursor = info.cursorId;
      if (!prepared.consumed()) {
        throw new CoupledJoinService.Failure("Join dependency disappeared during registration");
      }
      prepared.validateInitial.run();
      prepared.plan.verifyDependencies();
      this.joinCursors.add(info.cursorId);
      prepared.registered(() -> this.joinCursors.remove(info.cursorId));
      return info;
    } catch (Exception | Error e) {
      if (cursor != null) {
        this.delegate.killCursor(cursor);
      }
      if (prepared != null) {
        try {
          prepared.close();
        } catch (IOException cleanup) {
          e.addSuppressed(cleanup);
        }
      }
      if (e instanceof IOException io) {
        throw io;
      }
      if (e instanceof InterruptedException interrupted) {
        throw interrupted;
      }
      if (e instanceof IndexUnavailableException unavailable) {
        throw unavailable;
      }
      if (e instanceof Error error) {
        throw error;
      }
      throw new InvalidQueryException(e.getMessage());
    }
  }

  @Override
  public IntermediateSearchCursorInfo newIntermediateCursors(
      String db,
      String collection,
      UUID uuid,
      Optional<String> view,
      SearchQuery query,
      int version,
      QueryCursorOptions options,
      QueryOptimizationFlags flags,
      Optional<SearchEnvoyMetadata> metadata)
      throws IOException, InvalidQueryException, IndexUnavailableException, InterruptedException {
    if (CoupledJoinService.contains(query)) {
      throw new InvalidQueryException("Joins do not support intermediate/sharded search execution");
    }
    return this.delegate.newIntermediateCursors(
        db, collection, uuid, view, query, version, options, flags, metadata);
  }

  @Override
  public MongotCursorResultInfo getNextBatch(long id, Bytes limit, BatchCursorOptions options)
      throws MongotCursorNotFoundException, IOException, IndexUnavailableException {
    requireNative(id);
    boolean join = this.joinCursors.contains(id);
    try {
      return this.delegate.getNextBatch(id, limit, options);
    } catch (IOException | RuntimeException | Error e) {
      if (join) {
        this.delegate.killCursor(id);
      }
      if (e instanceof CoupledJoinService.Failure
          || e instanceof java.util.concurrent.CancellationException) {
        throw new IndexUnavailableException(
            java.util.Objects.toString(e.getMessage(), "Join request failed"));
      }
      throw e;
    }
  }

  @Override
  public QueryBatchTimerRecorder getIndexQueryBatchTimerRecorder(long id)
      throws MongotCursorNotFoundException {
    requireNative(id);
    return this.delegate.getIndexQueryBatchTimerRecorder(id);
  }

  @Override
  public Optional<ExplainQueryState> getExplainQueryState(long id)
      throws MongotCursorNotFoundException {
    requireNative(id);
    return this.delegate.getExplainQueryState(id);
  }

  @Override
  public void killCursor(long id) {
    if (!this.delegate.isDirectCursor(id)) {
      this.delegate.killCursor(id);
    }
  }

  @Override
  public void killIndexCursors(GenerationId id) {
    this.delegate.killIndexCursors(id);
  }

  @Override
  public boolean hasOpenCursors(GenerationId id) {
    return this.delegate.hasOpenCursors(id);
  }

  @Override
  public void close() {
    /* The engine owns the delegate. */
  }
}
