package com.xgen.mongot.standalone.coexistence;

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

  public NativeCursorManager(MongotCursorManagerImpl delegate) {
    this.delegate = delegate;
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
    return this.delegate.newCursor(db, collection, uuid, view, query, options, flags, metadata);
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
    return this.delegate.newIntermediateCursors(
        db, collection, uuid, view, query, version, options, flags, metadata);
  }

  @Override
  public MongotCursorResultInfo getNextBatch(long id, Bytes limit, BatchCursorOptions options)
      throws MongotCursorNotFoundException, IOException, IndexUnavailableException {
    requireNative(id);
    return this.delegate.getNextBatch(id, limit, options);
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
