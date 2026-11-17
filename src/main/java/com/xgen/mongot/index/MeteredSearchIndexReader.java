package com.xgen.mongot.index;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.cursor.batch.BatchSizeStrategy;
import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.lucene.facet.TokenFacetsStateCache;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.Query;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.mongot.index.query.operators.JoinField;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.util.FieldPath;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.util.BytesRef;

public class MeteredSearchIndexReader implements SearchIndexReader {

  @Override
  public long collectDocumentIds(
      org.bson.BsonDocument predicate,
      boolean vector,
      boolean all,
      java.util.function.Supplier<DocumentIdConsumer> consumers,
      Runnable check)
      throws IOException, ReaderClosedException, InvalidQueryException {
    return this.indexReader.collectDocumentIds(predicate, vector, all, consumers, check);
  }

  @Override
  public void collectDocumentIds(
      org.bson.BsonDocument predicate,
      boolean vector,
      boolean all,
      java.util.function.Consumer<byte[]> ids,
      Runnable check)
      throws IOException, ReaderClosedException, InvalidQueryException {
    this.indexReader.collectDocumentIds(predicate, vector, all, ids, check);
  }

  @Override
  public com.xgen.mongot.index.statistics.LuceneStatisticsSnapshot acquireStatisticsSnapshot()
      throws IOException, ReaderClosedException {
    return this.indexReader.acquireStatisticsSnapshot();
  }

  private final SearchIndexReader indexReader;
  private final IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater;
  private final QueryMetricsRecorder queryMetricsRecorder;
  private final BooleanSupplier recordTotalFacetBucketsMetric;

  public MeteredSearchIndexReader(
      SearchIndexReader indexReader,
      IndexMetricsUpdater.QueryingMetricsUpdater queryingMetricsUpdater,
      BooleanSupplier recordTotalFacetBucketsMetric) {
    this.indexReader = indexReader;
    this.queryingMetricsUpdater = queryingMetricsUpdater;
    this.recordTotalFacetBucketsMetric = recordTotalFacetBucketsMetric;
    this.queryMetricsRecorder =
        new QueryMetricsRecorder(
            queryingMetricsUpdater.getQueryFeaturesMetricsUpdater(),
            queryingMetricsUpdater.getSearchAggregationMetricsReporter());
  }

  @Override
  public SearchProducerAndMetaResults query(
      Query query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    return query(
        query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags, Optional.empty());
  }

  private SearchProducerAndMetaResults query(
      Query query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags,
      Optional<QuerySnapshot> snapshot)
      throws IOException, InvalidQueryException, InterruptedException {
    this.queryingMetricsUpdater.getTotalQueryCounter().increment();
    try {
      SearchProducerAndMetaResults result =
          snapshot.isPresent()
              ? snapshot
                  .get()
                  .query(query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags)
              : this.indexReader.query(
                  query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
      this.queryingMetricsUpdater.recordTotalFacetBucketsIfApplicable(
          query, this.recordTotalFacetBucketsMetric);
      this.queryMetricsRecorder.record(query, queryCursorOptions);
      return result;
    } catch (Exception e) {
      this.queryingMetricsUpdater.handleQueryException(
          e, "from SearchIndexReader.query, shape=" + QueryShape.of(query));
      throw e;
    }
  }

  @Override
  public SearchProducerAndMetaProducer intermediateQuery(
      SearchQuery query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags)
      throws IOException, InvalidQueryException, InterruptedException {
    return intermediateQuery(
        query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags, Optional.empty());
  }

  private SearchProducerAndMetaProducer intermediateQuery(
      SearchQuery query,
      QueryCursorOptions queryCursorOptions,
      BatchSizeStrategy batchSizeStrategy,
      QueryOptimizationFlags queryOptimizationFlags,
      Optional<QuerySnapshot> snapshot)
      throws IOException, InvalidQueryException, InterruptedException {
    this.queryingMetricsUpdater.getTotalQueryCounter().increment();
    try {
      SearchProducerAndMetaProducer result =
          snapshot.isPresent()
              ? snapshot
                  .get()
                  .intermediateQuery(
                      query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags)
              : this.indexReader.intermediateQuery(
                  query, queryCursorOptions, batchSizeStrategy, queryOptimizationFlags);
      this.queryingMetricsUpdater.recordTotalFacetBucketsIfApplicable(
          query, this.recordTotalFacetBucketsMetric);
      this.queryMetricsRecorder.record(query, queryCursorOptions);
      return result;
    } catch (Exception e) {
      this.queryingMetricsUpdater.handleQueryException(
          e, "from SearchIndexReader.intermediateQuery, shape=" + QueryShape.of(query));
      throw e;
    }
  }

  @Override
  public long getNumEmbeddedRootDocuments() throws IOException, ReaderClosedException {
    return this.indexReader.getNumEmbeddedRootDocuments();
  }

  @Override
  public List<FieldInfos> getFieldInfos() throws IOException, ReaderClosedException {
    return this.indexReader.getFieldInfos();
  }

  @Override
  public TokenFacetsStateCache.Snapshot getTokenFacetsStateCacheSnapshot() {
    return this.indexReader.getTokenFacetsStateCacheSnapshot();
  }

  @Override
  public void refresh() throws IOException, ReaderClosedException {
    this.indexReader.refresh();
  }

  @Override
  public QuerySnapshot acquireQuerySnapshot(boolean concurrent)
      throws IOException, ReaderClosedException {
    return new MeteredQuerySnapshot(this.indexReader.acquireQuerySnapshot(concurrent));
  }

  private final class MeteredQuerySnapshot implements QuerySnapshot {
    private final QuerySnapshot snapshot;

    private MeteredQuerySnapshot(QuerySnapshot snapshot) {
      this.snapshot = snapshot;
    }

    @Override
    public int partitions() {
      return this.snapshot.partitions();
    }

    @Override
    public void setCancellationCheck(Runnable check) {
      this.snapshot.setCancellationCheck(check);
    }

    @Override
    public void disableQueryCache() {
      this.snapshot.disableQueryCache();
    }

    @Override
    public List<JoinField> joinFields(FieldPath path) throws InvalidQueryException {
      return this.snapshot.joinFields(path);
    }

    @Override
    public void collectKeys(
        Operator operator,
        FieldPath path,
        JoinField.Type type,
        Consumer<BytesRef> keys,
        Runnable checkWork)
        throws IOException, InvalidQueryException {
      this.snapshot.collectKeys(operator, path, type, keys, checkWork);
    }

    @Override
    public SearchProducerAndMetaResults query(
        Query query,
        QueryCursorOptions options,
        BatchSizeStrategy strategy,
        QueryOptimizationFlags flags)
        throws IOException, InvalidQueryException, InterruptedException {
      return MeteredSearchIndexReader.this.query(
          query, options, strategy, flags, Optional.of(this.snapshot));
    }

    @Override
    public SearchProducerAndMetaProducer intermediateQuery(
        SearchQuery query,
        QueryCursorOptions options,
        BatchSizeStrategy strategy,
        QueryOptimizationFlags flags)
        throws IOException, InvalidQueryException, InterruptedException {
      return MeteredSearchIndexReader.this.intermediateQuery(
          query, options, strategy, flags, Optional.of(this.snapshot));
    }

    @Override
    public void close() throws IOException {
      this.snapshot.close();
    }
  }

  @Override
  public void close() {
    this.indexReader.close();
  }

  @Override
  public void open() {
    this.indexReader.open();
  }

  @VisibleForTesting
  public SearchIndexReader unwrap() {
    return this.indexReader;
  }
}
