package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.IndexMetricsUpdater.IndexingMetricsUpdater;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.Priority;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.SchedulerBatch;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import org.bson.types.ObjectId;

public class IndexingSchedulerBatch extends SchedulerBatch {
  public final List<DocumentEvent> events;
  public final DocumentIndexer indexer;
  final Optional<Callable<Void>> operation;
  public final Optional<IndexCommitUserData> commitUserData;
  // Excludes this field for serialization, deserialization, toString, hashcode and equals
  public final transient IndexingMetricsUpdater indexingMetricsUpdater;

  IndexingSchedulerBatch(
      List<DocumentEvent> events,
      Priority priority,
      DocumentIndexer indexer,
      CompletableFuture<Void> future,
      GenerationId generationId,
      Optional<ObjectId> attemptId,
      Optional<IndexCommitUserData> commitUserData,
      IndexingMetricsUpdater indexingMetricsUpdater) {
    this(
        events,
        priority,
        indexer,
        future,
        generationId,
        attemptId,
        commitUserData,
        indexingMetricsUpdater,
        Optional.empty());
  }

  IndexingSchedulerBatch(
      List<DocumentEvent> events,
      Priority priority,
      DocumentIndexer indexer,
      CompletableFuture<Void> future,
      GenerationId generationId,
      Optional<ObjectId> attemptId,
      Optional<IndexCommitUserData> commitUserData,
      IndexingMetricsUpdater indexingMetricsUpdater,
      Optional<Callable<Void>> operation) {
    super(priority, future, generationId, attemptId);
    this.operation = operation;
    this.events = events;
    this.indexer = indexer;
    this.commitUserData = commitUserData;
    this.indexingMetricsUpdater = indexingMetricsUpdater;
  }

  @Override
  public String toString() {
    return "IndexingSchedulerBatch{"
        + "size="
        + this.events.size()
        + ", priority="
        + this.priority
        + ", generationId="
        + this.generationId
        + ", commitUserData="
        + this.commitUserData
        + ", sequenceNumber="
        + this.sequenceNumber
        + '}';
  }

  @Override
  public int size() {
    return this.events.size();
  }
}
