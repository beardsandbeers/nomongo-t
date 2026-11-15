package com.xgen.mongot.standalone.ingestion;

import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.util.regex.RegexBudget;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/** API policy over the ordinary writer; no replication checkpoint is accepted or manufactured. */
final class ApiDocumentIndexer implements DocumentIndexer {
  private final InitializedIndex index;

  ApiDocumentIndexer(InitializedIndex index) {
    this.index = index;
  }

  @Override
  public void indexDocumentEvent(DocumentEvent event) throws FieldExceededLimitsException {
    try (var ignored = RegexBudget.openScope()) {
      this.index.getWriter().updateIndex(event);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public List<DocumentEvent> prepareBatch(List<DocumentEvent> events) {
    try {
      return this.index.getWriter().prepareBatch(events);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void commit() throws IOException {
    this.index.getWriter().commit(EncodedUserData.EMPTY);
  }

  @Override
  public void stageCommitUserData() {}

  @Override
  public void updateCommitUserData(IndexCommitUserData data) {
    throw new UnsupportedOperationException("Push indexes have no replication checkpoint");
  }

  @Override
  public void updateCommitUserDataSupplier(Supplier<IndexCommitUserData> data) {
    throw new UnsupportedOperationException("Push indexes have no replication checkpoint");
  }

  @Override
  public void clearIndex(IndexCommitUserData data) {
    throw new UnsupportedOperationException("Push rebuilds use a new generation");
  }

  @Override
  public Optional<ExceededLimitsException> exceededLimits() {
    return this.index.getWriter().exceededLimits();
  }

  @Override
  public IndexDefinition getIndexDefinition() {
    return this.index.getDefinition();
  }
}
