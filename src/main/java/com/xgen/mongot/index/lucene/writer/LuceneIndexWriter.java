package com.xgen.mongot.index.lucene.writer;

import com.xgen.mongot.index.IndexWriter;
import com.xgen.mongot.index.WriterClosedException;

/** An extended interface of {@link IndexWriter} with Lucene-specific methods. */
public interface LuceneIndexWriter extends IndexWriter {
  /** Constructs a replacement ingestion policy without touching Lucene data or the live policy. */
  default Runnable prepareSchemaUpdate(
      org.apache.lucene.analysis.Analyzer analyzer,
      com.xgen.mongot.index.definition.SearchFieldDefinitionResolver resolver) {
    throw new UnsupportedOperationException("This writer does not support live search schemas");
  }

  long getNumLuceneMaxDocs() throws WriterClosedException;

  int getMaxLuceneMaxDocs() throws WriterClosedException;

  /** The number of {@link IndexWriter}s associated with this writer. Defaults to 1. */
  default int getNumWriters() {
    return 1;
  }
}
