package com.xgen.mongot.index.lucene.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;

/** Marks a preserved generation whose writer must use APPEND and never create an empty index. */
public final class ExistingIndexDirectory extends FilterDirectory {
  public ExistingIndexDirectory(Directory delegate) {
    super(delegate);
  }
}
