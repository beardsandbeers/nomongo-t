package com.xgen.mongot.index.lucene.streaming;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.util.Collections;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;

public class PointRangeMembershipTest {
  @Test
  public void test_collidingDocumentIds_checksExactBatchMembership() throws IOException {
    var outside = new Document();
    outside.add(new LongPoint("price", 5));
    var inside = new Document();
    inside.add(new LongPoint("price", 10));
    try (var dir = new ByteBuffersDirectory();
        var writer = new IndexWriter(dir, new IndexWriterConfig())) {
      writer.addDocuments(Collections.nCopies(2, outside));
      writer.addDocuments(Collections.nCopies(8190, new Document()));
      writer.addDocuments(Collections.nCopies(2, inside));
      writer.forceMerge(1);
      try (var reader = DirectoryReader.open(writer)) {
        var query = (PointRangeQuery) LongPoint.newRangeQuery("price", 10, 20);
        var membership =
            PointRangeMembership.compile(reader.leaves().getFirst().reader(), query, null);
        var matches = new boolean[3];

        membership.test(new int[] {0, 1, 8192}, matches);

        assertThat(matches).asList().containsExactly(false, false, true).inOrder();
      }
    }
  }
}
