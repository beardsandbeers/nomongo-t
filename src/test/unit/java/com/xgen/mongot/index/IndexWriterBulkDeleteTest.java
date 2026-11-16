package com.xgen.mongot.index;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.util.BsonUtils;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class IndexWriterBulkDeleteTest {
  @Test
  public void deleteDocuments_fallback_preservesPreparationAndTypedIds() throws Exception {
    var writer = mock(IndexWriter.class, Mockito.CALLS_REAL_METHODS);
    when(writer.prepareBatch(any())).thenAnswer(invocation -> invocation.getArgument(0));
    var integerId = encode(new BsonDocument("_id", new BsonInt32(1)));
    var longId = encode(new BsonDocument("_id", new BsonInt64(1)));
    var applied = new AtomicInteger();

    writer.deleteDocuments(List.of(integerId, longId), () -> {}, applied::addAndGet);

    var events = ArgumentCaptor.forClass(DocumentEvent.class);
    verify(writer).prepareBatch(any());
    verify(writer, times(2)).updateIndex(events.capture());
    assertThat(events.getAllValues().stream().map(DocumentEvent::getDocumentId).toList())
        .containsExactly(new BsonInt32(1), new BsonInt64(1))
        .inOrder();
    assertThat(applied.get()).isEqualTo(2);
  }

  private static byte[] encode(BsonDocument document) {
    var raw = BsonUtils.documentToRaw(document);
    var bytes = new byte[raw.getByteBuffer().remaining()];
    raw.getByteBuffer().get(bytes);
    return bytes;
  }
}
