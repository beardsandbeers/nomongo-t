package com.xgen.mongot.cursor;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.index.IndexUnavailableException;
import com.xgen.mongot.index.InitializedSearchIndex;
import com.xgen.mongot.index.SearchIndexReader.QuerySnapshot;
import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.index.query.SearchQuery;
import com.xgen.testing.mongot.index.version.GenerationIdBuilder;
import java.util.Optional;
import org.bson.BsonDocument;
import org.junit.Test;

public class CursorFactoryTest {
  @Test
  public void createCursor_generationChanged_rejectsBeforeConsumingSnapshot() throws Exception {
    var planned = GenerationIdBuilder.create();
    var index = mock(InitializedSearchIndex.class);
    when(index.getGenerationId()).thenReturn(GenerationIdBuilder.incrementUser(planned));
    var snapshot = mock(QuerySnapshot.class);
    var query = SearchQuery.fromBson(BsonDocument.parse("{exists:{path:'key'}}"));
    var factory = new CursorFactory(CursorIdSupplier.createDefault());

    assertThrows(
        IndexUnavailableException.class,
        () ->
            factory.createCursor(
                "db.collection",
                index,
                new CursorQuery.Search(query, Optional.of(snapshot), Optional.of(planned)),
                QueryCursorOptions.empty(),
                QueryOptimizationFlags.DEFAULT_OPTIONS));

    verifyNoInteractions(snapshot);
  }
}
