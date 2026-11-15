package com.xgen.mongot.catalogservice;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.xgen.mongot.catalogservice.schema.AuthoritativeIndexKey;
import com.xgen.mongot.catalogservice.schema.IndexEntry;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import java.util.List;
import java.util.Optional;
import org.bson.BsonDocument;
import org.junit.Test;

public class DefaultAuthoritativeIndexCatalogTest {
  @Test
  public void listCompleteIndexDefinitions_unparseableEntry_rejectsPartialSnapshot()
      throws Exception {
    var source = source();
    when(source.list(new BsonDocument()))
        .thenReturn(List.of(BsonDocument.parse("{_id:'invalid'}")));
    var catalog = new DefaultAuthoritativeIndexCatalog(source);

    var error = assertThrows(MetadataServiceException.class, catalog::listCompleteIndexDefinitions);

    assertThat(error.isFailed()).isTrue();
    assertThat(catalog.listIndexDefinitions()).isEmpty();
  }

  @Test
  public void listCompleteIndexDefinitions_unparseableDefinition_rejectsOtherwiseValidRows()
      throws Exception {
    var source = source();
    var definition = SearchIndexDefinitionBuilder.VALID_INDEX;
    var valid =
        IndexEntry.create(AuthoritativeIndexKey.from(definition), definition, 1, Optional.empty())
            .toBson();
    var invalid = valid.clone();
    invalid.put("definition", new BsonDocument());
    when(source.list(new BsonDocument())).thenReturn(List.of(valid, invalid));
    var catalog = new DefaultAuthoritativeIndexCatalog(source);

    assertThrows(MetadataServiceException.class, catalog::listCompleteIndexDefinitions);

    assertThat(catalog.listIndexDefinitions()).containsExactly(definition);
  }

  @Test
  public void listCompleteIndexDefinitions_validRows_returnsNativeDefinitions() throws Exception {
    var source = source();
    var definition = SearchIndexDefinitionBuilder.VALID_INDEX;
    var entry =
        IndexEntry.create(AuthoritativeIndexKey.from(definition), definition, 1, Optional.empty())
            .toBson();
    when(source.list(new BsonDocument())).thenReturn(List.of(entry));
    var catalog = new DefaultAuthoritativeIndexCatalog(source);

    var result = catalog.listCompleteIndexDefinitions();

    assertThat(result).containsExactly(definition);
  }

  @SuppressWarnings("unchecked")
  private static MetadataClient<IndexEntry> source() {
    return mock(MetadataClient.class);
  }
}
