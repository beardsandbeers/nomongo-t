package com.xgen.mongot.index.query.operators;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.apache.lucene.util.BytesRef;
import org.bson.BsonDocument;
import org.junit.Test;

public class JoinOperatorTest {
  private static final String JOIN =
      """
      {join:{from:'accounts',localField:'account.key',foreignField:'key',
        operator:{equals:{path:'region',value:'Berlin'}}}}
      """;

  @Test
  public void fromBson_namedIndex_roundTripsWithForeignPredicate() throws Exception {
    var operator = (JoinOperator) parse(JOIN);

    var roundTrip = parse(operator.toBson().toJson());

    assertThat(operator.from()).hasValue(new JoinSource.StandaloneIndex("accounts"));
    assertThat(operator.localField()).isEqualTo(FieldPath.parse("account.key"));
    assertThat(operator.foreignField()).isEqualTo(FieldPath.parse("key"));
    assertThat(operator.operator()).isInstanceOf(EqualsOperator.class);
    assertThat(roundTrip).isEqualTo(operator);
  }

  @Test
  public void fromBson_omittedFrom_preservesSelfJoin() throws Exception {
    var operator = (JoinOperator) parse(JOIN.replace("from:'accounts',", ""));

    assertThat(operator.from()).isEmpty();
    assertThat(parse(operator.toBson().toJson())).isEqualTo(operator);
  }

  @Test
  public void toBson_materializedKeys_keepsInternalMembershipOutOfWireFormat() throws Exception {
    var parsed = (JoinOperator) parse(JOIN);
    var keys = new JoinKeySet.Builder(ignored -> {});
    keys.add(new BytesRef("private-membership-key"));
    var materialized =
        new MaterializedJoinOperator(
            parsed,
            new JoinField(JoinField.Type.TOKEN, StockNormalizerName.NONE),
            keys.build(),
            false,
            1000);

    var bson = materialized.toBson();

    assertThat(bson).isEqualTo(parsed.toBson());
    assertThat(bson.toJson()).doesNotContain("private-membership-key");
  }

  @Test
  public void fromBson_blankFrom_rejectsInvalidName() {
    assertThrows(BsonParseException.class, () -> parse(JOIN.replace("'accounts'", "' '")));
  }

  @Test
  public void fromBson_objectFrom_rejectsOldNestedSyntax() {
    assertThrows(
        BsonParseException.class,
        () -> parse(JOIN.replace("from:'accounts'", "from:{indexId:'accounts'}")));
  }

  @Test
  public void fromBson_missingLocalField_rejectsMissingPath() {
    assertThrows(
        BsonParseException.class, () -> parse(JOIN.replace("localField:'account.key',", "")));
  }

  @Test
  public void fromBson_missingForeignField_rejectsMissingPath() {
    assertThrows(BsonParseException.class, () -> parse(JOIN.replace("foreignField:'key',", "")));
  }

  @Test
  public void fromBson_missingOperator_rejectsImplicitFullScan() {
    assertThrows(
        BsonParseException.class,
        () -> parse("{join:{from:'accounts',localField:'key',foreignField:'key'}}"));
  }

  @Test
  public void fromBson_scoreOption_rejectsJoinScoring() {
    assertThrows(
        BsonParseException.class,
        () -> parse(JOIN.replace("from:'accounts'", "score:{constant:{value:1}},from:'accounts'")));
  }

  @Test
  public void fromBson_foreignSearchOptions_rejectsFullQueryInsteadOfOperator() {
    assertThrows(
        BsonParseException.class,
        () -> parse(JOIN.replace("operator:{equals:", "operator:{index:'accounts',equals:")));
  }

  @Test
  public void fromBson_collectionSource_defaultsIndexAndRoundTrips() throws Exception {
    var operator =
        (JoinOperator) parse(JOIN.replace("from:'accounts'", "from:{collection:'accounts'}"));

    assertThat(operator.from()).hasValue(new JoinSource.CollectionIndex("accounts", "default"));
    assertThat(parse(operator.toBson().toJson())).isEqualTo(operator);
  }

  @Test
  public void fromBson_collectionSource_preservesCustomIndex() throws Exception {
    var operator =
        (JoinOperator)
            parse(JOIN.replace("from:'accounts'", "from:{collection:'accounts',index:'active'}"));

    assertThat(operator.from()).hasValue(new JoinSource.CollectionIndex("accounts", "active"));
    assertThat(parse(operator.toBson().toJson())).isEqualTo(operator);
  }

  @Test
  public void fromBson_nullIndex_rejectsRatherThanDefaulting() {
    assertThrows(
        BsonParseException.class,
        () -> parse(JOIN.replace("from:'accounts'", "from:{collection:'accounts',index:null}")));
  }

  @Test
  public void fromBson_blankCollection_rejects() {
    assertThrows(
        BsonParseException.class,
        () -> parse(JOIN.replace("from:'accounts'", "from:{collection:' '}")));
  }

  @Test
  public void fromBson_crossDatabaseSource_rejects() {
    assertThrows(
        BsonParseException.class,
        () -> parse(JOIN.replace("from:'accounts'", "from:{collection:'accounts',db:'other'}")));
  }

  private static Operator parse(String json) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(BsonDocument.parse(json)).build()) {
      return Operator.exactlyOneFromBson(parser);
    }
  }
}
