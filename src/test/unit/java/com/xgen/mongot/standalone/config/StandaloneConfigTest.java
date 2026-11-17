package com.xgen.mongot.standalone.config;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class StandaloneConfigTest {
  @DataPoints
  public static final String[] INVALID_MEMORY_LIMITS = {
    "0", "-1", "1.5", "not-bytes", "9223372036854775808"
  };

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void fromText_omittedJoinMemoryLimits_preservesDefaults() throws Exception {
    var parsed =
        StandaloneConfig.fromText(
            "storage: {dataPath: data}", Path.of("standalone.yaml"), false, Map.of());

    assertThat(parsed.joinMemoryLimits().maxQueryBytes()).isEqualTo(33554432L);
    assertThat(parsed.joinMemoryLimits().maxConcurrentBytes()).isEqualTo(268435456L);
  }

  @Test
  public void fromText_yamlJoinMemoryLimits_usesConfiguredBytes() throws Exception {
    var parsed =
        StandaloneConfig.fromText(
            """
            storage: {dataPath: data}
            querying:
              joins:
                maxQueryBytes: 67108864
                maxConcurrentBytes: 536870912
            """,
            Path.of("standalone.yaml"),
            false,
            Map.of());

    assertThat(parsed.joinMemoryLimits().maxQueryBytes()).isEqualTo(67108864L);
    assertThat(parsed.joinMemoryLimits().maxConcurrentBytes()).isEqualTo(536870912L);
  }

  @Test
  public void fromText_environmentJoinMemoryLimits_overrideYaml() throws Exception {
    var parsed =
        StandaloneConfig.fromText(
            "storage: {dataPath: data}\n"
                + "querying: {joins: {maxQueryBytes: 123, maxConcurrentBytes: 456}}",
            Path.of("standalone.yaml"),
            false,
            Map.of(
                "MONGOT_JOIN_MAX_QUERY_BYTES", "67108864",
                "MONGOT_JOIN_MAX_CONCURRENT_BYTES", "536870912"));

    assertThat(parsed.joinMemoryLimits().maxQueryBytes()).isEqualTo(67108864L);
    assertThat(parsed.joinMemoryLimits().maxConcurrentBytes()).isEqualTo(536870912L);
  }

  @Test
  public void fromText_oneEnvironmentJoinMemoryLimit_preservesOtherYamlLimit() throws Exception {
    var parsed =
        StandaloneConfig.fromText(
            "storage: {dataPath: data}\nquerying: {joins: {maxConcurrentBytes: 536870912}}",
            Path.of("standalone.yaml"),
            false,
            Map.of("MONGOT_JOIN_MAX_QUERY_BYTES", "67108864"));

    assertThat(parsed.joinMemoryLimits().maxQueryBytes()).isEqualTo(67108864L);
    assertThat(parsed.joinMemoryLimits().maxConcurrentBytes()).isEqualTo(536870912L);
  }

  @Theory
  public void fromText_invalidYamlJoinMemoryLimit_rejects(String value) {
    assertThrows(
        Exception.class,
        () ->
            StandaloneConfig.fromText(
                "storage: {dataPath: data}\nquerying: {joins: {maxQueryBytes: " + value + "}}",
                Path.of("standalone.yaml"),
                false,
                Map.of()));
  }

  @Theory
  public void fromText_invalidEnvironmentJoinMemoryLimit_namesSetting(String value) {
    var error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                StandaloneConfig.fromText(
                    "storage: {dataPath: data}",
                    Path.of("standalone.yaml"),
                    false,
                    Map.of("MONGOT_JOIN_MAX_CONCURRENT_BYTES", value)));

    assertThat(error).hasMessageThat().contains("MONGOT_JOIN_MAX_CONCURRENT_BYTES");
  }

  @Test
  public void fromText_blankEnvironmentJoinMemoryLimit_rejects() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StandaloneConfig.fromText(
                "storage: {dataPath: data}",
                Path.of("standalone.yaml"),
                false,
                Map.of("MONGOT_JOIN_MAX_QUERY_BYTES", "")));
  }

  @Test
  public void fromFile_minimalConfig_usesIndexStorageAndLoopbackDefaults() throws Exception {
    Path file = config("storage:\n  dataPath: data\n");

    var parsed = StandaloneConfig.fromFile(file);

    assertThat(parsed.address().getAddress().isLoopbackAddress()).isTrue();
    assertThat(parsed.dataPath()).isEqualTo(this.folder.getRoot().toPath().resolve("data"));
    assertThat(parsed.documentLimits().maxBatchDocuments()).isEqualTo(1000);
  }

  @Test
  public void fromFile_nonLoopbackWithoutSecurity_rejects() throws Exception {
    Path file =
        config("storage:\n  dataPath: data\nserver:\n  grpc:\n    address: '0.0.0.0:27029'\n");

    assertThrows(IllegalArgumentException.class, () -> StandaloneConfig.fromFile(file));
  }

  @Test
  public void fromFile_duplicateSettings_rejects() throws Exception {
    Path file = config("storage:\n  dataPath: first\n  dataPath: second\n");

    assertThrows(IOException.class, () -> StandaloneConfig.fromFile(file));
  }

  @Test
  public void fromFile_unknownMongoDbSyncSource_rejects() throws Exception {
    Path file = config("storage:\n  dataPath: data\nsyncSource: 'mongodb://localhost'\n");

    assertThrows(IOException.class, () -> StandaloneConfig.fromFile(file));
  }

  @Test
  public void fromFile_acceptanceOnlyCompletion_rejects() throws Exception {
    Path file = config("storage:\n  dataPath: data\ningestion:\n  writeCompletion: durable\n");

    assertThrows(IOException.class, () -> StandaloneConfig.fromFile(file));
  }

  @Test
  public void fromFile_bearerPrincipal_retainsDigestAndExplicitPermissions() throws Exception {
    Files.writeString(
        this.folder.getRoot().toPath().resolve("token"), "test-token-" + "x".repeat(32));
    Path file =
        config(
            "storage:\n"
                + "  dataPath: data\n"
                + "server:\n"
                + "  grpc:\n"
                + "    principals:\n"
                + "      - name: app\n"
                + "        tokenFile: token\n"
                + "        roles: [write]\n"
                + "        indexes: ['*']\n");

    var parsed = StandaloneConfig.fromFile(file);

    assertThat(parsed.principals()).hasSize(1);
    assertThat(parsed.principals().getFirst().tokenDigest()).hasLength(32);
    assertThat(parsed.principals().getFirst().roles()).containsExactly(StandaloneConfig.Role.WRITE);
  }

  private Path config(String yaml) throws IOException {
    return Files.writeString(this.folder.getRoot().toPath().resolve("standalone.yaml"), yaml);
  }
}
