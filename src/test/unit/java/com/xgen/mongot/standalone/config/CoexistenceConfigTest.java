package com.xgen.mongot.standalone.config;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CoexistenceConfigTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void fromFile_nativeSource_enablesLocalServicesByDefault() throws Exception {
    var file = config("syncSource: {replicaSet: {hostAndPort: localhost:27017}}\n");

    var parsed = CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false);

    assertThat(parsed).isPresent();
  }

  @Test
  public void fromFile_sharedExtension_usesNativeAddressAndStorage() throws Exception {
    var file = config("standalone:\n  grpc: {listener: shared}\n");

    var parsed =
        CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false).orElseThrow();

    assertThat(parsed.address()).isEqualTo(address());
    assertThat(parsed.dataPath()).isEqualTo(data().toAbsolutePath());
    assertThat(parsed.documentLimits()).isEqualTo(StandaloneConfig.Limits.defaults());
  }

  @Test
  public void fromFile_unknownExtensionSetting_rejectsTypo() throws Exception {
    var file = config("standalone:\n  ingestoin: {}\n");

    assertThrows(
        IOException.class,
        () -> CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false));
  }

  @Test
  public void fromFile_listenerOverride_rejectsNativeTransportChange() throws Exception {
    var file = config("standalone:\n  grpc: {address: localhost:1234}\n");

    assertThrows(
        IllegalArgumentException.class,
        () -> CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false));
  }

  @Test
  public void fromFile_nonLoopbackWithoutTokens_preservesNativeListener() throws Exception {
    var file = config("standalone: {}\n");

    var parsed =
        CoexistenceConfig.fromFile(
                Optional.of(file), data(), new InetSocketAddress("0.0.0.0", 27028), false)
            .orElseThrow();

    assertThat(parsed.sharedListener()).isTrue();
    assertThat(parsed.tls()).isEmpty();
    assertThat(parsed.principals()).isEmpty();
  }

  @Test
  public void hasSyncSource_withoutSource_selectsStandalone() throws Exception {
    var file = config("storage: {dataPath: data}\n");
    assertThat(CoexistenceConfig.hasSyncSource(Optional.of(file))).isFalse();
  }

  @Test
  public void fromFile_duplicateNativeSettings_preservesNativeParsing() throws Exception {
    var file =
        config(
            "syncSource: {replicaSet: {hostAndPort: localhost:27017}}\n"
                + "logging: {verbosity: INFO}\nlogging: {verbosity: DEBUG}\n");

    assertThat(CoexistenceConfig.hasSyncSource(Optional.of(file))).isTrue();
    assertThat(CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false)).isPresent();
  }

  @Test
  public void hasSyncSource_withSource_selectsDual() throws Exception {
    var file = config("syncSource: {replicaSet: {hostAndPort: localhost:27017}}\n");
    assertThat(CoexistenceConfig.hasSyncSource(Optional.of(file))).isTrue();
  }

  @Test
  public void hasSyncSource_malformedSource_stillRequiresNativeValidation() throws Exception {
    var file = config("syncSource: null\n");
    assertThat(CoexistenceConfig.hasSyncSource(Optional.of(file))).isTrue();
  }

  @Test
  public void fromFile_standaloneStorageOverride_rejectsConflictingDataDirectory()
      throws Exception {
    var file = config("standalone: {storage: {dataPath: elsewhere}}\n");

    assertThrows(
        IOException.class,
        () -> CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false));
  }

  @Test
  public void fromFile_explicitEnableFlag_rejectsObsoleteSelector() throws Exception {
    var file = config("standalone: {enabled: false}\n");
    assertThrows(
        IOException.class,
        () -> CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false));
  }

  @Test
  public void fromFile_cliOnlySource_usesDefaults() throws Exception {
    assertThat(CoexistenceConfig.fromFile(Optional.empty(), data(), address(), false)).isPresent();
  }

  private InetSocketAddress address() {
    return new InetSocketAddress("127.0.0.1", 27028);
  }

  private Path data() {
    return this.folder.getRoot().toPath().resolve("data");
  }

  private Path config(String yaml) throws IOException {
    return Files.writeString(this.folder.getRoot().toPath().resolve("native.yaml"), yaml);
  }
}
