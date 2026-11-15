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
  public void fromFile_nativeOnly_doesNotEnableLocalServices() throws Exception {
    var file = config("syncSource: {replicaSet: {hostAndPort: localhost:27017}}\n");

    var parsed = CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false);

    assertThat(parsed).isEmpty();
  }

  @Test
  public void fromFile_sharedExtension_usesNativeAddressAndStorage() throws Exception {
    var file = config("standalone:\n  enabled: true\n  grpc: {listener: shared}\n");

    var parsed =
        CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false).orElseThrow();

    assertThat(parsed.address()).isEqualTo(address());
    assertThat(parsed.dataPath()).isEqualTo(data().toAbsolutePath());
    assertThat(parsed.documentLimits()).isEqualTo(StandaloneConfig.Limits.defaults());
  }

  @Test
  public void fromFile_unknownExtensionSetting_rejectsTypo() throws Exception {
    var file = config("standalone:\n  enabled: true\n  ingestoin: {}\n");

    assertThrows(
        IOException.class,
        () -> CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false));
  }

  @Test
  public void fromFile_listenerOverride_rejectsNativeTransportChange() throws Exception {
    var file = config("standalone:\n  enabled: true\n  grpc: {address: localhost:1234}\n");

    assertThrows(
        IllegalArgumentException.class,
        () -> CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false));
  }

  @Test
  public void fromFile_nonLoopbackWithoutTokens_rejectsUnprotectedApi() throws Exception {
    var file = config("standalone: {enabled: true}\n");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            CoexistenceConfig.fromFile(
                Optional.of(file), data(), new InetSocketAddress("0.0.0.0", 27028), true));
  }

  @Test
  public void fromFile_duplicateNativeSettingsWithoutExtension_preservesNativeParsing()
      throws Exception {
    var file = config("logging: {verbosity: INFO}\nlogging: {verbosity: DEBUG}\n");

    var parsed = CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false);

    assertThat(parsed).isEmpty();
  }

  @Test
  public void fromFile_standaloneStorageOverride_rejectsConflictingDataDirectory()
      throws Exception {
    var file = config("standalone: {enabled: true, storage: {dataPath: elsewhere}}\n");

    assertThrows(
        IOException.class,
        () -> CoexistenceConfig.fromFile(Optional.of(file), data(), address(), false));
  }

  @Test
  public void fromFile_disabledExtension_doesNotRequireApiTransportOrCredentials()
      throws Exception {
    var file = config("standalone: {enabled: false}\n");

    var parsed =
        CoexistenceConfig.fromFile(
            Optional.of(file), data(), new InetSocketAddress("0.0.0.0", 27028), false);

    assertThat(parsed).isEmpty();
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
