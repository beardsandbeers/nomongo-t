package com.xgen.mongot.standalone.config;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;

/** Adapts optional API settings to the automatically combined native runtime. */
public final class CoexistenceConfig {
  private CoexistenceConfig() {}

  /** True when a source block is supplied; malformed source configuration must fail validation. */
  public static boolean hasSyncSource(Optional<Path> path) throws IOException {
    if (path.isEmpty()) {
      return false;
    }
    // Source detection must not tighten the existing native YAML parser's rules.
    var mapper = JsonMapper.builder(new YAMLFactory()).build();
    var config = mapper.readTree(Files.readString(path.get()));
    return config != null && config.has("syncSource");
  }

  /** Reads extension settings while retaining the native listener and storage configuration. */
  public static Optional<StandaloneConfig> fromFile(
      Optional<Path> configPath, Path dataPath, InetSocketAddress address, boolean nativeTls)
      throws IOException {
    var mapper = JsonMapper.builder(new YAMLFactory()).build();
    var nativeConfig =
        configPath.isPresent()
            ? mapper.readTree(Files.readString(configPath.get()))
            : mapper.createObjectNode();
    // Retain native parsing when only native settings are supplied. Explicit API settings keep
    // the standalone parser's duplicate-key validation.
    var extension =
        nativeConfig.has("standalone")
            ? JsonMapper.builder(
                    YAMLFactory.builder()
                        .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                        .build())
                .build()
                .readTree(Files.readString(configPath.orElseThrow()))
                .get("standalone")
            : mapper.createObjectNode();
    if (!extension.isObject()) {
      throw new IllegalArgumentException("standalone settings must be an object");
    }
    var fields = extension.fieldNames();
    while (fields.hasNext()) {
      String field = fields.next();
      if (!Set.of("grpc", "ingestion").contains(field)) {
        throw new IOException(
            "Unknown standalone configuration field: "
                + field
                + "; MongoDB configuration automatically enables both ingestion paths");
      }
    }
    var settings = ((ObjectNode) extension).deepCopy();
    var grpc = settings.has("grpc") ? settings.remove("grpc") : mapper.createObjectNode();
    if (!grpc.isObject()) {
      throw new IllegalArgumentException("standalone.grpc must be an object");
    }
    var listener = ((ObjectNode) grpc).remove("listener");
    if (listener != null && !listener.asText().equals("shared")) {
      throw new IllegalArgumentException("standalone.grpc.listener must be shared");
    }
    if (grpc.has("address") || grpc.has("tls")) {
      throw new IllegalArgumentException(
          "Shared listener address/TLS are configured by server.grpc");
    }
    ((ObjectNode) grpc)
        .put(
            "address",
            HostAndPort.fromParts(address.getAddress().getHostAddress(), address.getPort())
                .toString());
    if (nativeTls) {
      var tls = mapper.createObjectNode();
      tls.put("certificate", "unused-shared-listener");
      tls.put("privateKey", "unused-shared-listener");
      ((ObjectNode) grpc).set("tls", tls);
    }
    settings.put("mode", "standalone");
    var storage = mapper.createObjectNode();
    storage.put("dataPath", dataPath.toAbsolutePath().toString());
    settings.set("storage", storage);
    var server = mapper.createObjectNode();
    server.set("grpc", grpc);
    settings.set("server", server);
    var parsed =
        StandaloneConfig.fromSharedListenerText(
            settings.toString(), configPath.orElse(Path.of("mongot.yaml")));
    return Optional.of(parsed);
  }
}
