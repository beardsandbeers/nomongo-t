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

/** Strict adapter for the optional native-launch standalone block. */
public final class CoexistenceConfig {
  private CoexistenceConfig() {}

  /** Reads extension settings while retaining the native listener and storage configuration. */
  public static Optional<StandaloneConfig> fromFile(
      Optional<Path> configPath, Path dataPath, InetSocketAddress address, boolean nativeTls)
      throws IOException {
    if (configPath.isEmpty()) {
      return Optional.empty();
    }
    String text = Files.readString(configPath.get());
    // Native-only launches retain their existing permissive YAML behavior.
    if (!JsonMapper.builder(new YAMLFactory()).build().readTree(text).has("standalone")) {
      return Optional.empty();
    }
    var mapper =
        JsonMapper.builder(
                YAMLFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
            .build();
    var nativeConfig = mapper.readTree(text);
    var extension = nativeConfig.get("standalone");
    if (extension == null) {
      return Optional.empty();
    }
    if (!extension.isObject() || !extension.path("enabled").isBoolean()) {
      throw new IllegalArgumentException("standalone.enabled must be an explicit boolean");
    }
    var fields = extension.fieldNames();
    while (fields.hasNext()) {
      String field = fields.next();
      if (!Set.of("enabled", "grpc", "ingestion").contains(field)) {
        throw new IOException("Unknown standalone configuration field: " + field);
      }
    }
    var settings = ((ObjectNode) extension).deepCopy();
    boolean enabled = settings.remove("enabled").booleanValue();
    if (!enabled) {
      return Optional.empty();
    }
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
    var parsed = StandaloneConfig.fromText(settings.toString(), configPath.get());
    return Optional.of(parsed);
  }
}
