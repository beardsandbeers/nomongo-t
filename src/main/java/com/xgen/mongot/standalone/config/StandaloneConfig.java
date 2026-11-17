package com.xgen.mongot.standalone.config;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.net.HostAndPort;
import com.xgen.mongot.extensions.join.JoinPlanner.MemoryLimits;
import com.xgen.mongot.standalone.catalog.LocalIndexCatalog;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.jetbrains.annotations.Nullable;

/** Standalone-only configuration. There is no sync source or MongoDB connection configuration. */
public record StandaloneConfig(
    Path dataPath,
    InetSocketAddress address,
    Limits documentLimits,
    int maxInFlightRequests,
    long maxInFlightBytes,
    long minFreeDiskBytes,
    Duration commitInterval,
    Duration refreshInterval,
    Duration maxRpcDuration,
    Optional<Tls> tls,
    List<Principal> principals,
    boolean sharedListener,
    MemoryLimits joinMemoryLimits) {

  public record Limits(
      int maxDocumentBytes, int maxDepth, int maxBatchDocuments, int maxBatchBytes) {
    public Limits {
      if (maxDocumentBytes < 5
          || maxDepth < 1
          || maxBatchDocuments < 1
          || maxBatchBytes < maxDocumentBytes) {
        throw new IllegalArgumentException("Invalid BSON limits");
      }
    }

    public static Limits defaults() {
      return new Limits(16777216, 100, 1000, 33554432);
    }
  }

  public enum Role {
    CATALOG,
    WRITE,
    QUERY
  }

  public record Tls(Path certificate, Path privateKey) {}

  /**
   * Only a token digest is retained in memory. Roles and index grants are checked on every request.
   */
  public record Principal(String name, byte[] tokenDigest, Set<Role> roles, Set<String> indexes) {
    public boolean allows(String indexId) {
      return this.indexes.contains("*") || this.indexes.contains(indexId);
    }
  }

  /** Configuration for an API-owned listener, retaining its existing security requirements. */
  public StandaloneConfig(
      Path dataPath,
      InetSocketAddress address,
      Limits documentLimits,
      int maxInFlightRequests,
      long maxInFlightBytes,
      long minFreeDiskBytes,
      Duration commitInterval,
      Duration refreshInterval,
      Duration maxRpcDuration,
      Optional<Tls> tls,
      List<Principal> principals) {
    this(
        dataPath,
        address,
        documentLimits,
        maxInFlightRequests,
        maxInFlightBytes,
        minFreeDiskBytes,
        commitInterval,
        refreshInterval,
        maxRpcDuration,
        tls,
        principals,
        false);
  }

  /** Configuration with default join memory limits. */
  public StandaloneConfig(
      Path dataPath,
      InetSocketAddress address,
      Limits documentLimits,
      int maxInFlightRequests,
      long maxInFlightBytes,
      long minFreeDiskBytes,
      Duration commitInterval,
      Duration refreshInterval,
      Duration maxRpcDuration,
      Optional<Tls> tls,
      List<Principal> principals,
      boolean sharedListener) {
    this(
        dataPath,
        address,
        documentLimits,
        maxInFlightRequests,
        maxInFlightBytes,
        minFreeDiskBytes,
        commitInterval,
        refreshInterval,
        maxRpcDuration,
        tls,
        principals,
        sharedListener,
        MemoryLimits.defaults());
  }

  /** Validates resource limits and external-listener security before starting any services. */
  @SuppressWarnings("Var") // A compact record constructor normalizes its final component value.
  public StandaloneConfig {
    principals = List.copyOf(principals);
    Objects.requireNonNull(joinMemoryLimits);
    if (address.isUnresolved()
        || (!sharedListener
            && !address.getAddress().isLoopbackAddress()
            && (tls.isEmpty() || principals.isEmpty()))) {
      throw new IllegalArgumentException(
          "Non-loopback listeners require TLS and authenticated principals");
    }
    if (maxInFlightRequests < 1
        || maxInFlightRequests > 1024
        || maxInFlightBytes < documentLimits.maxBatchBytes()
        || minFreeDiskBytes < 0
        || commitInterval.isNegative()
        || commitInterval.isZero()
        || refreshInterval.isNegative()
        || refreshInterval.isZero()
        || maxRpcDuration.isNegative()
        || maxRpcDuration.isZero()
        || documentLimits.maxDocumentBytes() > 16 * 1024 * 1024
        || documentLimits.maxBatchBytes() > Integer.MAX_VALUE - 65536) {
      throw new IllegalArgumentException("Standalone limits or timing configuration is invalid");
    }
  }

  /** Reads a bounded, strictly typed YAML file; unknown and duplicate settings are rejected. */
  public static StandaloneConfig fromFile(Path configPath) throws IOException {
    if (Files.size(configPath) > 65536) {
      throw new IllegalArgumentException("Standalone configuration exceeds 64 KiB");
    }
    return fromText(Files.readString(configPath), configPath);
  }

  /**
   * Native listeners retain upstream transport policy; the API interceptor protects remote calls.
   */
  public static StandaloneConfig fromSharedListenerText(String text, Path configPath)
      throws IOException {
    return fromText(text, configPath, true);
  }

  /** Parses an adapter-produced standalone document using the same strict validation. */
  public static StandaloneConfig fromText(String text, Path configPath) throws IOException {
    return fromText(text, configPath, false);
  }

  private static StandaloneConfig fromText(String text, Path configPath, boolean sharedListener)
      throws IOException {
    return fromText(text, configPath, sharedListener, System.getenv());
  }

  static StandaloneConfig fromText(
      String text, Path configPath, boolean sharedListener, Map<String, String> environment)
      throws IOException {
    var mapper =
        JsonMapper.builder(
                YAMLFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .build();
    var raw = mapper.readValue(text, FileConfig.class);
    if (!or(raw.mode(), "standalone").equals("standalone")) {
      throw new IllegalArgumentException("Configuration mode must be standalone");
    }
    Path base = Objects.requireNonNull(configPath.toAbsolutePath().getParent());
    var storage = or(raw.storage(), new Storage(null));
    if (storage.dataPath() == null || storage.dataPath().isBlank()) {
      throw new IllegalArgumentException("storage.dataPath is required");
    }
    var server = or(raw.server(), new Server(null));
    var grpc = or(server.grpc(), new Grpc(null, null, null, null));
    var host =
        HostAndPort.fromString(or(grpc.address(), "127.0.0.1:27029")).requireBracketsForIPv6();
    var address = new InetSocketAddress(InetAddress.getByName(host.getHost()), host.getPort());
    Optional<Tls> tls =
        Optional.ofNullable(grpc.tls())
            .map(
                value ->
                    new Tls(resolve(base, value.certificate()), resolve(base, value.privateKey())));
    var principals = new ArrayList<Principal>();
    var names = new HashSet<String>();
    var digests = new HashSet<String>();
    for (var principal : or(grpc.principals(), List.<PrincipalConfig>of())) {
      if (principal.name().isBlank() || !names.add(principal.name())) {
        throw new IllegalArgumentException("Principal names must be nonempty and unique");
      }
      Path tokenPath = resolve(base, principal.tokenFile());
      if (Files.size(tokenPath) > 4097) {
        throw new IllegalArgumentException("Bearer token file is too large");
      }
      String token = Files.readString(tokenPath, StandardCharsets.UTF_8).strip();
      if (!token.matches("[!-~]{32,4096}")) {
        throw new IllegalArgumentException(
            "Bearer tokens must contain 32 to 4096 non-whitespace ASCII characters");
      }
      byte[] digest = LocalIndexCatalog.digest(token.getBytes(StandardCharsets.US_ASCII));
      if (!digests.add(HexFormat.of().formatHex(digest))) {
        throw new IllegalArgumentException("Bearer tokens must be unique across principals");
      }
      var roles = new HashSet<Role>();
      for (String role : principal.roles()) {
        roles.add(Role.valueOf(role.toUpperCase(Locale.ROOT)));
      }
      if (roles.isEmpty() || principal.indexes().isEmpty()) {
        throw new IllegalArgumentException("Principals require explicit roles and indexes");
      }
      for (String index : principal.indexes()) {
        if (!index.equals("*")) {
          LocalIndexCatalog.validateIndexId(index);
        }
      }
      principals.add(
          new Principal(
              principal.name(), digest, Set.copyOf(roles), Set.copyOf(principal.indexes())));
    }
    var ingestion =
        or(raw.ingestion(), new Ingestion(null, null, null, null, null, null, null, null));
    var querying = or(raw.querying(), new Querying(null, null));
    var joins = or(querying.joins(), new Joins(null, null));
    long maxConcurrentJoinBytes =
        memoryLimit(
            environment,
            "MONGOT_JOIN_MAX_CONCURRENT_BYTES",
            or(joins.maxConcurrentBytes(), MemoryLimits.defaults().maxConcurrentBytes()));
    var defaultJoinLimits = MemoryLimits.fromConcurrentBytes(maxConcurrentJoinBytes);
    return new StandaloneConfig(
        resolve(base, storage.dataPath()),
        address,
        new Limits(
            or(ingestion.maxDocumentBytes(), 16777216),
            or(ingestion.maxNestingDepth(), 100),
            or(ingestion.maxBatchDocuments(), 1000),
            or(ingestion.maxBatchBytes(), 33554432)),
        or(ingestion.maxInFlightRequests(), 64),
        or(ingestion.maxQueuedBytes(), 268435456L),
        or(ingestion.minFreeDiskBytes(), 268435456L),
        Duration.ofMillis(or(ingestion.commitIntervalMillis(), 1000L)),
        Duration.ofMillis(or(querying.refreshIntervalMillis(), 1000L)),
        Duration.ofMillis(or(grpc.maxRpcDurationMillis(), 60000L)),
        tls,
        principals,
        sharedListener,
        new MemoryLimits(
            memoryLimit(
                environment,
                "MONGOT_JOIN_MAX_QUERY_BYTES",
                or(joins.maxQueryBytes(), defaultJoinLimits.maxQueryBytes())),
            maxConcurrentJoinBytes));
  }

  private static long memoryLimit(Map<String, String> environment, String name, long fallback) {
    String value = environment.get(name);
    if (value == null) {
      return fallback;
    }
    try {
      long bytes = Long.parseLong(value);
      if (bytes > 0) {
        return bytes;
      }
    } catch (NumberFormatException ignored) {
      // Report the setting name for malformed and overflowing values alike.
    }
    throw new IllegalArgumentException(name + " must be a positive integer byte count");
  }

  public static StandaloneConfig development(Path dataPath, int port) {
    return new StandaloneConfig(
        dataPath,
        new InetSocketAddress(InetAddress.getLoopbackAddress(), port),
        Limits.defaults(),
        64,
        268435456L,
        0,
        Duration.ofSeconds(1),
        Duration.ofMillis(50),
        Duration.ofSeconds(60),
        Optional.empty(),
        List.of());
  }

  private static <T> T or(@Nullable T value, T fallback) {
    return value == null ? fallback : value;
  }

  private static Path resolve(Path base, String value) {
    return base.resolve(value).normalize().toAbsolutePath();
  }

  private record FileConfig(
      @Nullable String mode,
      @Nullable Storage storage,
      @Nullable Server server,
      @Nullable Ingestion ingestion,
      @Nullable Querying querying) {}

  private record Storage(@Nullable String dataPath) {}

  private record Server(@Nullable Grpc grpc) {}

  private record Grpc(
      @Nullable String address,
      @Nullable TlsConfig tls,
      @Nullable List<PrincipalConfig> principals,
      @Nullable Long maxRpcDurationMillis) {}

  private record TlsConfig(String certificate, String privateKey) {}

  private record PrincipalConfig(
      String name, String tokenFile, List<String> roles, List<String> indexes) {}

  private record Querying(@Nullable Long refreshIntervalMillis, @Nullable Joins joins) {}

  private record Joins(@Nullable Long maxQueryBytes, @Nullable Long maxConcurrentBytes) {}

  private record Ingestion(
      @Nullable Integer maxInFlightRequests,
      @Nullable Integer maxDocumentBytes,
      @Nullable Integer maxNestingDepth,
      @Nullable Integer maxBatchDocuments,
      @Nullable Integer maxBatchBytes,
      @Nullable Long maxQueuedBytes,
      @Nullable Long minFreeDiskBytes,
      @Nullable Long commitIntervalMillis) {}
}
