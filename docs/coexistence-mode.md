# Replication and standalone indexes in one process

The native `mongot` launcher can also expose the standalone gRPC API. Replicated indexes keep
MongoDB's existing management, query, replication, and checkpoint behavior. Standalone indexes
accept direct BSON writes and use their own catalog. Both use the same engine, data directory,
ordinary indexing scheduler, and gRPC listener.

Both packaged launchers (`mongot` and `mongot-standalone`) select the ingestion paths from the
configuration. Without `syncSource`, they start standalone with no MongoDB connection attempt.
With `syncSource.replicaSet`, they start replication and the standalone API together. A malformed
source block is an error; it never silently disables replication.

Build `//deploy:mongot-standalone` to package both launchers, or build
`//deploy:mongot-community`. Keep the existing native `syncSource`, `storage`, and `server`
configuration. The `CatalogService`, `DocumentService`, `QueryService`, and `OperationsService`
services are automatically available on `server.grpc.address` alongside native MongoDB commands.
Optional standalone API limits and bearer principals go under `standalone.ingestion` and
`standalone.grpc`. No `standalone.enabled`, replication-enable flag, or Docker mode selector is
required. The old experimental `standalone.enabled` setting is rejected; remove it when upgrading.

```yaml
syncSource:
  replicaSet:
    hostAndPort: localhost:27017
    scramAuth:
      username: mongotUser
      passwordFile: /path/to/password
      authSource: admin
      tls:
        enabled: false
storage:
  dataPath: /path/to/search-data
server:
  grpc:
    address: 127.0.0.1:27028
```

Run `./mongot --config coexistence.yaml` or `./mongot-standalone --config coexistence.yaml`.
MongoDB must use its supported mongot transport; for MongoDB 8.3 set `useGrpcForSearch: true`,
`mongotHost`, and `searchIndexManagementHostAndPort` to this listener.

For the standalone container, supply the same configuration:

```sh
docker run --rm \
  -e MONGOT_CONTROL_ADDRESS=127.0.0.1:27028 \
  -e MONGOT_CONTROL_PLAINTEXT=true -e MONGOT_CONTROL_TOKEN_FILE= \
  -v /path/to/native-config.yaml:/etc/mongot/coexistence.yaml:ro \
  -v /path/to/existing-data:/var/lib/mongot/data \
  mongot-standalone:local --config /etc/mongot/coexistence.yaml
```

The configuration must refer to the mounted data and credential paths. Configure networking to
reach mongod as appropriate for your deployment. These health settings assume a plaintext loopback
listener; use `MONGOT_CONTROL_CA` and `MONGOT_CONTROL_TOKEN_FILE` with TLS and bearer authentication.

## Configuration and authorization

The extension accepts `grpc` and `ingestion` settings from standalone mode. Engine settings,
including the refresh interval, come from the native configuration. Under
`standalone.grpc`, `principals` and `maxRpcDurationMillis` are supported.
Listener address and TLS come from the native `server.grpc` configuration. Unknown extension
settings are rejected. Native listener settings retain their upstream semantics, including the
plaintext Docker example. Remote requests to the added standalone API require an actual TLS
connection and explicit standalone bearer principals; otherwise they return `UNAUTHENTICATED`.
Native commands retain their existing authentication requirements. With native mutual
TLS, standalone callers must also present a client certificate accepted by that listener. Generated
gRPC stubs can use the appropriate channel credentials.

The native server retains its inbound message limit. Keep standalone batch sizes below that limit,
including protobuf overhead. Standalone admission bounds the number and bytes of outstanding API
requests, including requests whose clients disconnect while work drains. CPU, heap, disk, merge
workers, and scheduler capacity are shared, so heavy ingestion can affect either workload.

Native list/status/query commands see replicated indexes. Standalone list/status/query and management
commands see standalone indexes. An index has one writer source for its entire lifetime. Delete it
through its source's normal management interface; ownership conversion and detached replication
are not supported. Local mapping updates continue to create an empty generation that must be
reingested, as in standalone mode.

## Existing data and recovery

To enable the API on an existing replicated directory, keep its native configuration and
`storage.dataPath`. The API is enabled automatically. The native journal, index identities,
Lucene generations, and replication commit metadata retain their existing formats. The extension
adds `indexes.json`, `standalone-owned-ids.json`, `standalone.lock`, and a `coexistence.json` marker.
Enabling the API does not itself rebuild existing replicated indexes.

Keep the whole directory together. Ownership metadata is written atomically before local index
creation and retains dropped identities so a retiring generation cannot change source. Once the
coexistence marker exists, missing ownership metadata or a launch without MongoDB source configuration is an
error. The pure standalone launcher rejects a marked coexistence directory.

Local desired state is reconciled with the most recently accepted remote catalog snapshot. Source
outages and incomplete/unparseable catalog reads preserve that remote snapshot; they do not erase
replicated indexes. Source reconfiguration runs separately from local reconciliation. Local writes
and queries remain available when replication is paused or disconnected, subject to shared process
and disk resources. Source identity/topology checks retain the native bootstrap's existing rules.

## Write completion and health

API batches share the native indexing executor and use the normal writer mapping pipeline.
They are admitted by the bounded RPC dispatcher and pin their generation until work and the
requested refresh/commit barrier finish. Documents within a batch remain ordered; independent
batches may run concurrently, including on the same index. Replication retains its generation
FIFO and checkpoint behavior. `INDEXED`, `VISIBLE`, `COMMITTED`, partial-prefix failures, and
retry behavior retain the [standalone API semantics](standalone-mongot.md). Periodic commits
remain independent of writes and coordinate with explicit commits per generation.

Generation retirement stops new access and waits for outstanding work and statistics leases before
closing its writer. In coexistence mode the physical drain runs outside catalog reconciliation;
an exceptional drain is not treated as permission to delete live resources.

The native empty-name gRPC health response keeps native readiness semantics. Standalone health is
published under its full service names, including `mongot.standalone.v1.OperationsService`.
`mongotctl health` checks that named service in both launch modes. Its settings remain
`MONGOT_CONTROL_ADDRESS`, `MONGOT_CONTROL_CA`, `MONGOT_CONTROL_TOKEN_FILE`, and
`MONGOT_CONTROL_PLAINTEXT`.

## Upstream maintenance

Fork-specific composition and ownership filtering live in `standalone/coexistence`. The native
protocol schema, wire metadata rules, command registration, and scheduling priorities are unchanged.
Shared code has opt-in overloads for externally owned schedulers, extra service definitions, and
producer-owned ordered operations. Existing callers retain their defaults. Deferred retirement is
also opt-in; it is the shared lifecycle seam that deserves particular attention during upstream
merges. Compare `CoexistenceConfigManager` source reconfiguration with the upstream disk and
auto-embedding restart policies when those policies change. Re-run native command, queue ordering,
lifecycle, and standalone API tests after merging.

## Validation

The regression suites cover configuration selection, standalone API behavior, native commands,
index ownership, cursor isolation, concurrent writes, cancellation, retirement, and replication
scheduling. `scripts/tests/community/coexistence_test.py` exercises both ingestion paths in one
process, source outages, restart recovery, catalog isolation, and independent deletion. It uses
native gRPC commands directly, allowing source-replication tests independently of mongod's search
transport version. Run it with the Python client and dependencies available:

```sh
PYTHONPATH=clients/python python scripts/tests/community/coexistence_test.py \
  --mongod /path/to/mongod --mongot /path/to/package/mongot \
  --workdir /path/to/scratch/coexistence-test
```

Use a fresh scratch directory and remove its database and index data after the run.

## Standard Docker replacement regression

`make check.docker.compatibility` builds the `community-compatible` image and runs
`scripts/tests/community/docker_compatibility_test.py`. Install `clients/python` (which supplies
pymongo and grpcio) in your Python environment first, or set `COMPATIBILITY_PYTHON` to that
interpreter. Docker must be available. This is the backwards-compatibility gate for changes to
shared configuration, bootstrap, replication, lifecycle, native protocols, or packaging.

The fixture follows the [official Docker installation](https://www.mongodb.com/docs/search/self-managed/current/installation/docker/):
start the official MongoDB and mongot images, use a `searchCoordinator` user, and create/query a
replicated full-text and vector indexes. Then replace only the mongot image, retaining its DNS alias, exact YAML,
credentials, and data volume. Verify the original index identity and generation, catch-up for
writes during the swap, subsequent insert/update/delete replication, paging, explain, and restart.
Also verify that the added API does not expose unauthenticated remote writes on the plaintext
native listener. The separate coexistence test covers local API ingestion; API authentication has dedicated
regression tests.

```sh
make check.docker.compatibility COMPATIBILITY_PYTHON=/path/to/venv/bin/python
# Test an already-built candidate, optionally pinning the official images by digest:
make test.docker.compatibility COMMUNITY_IMAGE=my-candidate \
  COMPATIBILITY_PYTHON=/path/to/venv/bin/python \
  COMPATIBILITY_OUTPUT=/path/to/test-results \
  COMPATIBILITY_TEST_OPTS='--reference-image mongodb/mongodb-community-search@sha256:... --mongodb-image mongodb/mongodb-community-server@sha256:...'
```

Defaults use the official `latest` tags and record their resolved image IDs/digests and versions.
The test removes its containers, network, volumes, credentials, and scratch directory on success
or failure. Logs, the configuration used, index-journal snapshots, and `results.json` remain in
`COMPATIBILITY_OUTPUT`. A failure against the official baseline is reported separately from a
failure after replacement. This gate does not claim compatibility with every historical release.

The `community-compatible` Docker target matches the official image's root user and default
`/mongot-community/config.default.yml` path so existing root-owned data remains readable. The
standalone Docker target retains its non-root user and generated TLS/bearer configuration.
