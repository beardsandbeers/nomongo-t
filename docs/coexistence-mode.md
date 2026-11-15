# Replication and standalone indexes in one process

The native `mongot` launcher can also expose the standalone gRPC API. Replicated indexes keep
MongoDB's existing management, query, replication, and checkpoint behavior. Standalone indexes
accept direct BSON writes and use their own catalog. Both use the same engine, data directory,
ordinary indexing scheduler, and gRPC listener.

Build `//deploy:mongot-standalone` to package both launchers, or build
`//deploy:mongot-community` for the native launcher. Enable the extension in your existing native
configuration:

```yaml
# Keep existing syncSource, storage, server, and other native settings.
standalone:
  enabled: true
  grpc:
    listener: shared
```

Run `./mongot --config coexistence.yaml`. A complete localhost example ships as
`coexistence.yaml`. For replication setup, use the existing native configuration and credentials.
No changes to mongod commands or client metadata are needed. The standalone API's `CatalogService`,
`DocumentService`, `QueryService`, and `OperationsService` are additional services on
`server.grpc.address`. The API and Python client are described in
[the standalone guide](standalone-mongot.md).

For the standalone container, supply a native configuration and select the native launcher:

```sh
docker run --rm -e MONGOT_MODE=coexistence \
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
`grpc`, `listener` must be `shared`; `principals` and `maxRpcDurationMillis` are supported.
Listener address and TLS come from the native `server.grpc` configuration. Unknown extension
settings are rejected. A non-loopback listener requires native TLS and explicit standalone bearer
principals; native callers retain their existing authentication requirements. With native mutual
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
`storage.dataPath`, then add `standalone.enabled: true`. The native journal, index identities,
Lucene generations, and replication commit metadata retain their existing formats. The extension
adds `indexes.json`, `standalone-owned-ids.json`, `standalone.lock`, and a `coexistence.json` marker.
Enabling the API does not itself rebuild existing replicated indexes.

Keep the whole directory together. Ownership metadata is written atomically before local index
creation and retains dropped identities so a retiring generation cannot change source. Once the
coexistence marker exists, missing ownership metadata or a launch without the extension is an
error. The pure standalone launcher rejects a marked coexistence directory.

Local desired state is reconciled with the most recently accepted remote catalog snapshot. Source
outages and incomplete/unparseable catalog reads preserve that remote snapshot; they do not erase
replicated indexes. Source reconfiguration runs separately from local reconciliation. Local writes
and queries remain available when replication is paused or disconnected, subject to shared process
and disk resources. Source identity/topology checks retain the native bootstrap's existing rules.

## Write completion and health

API writes are queued in generation order through the same scheduler used by replication, then use
the normal writer mapping pipeline. Their batch includes the requested refresh or commit barrier.
`INDEXED`, `VISIBLE`, `COMMITTED`, ordered partial-prefix failures, and retry behavior retain the
[standalone API semantics](standalone-mongot.md). They do not introduce write concerns or standalone
commit metadata into replication. Periodic API commits use the same queue.

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

The targeted regression set covers the standalone API, native gRPC handlers, native configuration
and reconciliation, cursor management, complete remote catalog reads, queue ordering and failure
drain, and deferred generation retirement. All 34 targets pass.

`scripts/tests/community/coexistence_test.py` provides a live regression test. It starts a scratch
replica set, creates and queries a native index, then enables coexistence on the same directory and
checks that the native identity and generation survive. It exercises simultaneous replication and
manual ingestion, catalog/query isolation, local management and committed writes during a source
outage, restart with the source unavailable, resumed replication, and independent deletion.

Run it with the Python client and its dependencies available, using a new scratch directory:

```sh
PYTHONPATH=clients/python python scripts/tests/community/coexistence_test.py \
  --mongod /path/to/mongod --mongot /path/to/extracted-package/mongot \
  --workdir /path/to/data-disk/coexistence-test
```

The exercised source fixture is MongoDB 8.0.11. The script sends native gRPC OP_MSG commands with
wire version 25 and also exercises native BSON management. MongoDB 8.0.11 itself uses TCP for search
transport; this fixture verifies replication from that version and the unchanged gRPC command
contract separately. It does not assert that an unmodified 8.0.11 `$search` client uses gRPC. Native
transport support and the supported mongod version range remain those of upstream main.
