# Running the push-mode container

Release 0.8.0 is available as `mybyte/nomongo-t:0.8.0` (Linux amd64). The `latest`,
`mongot-standalone`, and `mongot-standalone-v0.8.0` tags point to this release. Prefer a versioned
tag for reproducible upgrades.

The `community-0.8.0` and `community` tags provide the official-container replacement, using
`/mongot-community/config.default.yml` and the upstream runtime user. Both variants select
standalone or dual ingestion from `syncSource`; remote standalone API calls on a native listener
require TLS and configured bearer principals.

Version 0.7.0 enables concurrent gRPC ingestion batches, including into the same index. Each
batch targets one index and preserves document order internally. Writes default to `INDEXED`,
without waiting for reader refresh or durable commit. Background refresh and commits default to
one second. Request `VISIBLE` for read-after-write search or `COMMITTED` for durability. Upgrade
client bindings with the server: older bindings omit explicit `VISIBLE=0`, which now selects the
default `INDEXED` behavior. See the [ingestion guide](https://github.com/beardsandbeers/nomongo-t/blob/main/docs/standalone-mongot-grpc-guide.md).

The server also supports cross-index joins and self-joins inside search filters and exclusions.
Use `from: "indexName"` (omit for self-joins), explicit `localField`/`foreignField`, and a foreign
`operator` inside `compound.filter` or `compound.mustNot`. Both keys must be strings mapped as
`token` with matching normalizers. The current source also supports native `objectId` keys and explicitly mapped `_id` fields;
these extensions require a build containing the native-key join changes. Foreign-document
enrichment remains unsupported. See the [join examples](https://github.com/beardsandbeers/nomongo-t/blob/main/README.md#join-searches-across-indexes).

`GetIndexStats` and `GetIndexFieldStats` expose document counts, storage averages, and field
storage/memory diagnostics. `GetIndexMappings` exports static mappings. Matching Python, Rust,
Java, and TypeScript clients are available; see the [client guide](https://github.com/beardsandbeers/nomongo-t/blob/main/clients/README.md).

```sh
docker run -d --name mongot -p 127.0.0.1:27029:27029 \
  -v nomongo-t-push_state:/var/lib/mongot mybyte/nomongo-t:0.8.0
docker exec mongot mongotctl status
```

Or build the current sources locally:

```sh
docker compose -f deploy/standalone/compose.yaml up --build --wait
docker compose -f deploy/standalone/compose.yaml exec mongot mongotctl status
```

Or build without Compose:

```sh
docker build -f deploy/standalone/Dockerfile -t mongot-standalone:local .
docker run -d --name mongot -p 127.0.0.1:27029:27029 \
  -v mongot-standalone_state:/var/lib/mongot mongot-standalone:local
docker exec mongot mongotctl status
```

The image includes Java 21, native libraries, the API schema and an authenticated health/status
client. At first startup it generates a TLS certificate and bearer token in
`/var/lib/mongot/config`; those credentials persist across restarts. Export them with `docker cp`
for a client outside the container. The default host port is loopback-only. Supply your own
certificate and restricted index grants for production; see the [operating guide](../../docs/standalone-mongot.md).

Only Lucene content and index metadata persist. Stop the container before copying the entire
volume for backup. Do not run two containers against the same state directory.

Published `mybyte/nomongo-t:0.2.0` images use the older collection-backed implementation and
protocol. Use matching clients and start with a new state volume. Recreate mappings and reingest
your own source content. No automatic legacy data migration
or deletion is performed.

For a faster build after `make build.deploy.standalone`, use `make build.docker.standalone.local`.
The ordinary source Docker build always compiles the current checkout.


For an existing deployment using the official MongoDB Search Docker image, build
`make build.docker.community.local`. The `mongot-community:local` image accepts the official
`/mongot-community/config.default.yml` mount and preserves the official runtime user for access
to existing data. Run `make check.docker.compatibility` before shipping changes to this path;
it tests an image-only swap from official mongot using the same config and data, including
full-text/vector queries and replication recovery. See the
[compatibility test guide](../../docs/coexistence-mode.md#standard-docker-replacement-regression).
