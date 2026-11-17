# Running mongot in push mode

The standalone executable uses mongot's ordinary Lucene indexes, BSON mapping validation,
text/vector readers, and cursor manager. Without `syncSource`, it does not connect to MongoDB. Adding a MongoDB source starts
both ingestion paths in one process; see [dual ingestion](coexistence-mode.md). It stores content
only through Lucene, including `storedSource` when explicitly configured in the index schema.

## Build and run

```sh
make build.deploy.standalone
tar -xzf bazel-bin/deploy/mongot-standalone.tgz -C /tmp
cd /tmp/mongot-standalone
./mongot-standalone --config standalone.yaml
```

The archive includes Java 21, native libraries, the API schema, and example configurations.
It also includes the native `mongot` launcher for [replication and standalone indexes together](coexistence-mode.md).
Docker build and TLS bootstrap instructions are in [the container guide](../deploy/standalone/DOCKERHUB.md).

## Configuration

```yaml
storage:
  dataPath: ./data
server:
  grpc:
    address: 127.0.0.1:27029
ingestion:
  maxInFlightRequests: 64
  maxDocumentBytes: 16777216
  maxNestingDepth: 100
  maxBatchDocuments: 1000
  maxBatchBytes: 33554432
  maxQueuedBytes: 268435456
  minFreeDiskBytes: 268435456
  commitIntervalMillis: 1000
querying:
  refreshIntervalMillis: 1000
```

Non-loopback listeners require TLS and bearer authentication. Paths resolve relative to the YAML file.
Unknown and duplicate configuration keys are rejected. Configure index grants by ObjectId hex ID:

```yaml
server:
  grpc:
    address: 0.0.0.0:27029
    tls:
      certificate: server.crt
      privateKey: server.key
    principals:
      - name: admin
        tokenFile: admin.token
        roles: [catalog, write, query]
        indexes: ['*']
      - name: reader
        tokenFile: reader.token
        roles: [query]
        indexes: ['REPLACE_WITH_INDEX_ID']
```

Tokens contain 32–4096 non-whitespace ASCII characters. Creation and instance status require a
wildcard grant. Catalog, write, and query roles are separate. Cursor access is scoped to the
principal and index. RPC concurrency, aggregate queued bytes, deadlines, batch sizes and cursor
count are bounded. Disk admission checks free space for Lucene.

## Backpressure and overload

Admission is bounded in memory. Defaults allow 64 running or queued dispatched RPCs and
256 MiB of aggregate serialized request payloads, using up to eight shared workers and a bounded
worker queue. These are request accounting limits, not a total JVM memory limit: protobuf decoding,
BSON conversion, Lucene buffers and responses also consume memory. Transport admission additionally
limits concurrent calls to `maxInFlightRequests + 16` and caps each inbound message at
`maxBatchBytes + 65536` bytes.

When the request-count, payload-byte, transport-call or worker-queue limit fills, new calls fail
with gRPC `RESOURCE_EXHAUSTED`. Disk headroom exhaustion also rejects writes with that status.
Use bounded client concurrency and exponential backoff with jitter. Clients perform no automatic
retries; preserve ordering against newer writes when retrying replacements or deletions.

Indexing runs synchronously on the RPC workers. Slow Lucene writes hold admission capacity until
they finish, so the limits eventually reject excess traffic instead of growing a persistent queue.
Concurrent ingestion batches run outside the catalog monitor, including batches targeting the
same index. Background commits run independently of writes, with per-generation commit coordination.
The shared worker pool bounds concurrency, so slow requests can still delay other RPCs. Lucene
indexing, analysis, refresh and commit can still compete for CPU, memory and disk resources.
There is no adaptive or per-index rate limiter. Client and server deadlines cancel cooperatively
between documents and before queued work starts; they do not interrupt a Lucene call in progress.

GetStatus exposes the `standalone.rpc.inflight`, `standalone.rpc.bytes`, `standalone.rpc.queued`
and `standalone.rpc.rejected` metrics. The release checks cover functional RPC behavior, TLS,
index authorization and committed crash recovery. They are not a sustained saturation benchmark.

## API

The authoritative [protobuf schema](../src/proto/standalone/v1/standalone.proto) defines:

| Service | Operations |
| --- | --- |
| CatalogService | CreateIndex, GetIndex, GetIndexMappings, GetIndexStats, GetIndexFieldStats, ListIndexes, UpdateIndex, DropIndex |
| DocumentService | Upsert, Delete, DeleteByQuery |
| QueryService | Search, SearchMeta, VectorSearch, GetMore, KillCursor |
| OperationsService | GetStatus |

Create an index by name, explicit type (`SEARCH` or `VECTOR_SEARCH`), and user mapping BSON.
Poll GetIndex until `READY`. BSON uses the ordinary mongot schema, such as
`{mappings: {dynamic: true}}` for text or
`{fields: [{type: 'vector', path: 'embedding', numDimensions: 3, similarity: 'cosine'}]}` for vectors.
An equivalent CreateIndex with the same name returns the existing index; a conflicting definition
fails. Index names are instance-wide. Up to 64 indexes are supported.

`GetIndexMappings` exports a ready search index's configured mappings merged with dynamic types
found in Lucene metadata, with `dynamic: false` at every document level. It returns the mappings
and a complete reusable public index definition as Extended JSON, along with the generation and
active revision. Readers are refreshed before the snapshot. The `catalog` role and index access
are required. See [the mapping export example](../clients/README.md#export-a-dynamic-mapping) for
creating a static index and the limits of metadata-based inference.

Upsert carries an `index_id` and an ordered batch of complete BSON documents. Every document
requires an `_id` (string, ObjectId, int32 or int64). Replacing the same ID removes the previous
indexed fields; omitted fields disappear. Delete carries an `index_id` and BSON wrappers
`{_id: value}`. Missing IDs succeed; the response counts processed IDs, not prior existence.
BSON ID widths are preserved. Invalid BSON, duplicate field names, unsupported IDs, excessive
nesting and IDs exceeding Lucene's term limit fail validation before the batch is applied.
Existing mongot mapping rules determine which fields produce indexed values; a successful write
does not guarantee that a mismatched field value was indexed.

Release 0.7.0 defaults Upsert and Delete to `INDEXED`: the batch is applied without waiting
for reader refresh or durable commit. The default refresh and commit intervals are one second.
Explicit `VISIBLE` waits for search visibility; `COMMITTED` waits for durability. Use matching
0.7.0 client bindings to transmit explicit `VISIBLE=0`; older bindings omit it and therefore use
the new default. See the [ingestion guide](standalone-mongot-grpc-guide.md) for protocol details.

Concurrent requests can index through up to eight bounded RPC workers, including into the same
index. Documents within each batch execute in order; concurrent batches can interleave. Sequence
writes and retries affecting the same IDs when order matters.

A write targets exactly one index. For multiple text/vector indexes, the caller submits content
to each index. There is no fan-out, cross-index transaction, collection, document read API,
strict INSERT, request-ID deduplication, or replay endpoint.

### Delete by query

`DocumentService.DeleteByQuery` streams matching IDs into bounded delete batches from one index. Its
write concern defaults to `VISIBLE`; the `INDEXED` default for Upsert/Delete does not change
DeleteByQuery completion. Supply
exactly one selector: `search_query_bson` containing a search operator, `vector_filter_bson`
containing a vector metadata filter, or `match_all: true`. Both `write` and `query` roles and an
index grant are required. Nested and namespaced vector indexes are currently unsupported.

Every successful request exhausts the matching roots in each pinned reader. There is no document
limit option or fixed total-match, total-ID-byte, or five-second scan limit. `max_matches` has
been removed; its protobuf field number and name are reserved. Older messages carrying that
retired field cannot limit deletion. Upgrade older servers that still impose selection caps.

Search and vector collectors use the existing bounded concurrent search executor when available,
with a serial fallback. The standalone launcher provisions that pool by default. Each collector
holds at most 100 IDs, further bounded by the configured batch document and byte limits, and
flushes at leaf completion. Native term deletes run once per nonempty partition batch through
normal ingestion leases and dirty tracking. Buffers and outstanding writes are bounded by active
collectors, independently of total matches. The final requested refresh/commit runs after all
workers and writes drain.

`dry_run` counts every matching root without retaining IDs or deleting documents. `processed_count`
counts applied source-ID operations, including concurrently removed IDs. Generation fences and
the existing write concerns are supported. The effective client/server RPC deadline still applies;
cancellation and scan/write failures return errors and may leave earlier batches deleted.

Ingestion continues during scanning and between batches. Each partition uses a pinned reader,
so the operation's own deletes do not skip later matches. Documents added after that reader was
pinned may survive; this is expected and does not require automatic rescanning or a zero-match
live query before success. Replacements of selected IDs can be deleted. An additional pass is
optional when callers want to select newer matches. See the
[design](standalone-delete-by-query-design.md) for predicate syntax and completion semantics.

### Join filters

Server 0.6.0 adds cross-index and self-join predicates to Search BSON. For example:

```json
{
  "compound": {
    "filter": [{"join": {
      "from": "accounts_search",
      "localField": "accountKey",
      "foreignField": "accountKey",
      "operator": {"equals": {"path": "region", "value": "Berlin"}}
    }}]
  }
}
```

The request's `index_id` selects the returned documents. `from` is an exact index name;
omit it for a self-join. Place the predicate in `compound.mustNot` for exclusion.
Both keys must have matching indexed types: string fields mapped as `token` with matching
normalizers, native `objectId` fields, or integer-only `number` fields. Server 0.9.0 supports
integer joins with `representation: "int64"`, `indexIntegers: true`, and `indexDoubles: false`
on both fields. BSON int32 and int64 compare by numeric value, without losing large integers.
Explicitly mapped `_id` fields are supported. Arrays match on any shared indexed key. Double and
decimal keys, nested joins, vector joins, and attaching foreign documents remain unsupported.

QUERY access is required to every participating index. Cursors retain their initial
membership and local reader snapshots; dropping a dependency or retiring its generation
invalidates them. A join fails with `RESOURCE_EXHAUSTED` if it exceeds its key, memory,
or work budgets. See the [join guide](join-design-guide.md) for mappings, full syntax,
limits, and token-length caveats; the guide also ships beside this file in the server archive.

### Completion and retries

- `INDEXED` (default): applies to the writer without waiting for refresh or durable commit.
- `VISIBLE`: applies the batch and refreshes the Lucene reader. Fresh searches see it.
- `COMMITTED`: commits to Lucene before returning. Visibility follows reader refresh.

Background commits run at `commitIntervalMillis`. Shutdown commits outstanding writes before
closing indexes. Uncommitted writes can be lost on a crash; replay them from your own source.
`VISIBLE` does not imply durability. Use `COMMITTED` for crash persistence.

Documents within a batch are ordered; concurrent batches can interleave. Batches are not atomic. A failure, deadline, or disconnect can leave a prefix applied.
Repeating a replacement/delete is idempotent in isolation, but there is no durable request ledger:
callers must order retries so an old retry cannot overwrite a newer update. Optional
`generation_id` fences writes against physical index replacement. Compatible schema changes keep
that identity and accept writes; explicit rebuilds and other generation replacements invalidate it.

### Mapping changes and index lifecycle

UpdateIndex requires the current `expected_revision`. Its default `AUTO` mode preserves content
for supported additions to static search mappings, new string multi fields, new custom analyzers,
and query-only `searchAnalyzer` changes. Existing field encodings and global indexing settings
must remain unchanged. Adding an explicit field under a dynamic mapping is currently a rebuild,
because it replaces the implicit mapping. Vector mapping changes and additions of embedded,
autocomplete, geo, or legacy facet representations also use the rebuild path in this release.

A compatible change keeps the same physical Lucene index, writer, files, and `generation_id`.
It increments the schema revision and publishes new ingestion and query configuration in place.
Existing cursors and foreign-join readers remain usable with their captured query configuration.
Preparation runs while writes continue. Publication briefly serializes against executing mutation
batches and their write-concern barriers; queued writes then use the new schema. It does not copy
index files or require temporary capacity for a second index.

A successful in-place response reports the new revision as `READY`. The update itself does not
refresh or commit pending document writes: their normal `INDEXED`, `VISIBLE`, and `COMMITTED`
guarantees still apply. If metadata publication is interrupted after durable acceptance, the
server resumes the accepted schema on the same physical index. Retry the identical definition
using the current desired revision, or restart; do not assume a lost response means rejection.
The durable catalog binding never authorizes recreating missing data as an empty index.

New fields are **not backfilled** into old documents, even when stored source contains them.
Upsert complete documents again to populate the new fields. Existing documents remain searchable
through their previously indexed fields.

Equivalent definitions are no-ops: revision, generation, content, and cursors stay unchanged.
Use the revision returned by the API instead of assuming it increments. An identical-definition
update no longer resets an index. Use `INDEX_UPDATE_MODE_REBUILD_EMPTY` for an explicit reset.

Set `update_mode` to `INDEX_UPDATE_MODE_REQUIRE_PRESERVE` to reject unsupported changes with
`FAILED_PRECONDITION` and leave the catalog untouched. `AUTO` retains the existing empty-rebuild
behavior for incompatible changes; reingest the desired content after that rebuild. A failure
while applying a compatible update never falls back to an empty rebuild. Concurrent requests
recheck `expected_revision` at publication, so only one update can claim a given revision.

A failed empty create or rebuild reports `FAILED` with the initialization error, even while an
older live generation remains searchable. An identical `AUTO` or `REQUIRE_PRESERVE` update of an
unfinished failed initialization returns `FAILED_PRECONDITION` with recovery instructions. After
repairing the underlying problem, restart to resume the same authorized generation, or explicitly
use `REBUILD_EMPTY` with the current desired revision to start a fresh empty generation. Drop is
also available. These paths do not restore empty-creation permission to a previously ready index.

Catalog version 4 records in-place schema bindings and explicit authorization to initialize a
new empty generation. Versions 1–3 are readable, including recovery of already accepted legacy
copy migrations; new compatible updates never start such migrations. Every partition must have a
durable commit before the index is ready. After readiness, missing commits fail the affected index instead of recreating it empty.
Legacy indexes without initialization records reopen only when all commits are valid; incomplete
legacy data requires restoration or an explicit rebuild. Older binaries cannot open a version-4
catalog. A catalog mutation can upgrade older metadata, so retain a stopped backup before upgrade.
Unreadable live or staged local journal entries stop startup before native quarantine cleanup can
delete local files, including when native quarantine is enabled. Restore compatible journal
metadata from the matching backup before restarting. See the
[design and recovery protocol](standalone-index-update-design.md) for compatibility rules and
failure handling.

DropIndex removes the definition and its Lucene data through the ordinary mongot lifecycle.
A later index with the same name receives a new ID and starts empty. Physical cleanup runs
asynchronously. Query cursors pin their reader snapshot; index drop invalidates those cursors.

`SearchMeta` accepts the unchanged `$searchMeta` stage body and returns count/facet metadata
without a public cursor. Its `index` option selects a name (default `default`); optional IDs fence
that selection. See the [metadata-only RPC guide](standalone-mongot-grpc-guide.md#metadata-only-search).

Search and VectorSearch execute ordinary BSON search expressions against `index_id`; use
GetMore for nonzero cursor IDs and KillCursor to release them early. Results contain IDs/scores
and optional configured Lucene stored source. There is no external document hydration.

### Query explain

`QueryService.Search` and `QueryService.VectorSearch` accept an `explain_verbosity` enum outside
`query_bson`. The default, `EXPLAIN_VERBOSITY_UNSPECIFIED`, disables explain. Supported levels are
`EXPLAIN_VERBOSITY_QUERY_PLANNER`, `EXPLAIN_VERBOSITY_EXECUTION_STATS`, and
`EXPLAIN_VERBOSITY_ALL_PLANS_EXECUTION`; unknown values return `INVALID_ARGUMENT`.

Explained responses add `explain_bson`, a complete BSON document in the upstream Lucene explain
format. Planner requests return an empty `results` array and no live cursor. Execution levels
return normal result batches and cumulative explain snapshots, including on `GetMore`. They
measure the work performed so far; the server does not automatically drain the cursor. Use the
final response for complete execution statistics. Tracing is opt-in and adds profiling overhead.
Planner requests can still perform preparation, join key collection, and metadata/count work.

The fork-specific `luceneQuery` section shows the compiled query after analysis (`analyzed`) and
after Lucene rewrite (`rewritten`) at every verbosity. It accompanies the structured `query` tree
and is emitted per local partition when applicable. Identical rendering pairs are deduplicated
across batches; multiple pairs are represented as an array. See the client guide for retention
and rendering details. This additive section uses the shared engine and is also available in
MongoDB-backed explain.

Inline joins also add `joins.preparation` at the top level and `joins.execution` per local
partition (or at the top level for a single partition). Preparation identifies each foreign index
and equality domain and reports keys, admission estimates and limits. Execution verbosity adds
phase timings, sampled local cost estimates, probe work, and strategy/fallback counters that
accumulate across `GetMore` batches. Foreign scorer trees remain separate from the local `query`
and collector statistics. See [join explain](join-design-guide.md#join-explain) for field meanings.

The ordinary query role and per-index grants apply, including every foreign index in a join.
Existing generation fences, reader snapshots, cursor ownership, idle expiration, cancellation,
and dependency invalidation also apply. Explain state is restored on each batch and released with
the cursor. Oversized explain output returns `RESOURCE_EXHAUSTED`; output is not silently truncated.
This uses the shared explain and cursor machinery without changing the MongoDB-backed command
path. It returns diagnostics in the RPC response and does not configure external trace export.

Build matching server/client bindings for these added fields. See the
[client examples](../clients/README.md#explain-and-trace-queries) for Python, Java, TypeScript,
and Rust usage and the available output sections.

## Persistence and upgrades

Persistent state consists of Lucene directories, `indexes.json` containing index definitions,
the existing mongot configuration journal/feature flags, and a directory lock. No application
source documents or mutation payloads are written outside Lucene. Only one process may own a
data directory. Copy the full directory while stopped for backups.

The push-only API and state layout are incompatible with collection-backed 0.2.x releases.
Use a new data directory, recreate index definitions, and reingest from your source. Startup
rejects a legacy document-store directory. It does not migrate or delete legacy data.
Release 0.5.0 uses this push-only API. Use matching server and client packages;
0.2.x images and clients implement the older protocol.

MongoDB synonym sources, views, automatic embedding and custom vector engines are unsupported.
Text indexes, explicit client-provided vectors, partitions, analyzers, sort, and configured
Lucene stored source use the existing mongot implementation.
