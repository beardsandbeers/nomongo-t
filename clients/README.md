# Standalone mongot push clients

Java, Rust, Python and TypeScript clients share the
[protobuf API](../src/proto/standalone/v1/standalone.proto). Create an index using the normal
mongot schema, then upsert BSON directly into that index. There is no collection creation,
source document store, durable retry identity or automatic backfill. Schema updates create an
empty generation; reingest from your own source.

Versions 0.3.0 and later use the push API, which is incompatible with collection-backed 0.2.x clients.
Use matching server and client versions. Each wrapper provides TLS/bearer authentication,
per-call deadlines, BSON helpers and all current RPCs. No automatic retries are performed.

## Python

```sh
python3 -m pip install mongot-standalone-client==0.9.0
```

```python
from mongot_standalone import Client, api, encode_bson, decode_bson
import time

with Client("127.0.0.1:27029") as client:
    index = client.create_index(api.CreateIndexRequest(
        name="articles", type=api.INDEX_TYPE_SEARCH,
        definition_bson=encode_bson({"mappings": {"dynamic": True}}),
    ))
    while client.get_index(api.GetIndexRequest(index_id=index.index_id)).lifecycle_state != "READY":
        time.sleep(0.05)
    client.upsert(index.index_id, {"_id": "a", "title": "Coffee guide"}, concern="visible")
    response = client.search(api.QueryRequest(
        index_id=index.index_id,
        query_bson=encode_bson({"text": {"path": "title", "query": "coffee"}}),
    ))
    print(decode_bson(response.batch_bson))
    client.delete(index.index_id, "a")
```

For TLS, pass `ca_file` and `token_file` to Client. Bind helpers with
`client.index(index_id)`; the result has upsert/upsert_many/delete/delete_many methods.
Writes default to `indexed`, returning without waiting for reader refresh or durable commit.
Use `concern="visible"` for read-after-write searches, or `concern="committed"` for a Lucene commit
before acknowledgement. Background refresh and commits default to one second. Each batch targets
exactly one `index_id`. Concurrent batches can index in parallel against the same or different
indexes; documents within a batch remain ordered. Batches can partially apply on error; sequence overlapping writes and retries yourself. Delete returns a
processed-ID count. Older generated clients must be upgraded to transmit explicit `VISIBLE=0`;
see the [protocol compatibility notes](../docs/standalone-mongot-grpc-guide.md).

## Backpressure

Bound concurrent requests and back off with jitter on `RESOURCE_EXHAUSTED`. The server caps request
count and aggregate request bytes; slow synchronous Lucene work retains that capacity. There is
no persistent ingestion queue or automatic retry. A timed-out write can have applied a prefix;
order retries against newer updates. See the [server backpressure contract](../docs/standalone-mongot.md#backpressure-and-overload)
for admission defaults and indexing concurrency.

## Other languages

- Java: `mvn -f clients/java/pom.xml package`. Generated messages are in
  `com.xgen.mongot.standalone.grpc`; `StandaloneClient` exposes typed blocking calls.
- TypeScript: run `npm ci && npm run build` in `clients/node`. `encodeBson` produces BSON bytes;
  `StandaloneClient` exposes typed Promise methods. uint64 values use decimal strings.
- Rust: `cargo build --manifest-path clients/rust/Cargo.toml`. `Client` exposes async methods;
  generated messages are in its `api` module. BSON helpers preserve native BSON types.

UpsertRequest contains `index_id`, `documents_bson`, optional `generation_id` and `write_concern`.
DeleteRequest substitutes `ids_bson`, each containing `{_id: value}`. Queries continue to use
`index_id` and BSON search/vector expressions. CreateIndex takes name, type and definition BSON;
ListIndexes takes an empty request. Index names are instance-wide.

## Explain and trace queries

`Search` and `VectorSearch` accept `QueryRequest.explain_verbosity`. Regenerate/build the clients
together with a server containing these fields. Omitted or `EXPLAIN_VERBOSITY_UNSPECIFIED` keeps
ordinary query behavior and leaves `QueryResponse.explain_bson` empty.

| Verbosity | Behavior |
| --- | --- |
| `EXPLAIN_VERBOSITY_QUERY_PLANNER` | Returns the Lucene query plan, an empty results array, and `cursor_id = 0`. |
| `EXPLAIN_VERBOSITY_EXECUTION_STATS` | Returns normal result batches with the plan, execution timings, collector statistics, and resource usage where provided by the engine. |
| `EXPLAIN_VERBOSITY_ALL_PLANS_EXECUTION` | Uses the engine's most detailed explain level, retaining all recorded query plans across batches. |

```python
from bson.json_util import dumps

response = client.search(api.QueryRequest(
    index_id=index.index_id,
    query_bson=encode_bson({"text": {"path": "title", "query": "coffee"}}),
    batch_size=100,
    explain_verbosity=api.EXPLAIN_VERBOSITY_EXECUTION_STATS,
))
while True:
    print(decode_bson(response.batch_bson)["results"])
    print(dumps(decode_bson(response.explain_bson), indent=2))
    if not response.cursor_id:
        break
    response = client.get_more(api.GetMoreRequest(
        cursor_id=response.cursor_id, batch_size=100,
    ))
```

The same options apply to `client.vector_search(...)`. Keep the search/vector expression in
`query_bson`; explain is a request option outside that expression. `explain_bson` is a complete
BSON document using the upstream engine's format. Sections depend on the query and verbosity;
examples include `query`, `collectors`, `resultMaterialization`, `resourceUsage`, and
`indexPartitionExplain` for partitioned indexes.

This fork also adds `luceneQuery` at every verbosity, alongside the structured `query` tree:

- `analyzed` is Lucene's rendering of the compiled query before Lucene rewrites it. For example,
  an English-analyzed phrase `RUNNING FOXES` contains the stemmed phrase `"run fox"` here.
- `rewritten` is the rendering after Lucene's rewrite, showing such changes as fuzzy-term
  expansion or replacement with `MatchNoDocsQuery` when no terms match the index. Both fields
  are present even when the renderings are identical.

For a single-partition index, inspect `decode_bson(response.explain_bson)["luceneQuery"]`.
For partitioned indexes, inspect `luceneQuery` in each `indexPartitionExplain` entry. The field
is an object for one distinct pair of renderings, or an array when several are recorded. Repeated
pairs across `GetMore` batches are deduplicated within the retained history. Planner/execution
verbosity retains at most ten distinct pairs in insertion order, evicting the oldest when a new
pair arrives; all-plans verbosity retains all recorded pairs.
These are diagnostic strings from the actual Lucene query objects, including internal field names
and query wrappers, rather than BSON expressions or a complete analyzer token-stream dump.
Custom query renderings may summarize their contents, and analyzer names, token offsets, and
position increments are not added by this section. The structured tree remains available for
per-clause arguments and timings. This shared-engine addition also appears in MongoDB-backed
explain responses.

For joins, the query tree describes the local predicate after foreign keys have been materialized;
partitioned local indexes report their plans and collector statistics in `indexPartitionExplain`.
The `luceneQuery` renderings likewise describe local execution. Foreign key collection does not
emit a separate scorer tree or contribute to local collector statistics, including for self-joins.
Its preparation is reported separately in top-level `joins.preparation`, including foreign index
identity, equality domains, key counts, resource estimates and limits. Execution verbosity adds
preparation and foreign-collection timings and work units. Foreign indexes may use a different
partition count.

Local `joins.execution` appears alongside each partition's `query`; on a single-partition index
it appears at the top level. Match execution `id` values to `joins.preparation.sources[].id`.
For adaptive joins, segment records include sampled cost estimates, probe budgets, actual probe
work, and fallback counts with `probeBudgetExceeded` reasons. Numeric joins report point versus
DocValues choices. Small key sets use Lucene's direct term-set path and have no custom segment
counters. Planner verbosity omits execution counters and timings. See [join explain](../docs/join-design-guide.md#join-explain)
for the field definitions and timing caveats.

Execution-level `GetMore` responses carry cumulative explain snapshots for that cursor; do not
sum snapshots. Statistics cover work performed so far, including any engine prefetch, rather than
an automatic drain of every matching document. Consume the cursor to completion for the full
execution, or call `KillCursor` when finished early. Explain state stays with the existing cursor,
including its index generation, snapshots, idle timeout, authorization, and join dependencies.

Other generated clients expose the same fields:

- Java: `QueryRequest.newBuilder().setExplainVerbosity(ExplainVerbosity.EXPLAIN_VERBOSITY_EXECUTION_STATS)`;
  decode `response.getExplainBson()` with `StandaloneClient.decodeBson(...)`.
- TypeScript: pass `explainVerbosity: 'EXPLAIN_VERBOSITY_EXECUTION_STATS'`; decode
  `response.explainBson` with `decodeBson(...)`.
- Rust: set `explain_verbosity: api::ExplainVerbosity::ExecutionStats as i32`; decode
  `response.explain_bson` with `decode_bson(...)`.

Tracing adds profiling work, so enable it for queries you want to inspect. Planner requests still
perform query preparation, including join key materialization and metadata/count work where
required. Explain output obeys the existing query response limits; oversized output fails with
`RESOURCE_EXHAUSTED` rather than being silently truncated. Unknown verbosity values fail with
`INVALID_ARGUMENT`. This API returns query diagnostics directly; it does not configure an
external trace collector. See the [server contract](../docs/standalone-mongot.md#query-explain).

## Join searches across indexes

Join support is available in server/client release 0.6.0 and later. Existing SDKs
carry the expression through `QueryRequest.query_bson`.
A join filters local documents by the existence of qualifying foreign documents.

For example, assume `accounts_search` and `contacts_search` are ready, both map
`accountKey` as `token` with the same normalizer, accounts map `region` as `token`,
and contacts map `title` as `string`. Configure `storedSource` on contacts to use
`returnStoredSource` below:

```python
query = {
    "compound": {
        "must": [{"text": {"path": "title", "query": "architect"}}],
        "filter": [{
            "join": {
                "from": "accounts_search",
                "localField": "accountKey",
                "foreignField": "accountKey",
                "operator": {"equals": {"path": "region", "value": "Berlin"}},
            }
        }],
    },
    "returnStoredSource": True,
}
response = client.search(api.QueryRequest(
    index_id=contacts_index_id, query_bson=encode_bson(query), batch_size=100,
))
hits = decode_bson(response.batch_bson)["results"]
```

`from` is an instance-wide index name; omit it for a self-join. Use the predicate
in `compound.mustNot` for exclusion. The caller needs QUERY access to every index
in the join. Foreign matches do not duplicate local hits or affect their scores.
Arrays match when any indexed key is shared. Current builds also support native `objectId`
keys, including `_id` with an explicit matching mapping. Server 0.9.0 also supports BSON
int32/int64 join keys with both fields mapped as `number`, `representation: "int64"`,
`indexIntegers: true`, and `indexDoubles: false`. Integers compare by numeric value across BSON
widths. For mixed IDs, map both fields with a list containing token, ObjectId, and the integer-only
number definition; the join combines same-type matches without duplicating hits. Use `bson.Int64` in Python or `Long.fromString(...)` from the Node BSON package when
constructing large int64 IDs; avoid converting IDs through JavaScript numbers. See the
[integer mapping example](../docs/join-design-guide.md#integer-join-keys). Double and decimal join
keys and foreign-document enrichment remain unsupported.

`GetMore` retains the initial local readers and foreign membership. Dropping a
dependency or retiring its generation invalidates the cursor. Budget violations
fail with `RESOURCE_EXHAUSTED` rather than returning a truncated key set. See the
[join guide](../docs/join-design-guide.md) for mappings, limits, and lifecycle details.

## Export a dynamic mapping

After indexing representative documents into a search index, call `CatalogService.GetIndexMappings`
to export a static mapping. The RPC requires the `catalog` role and access to that index.
`GetIndexMappings` is available in server and client versions 0.4.0 and later:

```python
from bson.json_util import loads
from mongot_standalone import api, encode_bson

exported = client.get_index_mappings(api.GetIndexMappingsRequest(index_id=index.index_id))
print(exported.mappings_json)
static_index = client.create_index(api.CreateIndexRequest(
    name="articles-static",
    type=api.INDEX_TYPE_SEARCH,
    definition_bson=encode_bson(loads(exported.definition_json)),
))
```

`mappings_json` is the public `mappings` object. `definition_json` contains the complete public
index definition with those mappings, preserving index-level analyzers, stored source, and other
options. Both use Extended JSON. A path observed as both a number and a string becomes a list of
mapping types. Explicit fields and options are preserved even when no document uses them;
dynamic document and embedded-document fields are expanded with `dynamic: false` at every level.
Reingest your documents into the new index after it becomes ready.

The request accepts an optional `generation_id` fence. The response identifies the generation and
active revision, and the server refreshes its readers before collecting metadata from all partitions.
This is a snapshot of materialized types: deleted fields can remain until segment merges, while
null-only fields, empty objects/arrays, and unindexed values cannot supply an inferred type.
Concurrent writes can add fields after the snapshot. Vector-search indexes return
`FAILED_PRECONDITION`; unavailable or transitioning search indexes return `UNAVAILABLE`.

## Index and field statistics

Server and client versions 0.5.0 and later provide `CatalogService.GetIndexStats` and
`CatalogService.GetIndexFieldStats`. Both require the `catalog` role and a grant for the requested
index. They work for search and vector-search indexes. `GetStatus` still requires wildcard access.

```python
stats = client.get_index_stats(api.GetIndexStatsRequest(
    index_id=index.index_id, refresh=True,
))
print(stats.document_count, stats.index_size_bytes)
if stats.HasField("average_index_bytes_per_document"):
    print(stats.average_index_bytes_per_document)

request = api.GetIndexFieldStatsRequest(index_id=index.index_id, page_size=100)
while True:
    page = client.get_index_field_stats(request)
    if page.state == api.STATS_COLLECTION_STATE_COLLECTING:
        time.sleep(page.retry_after_millis / 1000)
        continue
    for field in page.fields:
        measured = field.attributed_disk_bytes
        print(field.path, measured.value if measured.HasField("value") else None)
    if not page.next_page_token:
        break
    request.page_token = page.next_page_token
```

Typed equivalents are `client.getIndexStats(request)` / `getIndexFieldStats(request)` in Java
and TypeScript, and `client.get_index_stats(request).await?` / `get_index_field_stats(request).await?`
in Rust. Generated request/response types use the same RPC names. TypeScript counts and byte
values are decimal strings, including `ByteMeasurement.value` when present.

`document_count` counts live source/root documents; `lucene_document_count` also includes embedded
children. `index_size_bytes` counts files referenced by the pinned searchable snapshot.
`directory_size_bytes` is a separate `ByteMeasurement` including merge/retention files; its sample
is not atomic with the snapshot. Empty-index averages are absent. File lengths differ from
filesystem allocated blocks and incremental storage cost.

The field response includes its matching `index_stats` and snapshot-wide attributed/shared/
unattributed totals. These reconcile exactly with that summary's `index_size_bytes`. Filters
(`paths=["title"]`) and pages select rows without changing global totals or coverage. Exclusive
codec ranges are attributed; shared postings and compressed stored source are kept in named
buckets. Each representation preserves its mapping type, multi-field name and embedded scope.
Auto-embedding measurements and path filters use the public field path, such as `title`, while
representation names preserve the derived Lucene path, such as `$type:knnVector/_autoEmbed.title`.
Lexical and derived vector representations of the same public field share one row.

Memory domains are separate: accounted reader heap, writer buffers, caches, and desired vector
off-heap working set. Per-field heap is available only where ownership can be established. These
are partial accounting and capacity measurements, not process RSS or guaranteed removal savings.
Always inspect `method`, `completeness`, and included/omitted components. Missing `value` means
unavailable; present zero means a supported measurement returned zero.

Summary caches last five seconds and field caches sixty seconds. `refresh=True` starts/coalesces
a bounded collection and can refresh readers; it does not force a commit, merge, or data scan.
Poll field collections without repeatedly setting refresh. When cached results are returned,
`refresh_in_progress` and summary `age_millis` describe freshness. All pages bind to one immutable
snapshot, generation, principal, filters and page size and expire after two minutes (or eviction).
Do not set refresh on subsequent pages; repeat the same filters and page size. Supply an optional
`generation_id` fence when a generation replacement should fail the request.

Limits return `RESOURCE_EXHAUSTED`; reduce selected paths or retry queue admission with backoff.
Expired snapshots return `FAILED_PRECONDITION`, requiring a fresh first page. See the
[design and attribution rules](../docs/standalone-index-stats-design.md) for supported components.

## Verify

```sh
bash clients/check.sh --launcher /path/to/mongot-standalone
```

This builds all clients and runs each RPC over TLS against an isolated real server. Pass
`--plaintext` for local plaintext verification. The suite checks direct writes, index isolation,
empty mapping rebuilds, cursor behavior and restart persistence. It never uses MongoDB.
Environment overrides: `MONGOT_PYTHON`, `MONGOT_MVN`, and per-language `MONGOT_*_COMMAND`.

Release packaging remains in `scripts/standalone/package_release.py`; verification installs
artifacts independently using `scripts/standalone/verify_release.py`. Neither publishes artifacts.

Compare an actual MongoDB `$searchMeta` pipeline with the standalone RPC using the same engine
build, data, mappings, and Community feature defaults:

```sh
clients/python/.venv/bin/python clients/tests/search_meta_parity.py \
  --package /path/to/extracted/mongot-standalone \
  --runtime-image mongot-standalone:local \
  --mongodb-image mongo:8.3.17-noble --mongodb-version 8.3.17 \
  --output /tmp/search-meta-parity-results
```

Both images must already exist locally. The runtime image supplies system libraries; its engine
is replaced by the extracted package. The suite checks engine bytecode identity and the MongoDB
version, and records image IDs. It compares BSON types, bucket ordering, absent metadata, accepted
and rejected syntax, and query-error messages over 1/2/4 partitions, plus explain modes and empty
indexes. It creates a private Docker network and volumes, uses random localhost ports and bounded
CPU/memory, and removes only its own resources. The output directory must be new.

## Delete by query

```python
result = client.delete_by_query(api.DeleteByQueryRequest(
    index_id=index.index_id,
    search_query_bson=encode_bson({"text": {"path": "title", "query": "coffee"}}),
    dry_run=False,
))
print(result.matched_count, result.processed_count)
```

Java and TypeScript expose `deleteByQuery(request)`; Rust exposes
`delete_by_query(request).await?`. Vector indexes accept `vector_filter_bson` with the contents
of their metadata filter. Set `match_all=True` to select all documents. Requests always exhaust
matching roots in the pinned readers; there is no `max_matches` argument. Upgrade/regenerate old
SDKs to remove that retired option. Old protobuf messages carrying its field cannot impose a
ceiling on a new server. Servers predating exhaustive streaming still need an upgrade.

IDs stream through bounded collectors into native bulk deletes. RPC deadlines still apply.
A dry run counts all matches without retaining IDs or deleting documents. Both query and write
roles are required. New matching documents inserted after a reader was pinned may survive
normally; the operation does not chase concurrent inserts or rescan automatically. A later pass
is optional. Scan/write failures and cancellation can leave partial deletions. Counts represent
selected and processed IDs, rather than confirmed removals under concurrent writes.

## Metadata-only queries

Use `client.search_meta(api.SearchMetaRequest(query_bson=encode_bson(stage_body)))` in Python,
`search_meta` in Rust, or `searchMeta` in Java/TypeScript. Pass the same stage body as coupled
`$searchMeta`, including its `index` name (default `default`). Optional `index_id` and
`generation_id` are identity fences. The response contains optional `metadata_bson`, the
executed generation, and optional explain BSON; it has no hit batch or cursor. Planner-only
explain omits metadata. See the [RPC guide](../docs/standalone-mongot-grpc-guide.md#metadata-only-search)
for a facet example. This additive RPC requires a server built with SearchMeta support.


### Verify reads across ownership types

All client request types support ownership filters and pagination, SearchMeta `target_index_id`,
and Search/SearchMeta `join_indexes`. The JSON CLIs accept the corresponding protobuf JSON fields
(`ownership`, `pageSize`, `pageToken`, `targetIndexId`, and `joinIndexes`). Helpers verify the server's
read-contract acknowledgment when using these additions; they fail instead of silently accepting
legacy name lookup from an old server.

If an unacknowledged Search or VectorSearch reply contains a cursor, clients attempt KillCursor
before reporting the contract error. Cleanup uses the same connection and credentials, with a
fresh timeout capped at five seconds. A cleanup failure preserves the original contract error.
After building all four clients, verify this behavior against a local gRPC fixture with:

```sh
clients/python/.venv/bin/python clients/tests/read_contract.py
```

After building the clients and extracting the current `//deploy:mongot-standalone` package, run the
live replication suite on Linux with existing Docker images:

```sh
clients/python/.venv/bin/python clients/tests/all_index_reads.py \
  --package /absolute/path/to/extracted/package \
  --runtime-image mybyte/nomongo-t:0.9.0 \
  --mongodb-image mongo:8.3.17-noble \
  --output /tmp/all-index-read-results
```

The suite creates isolated containers and volumes with ephemeral ports. It checks direct reads
of genuinely replicated data, native SearchMeta parity, autocomplete, vector queries, mappings,
statistics, pagination, all four join ownership combinations, mutation rejection, and native
rebuild/drop cursor invalidation. It cleans up its containers and volumes and preserves logs and
a result summary in the output directory.
