# Push-mode architecture

Standalone mongot accepts BSON directly into selected indexes without pulling from MongoDB.
The unit of public identity, authorization, ingestion and deletion is an index.

```text
CreateIndex(mapping BSON) -> index definition -> existing mongot configuration/lifecycle
Upsert(index_id, BSON)    -> validate BSON -> prepareBatch -> IndexWriter.updateIndex
Delete(index_id, _id)     -> validate BSON -> IndexWriter.updateIndex(delete event)
Search(index_id, BSON)   -> existing query parser -> Lucene reader -> cursor manager
```

`LocalIndexCatalog` persists index definitions in an atomically replaced JSON file and holds the
data-directory lock. Its internal definitions retain mongot's required namespace/UUID fields;
these are implementation metadata derived from index identity, with no public collection resource.

`StandaloneRuntime` composes the existing Lucene factory, configuration manager, lifecycle,
initialized/live index catalogs and cursor manager. `ApiIngestionManager` implements the lifecycle's
replication interface as a push writer, with no MongoDB sync source. Lifecycle registration marks
an initialized index ready only after all local partitions have durable commits and catalog-v4
initialization authorization has been revoked. Existing data reopens strictly; missing commits
fail the affected local generation without automatic empty recovery. API writes validate the batch and apply ordinary update/delete events
through the current writer's BSON mapping pipeline. Index retirement and mapping changes coordinate
with writes so a closed writer is never used.

Concurrent batches use the existing thread-safe Lucene writers through the bounded RPC workers
(up to eight workers, bounded further by `maxInFlightRequests`). Documents in one batch execute
in order; batches can interleave, even for the same index. Callers must serialize writes and
retries affecting the same IDs when ordering matters. No extra indexing thread pool is created.
A fair per-index read/write gate gives each executing mutation batch one ingestion schema.
Compatible schema publication briefly takes its exclusive side after preparing the new analyzers,
resolvers, and query readers. Queued writes wait and then use the new schema; read pins and cursors
do not delay publication. The physical writer and searcher managers remain open. The shared
configuration journal replaces only the schema attached to the same physical generation.

Typed generation leases pin resources until operations finish. Physical rebuilds and drops can
retire a generation while an admitted batch finishes against it; such a batch is not redirected
into the replacement. Drop and shutdown wait for every resource pin. Shutdown drains active
operations and commits before permitting physical close. Legacy pending copy migrations retain
their existing mutation-drain and recovery protocol.

Omitted write concern defaults to `INDEXED`: completion means applied to the writer without waiting
for refresh or commit. Explicit `VISIBLE` waits for reader refresh; `COMMITTED` waits for a durable
Lucene commit. The default background refresh and commit intervals are both one second. These
intervals are scheduling delays, not hard visibility or durability deadlines under load. Background
commits run outside the ingestion monitor; Lucene can still contend for CPU, memory and disk.
Commit versions retain writes that race an in-progress commit for a subsequent background commit.

The concern fields now have protobuf presence. Existing enum numbers are preserved (`VISIBLE=0`,
`INDEXED=1`, `COMMITTED=2`). Older generated clients omit the zero-valued `VISIBLE` on the wire,
so it is treated as the new default `INDEXED`; regenerate bindings or upgrade the client to request
`VISIBLE` explicitly. Updated bindings transmit an explicitly selected zero value. Python helpers
also default to `concern="indexed"`; for read-after-write flows pass `concern="visible"`.

Completion is expressed as applied, visible, or committed Lucene writes. Periodic commits and the
ordinary reader refresh operate independently. Graceful shutdown commits pending changes before
physical indexes close. There is no separate durable acceptance boundary, source document store,
mutation log, sequence watermark, snapshot builder, retention pin or persisted retry result.

Compatible mapping updates change ingestion and query schemas on the same physical generation.
Existing documents keep their old indexed fields; added fields apply to future writes and explicit
re-upserts. Existing cursors retain their query views. Schema publication is not a document refresh
or commit barrier. Equivalent definitions retain their revision and generation.
Use `INDEX_UPDATE_MODE_REQUIRE_PRESERVE` to reject incompatible updates and
`INDEX_UPDATE_MODE_REBUILD_EMPTY` for an explicit reset. Default `AUTO` updates still rebuild empty
for incompatible changes. See the [update contract](standalone-mongot.md#mapping-changes-and-index-lifecycle).

The dedicated bounded gRPC dispatcher, TLS/bearer interceptor and index grants remain. Query
execution uses the existing readers and cursors. Source fields are returned only when the index
schema explicitly configures Lucene stored source.

`QueryRequest.explain_verbosity` enables the shared Lucene explain context around query creation
and execution. `QueryResponse.explain_bson` returns the engine's BSON diagnostics. Planner
requests return no hits or retained cursor; execution levels page hits normally and restore the
cursor's explain state on each `GetMore`, returning cumulative statistics. This preserves existing
cursor cleanup and join dependency checks. See [query tracing](../clients/README.md#explain-and-trace-queries).

The former standalone source store and document-management workflows are intentionally removed.
The push-only protocol breaks compatibility with the collection-backed 0.2.x API; see the
[operating guide](standalone-mongot.md) for configuration and migration by reingestion.

## Metadata-only search

`QueryService.SearchMeta` accepts the unchanged BSON body of a `$searchMeta` stage and returns
its count/facet metadata without a hit batch or public cursor. `Search` continues to return hits
and metadata together. Operator-only requests use the coupled default `lowerBound` count;
facets without an operator match all documents.

```python
response = client.search_meta(api.SearchMetaRequest(
    query_bson=encode_bson({
        "index": "articles",
        "facet": {"facets": {
            "brands": {"type": "string", "path": "brand", "numBuckets": 10},
        }},
        "count": {"type": "total"},
    }),
))
print(decode_bson(response.metadata_bson))
```

The `index` option selects an instance-wide index name and defaults to `default`. Optional
`index_id` and `generation_id` fields fence that selection; they do not override the query's
index name. A mismatched fence returns `FAILED_PRECONDITION`. An unfenced missing index uses
the coupled empty-index metadata (`count.total: 0`). Ordinary zero-match queries retain their
requested count label and facet shape.

The RPC uses the same parser and feature settings as coupled search, including aggregation
metrics, 10k facet bucket limits, and embedded scoped sort. Partition results merge before final
bucket limits are applied. BSON metadata retains date keys and 64-bit counts. Mapping validation,
such as embedded stored-source requirements for `returnScope`, still applies.

`explain_verbosity` uses the existing engine explain document. Planner-only explain omits
`metadata_bson`; execution levels include metadata. Check protobuf field presence before decoding
optional metadata. The call obeys query authorization, admission limits, deadlines, and the
configured combined response-byte limit. It never retains a cursor, including on failure.

Client methods are `search_meta` in Python/Rust and `searchMeta` in Java/TypeScript. Older servers
return `UNIMPLEMENTED`; clients do not silently fall back to fetching hits.


## Reading replicated indexes

In coexistence deployments the same CatalogService and QueryService serve both ownership types.
ListIndexes returns granted indexes from both sources; set `ownership` to
`INDEX_OWNERSHIP_STANDALONE` or `INDEX_OWNERSHIP_REPLICATED` to filter them. Use `page_size` (maximum
500) and `next_page_token` for bounded discovery. A zero page size retains unpaged behavior within
the response byte limit. Tokens expire after two minutes and bind the principal and list options.

GetIndex describes desired and active definitions separately. `queryable` reports whether an
active initialized reader can serve queries, even while a replacement is building or replication
is recovering. Replicated native definition versions can be zero; `active_version` and
`desired_version` distinguish a known zero from an absent version. They are not direct mutation
revisions. The namespace identifies the source database, collection UUID, observed name, and view.

If a replacement definition is quarantined, `build.state` is `QUARANTINED` and `build.generation_id`
identifies that replacement. Its unreadable definition and unknown version are omitted. The
summary reports `FAILED` while `queryable` and `source_status` continue to describe the active
generation, which can remain available for queries.

Poll `CatalogService.GetIndex` or `ListIndexes` for `build.state`, `build.message`, and
`build.progress.phase`. `OperationsService.GetStatus` includes the same build summary with
definition BSON omitted. A pending build can appear before Mongot assigns its generation ID.
The build summary can describe a replacement while the active generation remains queryable.

For replicated initial sync, `build.progress` reports collection scanning, final change-stream
catch-up, and the first commit. `collection_scan_documents` and `collection_scan_bytes` count work
applied by completed scan batches in the current attempt. A resumed attempt counts new batches
from its resume point. These counters measure scan work, so they can include work that has yet to
be committed. The API omits counters when no scan attempt is being tracked.

Push-fed indexes initialize an empty generation and then accept writes. Poll `GetIndexStats`
with `refresh: true` for the current document count during an upload. The client supplies the
documents and owns any expected upload total.

Use `index_id` for Search, VectorSearch, mappings, and statistics. Autocomplete remains a Search
operator and requires the corresponding field mapping. Mapping export returns `mappings_json`
for search indexes and `vector_fields_json` for vector indexes, plus the active public definition
in `definition_json`. Native definitions can contain features that standalone creation does not
support, such as MongoDB synonym sources; exporting one is not an ownership conversion.

SearchMeta's existing `index_id` remains an optional identity fence on local name lookup. The new
`target_index_id` selects either source directly, avoiding ambiguity between indexes with the
same name in different collections. For example, with the Python client:

```python
from mongot_standalone import Client, api, encode_bson, decode_bson

with Client("127.0.0.1:27028") as client:
    page = client.list_indexes(api.ListIndexesRequest(
        ownership=api.INDEX_OWNERSHIP_REPLICATED, page_size=100))
    index = next(index for index in page.indexes if index.queryable)
    result = client.search_meta(api.SearchMetaRequest(
        target_index_id=index.index_id,
        generation_id=index.generation_id,
        query_bson=encode_bson({"exists": {"path": "title"}, "count": {"type": "total"}})))
    print(decode_bson(result.metadata_bson))
```

`join_indexes` on Search and SearchMeta binds each join's `from` alias to an `index_id` and optional
`generation_id`. Bindings can target either ownership type. Unbound names retain standalone
lookup, and omitted `from` remains a self-join. All dependencies require index grants. These joins
pin independent reader snapshots; they do not promise a common source transaction time.

Updated client helpers send and verify `mongot-direct-read-contract: all-index-reads-v1` when
using new target selectors, join bindings, owner filters, or pagination. Raw gRPC callers must
perform the same check before accepting results from an older server that might ignore unknown
protobuf fields. Legacy requests do not require this header.

Direct GetMore and KillCursor accept only the requesting principal's direct cursors, including
those querying replicated indexes. A rebuild or drop invalidates affected direct cursors and
statistics continuations. Native cursors cannot be consumed through this API. Generation fences
prevent a follow-up inspection from silently switching schemas.

The direct API returns this process's indexed data and configured stored source. It does not fetch
MongoDB documents or combine shard results. Direct writes and catalog mutations remain limited
to standalone-owned indexes. Existing wildcard grants include replicated reads after upgrading;
MongoDB user roles are not consulted by direct bearer authentication.
