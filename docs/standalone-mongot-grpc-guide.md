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
an initialized index ready. API writes validate the batch and apply ordinary update/delete events
through the current writer's BSON mapping pipeline. Index retirement and mapping changes coordinate
with writes so a closed writer is never used.

Concurrent batches use the existing thread-safe Lucene writers through the bounded RPC workers
(up to eight workers, bounded further by `maxInFlightRequests`). Documents in one batch execute
in order; batches can interleave, even for the same index. Callers must serialize writes and
retries affecting the same IDs when ordering matters. No extra indexing thread pool is created.
Generation leases keep readers and writers open until active writes, commits and statistics drain.
A mapping change or drop can retire a generation while an already admitted batch finishes against
that generation; it does not redirect the batch into a new generation. Shutdown drains active
operations and commits before permitting physical close.

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

Mapping updates create an empty generation and switch to it once initialized. The caller reingests
content. New indexes are empty regardless of content in other indexes. Drop removes index metadata
and schedules physical Lucene cleanup. Source ownership, replay, retry ordering and ingestion into
multiple indexes belong to the caller.

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
