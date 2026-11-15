# Standalone delete by query design

Status: Bounded synchronous API implemented. Phase metrics and limit tuning remain follow-up work.

Add `DocumentService.DeleteByQuery` to delete all source documents matching a predicate in one
standalone index. Support search operators on search indexes and metadata filters on vector
indexes. Preserve the existing generation fence, authorization model, and write concerns.

The first version is a bounded, synchronous operation: refresh readers, collect the complete
set of matching IDs, then delete those IDs in batches through the existing ingestion pipeline.
Ingestion continues during selection and can interleave with delete batches. Concurrent writes
may leave matching documents behind; callers can rerun the same query if they want another pass.
There is no document version checking or coordination across the whole operation.

Reject selections that exceed resource limits before deleting anything. Failures during
application can leave partial changes, as with existing writes. Acquire generation leases for batch writes and final completion, and keep a selection lease
while reader resources are in use. Catalog checks use the ingestion monitor; writes, refresh,
and commits run outside it. Large purges require narrower predicates initially; background jobs remain
separate follow-up work.

## Current behavior and motivation

Standalone is push-only. It has no authoritative source document store, mutation log, or durable
request ledger. For search indexes, applications currently have to run `Search`, page through
IDs, and submit `Delete` batches. This API moves that work into the server and avoids transferring
IDs to the client. It accepts the same races with concurrent ingestion as that client-side loop.
The public vector API has no exhaustive filter-only selection operation.

The relevant implementation is:

- [The protobuf API](../src/proto/standalone/v1/standalone.proto) exposes `Upsert`, ID-based
  `Delete`, and separate search RPCs. `DeleteResponse.processed_count` counts submitted IDs,
  including IDs that did not exist.
- [ApiIngestionManager](../src/main/java/com/xgen/mongot/standalone/ingestion/ApiIngestionManager.java)
  coordinates catalog changes and generation acquisition with one monitor. Writes run outside
  that monitor, and commits serialize per generation. It validates
  the active generation, applies delete events, and completes the requested write concern.
- [StandaloneQueryService](../src/main/java/com/xgen/mongot/standalone/server/StandaloneQueryService.java)
  parses BSON search expressions and creates cursors with pinned readers. Public cursor paging
  does not hold the ingestion coordinator across requests.
- [SingleLuceneIndexWriter](../src/main/java/com/xgen/mongot/index/lucene/writer/SingleLuceneIndexWriter.java)
  deletes by the encoded `_id` term.
  [MultiLuceneIndexWriter](../src/main/java/com/xgen/mongot/index/lucene/writer/MultiLuceneIndexWriter.java)
  routes that ID to its partition. Root and embedded Lucene documents share this ID, so the
  existing delete removes the entire source document's block.

Typical uses include deleting an expired time range, removing one tenant's indexed content,
and removing documents matching a search predicate without first downloading their IDs.
Deletion affects only the selected index. Applications still own deletion from their source
system and any other indexes containing the same documents.

## Scope

The initial release supports ordinary Lucene search and vector indexes, partitions, root document
counts, optional dry runs, and all existing `DocumentWriteConcern` values. Stored source is not
required. Search predicates may select a parent through `embeddedDocument`; deletion always
removes the whole source document.

The initial release excludes joins, nearest-neighbor selection, arbitrary MongoDB query
execution, aggregation pipelines, asynchronous jobs, resumable operations, durable deduplication,
cross-index transactions, and atomic visibility of the entire deletion. Vector predicates apply
to root-level filter fields; indexes with `nestedRoot` are currently rejected. Vector indexes
with `indexNamespace` are rejected until the API explicitly carries and validates namespace
selection. These restrictions also apply to dry runs; the index namespace restriction applies
to `match_all` as well.

## Proposed API

Add a unary RPC and new messages to the existing `mongot.standalone.v1` package. Existing messages
and RPCs retain their behavior.

```proto
// Added to DocumentService.
rpc DeleteByQuery(DeleteByQueryRequest) returns (DeleteByQueryResponse);

message DeleteByQueryRequest {
  string index_id = 1;
  oneof selector {
    bytes search_query_bson = 2; // One supported search operator; SEARCH indexes only.
    bytes vector_filter_bson = 3; // The contents of a vector-search filter; VECTOR_SEARCH only.
    bool match_all = 4; // Must be true. Explicitly selects every live source document.
  }
  string generation_id = 5; // Optional fence against a schema change.
  DocumentWriteConcern write_concern = 6; // VISIBLE by default.
  uint64 max_matches = 7; // Zero uses the server limit; otherwise a lower safety ceiling.
  bool dry_run = 8;
}

message DeleteByQueryResponse {
  string index_id = 1;
  string generation_id = 2;
  uint64 active_revision = 3;
  uint64 matched_count = 4; // Exact distinct source IDs in the completed selection.
  uint64 processed_count = 5; // Selected IDs submitted for deletion; zero for dry_run.
  bool dry_run = 6;
}
```

Require exactly one selector. Missing selectors, empty BSON predicates, `match_all: false`,
unknown write concerns, and a requested `max_matches` above the server limit are invalid.
`max_matches` is a ceiling on the complete result, not a request to delete the first N matches.
If the selection exceeds it, return `RESOURCE_EXHAUSTED` with no documents deleted. There is no
ordering, cursor, truncation, or continuation token.

`matched_count` counts distinct live source IDs in the selection's pinned readers, excluding
embedded children and records already deleted in those readers. `processed_count` counts IDs
whose delete was applied, including IDs concurrently deleted by another request. On successful
execution it equals `matched_count`; it is zero for a dry run. Neither count describes how many
documents match at response time. This follows ID-based `Delete` rather than claiming an exact
number of documents removed under concurrent writes.

A zero-match request succeeds with both counts zero. A dry run performs the same validation,
refresh, and bounded selection, but applies no deletes and does not commit; write concern has no
effect on a dry run. Dry runs do not reserve a result for a subsequent execution.

### Search predicates

`search_query_bson` contains one search operator, for example:

```javascript
{
  compound: {
    filter: [
      {equals: {path: "tenantId", value: "tenant-42"}},
      {range: {path: "expiresAt", lt: ISODate("2026-11-01T00:00:00Z")}}
    ]
  }
}
```

The example uses BSON date notation and assumes `tenantId` is mapped as `token` and `expiresAt`
as `date`. Selection uses the current index mapping, analyzers, and search matching semantics.
It does not inspect unindexed source fields or require stored source.

Use an explicit recursive allowlist for the first release: `compound`, `equals`, `in`, `range`,
`exists`, `text`, `phrase`, `autocomplete`, `regex`, `wildcard`, `geoShape`, `geoWithin`, and
`embeddedDocument`. Preserve each operator's ordinary matching rules, including
`minimumShouldMatch`. Reject unsupported operators at any nesting depth, including `join`,
`knnBeta`, and `vectorSearch`. Reject explicit score modifiers; collect matches without scoring.
New search operators require an explicit compatibility decision before deletion accepts them.

Reject search request options such as `index`, `sort`, `searchAfter`, `searchBefore`,
`returnScope`, `returnStoredSource`, `highlight`, `count`, and collectors. `index_id` alone selects
the target. Parse a predicate through the existing operator parser with unknown-field rejection,
rather than accepting a complete `SearchQuery` and silently ignoring options. In particular,
an empty expression must never fall back to `AllDocumentsOperator`.

### Vector metadata predicates

`vector_filter_bson` contains the existing vector filter document directly:

```javascript
{
  $and: [
    {tenantId: {$eq: "tenant-42"}},
    {expiresAt: {$lt: ISODate("2026-11-01T00:00:00Z")}}
  ]
}
```

Both paths must be mapped as root-level `filter` fields. Reuse the current clause parser and
mapping validation, including supported equality, range, membership, existence, and boolean
operators. This is the existing vector filter dialect, not full MongoDB collection matching.
Missing-field, null, array, and type behavior follows that dialect.

No vector, vector path, `numCandidates`, `limit`, or distance threshold is accepted. Evaluate
the filter independently of nearest-neighbor search, including live roots that have no usable
vector. Nested vector indexes are currently rejected, including root-only filters and match-all.

`match_all: true` is the explicit way to select all roots for either supported index type. It
retains the same limits and ID deletion path; it does not invoke an index reset or change the
generation. Explicit predicates can also match every document, so this selector is not a general
protection against broad predicates.

## Selection and concurrency contract

Resolve the desired revision and ready generation inside a short
`ApiIngestionManager.coordinate` call and acquire a generation lease. Release the coordinator
before refreshing, pinning readers, or evaluating the predicate. The lease protects resources
from physical close/drop; it does not block ingestion or catalog changes.

Refresh each partition before pinning its reader, making prior `INDEXED` writes eligible for
selection. Concurrent mutations may or may not appear, depending on when that partition's
reader was acquired. The selected readers need not represent one common instant across all
partitions. Once pinned, each reader's selection is stable.

Apply selected IDs through ordinary delete batches, briefly acquiring the coordinator
to validate the catalog and lease the generation before each batch. Do not re-evaluate the predicate or check document versions before deletion.
The accepted behavior is:

- A new match inserted after selection can survive this pass and be selected on the next pass.
- An upsert after an ID's deletion can recreate it, including with fields that still match.
- A selected ID replaced before its deletion can be deleted even if the replacement no longer
  matches. Repeating the query can catch remaining matches; it does not restore that replacement.

These races are part of the API contract. Callers can run the same predicate again when the
remaining results do not meet their needs. Successful completion means the selected IDs were
processed with the requested write concern, not that the predicate currently matches nothing.

Honor the caller's optional generation fence at admission. Even when it is omitted, bind the
operation internally to the resolved generation and revision; recheck both against the catalog
before every batch and final completion. A schema change or drop stops further application,
possibly after earlier batches. Never carry selected IDs into a replacement generation.

Selection, batch writes, and final refresh/commit run outside the catalog monitor. Brief
generation checks still serialize; commits use the generation's commit lock.
Queries can observe partial deletion, and existing cursors can return their older snapshots.
The operation does not invalidate query cursors or statistics snapshots; cached statistics retain
their existing freshness semantics.

## Execution and implementation

1. Authenticate, require both `WRITE` and `QUERY`, and require a grant for `index_id` before
   looking up index metadata. The interceptor already enforces `WRITE` for `DocumentService`;
   the new handler must also call `requireRole(QUERY)`. Counts and dry runs use the same roles.
2. Admit the request through the bounded RPC dispatcher and the additional delete-by-query
   capacity limit. Validate BSON structure, size, selector, operator allowlist, and request
   options before taking the ingestion coordinator where possible.
3. Briefly acquire the coordinator, recheck cancellation, resolve the catalog entry, desired
   revision, live generation, and supported definition, check initial write/disk admission,
   and acquire a generation lease. Capture the generation and revision, then release the lock.
4. With the lease held, validate the predicate against that definition, refresh readers, and
   acquire scoped selection snapshots. Compile against those readers and intersect with the
   appropriate root-document filter. Collect matching root IDs outside the coordinator,
   checking deadlines and resource budgets throughout.
5. Finish selection across all partitions before any delete. Verify that collection exhausted
   normally and did not time out, truncate, or terminate early. Validate collected ID encodings
   before applying them. On a limit or selection error, release resources and fail without
   deleting documents. A dry run rechecks the generation and revision and returns the count here.
6. Close selection readers once IDs are materialized. Submit ordinary delete events in internal
   batches within existing document and byte limits. Each batch checks disk admission,
   then briefly acquires the coordinator to validate generation/revision and obtain a write lease.
   It applies deletes outside the coordinator, checks cancellation between IDs, and advances the
   generation's write version in a finally block so partial work remains eligible for commit.
7. Acquire a completion lease while revalidating generation/revision and cancellation, then
   complete the requested write concern outside the coordinator. Return counts and the selected
   generation. Always close the lease and remaining readers outside the coordinator, and release
   the capacity reservation, including on errors and failed response delivery.

Extract the shared event application and completion logic from `ApiIngestionManager.write` so
both ID-based deletion and query deletion use the same dirty tracking, partition routing,
writer-limit handling, and failure transitions. Internal batches apply outside catalog coordination
with `INDEXED`; perform the requested refresh/commit only at final completion.
Background refresh and commit may also occur between batches. A non-dry-run zero-match request
still validates the generation and honors the requested completion boundary.

### Reader support

Add an internal, scoped root-ID collection capability to the Lucene reader layer. For search,
extend the pinned snapshot pattern in
[SearchIndexReader](../src/main/java/com/xgen/mongot/index/SearchIndexReader.java) and
[LuceneSearchIndexReader](../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java).
Its existing `collectKeys` method collects token-field strings for joins; it cannot substitute
for typed source-ID collection. Add corresponding vector-reader support and multi-partition
delegation. All partitions share one count, byte budget, and deadline.

For vector indexes, expose filter-only compilation through the vector query factory, reusing
[VectorSearchFilterQueryFactory](../src/main/java/com/xgen/mongot/index/lucene/query/VectorSearchFilterQueryFactory.java).
Do not fabricate a vector query with an oversized `limit`. Apply a root filter even when a
negative predicate could otherwise match children. Reject predicates under `nestedRoot` before
compilation rather than treating them as root fields that happen to return no results.

Use an exhaustive collector with `ScoreMode.COMPLETE_NO_SCORES`, not a top-hits collector or a
public cursor. Lucene's default top-hits APIs can report a lower-bound hit count, so that value
cannot establish completion or enforce an exact deletion ceiling.
See [IndexSearcher](https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/search/IndexSearcher.html).

Read only the stored internal ID field; do not deserialize stored source. Preserve the exact
encoded BSON ID bytes using
[LuceneDocumentIdEncoder](../src/main/java/com/xgen/mongot/index/lucene/util/LuceneDocumentIdEncoder.java).
Use byte equality for deduplication, and copy the actual `BytesRef` slice before retaining it.
String, ObjectId, int32, and int64 identities must remain distinct, including int32 and int64
with the same numeric value. Apply deletes through `DocumentEvent.createDelete` using the decoded
typed value. The existing writer removes the root and all children carrying that ID.

Reuse the existing `ApiIngestionManager.IndexLease` mechanism as the generation lease
for selection and statistics. Retirement may mark the generation unavailable immediately,
but physical resource close waits for its leases to drain. Check retirement between selection
phases and before applying batches. Close pinned readers before releasing the lease, and complete
lease-drain futures outside the ingestion monitor, following the existing implementation. This
resource protection does not add document conflict detection or serialize concurrent ingestion.

## Limits and cancellation

Proposed initial limits are conservative defaults to validate with benchmarks:

| Limit | Proposed value and behavior |
| --- | --- |
| Predicate BSON | 1 MiB and depth 100, matching the current standalone query boundary. |
| Selected source IDs | 10,000 per operation, lowered by a nonzero `max_matches`. Detect the next distinct match and fail before application. |
| Retained IDs | 16 MiB of copied encoded ID payload, plus bounded per-ID bookkeeping. Reject before retaining bytes beyond the budget. |
| Selection duration | 5 seconds from lease acquisition, including refresh and query construction, further bounded by the RPC deadline. |
| Total duration | The existing effective client/server RPC deadline, checked during selection, application, and before completion. |
| Concurrent operations | One admitted delete-by-query operation per process, including dry runs. Additional requests fail with `RESOURCE_EXHAUSTED`. |

The initial implementation uses fixed count, retained-byte, and selection-duration limits.
A configurable `ingestion.deleteByQuery` object can follow when operational needs require it. Keep one admitted operation initially to bound
aggregate collection memory and scan work. Acquire its reservation when execution starts and release it on completion. This limit does not reserve ingestion
capacity or prevent ordinary writes during selection.

Request byte accounting alone does not bound the generated ID list or Lucene query allocations.
Bound IDs independently and retain existing query clause/automaton safeguards. Check cancellation
at partition/leaf boundaries and between collected IDs, and use query execution timeout hooks
so a large scan with few matches can also stop. Query rewrite and expensive construction paths
must propagate cancellation or reject the unsupported expensive form before shipping. A partial
search result must never proceed to deletion.

The time limits are cooperative, not hard latency guarantees. Waiting to enter the existing Java
monitor cannot be cancelled, and a Lucene refresh, write, or commit already in progress may
overrun a deadline. Recheck cancellation immediately after monitor acquisition and every such
call; do not continue mutation work after observing it. Retain admission capacity until server
work actually stops. The selection timeout runs without holding the ingestion lock. Writes and refresh/commit can still compete for CPU, memory, and disk,
and shared RPC workers remain a source of contention.

Retain existing disk admission for deletion: applying and committing deletes can require disk
headroom. This API is not an emergency way to recover a full volume. It schedules no force
merge, and successful deletion does not promise immediate storage reclamation.

## Completion failures and retries

| Write concern | Successful non-dry-run completion |
| --- | --- |
| `INDEXED` | Every selected ID's delete has been applied to the writer. Subsequent refresh/commit is independent. |
| `VISIBLE` | Every selected ID's delete has been applied and every partition refreshed. No crash-durability promise. |
| `COMMITTED` | Every selected ID's delete has been applied and every partition committed. Visibility follows reader refresh. |

The initial refresh establishes selection freshness regardless of concern; it does not provide
the final deletion's visibility or durability. Write concerns cover the applied delete operations;
they do not prevent a concurrent upsert from replacing their effects. A successful `VISIBLE`
response can therefore coexist with matching documents. Commits include other pending writes
to that generation, just as existing commits do. Multi-partition commit is sequential: a failed
commit or a crash can leave only some partitions durable. Only successful completion promises
that all partitions reached the requested concern.

| Condition | Status and effect |
| --- | --- |
| Malformed BSON, unsupported selector/operator/options, invalid limits | `INVALID_ARGUMENT`; no deletes. |
| Missing identity or insufficient role/index grant | `UNAUTHENTICATED` or `PERMISSION_DENIED`; no deletes or counts. |
| Missing or dropped index | `NOT_FOUND`; no deletes at admission, potentially partial changes if dropped between batches. |
| Selector incompatible with index type or unsupported namespace definition | `FAILED_PRECONDITION`; no deletes. |
| Stale or replaced generation | `FAILED_PRECONDITION`; no deletes at admission, potentially partial changes if replaced between batches. |
| Initializing or transitioning generation | `UNAVAILABLE`; no deletes at admission, potentially partial changes if a transition starts between batches. |
| Admission, disk, selection count/bytes, or query complexity limit | `RESOURCE_EXHAUSTED`; no deletes when rejected before application. |
| Selection duration or RPC deadline; caller cancellation | `DEADLINE_EXCEEDED` or `CANCELLED`; no deletes if still selecting, potentially partial changes after application starts. |
| Writer limit during application | `RESOURCE_EXHAUSTED`; potentially partial changes, following ordinary writer-limit behavior. |
| Lucene I/O failure during application, refresh, or commit | `UNAVAILABLE`; potentially partial changes. Follow existing index failure handling. |

Selection read/refresh I/O failures also return `UNAVAILABLE`; before application they cause no
deletes. Unexpected server faults return `INTERNAL`, with an unknown mutation outcome if
application had begun. Error descriptions should identify the phase without including raw
predicates or document IDs. Do not return a normal success response with a partial count.

There is no rollback. On cancellation or failure after deletion starts, some selected IDs may
already be removed, refreshed, or committed. Preserve dirty tracking so background commit and
shutdown handle applied changes. Do not attempt rollback of the writer, which also contains
unrelated writes. A disconnect can lose the response after the entire operation succeeded;
clients must treat such an outcome as unknown rather than zero deletions.

Rerunning the same predicate is the normal way to make another pass after concurrent ingestion
or an interrupted operation. Each call refreshes and selects again, so it can catch documents
missed or recreated during an earlier pass. Once writes stop and calls complete successfully
within the limits, repeated passes leave no matches; continuous ingestion has no such guarantee.
Selection-limit failures require a narrower predicate rather than repeating the same oversized
selection. SDKs retain their existing policy of caller-controlled retries. No request ledger,
version check, or conflict-resolution workflow is required.

## Alternatives

Running a public search cursor and submitting ID batches is possible today and has acceptable
concurrency semantics for this feature. The server-side implementation removes client paging
and ID transfer; an internal collector also avoids scoring, result serialization, and public
cursor ownership.

Passing the translated predicate directly to `IndexWriter.deleteDocuments(Query...)` avoids
materializing IDs. However, a search predicate can select a parent without selecting its embedded
children; a raw query delete is not automatically a source-document delete. It also does not
provide this API's preflight count/byte ceiling and dry-run contract. Lucene returns a sequence
number rather than a deleted-source count, and writer-level atomicity does not provide a
transaction across our partition writers.
See [IndexWriter](https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/index/IndexWriter.html#deleteDocuments(org.apache.lucene.search.Query...)).
An optimized path can be considered later if it preserves the same root/block and failure rules.

Holding the global coordinator across selection and deletion would prevent replacement races,
but would delay ingestion and fresh searches for the full operation. That guarantee is outside
the intended contract. Document version checks and conflict retries would add similar complexity
without being required; use ordinary ID deletion and caller-initiated repeat passes instead.

Asynchronous deletion would support purges beyond synchronous limits. It requires task ownership,
progress, cancellation, restart/recovery semantics, and an explicit decision about mutation
logging. Defer it until workloads justify those changes rather than treating an RPC timeout as
an implicit background job.

## Delivery and validation

Implement the parser and root-ID collector first, then generation leases, batched writes, and the RPC.
Expose the new method in Java, Python, Rust, and TypeScript clients, regenerate protocol bindings
and the Rust schema copy, and add optional generation, concern, ceiling, and dry-run arguments
to index-bound helpers. TypeScript counts use decimal strings like the existing uint64 fields.
No client performs automatic retries.

Update the standalone operating guide, architecture guide, client examples, and release checks
when implementation lands. The change is additive and requires no Lucene format migration or
new persistent state. Old clients continue using existing RPCs; a new client calling an old
server receives `UNIMPLEMENTED`. This draft does not change the current API documentation.

Required tests should establish behavior, including:

- Compare deletion's selected IDs with supported search/filter matching on the same snapshot;
  cover zero matches, explicit match-all, analyzers, missing/null values, compound negation,
  embedded parents, and vector roots without vectors. Reject every excluded option recursively.
- Cover typed ID distinctions, duplicates in child blocks, multiple partitions, stored source
  disabled, maximum-size IDs, and boundary values for count and byte budgets. An overflow in
  the last partition must leave all partitions untouched.
- Verify ingestion progresses during selection and can interleave between delete batches.
  Cover concurrent removal, replacement, and insertion, then rerun after writers stop to remove
  remaining matches. Counts report processed IDs rather than inferred removals. Race mapping
  update, drop, retirement, and shutdown; no batch may target a replacement generation. Verify
  leases and admission reservations are released.
- Verify pre-selection refresh includes prior `INDEXED` upserts and deletes. Verify all three
  completion concerns, old-cursor behavior, and that deletion removes entire embedded blocks.
- Inject selection timeout, cancellation with no matching hits, cancellation between deletes,
  writer failure, refresh failure, and a commit failure between partitions. Check zero mutation
  before application, dirty tracking afterward, and committed recovery after a process crash.
- Require both roles and the target index grant for execution and dry run. Exercise all four
  client wrappers over TLS, generation mismatch, deadlines, and old-server `UNIMPLEMENTED`.
- Measure broad sparse scans, expensive text predicates, large IDs, and partitioned indexes
  under concurrent search/upsert load. Record heap, coordinator hold time, delayed commits,
  unrelated-index write/search latency, cursor latency under worker saturation, and cancellation
  delay before finalizing default limits.

Existing RPC metrics include operation `DeleteByQuery`. Future diagnostics can add bounded labels
for selector kind, dry run, completion phase, and rejection reason, plus selection/application time,
coordinator wait/hold time, matched/applied IDs, and retained ID bytes. Those counters should record
applied work even when response delivery fails and exclude raw queries, IDs, and request-specific
labels.

## Decisions to confirm in review

1. Is the initial 10,000-document, synchronous ceiling sufficient for expected deletion workloads,
   or must the first release support large background purges?
2. Are root-level vector filters and the listed search operators enough initially, or are nested
   vector selection, namespaced indexes, or join predicates required at launch?

The proposal above supplies defaults for these choices; changing them requires revisiting the
selection, resource, and concurrency contracts together.
