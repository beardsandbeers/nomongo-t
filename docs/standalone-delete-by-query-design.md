# Standalone delete by query design

Status: Exhaustive streaming, native bulk term deletes, and parallel segment collectors are
implemented. The retired `max_matches` field is reserved. Phase metrics remain follow-up work.

Add `DocumentService.DeleteByQuery` to delete all source documents matching a predicate in one
standalone index. Support search operators on search indexes and metadata filters on vector
indexes. Preserve the existing generation fence, authorization model, and write concerns.

The operation is synchronous and streams matching root IDs from pinned readers into bounded
batches through the existing ingestion pipeline. It does not retain the complete selection or
impose a fixed total-match or total-ID-byte cap. Ingestion continues during scanning and can
interleave with delete batches. Concurrent writes may leave matching documents behind; callers
can rerun the same query if they want another pass. There is no document version checking or
coordination across the whole operation.

Every successful deletion must exhaust all matching source documents visible in the readers
pinned for that operation. Clients must not specify an upper bound, and the target API has no
optional or required total-match ceiling. Documents added after a partition's reader was pinned
may survive the pass; that is expected and does not make a completed deletion unsuccessful.
The operation does not chase new writes or repeatedly rescan until the live index has no matches.

Scan failures, cancellation, and write failures can leave earlier batches deleted. Acquire
generation leases for batch writes and final completion, and
keep a selection lease while reader resources are in use. Catalog checks use the ingestion
monitor; writes, refresh, and commits run outside it. The effective RPC deadline still applies;
background jobs remain separate follow-up work.

## Current behavior and motivation

Standalone is push-only. It has no authoritative source document store, mutation log, or durable
request ledger. Before this RPC, search-index applications had to run `Search`, page through IDs, and submit
`Delete` batches. This API moves that work into the server and avoids transferring
IDs to the client. It accepts the same races with concurrent ingestion as that client-side loop.
The public vector API has no exhaustive filter-only selection operation.

Matching root IDs stream from pinned readers into independent bounded collector buffers. The
writer layer submits native term batches through `ApiIngestionManager` leases, admission, dirty
tracking, and failure handling. Collection uses the existing bounded search executor when it is
available; the standalone launcher enables that executor by default. A serial fallback preserves
operation with upstream configurations that do not provision it.

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

## API

The unary RPC retains the selectors, generation fence, write concern, dry run, and response
counts. The former `max_matches` field is removed; its number 7 and name are reserved. Older
messages carrying that field cannot cap deletion. Regenerate or upgrade SDKs to remove the old
option. Servers predating exhaustive streaming require an upgrade to provide this contract.

```proto
rpc DeleteByQuery(DeleteByQueryRequest) returns (DeleteByQueryResponse);

message DeleteByQueryRequest {
  reserved 7;
  reserved "max_matches";

  string index_id = 1;
  oneof selector {
    bytes search_query_bson = 2; // One supported search operator; SEARCH indexes only.
    bytes vector_filter_bson = 3; // The contents of a vector-search filter; VECTOR_SEARCH only.
    bool match_all = 4; // Must be true. Explicitly selects every live source document.
  }
  string generation_id = 5; // Optional fence against a schema change.
  DocumentWriteConcern write_concern = 6; // VISIBLE by default.
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
and unknown write concerns are invalid. Selection is exhaustive: the API offers no upper limit,
top-N deletion, successful truncation, pagination, or continuation token. Batch size and worker
concurrency bound resources and must never limit the total number of matches processed. Dry runs
also exhaust the operation's pinned readers to count all matching roots.

`matched_count` counts distinct live source IDs in the selection's pinned readers, excluding
embedded children and records already deleted in those readers. `processed_count` counts IDs
whose delete was applied, including IDs concurrently deleted by another request. On successful
execution it equals `matched_count`; it is zero for a dry run. Neither count describes how many
documents match at response time. This follows ID-based `Delete` rather than claiming an exact
number of documents removed under concurrent writes.

A zero-match request succeeds with both counts zero. A dry run performs the same validation,
refresh, and streaming selection, but applies no deletes and does not commit; write concern has no
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

Apply selected IDs through ordinary delete batches as scanning proceeds, briefly acquiring
the coordinator to validate the catalog and lease the generation before each batch. The scan
keeps its pinned reader while batches apply, so its own deletes do not change subsequent
selection or require pagination. Do not re-evaluate the predicate or check document versions
before deletion.
The accepted behavior is:

- A new match inserted after its partition's reader was pinned can survive this pass, even if
  the scan has not yet reached that part of the index. This is expected snapshot behavior and
  is not a deletion failure. A later explicitly requested pass may select that new document.
- An upsert after an ID's deletion can recreate it, including with fields that still match.
- A selected ID replaced before its deletion can be deleted even if the replacement no longer
  matches. Repeating the query can catch remaining matches; it does not restore that replacement.

These races are part of the API contract. Callers can run the same predicate again when the
remaining results do not meet their needs. Successful completion means the selected IDs were
processed with the requested write concern, not that the predicate currently matches nothing.
Every matching root visible in each pinned reader must be processed before success. The readers
need not share one global point in time. There is no automatic rescan, tailing cursor, ingestion
pause, or requirement that callers keep retrying while new matching documents arrive.

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
   looking up metadata. Counts and dry runs use the same roles and index grant.
2. Admit the request through the bounded RPC dispatcher and existing delete-by-query capacity
   limit. Validate BSON, selector/operator allowlists, write concern, and supported index type.
3. Briefly acquire the ingestion coordinator, recheck cancellation, resolve the desired revision
   and ready generation, perform initial write/disk admission, and acquire a generation lease.
   Release the coordinator before refreshing or scanning.
4. Refresh and pin one partition's reader. Compile the predicate and intersect with the root
   filter. Use `CollectorManager` and `ScoreMode.COMPLETE_NO_SCORES` to exhaust segment slices
   through the existing concurrent search executor; traverse partitions sequentially.
5. Each collector owns its stored-fields reader and ID buffer. Validate copied encoded IDs once
   at the collection boundary. Flush before exceeding the configured batch byte limit, on
   reaching the lesser of 100 IDs and the batch document limit, and at successful leaf completion.
   Dry runs count roots without retaining IDs or submitting writes.
6. Each synchronous flush applies encoded IDs through `ApiIngestionManager.deleteDocuments`.
   The existing indexing scheduler captures the request context and leases the generation before
   queueing. Recheck revision, generation, cancellation, retirement, and write admission per batch.
   The writer groups terms by partition and invokes Lucene once per nonempty partition batch.
7. Drain collection and all admitted writes before releasing readers or the generation lease,
   including after failure or interruption. Sum successful collector counts. Recheck generation
   and revision and complete the requested final concern once, after all batches have applied.
   A dry run performs the completion validation with `INDEXED` and does not refresh/commit deletes.
8. Always release the capacity reservation, including on failed response delivery. On failure,
   discard unsubmitted buffers and retain already applied deletes; never report partial success.

A zero-match request still validates its generation and honors its completion boundary. Background
refresh and commit may run between batches. Deletion does not reserve ingestion capacity or block
ordinary writes across the scan.

### Native writer batches and lifecycle

`IndexWriter.deleteDocuments` accepts a bounded list of encoded BSON source IDs, a cooperative
check, and a callback reporting applied IDs. Its compatible default runs existing preparation
hooks and ordinary delete events. `MeteredIndexWriter` delegates and accounts for each applied
prefix, including before a later partition fails; staging/preparation capabilities remain visible.

`SingleLuceneIndexWriter` creates terms directly from the encoded bytes and invokes
`IndexWriter.deleteDocuments(Term...)` once under its normal shutdown shared lock.
`MultiLuceneIndexWriter` routes encoded IDs with the existing hash and delegates partition groups.
No ID round trip through BSON values or ordinary document events is needed in the native path.
All Lucene documents sharing an encoded source ID, including embedded children, are removed.
String, ObjectId, int32, and int64 identities remain distinct.

`ApiIngestionManager` shares the existing write scheduling and application lifecycle between
ordinary writes and encoded delete batches. Dirty tracking advances in a finally block even when
application fails. Required writer-limit checks occur once per native batch. Disk admission,
index ownership, generation/revision fencing, and index failure transitions remain in place.
Internal batches use `INDEXED`; final refresh or commit runs only after all work drains.

### Parallel reader ownership and cancellation

Search and vector readers share `LuceneDocumentIdCollector`. Each search slice receives its own
`DocumentIdConsumer`; each successful leaf finishes and clears its buffer during execution.
Reduction retains counts only, independent of total matches. The legacy sequential consumer
method remains available for callers that require serial callbacks.

The existing search pool bounds active collectors, and synchronous batch submissions provide
backpressure against the separate indexing scheduler. There is no task per match, unbounded list
of batch futures, global ID set, or shared match ceiling. The pinned reader has one live root per
source ID, and deterministic partition routing prevents duplicates across partitions. Retained
ID payload is bounded by active collector buffers and admitted batch work, not by total matches
or the number of segments. A serial fallback uses the same exhaustive collector semantics.

Capture the gRPC context in the request's cancellation check and attach it when submitting batch
writes. A volatile generation-retirement flag makes hot scan checks independent of the ingestion
monitor; batch admission still performs the full generation and revision checks. Query timeout hooks also invoke this check, including during scans with few matches.
The first slice failure stops other workers cooperatively, including failures in scorer
construction or scoring outside collector callbacks. Each slice publishes its failure before
Lucene waits for other tasks, preserving the original error. A local executor scope prevents
queued callbacks from starting after close and drains running callbacks before returning to the
reader owner, including if waiting on search futures is interrupted. Readers remain pinned and
the generation lease stays held until that draining finishes.

Vector selection reuses filter-only compilation through the existing vector query factory; it
does not fabricate an oversized nearest-neighbor query. Root filtering remains mandatory.
Nested/namespaced vector indexes retain their existing rejection at admission.

## Limits and cancellation

| Limit | Value and behavior |
| --- | --- |
| Predicate BSON | 1 MiB and depth 100, matching the standalone query boundary. |
| Total matching roots | Exhaust all matches in the pinned readers. No total-document ceiling or client limit option. |
| Buffered IDs | At most 100 per active collector, further bounded by configured batch document and byte limits. Dry runs retain no IDs. |
| Scan duration | No separate five-second timeout. The effective client/server RPC deadline applies. |
| Concurrent operations | One admitted delete-by-query operation per process, including dry runs. Additional requests fail with `RESOURCE_EXHAUSTED`. |

The capacity reservation is acquired when execution starts and released on completion. It bounds
concurrent scan work without reserving ingestion capacity or preventing ordinary writes.
The active ID buffers are independent of the total number or total encoded size of matches.
Lucene's pinned readers, query machinery, and buffered deletes still consume working memory
under the existing reader/writer settings; the collector bound is not a total-process heap bound.

Retain existing query clause/automaton safeguards. Check cancellation at partition/leaf boundaries
and between collected IDs, and use query timeout hooks so a large scan with few matches can also
stop. The callback checks cancellation and retirement before and after each batch. A failed scan
never returns a successful partial count, although earlier delete batches can remain applied.

The time limits are cooperative, not hard latency guarantees. Waiting to enter the existing Java
monitor cannot be cancelled, and a Lucene refresh, write, or commit already in progress may
overrun a deadline. Recheck cancellation immediately after monitor acquisition and every such
call; do not continue mutation work after observing it. Retain admission capacity until server
work actually stops. Scanning and deadline checks run without holding the ingestion lock.
Writes and refresh/commit can still compete for CPU, memory, and disk, and shared RPC workers
remain a source of contention.

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
| Admission, disk, or query complexity limit | `RESOURCE_EXHAUSTED`; no deletes before the first batch, potentially partial changes afterward. |
| RPC deadline or caller cancellation | `DEADLINE_EXCEEDED` or `CANCELLED`; potentially partial changes once a batch has been applied. |
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
missed or recreated during an earlier pass. If writes stop before readers are pinned, a completed
exhaustive pass removes every matching source ID. With concurrent ingestion, documents added or
recreated outside the selected snapshots can still match. A further pass is the caller's choice,
not a prerequisite for reporting success after exhausting the selected readers. SDKs retain
caller-controlled retries. No request ledger, version check, or conflict-resolution workflow is
required.

## Native bulk deletes and parallel collection

The native writer and collector paths above are the selected implementation. They retain the
ordinary writer fallback and preserve normal MongoDB replication, standalone ingestion, and
simultaneous operation into indexes with separate owners. The optimization changes neither
index formats nor the request/response shape apart from retirement of field 7.

A shared writer is not assumed to serialize every deletion. Native term batches reduce repeated
validation, event allocation, and writer entry calls. Parallel collection may improve throughput,
but scanning, writer bookkeeping, executor scheduling, refresh/commit, and storage bandwidth
must be measured separately. Logical deletion throughput is distinct from later space reclamation.

## Alternatives

Native term batches with parallel collectors are implemented above. The following alternatives
remain outside this change.

Running a public search cursor and submitting ID batches is possible today and has acceptable
concurrency semantics for this feature. The server-side implementation removes client paging
and ID transfer; an internal collector also avoids scoring, result serialization, and public
cursor ownership.

Passing the translated predicate directly to `IndexWriter.deleteDocuments(Query...)` avoids
materializing IDs. However, a search predicate can select a parent without selecting its embedded
children; a raw query delete is not automatically a source-document delete. It also does not
provide this API's source-ID counts and dry-run behavior. Lucene returns a sequence
number rather than a deleted-source count, and writer-level atomicity does not provide a
transaction across our partition writers.
See [IndexWriter](https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/index/IndexWriter.html#deleteDocuments(org.apache.lucene.search.Query...)).
Revisit direct predicate deletion only if it preserves root/block deletion, selected-ID counts,
and the existing races with concurrent replacements and inserts.

`IndexWriter.tryDeleteDocument(reader, docID)` avoids ID term lookup, but it is synchronized in
the current Lucene fork, needs a fallback when a segment has merged away, and targets only
document versions visible to that reader. It also requires explicit deletion of embedded
children. Do not use it as the basis for this optimization or mutate reader live-doc bits directly.

Holding the global coordinator across selection and deletion would prevent replacement races,
but would delay ingestion and fresh searches for the full operation. That guarantee is outside
the intended contract. Document version checks and conflict retries would add similar complexity
without being required; use ordinary ID deletion and caller-initiated repeat passes instead.

Asynchronous deletion would support purges that outlive the synchronous RPC deadline. It requires
task ownership, progress, cancellation, restart/recovery semantics, and a decision about mutation
logging. Defer it until workloads justify those changes rather than treating an RPC timeout as
an implicit background job.

## Delivery and validation

The protocol reserves the retired field number 7 and name `max_matches`. Python and TypeScript
bindings are regenerated from the authoritative schema; Rust carries the synchronized schema
copy and Java generates messages during its normal build. Index helpers expose selectors,
generation, concern, and dry run without a document-limit argument. Old wire messages carrying
field 7 have no effect on the new server's traversal. Existing clients control retries.

Correctness coverage includes exhaustive scans above 10,000 matches and 16 MiB of total IDs,
typed IDs, complete embedded blocks, partition routing, native batch application, wrapper metrics
on a partially applied prefix, and fallback writer preparation. Reader tests exercise parallel
segment slices, serial fallback, exact counts, inserts after pinning, and draining after worker
failure. Fault injection during scorer construction and scoring verifies that a failed slice
stops peer emission, suppresses pending leaf flushes, and preserves the original exception.
Ingestion tests cover dirty tracking and lease release after native writer failure;
existing cancellation, generation-fence, committed recovery, and authorization tests still apply.

Run the collector and end-to-end RPC regressions with enough time for the large-ID fixtures:

```sh
bazel test \
  //src/test/unit/java/com/xgen/mongot/index/lucene:LuceneDocumentIdCollectorTest \
  //src/test/unit/java/com/xgen/mongot/standalone/server:StandaloneGrpcServerTest \
  --test_timeout=300 --test_output=errors
```

Validate the affected standalone, MongoDB replication, and coexistence lifecycle/configuration
paths. Live deployment checks and benchmarks must identify the exact artifacts tested and any
gaps. Resource and performance checks should compare serial individual deletes, serial native
batches, and parallel native batches on identical data, including sparse/dense matches, large IDs,
multiple segments/partitions, and concurrent ingestion/search load. Record allocations, buffer
bounds, native call counts, CPU, throughput, writer contention, final completion cost, and drain
latency. Do not infer production scaling from an isolated Lucene microbenchmark.

## Follow-up scope

1. Do expected purges fit within the configured synchronous RPC deadline, or are background
   jobs with progress and recovery required?
2. Are root-level vector filters and the listed search operators enough initially, or are nested
   vector selection, namespaced indexes, or join predicates required at launch?

The implementation above supplies defaults for these choices; changing them requires revisiting the
selection, resource, and concurrency contracts together.
