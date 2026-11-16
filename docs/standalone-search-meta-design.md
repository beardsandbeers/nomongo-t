# Standalone SearchMeta gRPC design

Status: Implemented on `feature/standalone-search-meta`. The RPC, metadata-only execution,
shared feature policy and command validation, generation leases, cancellation checks, and all
four client helpers are present. The pinned MongoDB stage-adapter comparison remains a release check.

An observational local benchmark on 2026-11-16 compared Search (100 returned hits) and SearchMeta
using 10,000 documents, count/facet/sorted-metric queries, and 1/2/4 partitions. All nine metadata
comparisons matched, including BSON types and bucket ordering; serialized responses were
87.1–98.5% smaller. Each case used five warmup pairs and 30 measured pairs. The benchmark used one
CPU and a 2 GiB heap while an external workload was active; timings are not an isolated baseline.

Add `QueryService.SearchMeta` as a gRPC transport for the coupled architecture's `$searchMeta`
operation. Its query syntax, defaults, validation, feature gates, and metadata results must match
coupled `$searchMeta`. Faceting is the motivating use case, not a restriction on the accepted
query language. Reuse the coupled parser, validation, execution, and metadata reduction.

For the same stage body, indexed data, mapping, engine version, and effective feature settings,
the RPC must accept or reject the same query and produce the same metadata as coupled
`$searchMeta`. This includes operator-only queries using the default count, aggregation-capable
facets, and options that coupled execution accepts but ignores. The protobuf envelope replaces
the MongoDB command transport; it does not introduce a standalone search dialect.

Keep `Search` available for callers that need hits and facets from one execution. This proposal
does not expose a public hit cursor or implement an arbitrary MongoDB aggregation pipeline.

## Current behavior

[The protocol](../src/proto/standalone/v1/standalone.proto) exposes `Search`, `VectorSearch`,
`GetMore`, and `KillCursor`. `Search` accepts a facet collector today. In
[StandaloneQueryService](../src/main/java/com/xgen/mongot/standalone/server/StandaloneQueryService.java),
it parses with the engine's feature policy and default optimization flags, reserves cursor capacity,
creates a cursor, fetches the first results batch, and returns `cursor.metaResults` in `metadata_bson`. An unexhausted query
retains a cursor even when the caller only wants those facets. `batch_size = 0` means the default
of 100 hits; it does not select metadata-only execution.

The necessary engine support already exists:

- [QueryOptimizationFlags](../src/main/java/com/xgen/mongot/index/query/QueryOptimizationFlags.java)
  carries `omitSearchDocumentResults`. Pass it during both parsing and execution; the parser uses
  it when validating `returnScope` without stored source.
- [LuceneSearchIndexReader](../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java)
  chooses an `EmptySearchBatchProducer` when that flag is set. Highlighting and score-details
  helpers also honor the flag. Metadata is computed during cursor creation.
- [MultiLuceneSearchIndexReader](../src/main/java/com/xgen/mongot/index/lucene/MultiLuceneSearchIndexReader.java)
  collects intermediate facet buckets across partitions and reduces them before returning
  `MetaResults`. Merging each partition's already-truncated top buckets would be incorrect.
- [LuceneFacetCollectorSearchManager](../src/main/java/com/xgen/mongot/index/lucene/LuceneFacetCollectorSearchManager.java)
  still collects top documents alongside facets. Suppressing returned documents does **not** yet
  eliminate all top-hit collection and scoring work.

The coupled [SearchCommand](../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java),
sharded planning, and standalone `Search` now share
[SearchQueryFeaturePolicy](../src/main/java/com/xgen/mongot/featureflag/dynamic/SearchQueryFeaturePolicy.java).
Standalone reads the config manager's effective registry for each request and initializes enabled
aggregation support at startup. Coupled command-level validation, query preparation, and the
regex budget scope still need to be accounted for when implementing `SearchMeta`; sharing parse
options alone does not complete the new RPC's parity contract.

## Compatibility authority

Use coupled `$searchMeta` from the same release as the behavioral reference, including this
repository's supported extensions. Pin the MongoDB version used by end-to-end compatibility
tests; version-dependent stage behavior must be explicit. Do not substitute a narrower list of
documented options for the actual coupled parser and validator.

The public [$searchMeta reference](https://www.mongodb.com/docs/search/query/aggregation-stages/searchmeta/)
documents that `count` is optional and that an operator returns default count metadata. It also
includes `returnStoredSource`. The
[MongoDB stage adapter](https://github.com/mongodb/mongo/blob/v8.2/src/mongo/db/pipeline/search/document_source_search_meta.cpp)
passes the stage body to mongot and exposes the resulting metadata. Match both layers: a mongot
parser test alone cannot establish all externally visible stage behavior.

Transport-specific authentication, identity fences, deadlines, and gRPC status codes remain in
the envelope. Query validity and metadata semantics come from the coupled operation. Infrastructure
such as database namespaces, MongoDB pipeline placement, and mongos routing is supplied by the
coupled host rather than being additional syntax inside the standalone query document.

## Proposed wire contract

Add the following method and messages to `mongot.standalone.v1`. The RPC is `SearchMeta` on the
wire and `searchMeta` in Java/TypeScript helpers, consistent with existing naming conventions.

```proto
// Added to QueryService.
rpc SearchMeta(SearchMetaRequest) returns (SearchMetaResponse);

message SearchMetaRequest {
  string index_id = 1; // Optional identity fence; query.index selects the index by name.
  bytes query_bson = 2; // Unmodified BSON value of the $searchMeta stage.
  string generation_id = 3; // Optional generation fence.
  ExplainVerbosity explain_verbosity = 4;
}

message SearchMetaResponse {
  optional bytes metadata_bson = 1; // The coupled metadata document, if one is produced.
  string generation_id = 2;
  bytes explain_bson = 3; // Empty unless explain was requested.
}
```

Use dedicated messages so `batch_size`, `batch_bson`, and `cursor_id` cannot imply paging or hit
retrieval. Preserve BSON for numeric/date bucket keys and 64-bit counts. Return the same metadata
shape as `Search.metadata_bson`, without a `results`, `$searchMeta`, or `metadata` wrapper.

Preserve the coupled distinction between no document, an empty document, and a document containing
zero counts. Presence on `metadata_bson` represents whether a metadata document was produced;
when present, it is complete BSON, never zero bytes. Ordinary zero-match queries retain the
coupled count/facet shape rather than being converted to absent metadata.

Use `explain_verbosity` as the transport equivalent of explaining `$searchMeta`. Preserve coupled
planner versus execution behavior and shared engine diagnostics; do not manufacture zero counts
for a plan. MongoDB's outer aggregation explain envelope and host-specific execution statistics
are not reproduced by the gRPC envelope. Validate which metadata, if any, accompanies each explain
mode against the pinned coupled version. Unknown enum values are `INVALID_ARGUMENT`.

## Query syntax and semantics

For a coupled stage `{$searchMeta: Q}`, send BSON encoding of **the same `Q`** as `query_bson`.
The RPC method supplies the stage identity, so callers omit only the outer `$searchMeta` wrapper
and pipeline array. They must not rewrite operators, collector definitions, options, or values.
Clients should accept an existing stage body directly, preserving BSON types.

Do not require `facet` or an explicit `count`. For example, this body is a valid count-only query
with the coupled default count behavior:

```json
{"text": {"path": "title", "query": "coffee"}}
```

Use shared validation for operator/collector exclusivity, missing operators, empty facet maps,
unknown fields, and all option combinations. A facet with no `operator` retains the coupled
all-documents default.

Example expression, encoded as BSON in `SearchMetaRequest.query_bson`:

```json
{
  "facet": {
    "operator": {"text": {"path": "title", "query": "coffee"}},
    "facets": {
      "brands": {"type": "string", "path": "brand", "numBuckets": 10},
      "prices": {
        "type": "number",
        "path": "price",
        "boundaries": [0, 25, 50, 100],
        "default": "other"
      }
    }
  },
  "count": {"type": "total"}
}
```

Illustrative decoded `metadata_bson` for three matching documents:

```json
{
  "count": {"total": 3},
  "facet": {
    "brands": {"buckets": [{"_id": "Acme", "count": 2}, {"_id": "Brew", "count": 1}]},
    "prices": {"buckets": [
      {"_id": 0, "count": 1}, {"_id": 25, "count": 2},
      {"_id": 50, "count": 0}, {"_id": "other", "count": 0}
    ]}
  }
}
```

The index must have compatible facet mappings. Use ordinary search mapping validation, including
the existing string/token, number, and date support; stored source is not required. Preserve BSON
dates in date boundaries and output. The JSON examples above are display forms, not JSON-on-wire.

### Index selection

Resolve the parsed `index` name using the standalone catalog, preserving the coupled default
name `default` when omitted. Standalone names provide the lookup scope that the coupled collection
namespace supplies. Do not inject an arbitrary index name into `Q` as current standalone `Search`
does. This name-based resolution is necessary for copying a stage body unchanged.

`index_id`, when supplied, is an additional identity fence on the resolved target, not a query
selector or an override of `Q.index`. Likewise, `generation_id` fences the resolved generation.
An identity or generation mismatch fails the fence with `FAILED_PRECONDITION`. Invalid fence
encoding is `INVALID_ARGUMENT`. Examples with non-default indexes must include their existing
`index` option, just as in coupled `$searchMeta`.

Without fences, missing/misspelled index names must follow coupled `$searchMeta`'s missing-index
metadata behavior. Adapt `LocalIndexCatalog.getIndexByName` lookup failures to that shared path;
do not blindly propagate its `NOT_FOUND` or invent a new zero/empty response. With an explicit
identity/generation fence, an absent target fails the fence. Authorization must precede execution
or disclosure of target metadata.

### Shared options and preparation

Apply the coupled rules to every option, including `returnScope`, `returnStoredSource`,
`highlight`, `scoreDetails`, `sort`, `sortBetaV1`, `searchBefore`, `searchAfter`, `tracking`, and
`searchNodePreference`. Do not add a local allowlist, strip accepted options, or reject an option
because it appears unnecessary for metadata. Preserve accepted-but-ignored behavior and any
combination that coupled validation rejects. Internal command optimization flags remain separate
from the public stage body.

Derive `SearchQueryParseOptions` using the same runtime feature policy as coupled `SearchCommand`:
`ENABLE_10K_BUCKET_LIMIT`, `SEARCH_AGGREGATION`, and `ENABLE_EMBEDDED_SCOPED_SORT`. Expose the
runtime's effective registries to the adapter and share the policy builder with the coupled path.
Do not hard-code `SearchQueryParseOptions.DEFAULT`: it would disable enabled syntax. Match
execution-time and applicable planning gates as well, including aggregation requested-bucket
limits. Compare deployments with the same settings; do not enable every feature unconditionally.

Enabled aggregation facets are part of the initial contract, including nested facets and metrics.
Preserve `sortDrivesAggregation()` so `first`/`last` metrics consume their sort even when hits are
omitted. Legacy facet sorts remain accepted and ignored where coupled execution does so. These
rules already have coverage in
[SearchAggregationSortTest](../src/test/unit/java/com/xgen/mongot/index/query/SearchAggregationSortTest.java).

Use coupled operator/index compatibility checks and query preparation, including vector-related
validation or embedding when applicable. Do not introduce a blanket nearest-neighbor ban. If a
query needs a capability missing from standalone, providing that capability is parity work, not
grounds for silently narrowing this RPC's syntax. Reuse join planning only where its semantics
and restrictions match coupled execution; verify foreign-target resolution, authorization, and
local-document counting against that path.

Preserve the engine's metadata semantics:

- With no `count` option on an operator or facet query, use the existing `lowerBound` default
  and threshold of 1000. An explicit `total` requests an exact count. Facet execution computes an
  exact match count but serializes its label according to the requested count type; retain that
  behavior. A count threshold does not limit the matching documents used for facet buckets.
- Bucket counts describe the full matching population, not a first results page. Keep the engine's
  ordering, boundary intervals, default buckets, multi-value deduplication, and empty-result shape.
  Do not pad string buckets or invent buckets for missing values.
- Preserve supported `doesNotAffect` drill-sideways and embedded facet semantics, with the same
  engine feature gates as coupled `$searchMeta`. Bucket counts need not sum to the top-level
  count: documents can occupy several buckets, and drill-sideways facets can use a broader predicate.
- Merge all partitions before selecting final top buckets. Never implement the RPC by issuing
  independent partition `Search` calls and summing their top-N results.

## Execution and resource ownership

Implement a distinct `searchMeta` handler on `StandaloneQueryService`, dispatched through
`RpcDispatcher.unary`. Extract shared index resolution, BSON decoding, explain setup, and error
mapping where useful. Do not add another Boolean to `query(request, boolean vector)` to encode
three execution modes.

1. Authenticate through the existing interceptor (`QUERY` role). Decode the unmodified stage body
   using limits compatible with the coupled BSON request path.
2. Parse using `SearchQuery.fromBson(expression, new QueryOptimizationFlags(true), parseOptions)`
   with shared feature-derived options. Extract/reuse coupled command-level validation and query
   preparation, including the regex budget scope, rather than duplicating a subset in the adapter.
   Preserve coupled validation order: validate the query, then resolve its index name and enforce
   the grant/fences before index-dependent preparation. Handle absent indexes through the coupled
   metadata path.
   Set up explain before acquiring query snapshots, since searchers capture explain profilers
   at acquisition. Preserve the index name in explain metadata.
3. Acquire a generation lifetime lease and a pinned `SearchIndexReader.QuerySnapshot`. Reuse the
   `ApiIngestionManager` lease mechanism via a proposed generically named query-lease helper;
   reader references alone should not be assumed to protect generation-directory retirement.
   Acquire catalog/lifetime state under the coordinator, then release its monitor before searching.
   For joins, reuse the planner's snapshots and extend the same lifetime ownership to dependencies.
4. Attach a cancellation check capturing this call's gRPC `Context` through
   `QuerySnapshot.setCancellationCheck`. Pass that snapshot in `CursorQuery.Search`. Reuse the
   cursor manager with `QueryOptimizationFlags(true)` and the cursor options supplied by the
   coupled metadata path. Any smaller internal batch hint must first demonstrate identical
   count, aggregation, operator, and explain semantics; batch size is not a facet limit.
5. Read the completed `SearchCursorInfo.metaResults`, apply `applyAggregationLimit()` only at the
   terminal response boundary, and serialize with the existing BSON encoder. If explain is
   requested, consume the empty results batch to finalize existing cursor explain machinery.
   Assert that no search documents escape the adapter. Preserve the coupled presence and content
   of metadata for each explain mode. Audit producer paths that do not honor the omit-results
   flag; finish required execution and cleanup without exposing a hit cursor.
6. Check cancellation and generation/dependency validity before returning. Enforce the combined
   serialized protobuf response limit, including metadata, explain, and envelope fields.
7. In `finally`, kill the internal cursor, close any untransferred snapshot or join plan, and
   release generation leases and admission permits. Cover failure during cursor creation,
   metadata reduction, encoding, explain, cancellation, and response delivery. Cleanup must
   preserve the primary failure and tolerate a cursor already removed by exhaustion or drop.

The internal cursor exists only during execution. Never add it to the public `leases` map or
return its ID. Use the dispatcher's in-flight admission for this call; do not consume the service's
1024 retained-cursor permits. All execution resources must be released before delivery, so a
client disconnect cannot leave a cursor waiting for idle expiry.

Prefer this reuse of the cursor pipeline for the first version: it retains reader routing,
partition reduction, engine metrics, and explain support. A direct `queryMeta` reader API can
follow if profiling warrants removing the short-lived cursor allocation.

## Consistency, cancellation, and limits

Read the currently visible index state, as `Search` does, without forcing refresh or commit.
Callers needing visibility of completed writes should use `VISIBLE` write concern. Pin readers
for this call, but do not promise an atomic write boundary across partition snapshots. A separate
`Search` and `SearchMeta` can observe different refreshes even with the same `generation_id`.
That field fences a mapping generation, not a reader snapshot. Use `Search` when hits and facets
must belong to one execution.

Never switch generations midway. Apply the same reader retirement and query failure rules as
coupled execution, plus any explicitly requested identity/generation fences. Do not conflate a
missing index at query resolution with a failure of readers already acquired for an executing
query. A lifecycle change may race with delivery; identify the generation actually used.

The effective deadline is the earlier client deadline and the configured server maximum. Preserve
`CANCELLED` versus `DEADLINE_EXCEEDED`. `RpcDispatcher` checks the context at admission and delivery;
that alone does not stop an executing facet scan. Use the pinned-searcher cancellation hook and
add checks to long facet counting, ordinal enumeration, and partition-reduction loops where the
searcher's hook does not run. Propagate cancellation through concurrent partition workers and wait
for their cleanup before releasing leases. Never return partial facets as a successful response.

Use the coupled BSON, facet, aggregation, cardinality, and intermediate-work limits under matching
feature settings. Do not impose additional RPC-specific facet-count, total-bucket, query-size,
or response-size ceilings that reject otherwise supported coupled queries. Configure gRPC send/receive limits
and client channels to accommodate every supported coupled request/result plus protobuf overhead;
the transport's default message size must not silently narrow the API.

Retain bounded concurrent RPC admission and effective deadlines. Apply shared engine memory/work
budgets rather than inventing a second metadata policy. High-cardinality partition reductions
can enumerate far more terms than are returned, so verify those budgets and cancellation under
load. Any additional budget needed for safe execution should be implemented in the shared path
and tested in both modes. Never truncate buckets or change count semantics to fit a transport
limit; report transport admission failures distinctly from query validation failures.

## Errors and observability

| Status | Condition |
| --- | --- |
| `UNAUTHENTICATED` | Missing or invalid credentials. |
| `PERMISSION_DENIED` | Missing query role or grant on the local/required foreign index. |
| `INVALID_ARGUMENT` | Invalid IDs/BSON/options, unsupported index/operator, incompatible facet mapping, invalid boundaries, or parser bucket bounds. |
| `NOT_FOUND` | A coupled execution failure maps to this status; not an unconditional missing-name lookup result. |
| `FAILED_PRECONDITION` | Explicit identity/generation fence cannot be satisfied. |
| `UNAVAILABLE` | Index not query-ready or transient reader failure. |
| `RESOURCE_EXHAUSTED` | RPC admission or shared engine resource/explain limit exceeded. |
| `CANCELLED` / `DEADLINE_EXCEEDED` | Client cancellation or effective deadline reached. |

Map malformed BSON type access and known facet cardinality exceptions deliberately; the existing
generic query-error fallback can otherwise misclassify these as `UNAVAILABLE`. Unexpected
invariant failures should remain internal errors with details logged server-side.

Preserve coupled query error categories and useful field-specific messages when mapping into
gRPC status details. Transport status numbers need not equal MongoDB error numbers, but an accepted
coupled query must not become a standalone syntax error. Pin error precedence with invalid-query
fixtures, including those combined with missing indexes or incompatible mappings.

Existing dispatcher metrics label calls by `SearchMeta`. Retain engine query/facet metrics and
add metadata response bytes, requested/returned bucket counts, limit rejections, and cancellation
completion latency. Do not label metrics with facet names, field values, or raw query text.

## Clients and compatibility

The change is additive: existing messages, field numbers, and RPC behavior stay intact. New
clients calling an old server receive `UNIMPLEMENTED`; do not silently fall back to fetching hits
and opening a cursor. Update the canonical proto and regenerate packaged bindings, including
the Rust proto copy and Python/TypeScript generated code.

Add `search_meta(SearchMetaRequest)` to Python and Rust and `searchMeta(SearchMetaRequest)` to
Java and TypeScript. Reuse each client's authentication, deadline, and BSON handling. Expose the
response as metadata directly, without an iterator or automatic `GetMore`/`KillCursor` behavior.
Update the Node CLI method map, client examples, and standalone gRPC guide when implementing.
Keep count and numeric bucket decoding lossless in JavaScript.

## Implementation and validation

1. Add the proto and client method surfaces. Extract shared coupled parsing, feature-policy,
   validation, and preparation helpers. Implement name resolution and optional identity fences;
   do not create metadata-specific syntax validation.
2. Implement the handler, pinned snapshot/lifetime cleanup, explain, cancellation, response-size
   checks, and shared partition budget enforcement. Generalize join-specific snapshot helper
   error text when reusing it for non-join metadata queries.
3. Add integration coverage in
   [StandaloneGrpcServerTest](../src/test/unit/java/com/xgen/mongot/standalone/server/StandaloneGrpcServerTest.java)
   and client coverage in [the cross-language harness](../clients/tests/e2e.py). Validate:
   - Run a shared fixture corpus through an actual coupled `{$searchMeta: Q}` pipeline and this
     RPC using the identical `Q`, data, mappings, and effective flags. Compare accepted/rejected
     queries, error categories, metadata presence, BSON values/types, and bucket-array ordering.
     Ignore BSON document field order and nondeterministic host/timing diagnostics only; do not
     normalize away count labels, empty/missing fields, numeric types, or defined tie ordering.
   - Include operator-only queries without `count`, all explicit count modes, facets without an
     operator, string/number/date facets, zero matches, empty/missing indexes, default/explicit
     index names, arrays, boundaries/default buckets, and BSON type preservation.
   - Test every shared option and invalid combination, especially legacy sort, aggregation
     `first`/`last` sort, `returnScope`/`returnStoredSource`, highlighting, score details, pagination
     tokens, tracking, and search-node preference. Include applicable vector and join preparation.
   - Run feature-on/off cases for 10k limits, aggregation, scoped sort/facets, and applicable
     requested-bucket limits. Test nested aggregations, embedded scopes, drill-sideways, and joins.
     Test partitions where the global top bucket is absent from a partition's local top-N,
     as well as ties and deleted documents.
   - No hit materialization or public cursor, stored source disabled, and successful calls when
     retained cursor capacity is full. Repeated success/failure must return reader/lease/permit
     counts to baseline; test partial snapshot acquisition and partial partition failures.
   - Authorization, wrong index type, identity/generation fences, drop/update races, invalid options,
     planner and execution explain, each limit boundary, and metadata-plus-explain overflow.
   - Deadlines/disconnects during scan, counting, merge, and encoding, including concurrent queries;
     verify workers stop and no truncated metadata is delivered as success.
   - New-server/old-client compatibility and explicit `UNIMPLEMENTED` on old servers.
4. Benchmark against facet-bearing `Search` on the same data and predicates, comparing latency,
   CPU, allocation, bytes returned, and retained cursors under high cardinality and partition
   fan-out. Verify reduced hit-processing costs without claiming facet scans become cheap.

A later engine optimization can remove top-hit collectors and unnecessary scoring for metadata
queries. It must cover ordinary facets, both drill-sideways implementations, count-only queries,
partitioned execution, and explain. Keep score-dependent query membership and enabled aggregation
requirements intact. The initial RPC's guaranteed savings are hit materialization, transfer, and
retained cursor lifetime; further CPU improvements require that collector work and measurement.

Coupled parity is a release criterion, not follow-up work. A mismatch in stage syntax, defaulting,
validation, enabled capabilities, or metadata results blocks completion of this design's contract.
