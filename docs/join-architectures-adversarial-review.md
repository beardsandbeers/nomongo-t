# Adversarial review of the join architecture design

Status: Static design review completed. The findings below are incorporated into the
[design](join-architectures-design.md). The initial implementation and focused runtime checks are
now available; see [implemented scope and validation](coupled-joins.md).

The shared planner and native cursor decorator remain a reasonable architecture. The initial
draft underestimated cancellation integration and left several implicit assumptions about native
catalog and cursor behavior. It should not be implemented directly from the original draft.

This review traced the repository's query, catalog, executor, reader, and cursor code. The failure
sequences below are source-based counterexamples, not claims of experimentally reproduced failures
in a coupled join implementation. No coupled implementation or integration execution existed at the time of this static review.
The subsequent implementation does not imply that every fault-injection scenario below has been exercised. P1 denotes a correctness or availability gap to resolve before release;
P2 denotes an important lifecycle, explain, or API-contract correction.

## 1. P1 Native cancellation does not reach the proposed planner

**Evidence:**
[ServerCallHandler.handleMessage](../src/main/java/com/xgen/mongot/server/grpc/ServerCallHandler.java)
passes a stream-cancellation supplier to
[BulkheadCommandExecutor.execute](../src/main/java/com/xgen/mongot/server/executors/BulkheadCommandExecutor.java).
The regular asynchronous branch checks it once before `command.run()`. The guaranteed branch
dispatches `command::run` without that check. Neither branch installs the caller's gRPC context;
the inspected named-executor wrappers delegate to ordinary executor services. `GetMoreCommand`
uses the guaranteed branch. Stream cleanup in
[CommandManager](../src/main/java/com/xgen/mongot/server/grpc/CommandManager.java) waits for pending
commands to complete.

**Counterexample:** A request begins foreign key collection and the client cancels. Looking up
`Context.current()` inside the query worker does not recover the originating transport context.
The running collection continues, and stream cleanup cannot free it until it returns. A blocked
metadata request can likewise outlive cancellation.

There is a second timing issue: initial local search and metadata calculation occur inside
[LuceneSearchIndexReader.query](../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java),
before the batch producer is returned. Installing cancellation only on producer execution misses
that work, including total counts and facets.

**Required correction:** Capture and scope an optional execution context across both dispatch
branches. Pass captured callbacks to the planner and local snapshot before initial execution;
rebind on each continuation. Bound synchronous driver I/O separately. Add the dispatch seam to
the upstream-change estimate instead of assuming that the cursor wrapper is sufficient.

**Acceptance:** Cancel after dispatch during metadata lookup, foreign collection, initial local
count/facet calculation, and `getMore`. Verify prompt termination, full resource release, and no
context leakage to a later command on the same worker. Cover both BSON and wire-message transports.

## 2. P1 Native readiness permits a nonexistent foreign index

**Evidence:**
[LuceneSearchIndex.throwIfUnavailableForQuerying](../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndex.java)
explicitly permits `DOES_NOT_EXIST`. Ordinary
[CursorFactory](../src/main/java/com/xgen/mongot/cursor/CursorFactory.java) then checks that state
and returns an empty cursor. Foreign `QuerySnapshot.collectKeys` bypasses this factory.

**Counterexample:** A replicated foreign index transitions to `DOES_NOT_EXIST` while its generation
is still in the catalog. The draft's initialized-generation and native-availability checks pass,
and generation-only continuation checks do not notice the transition. Key collection may proceed
against retained indexed data instead of producing the specified missing-dependency error.

**Required correction:** Reject `DOES_NOT_EXIST` explicitly before foreign snapshot acquisition
and in dependency checks. Preserve native empty-result behavior for an outer index absent before
preparation. Do not accidentally reject serviceable stale/recovering indexes as part of this fix.

**Acceptance:** Transition foreign status without replacing the generation, before preparation
and between batches. Require an error and cleanup; test serviceable stale/recovering states as
separate cases.

## 3. P1 An empty view selector does not enforce the collection-only scope

**Evidence:**
[DefaultIndexCatalog.getIndex](../src/main/java/com/xgen/mongot/catalog/DefaultIndexCatalog.java)
only validates view identity when `viewName.isPresent()`. Index lookup otherwise uses database,
source collection UUID, and index name. View definitions use the source collection's UUID.

**Counterexample:** The coupled resolver looks up a source collection UUID and passes
`Optional.empty()` as proposed. A matching view-backed index is returned. Neither the absence of
an explicit view request nor a successful collection metadata lookup proves that the selected
index represents the untransformed source collection. This contradicts the stated no-view scope
and can change membership semantics.

**Required correction:** Check the resolved definitions on both sides for `getView().isPresent()`
and reject them, including self-joins. Also reject explicit outer views and metadata targets that
are views. Keep the existing upstream catalog API unchanged.

**Acceptance:** Resolve a view-backed index through its source collection with an empty view
selector, as both local and foreign index. Include omitted-source self-joins and legacy native
requests that omit `viewName`.

## 4. P1 Metadata resolution has no concrete admission or isolation boundary

**Evidence:**
[MongoDbMetadataClient.resolveCollectionInfos](../src/main/java/com/xgen/mongot/util/mongodb/MongoDbMetadataClient.java)
enumerates collections by database through
[MongoDbDatabase](../src/main/java/com/xgen/mongot/util/mongodb/MongoDbDatabase.java).
[MongoClientBuilder](../src/main/java/com/xgen/mongot/util/mongodb/MongoClientBuilder.java) provides
non-replication defaults intended for limited background concurrency. Native command workers may
be bounded or unbounded depending on executor configuration. The existing join memory budget
accounts for materialization, not a small dedicated bound on pending metadata operations.

**Counterexample:** Many new joins arrive during a metadata outage. They hold native worker slots
while waiting for connection acquisition, server selection, or network responses. Reusing the
background metadata client's pool can also delay control-plane work. Key limits do not help
because no keys have been collected yet; cancellation checks around the driver call do not
interrupt the call itself.

**Required correction:** Use filtered, bounded namespace lookup; a process-wide preparation
admission limit; end-to-end driver timeouts; and an isolated, explicitly sized client pool unless
shared-client isolation is demonstrated. Bind the provider/client/topology snapshot to a local
source-configuration epoch so reconfiguration cannot silently mix old and new sources.

**Acceptance:** Saturate preparation while metadata is slow or unavailable and verify bounded
workers/waits, cancellation, and ordinary-query/control-plane progress. Reconfigure the source
during a lookup and between batches without first changing catalog generation IDs. Permit and
timeout defaults remain implementation decisions, not proven capacity settings.

## 5. P2 Explain can use the wrong partition layout before generation validation

**Evidence:**
[SearchCommand.run](../src/main/java/com/xgen/mongot/server/command/search/SearchCommand.java)
creates explain state from an index lookup before calling the cursor manager. The proposed
decorator resolves the index again.
[MultiLuceneSearchIndexReader.acquireQuerySnapshot](../src/main/java/com/xgen/mongot/index/lucene/MultiLuceneSearchIndexReader.java)
enters per-partition explain contexts during acquisition.
[ExplainQueryState.enterIndexPartitionQueryContext](../src/main/java/com/xgen/mongot/index/lucene/explain/tracing/ExplainQueryState.java)
requires a previously allocated context for that partition.

**Counterexample:** The command initializes explain for two partitions, then the catalog switches
to a four-partition generation before the decorator resolves and acquires readers. Acquisition
can fail on the third context. A guard in `CursorFactory` runs too late. Swaps to fewer partitions
can also leave misleading explain structure.

**Required correction:** Before local snapshot acquisition, compare the active explain partition
count with the selected initialized index and reject a mismatch as a preparation race. Keep the
expected-generation guard for the separate registration race. Avoid a broad explain-engine rewrite.

**Acceptance:** Force a generation change between the command's explain setup and the decorator's
resolution, in both directions and between one and multiple partitions. Require controlled failure
without leaked readers. This is also a pre-existing race-sensitive seam in ordinary native search;
the review does not claim the new feature originally introduced it.

## 6. P2 Producer cleanup alone misses failures outside the producer

**Evidence:**
[MongotCursor](../src/main/java/com/xgen/mongot/cursor/MongotCursor.java) serializes explain and can
throw `ExplainTooLargeException` outside the producer's methods.
[IndexCursorManagerImpl.getNextBatch](../src/main/java/com/xgen/mongot/cursor/IndexCursorManagerImpl.java)
closes cursors on successful exhaustion, but does not close on every batch exception.
[GetMoreCommand](../src/main/java/com/xgen/mongot/server/command/search/GetMoreCommand.java) explicitly
kills for regex failures; its general exception branch only returns an error.

**Counterexample:** An explained join registers successfully, then a continuation fails while
serializing explain. The producer sees neither the error nor a close call from that branch. Its
membership reservation remains until idle expiry or another cleanup path despite a failed batch.

**Required correction:** The native join decorator must retire registered join cursors on batch
failure, including errors outside producer code, without changing ordinary query behavior. Any
lightweight join-ID marker must be removed by producer close, including idle cleanup. Keep
ownership in the producer and registration-failure cleanup idempotent.

**Acceptance:** Fail explain serialization and ordinary batch I/O after registration. Require
immediate cursor removal and budget release, then verify idle expiry removes markers for cursors
that never call back through the decorator.

## 7. P2 Pagination conflates cursor continuation and a new search request

**Evidence:** [SearchQuery](../src/main/java/com/xgen/mongot/index/query/SearchQuery.java) parses
`searchAfter` and `searchBefore` as part of a fresh query. These tokens do not carry a prepared
join plan or a server cursor ID.

**Counterexample:** A client issues one page, foreign membership changes, then it submits another
search with a sequence token. The planner necessarily rebuilds membership. The draft's general
pagination coverage could be read as promising the stability provided by `getMore` for this case.

**Required correction:** Specify stable membership for `getMore` only. Preserve fresh-query
semantics for sequence-token pagination and document that membership can change between requests.
Do not extend tokens or retain plans across independent queries in this release.

**Acceptance:** Mutate foreign membership between two token-based searches and between two batches
of one cursor. Verify and document their intentionally different outcomes.

## Accepted authorization caveat and remaining validation

Foreign-collection authorization was already identified in the draft and is now explicitly
deferred as a known caveat, tracked in [issue #3](https://github.com/beardsandbeers/nomongo-t/issues/3).
It does not block the coupled join implementation. A source check of
[MongoDB v8.3](https://github.com/mongodb/mongo/blob/v8.3/src/mongo/db/pipeline/search/lite_parsed_search.h)
and [master](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/lite_parsed_search.h)
confirms that the shared search lite parser requires `find` only on the outer namespace and reports
no foreign namespaces. The repository request definition supplies no foreign end-user authorization
proof. Thus stock search authorization cannot enforce the custom join dependency. A caller who can
search the outer collection may infer restricted foreign information from join matches or counts.
The subsequent request-builder and transport inspection, recorded in the design,
found no trusted caller identity on the search connection. A transparent search-only implementation
cannot fill this gap by querying MongoDB roles. A mongod authorization extension is the direct
candidate for a future fix. For the current work, do not add an authorization rejection gate,
require a shared-access opt-in, or change mongod solely to close this gap. Characterize the behavior
with an outer-only-user integration test and document it; denial becomes an acceptance criterion
when the follow-up is implemented. Keep existing outer-collection, transport, cursor-isolation,
and standalone checks. The standalone API stays directly queryable; additional granular controls
are deferred separately.

Also retain the real-MongoDB syntax-forwarding check, non-sharded topology requirement, and explicit
same-database/no-view scope. Reuse `MongodTopologyMonitor`, but require the verified non-sharded
result: `CatalogAccessGuard.requireTopologyMatch()` also succeeds for correctly configured sharded
deployments. Runtime compatibility, cancellation latency, and resource bounds are not certified by
this static review.

The revised design adds a small native dispatch seam to the original cursor/bootstrap estimate.
It still avoids new Lucene algorithms, replication algorithms, native wire schemas, and separate
cursor implementations. Proceed with the seven corrected design findings and their tests, while
tracking coupled foreign-collection authorization as the explicitly accepted caveat.

## Implementation review follow-up

Two P2 findings were confirmed after the initial implementation and are now addressed:

1. **Replication host changes invalidated pinned joins.** `ConfigState` advanced its source epoch
   on every replication update, and the coupled adapter compared snapshot object identity. Host
   loss or reselection within the same cluster could therefore terminate a valid cursor. The epoch
   now changes only for logical metadata connection/topology changes; current replication settings
   can be published without advancing it. Tests cover host loss/recovery, same-source restarts,
   logical cluster changes (including switching away and back), and real adapter continuation.
2. **The live pagination assertion tested mongod buffering.** The small dataset fit in mongot's
   first batch even with a MongoDB batch size of one. Instrumentation confirmed no additional
   retained mongot cursor or join reservation. A dedicated 4,096-document fixture now requires
   both before mutation, checks pinned membership through exhaustion, tests foreign retirement,
   and cancels the native transport after `getMore`. Cursor and reservation cleanup are asserted.

The original broader fault-injection and saturation acceptance targets still apply; these fixes do
not claim comprehensive concurrency certification. The accepted authorization caveat is unchanged.
