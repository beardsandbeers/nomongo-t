# Coexistence mode design

Status: Design baseline for this implementation. See [the operating guide](coexistence-mode.md)
for the concrete launch/configuration contract and implementation notes below.

Run one mongot process that preserves main-branch MongoDB replication and command behavior while
also hosting independent indexes populated through the standalone gRPC services. Older mongod
clients continue using their existing requests. Standalone clients use additional services without
changing the MongoDB protocol or taking over its indexes.

## Fixed requirements

- Preserve compatibility with the latest commit on `mongodb/mongot` main and the mongod versions
  that upstream supports. No separate version-floor decision is required. Do not require mongod
  upgrades, patches, new capability exchanges,
  request fields, headers, credentials, or revision conventions to enable coexistence.
- Keep replicated index management on the existing native command and authoritative-catalog paths.
  Add commands only for standalone indexes and their management. The standalone API does not
  create, update, drop, ingest into, or query MongoDB-owned indexes in this release.
- Replicated and standalone indexes coexist in one process. A generation has exactly one document
  writer: MongoDB replication or direct API ingestion. No ownership conversion is implicit.
- There are no arbitrary or additional mongot deployments to integrate with. Mixed mongot versions,
  competing external catalog writers, observer membership, and cross-mongot coordination are not
  requirements. Old **mongod clients** remain a compatibility requirement.
- Preserve the existing MongoDB-free standalone launch and its client behavior.
- Share the gRPC listener by default and add standalone service namespaces to it.
- Mounting an existing replicated mongot directory must continue its existing indexes and
  replication state without reingestion, a manual migration step, or backup automation.
- Replicated and manually maintained indexes remain separate for their lifetimes. Users delete
  replicated indexes normally; disabling replication, detaching, and ownership conversion are out
  of scope. Operational pauses/restarts for source failure or disk pressure remain supported.
- Feed both producers into the same indexing scheduler, queue, and writer pipeline, with explicit
  source validation and completion/checkpoint policies instead of a second API writer loop.
- Keep the delta in upstream-owned code small and localized so later `mongodb/mongot` main changes
  remain straightforward to merge. Prefer composition, existing interfaces, and additive hooks
  with unchanged native defaults over duplicated upstream implementations or broad rewrites.

The [adversarial review](coexistence-mode-adversarial-review.md) records which findings still apply
under these requirements. Previous proposals for replicated public CRUD, revision translation,
replication-admin roles, and a standalone feature allowlist on native commands are superseded.

## Native compatibility boundary

The reference is `mongodb/mongot` main, not this fork's local `main`. The public upstream head
verified for this investigation is
[`70116297003f2423f9dfcf3eeb984c6f8d70c82b`](https://github.com/mongodb/mongot/commit/70116297003f2423f9dfcf3eeb984c6f8d70c82b),
committed on 2026-11-02. Record the current upstream SHA when implementing or refreshing this work
so comparisons are reproducible; do not invent a separate mongod support policy.

Direct comparison found identical copies of `CommandStreamMethods`, `MongoDbGrpcProtocolInterceptor`,
`IndexingWorkScheduler`, `DefaultIndexingWorkScheduler`, `SchedulerQueue`, `DefaultDocumentIndexer`,
`MongoDbReplicationManager`, and `ConfigJournalV1` in the fork and that upstream commit. This verifies
those source files, not every native code path or runtime combination.

Keep the existing services in
[CommandStreamMethods](../src/main/java/com/xgen/mongot/server/grpc/CommandStreamMethods.java):

- `mongodb.CommandService`: `AuthenticatedCommandStream` and `UnauthenticatedCommandStream`,
  including existing wire-message framing and bidirectional streaming behavior.
- `mongodb.BsonCommandService`: the existing `search`, `vectorSearch`, and `manageSearchIndex`
  methods, including their BSON marshallers and streaming/unary method types.
- All commands carried through those methods, including legacy search forms, getMore, killCursors,
  index management, error documents, and any existing response metadata.

Do not rename or repurpose native commands. Preserve native namespaces, index IDs, cursor IDs,
definition revisions, BSON shapes, optional-field behavior, error codes, retry semantics, and
routing behavior. Native creation currently starts definition versions at zero; leave that alone.
The positive standalone revision convention remains local to the standalone API.

[MongoDbGrpcProtocolInterceptor](../src/main/java/com/xgen/mongot/server/grpc/MongoDbGrpcProtocolInterceptor.java)
currently advertises/accepts a maximum wire version of 26. Preserve main's negotiation and header
validation; coexistence does not require a wire-version bump or a new minimum. In particular, do
not add mandatory `mongodb-*` headers: unknown reserved headers are rejected by the current path.
A transport handshake alone does not prove compatibility with every command of an older mongod.

Preserve main's native feature support and validation under the same configuration. The
standalone restrictions on views, synonyms, automatic embedding, and custom engines apply only
to standalone definitions. Do not apply those restrictions to native create/update commands or
replace the existing replication, materialized-view, or feature-flag factories with reduced
standalone implementations. Any topology or feature already supported on the native path must
retain that path; additional mixed-mode test coverage is required before claiming equivalent support.

Internal lifecycle changes are allowed where needed for coexistence, but their native observable
behavior must be covered by regression tests. Coexistence is not an opportunity to redesign native
catalog concurrency, bump the MongoDB catalog schema, or add native optimistic-revision arguments.

## Additive standalone services

Keep the existing [standalone protobuf](../src/proto/standalone/v1/standalone.proto) and its
`mongot.standalone.v1` namespace. Its Catalog, Document, Query, and Operations services are additions
to the native surface. Existing standalone clients retain their method names and field numbers.

`CreateIndex` always creates an independent push index. `Upsert` and `Delete` only accept push
indexes. `UpdateIndex` keeps its current expected-revision check and empty-generation rebuild;
callers reingest their source content. `DropIndex` affects only local desired state and local data.
Get/list/mappings/statistics and standalone search/joins resolve only the local catalog view.
A supplied MongoDB-owned ID never reaches a standalone writer or catalog mutation; return the
existing not-found behavior of the local catalog without exposing native metadata.

Native commands see only MongoDB-owned indexes. Local synthetic namespaces and UUIDs are not
published into MongoDB's authoritative catalog, native list-index results, or server index stats.
Local and native index names can coincide because lookup is scoped by authority. Index/generation
IDs must remain unique in the shared engine; an ID collision is rejected, never resolved by changing
ownership. Ownership is persisted metadata, not a guess based on name or `__push` namespace.

No new public replicated-create RPC, source selector on `CreateIndex`, translated native revision,
or replicated CRUD wrapper is needed. Any future API spanning both ownership types is a separate
proposal. Cross-source joins and standalone statistics over replicated indexes are outside this
release; preserve existing push-to-push joins and all native query behavior.

## Server composition and launch

Use one engine runtime, with native services and standalone services composed above it. The default
coexistence arrangement adds the standalone service definitions to the native gRPC server on its
existing address. Do not wrap or replace the native methods with the standalone dispatcher.

Refactor `StandaloneGrpcServer` so service construction can be reused without constructing another
runtime or owning another global lifecycle. Its existing dedicated listener remains the wrapper
for pure standalone launches. Extract reusable native composition from `OnPremMongotBootstrapper`;
do not run both complete bootstrappers against one data directory.

Build the shared core from native `MongotConfigs` in a native/coexistence launch and apply process
globals once. The standalone runtime's hard-coded Lucene factory defaults must not override native
indexing/querying configuration. Per-owner ingestion settings can differ; shared engine settings
must have one explicit owner and retain the native values on this path.

Keep both existing launch contracts:

- A native launch with its existing YAML/CLI configuration and no standalone extension behaves as
  main does. In particular, `server.grpc` and `syncSource` keep their native meanings.
- The existing `mongot-standalone --config standalone.yaml` launch remains MongoDB-free, including
  its listener, TLS/bearer settings, ingestion limits, and queries.
- A native launch enables coexistence through an optional, strictly parsed `standalone` block.
  With that block enabled, local services are registered and local catalog recovery is activated.
  Package the native launcher and standalone launcher together so this does not require two servers.

Proposed localhost coexistence configuration, extending the native YAML schema:

```yaml
storage:
  dataPath: ./data
server:
  grpc:                         # Existing native address and TLS schema
    address: 127.0.0.1:27028
    tls:
      mode: disabled            # Existing localhost development setting
syncSource:                     # Existing native connection schema
  replicaSet:
    hostAndPort: [127.0.0.1:27017]
    scramAuth:
      username: mongotUser
      authSource: admin
      passwordFile: ./secrets/mongodb.password
      tls:
        enabled: false
standalone:                      # New optional extension; absent means disabled
  enabled: true
  grpc:
    listener: shared
  ingestion:
    maxInFlightRequests: 64
    maxQueuedBytes: 268435456
    commitIntervalMillis: 1000
```

The new block reuses standalone limits and principal definitions, with paths resolved consistently
for that block. Do not change native path resolution, CLI/YAML precedence, unknown-field handling,
or default settings to match the standalone parser. Validate native configuration through its
existing parser and mapper; parse the new block separately. Pure standalone configuration does
not suddenly acquire mandatory MongoDB fields or create source clients.

For deployments requiring separate transport policies, allow `standalone.grpc.listener: dedicated`
with the existing standalone address/TLS settings. It serves the same standalone service namespace
against the same engine, not another process or independently reconciled copy. Existing mongod
connections stay on the native endpoint. This avoids changing a native listener's TLS policy simply
to enable the standalone extension.

## Authentication, admission, and health

Scope interceptors to their services. Standalone bearer authentication, role/index grants,
deadlines, payload limits, and admission must not apply to `mongodb.*` calls. Preserve native
protocol and envoy-metadata processing on native services without imposing those headers on the
standalone services. Server-wide Netty settings must remain compatible with native message sizes
and streams; add standalone limits at the standalone method/admission boundary instead of lowering
the native transport's limits.

The native community bootstrap uses `SecurityConfig.createAuthDisabled()` and relies on its
configured transport/network trust; the authenticated stream name does not implement application
authentication. Preserve main's accepted native TLS/mTLS configurations. Do not introduce a new
native mTLS requirement as a prerequisite for old mongod compatibility. Standalone non-loopback
TLS/bearer requirements still apply to the added services. If a shared socket cannot satisfy both
policies, use the dedicated standalone listener rather than changing the native contract.

A shared listener can register only one `grpc.health.v1.Health` service. Preserve main's existing
empty-service/native health behavior and authentication expectations; add named health entries for
standalone services. Route authorization for named standalone health requests appropriately, without
making native health clients provide standalone tokens. A pure standalone listener keeps its existing
health behavior. Update `mongotctl` in coexistence configurations to check named standalone health,
so a MongoDB outage does not make a functioning standalone API appear unavailable.

Native `HealthManager` and `CommunityReadinessChecker` receive a remote-only config/index-info view.
Their initialization/readiness result must not start depending on a slow or failed local push index.
Standalone readiness depends on local recovery, admission, and reconciliation, not source connectivity.
Preserve native readiness/replication status formats; new local diagnostics belong to standalone
OperationsService and metrics, not extra fields required in native replies.

Filter standalone `GetStatus` metrics as well as its index list: using the shared registry must
not accidentally expose native index metadata through an otherwise local-only service.

Keep source/network handling and RPC admission bounded independently, but route their actual
indexing work through the shared scheduler below. Source sockets must not block indexing workers,
and RPC workers must not wait synchronously for queued indexing. Shared-process CPU,
heap, disk, and fatal failures remain shared; the guarantee is continued standalone operation through
ordinary source outages while local resources are healthy, not hard process-level fault isolation.

## Combined desired state and persistent ownership

Retain two authoritative inputs:

- `LOCAL/PUSH`: definitions persisted by `LocalIndexCatalog`, edited only through standalone RPCs.
- `MONGODB/MONGODB`: definitions obtained through the existing MongoDB authoritative-catalog path,
  edited through existing native commands. Local copies are recovery caches, not another authority.

One `DesiredIndexCoordinator` supplies the union to one `DefaultConfigManager`. The current
standalone reconciler and `CommunityConfigUpdater` each submit a complete desired list; allowing
both to do that independently would delete the other's indexes. Refactor them into owner-scoped
snapshot providers and make the coordinator the sole complete-state updater.

Fetch MongoDB metadata outside shared locks. Serialize logical snapshot application and combine
it with the latest local state. Reject obsolete fetch completions; do not let an old response
replace a newer accepted snapshot. Keep the last remote desired state on connection, authentication,
identity, topology, or parsing failure. A failed fetch is never an authoritative empty catalog.
A successful complete snapshot may remove only remote-owned definitions; a local drop removes
only its local ID.

Preserve raw malformed remote entries and identifiable desired IDs against destructive diffs;
if membership cannot be determined, defer remote deletions. `unreadableDesiredIds` is a deletion
protection, not a general readiness flag. Keep native validation/error behavior consistent with
main and avoid treating a feature unsupported by the standalone API as a malformed native index.
Validate engine identity invariants across the combined snapshot before applying it.

Supply owner-filtered catalog/config facades to all source-specific consumers, including native
query lookup, metadata publishing, missing-collection checks, dropped-collection cleanup,
`ReplicationOptimeUpdater`, `SourceCollectionCountUpdater`, and readiness/quarantine lookups.
The same filtering applies to counts, staged generations, and recovery work, not just list results.
Do not run MongoDB `collStats` or optime updates for synthetic push namespaces. Preserve the
existing remote server registration, heartbeat, stats, and native catalog schemas; no additional
mongot participant or observer protocol is being introduced.

Persist ownership before initializing any generation and retain it through retirement. Bind
internal derived generations to the native parent/source as needed by existing native features.
Never route an auto-embedding or other derived native generation to the API ingestion manager.
Copy accepted definition snapshots where native replication mutates observed namespace metadata
in memory, so desired-state persistence and in-flight reconciliation do not share mutable objects.

On startup, recover local desired state and cached remote state together. Never supply a local-only
set of desired IDs to `ConfigInitializer`, which treats other journaled IDs as removed. Unverified
remote catalog state must not cause remote data deletion. Missing/conflicting ownership is a
recovery error, not permission to guess an owner or erase data.

## Shared ingestion pipeline

### Integration point verified in the code

The shared entry point is
[IndexingWorkScheduler.schedule](../src/main/java/com/xgen/mongot/replication/mongodb/common/IndexingWorkScheduler.java),
which already accepts `List<DocumentEvent>`, a generation ID, a `DocumentIndexer`, priority,
optional attempt/checkpoint metadata, and returns a completion future. Both initial collection
scans and decoded change-stream batches reach this method. API documents can reach the same
method after standalone BSON/ID validation creates update/delete events.

```text
MongoDB scan/change stream -> existing decode/normalization -> DocumentEvent batch
API Upsert/Delete         -> BSON validation and conversion -> DocumentEvent batch

Both -> shared IndexingWorkScheduler / SchedulerQueue
     -> shared indexing executor and document-event application
     -> the selected generation's IndexWriter / Lucene
```

This is one scheduler instance for ordinary Lucene indexing, not two copies of the queue.
Preserve native specialized strategies for embedding/custom engines; supported push text and
explicit-vector indexes use the default strategy. Each batch targets one generation and one owner.
Sharing a queue does not merge indexes or allow two sources to write the same index.

[SchedulerQueue](../src/main/java/com/xgen/mongot/replication/mongodb/common/SchedulerQueue.java)
already serializes batches per generation while allowing other generations to run. The default
scheduler allows two active indexing batches and uses a shared executor. The queue uses unbounded
collections: its execution semaphore does not bound accepted payloads or queued work.

Do not inject API documents into `InitialSyncQueue` or fabricate change-stream BSON, sessions,
resume tokens, or collection UUIDs. Those layers own MongoDB discovery, cursors, attempts, and
checkpoints. `DecodingWorkScheduler` is also unnecessary for already validated API documents.
Converge at the document-event boundary and keep the native source producers intact.

### Required adaptations

1. **Runtime ownership.** `MongoDbReplicationManager.create()` allocates the scheduler factory,
   and its shutdown closes the schedulers. Move shared factory/executor ownership to the engine
   runtime and inject it into both producers. Source restart cancels/drains only remote generations;
   it must not close the queue used by local indexes. Final runtime shutdown drains both producers
   and terminates shared schedulers/executors once, including strategies that share an executor.
   Keep existing factory signatures and their owning behavior; add an injection overload with an
   explicit resource-ownership contract for coexistence rather than changing every native caller.

2. **Ordered API batches.** `DefaultIndexingWorkScheduler` applies documents within a batch in
   parallel. Native change-stream decoding deduplicates events before scheduling; API batches
   permit repeated IDs and promise ordered application and partial-prefix failure. Add an ordered
   execution option to the common batch contract, defaulting to existing native behavior. API work
   runs `prepareBatch` once, then applies events sequentially with cancellation and limit checks
   between events. Do not deduplicate API requests or reduce native concurrency to satisfy this.
   Keep accepted requests FIFO within a generation, including repeated updates/deletes of one ID.

3. **Shared application with source policies.** Use the existing `DocumentIndexer` interface for
   an API adapter over the existing `IndexWriter` mapping/application path. Keep native
   `DefaultDocumentIndexer` unchanged where possible: it crashes on writer IO errors and encodes
   replication commit data, which must not become the API policy. The adapter supplies API error
   handling and its existing empty commit metadata policy; the common scheduler owns execution.
   Do not copy Lucene mapping/indexing internals into the adapter. API batches pass no replication
   checkpoint; omitting that field alone does not configure commit/error behavior.

4. **Completion barriers.** Scheduler completion means applied work, not visibility or durability.
   Include the API completion action in scheduled work: `INDEXED` completes after application,
   `VISIBLE` after reader refresh, and `COMMITTED` after the push generation's Lucene commit.
   Hold the generation's scheduling slot and write lease until that action settles, before the
   next API batch or writer retirement. A callback attached after today's scheduler future is too
   late: the queue is finalized before that future completes. Schedule/coalesce periodic push
   commits through the same generation lane. Keep native commits/checkpoints and their ordering.

5. **Failure/cancellation settlement.** The current scheduler expects the source to cancel the
   generation after a failed batch; an API error must not leave its queue head stuck. Explicitly
   remove/settle the failed API batch and release its lane after workers stop. Request cancellation
   marks only that request; existing `cancel(generationId, ...)` cancels every queued batch for the
   generation and is unsuitable for a single RPC. Check a captured deadline/cancellation token
   before work and between documents; do not interrupt an active Lucene call. A genuine generation
   failure fences it and fails its waiting work. Preserve existing native failure handling.

6. **Admission follows real work.** Retain request/byte credits until indexing, completion barriers,
   and writer leases settle, including after client disconnect. `RpcDispatcher.async` supports
   futures but cancels the pending future and releases credits on RPC cancellation. Separate the
   response future from a non-client-cancellable work-drained future; otherwise cancelled requests
   can accumulate unaccounted work. Bound retained decoded payloads/batches and reject excess API
   admission with `RESOURCE_EXHAUSTED`. Preserve native producer backpressure and limits.

7. **Priority and metrics.** Reuse the existing steady-state priority for API scheduling: below
   initial-sync change-stream catch-up and above collection scans. Record API origin separately
   from scheduling priority. This preserves the existing priority enum, comparator, FIFO behavior,
   and one-priority-per-generation invariant without introducing a second equal-ranked label.
   Bound API intake and measure starvation rather than claiming an untested latency guarantee.
   Existing scheduler metrics are tagged replication; retain their native meaning and expose
   API/combined counts through a narrow metrics hook without changing native metric schemas.

`ApiIngestionManager` becomes an admission, generation-fencing, and completion adapter. Remove its
synchronous writer loop from RPC workers. Never hold its global monitor while waiting for shared
queue execution: atomically acquire a generation write lease and submit under short coordination,
then settle asynchronously. Update/drop fences new submissions and drains/cancels queued/running
work through the shared lifecycle before retiring the writer.

This shares scheduling, batching, mapping, and execution while preserving MongoDB checkpoints and
API write concerns. It does not add an API mutation log, cross-index transaction, or source conversion.

## Lifecycle and safe retirement

Use a stable ingestion coordinator to route each initialized generation to its owner backend.
Keep `ApiIngestionManager` alive while MongoDB replication pauses, changes source host, or restarts.
Refactor the lifecycle's global replication-enabled flag, manager replacement, and restart traversal
to affect only remote generations. Source-scoped status must distinguish native replication from
push readiness; the push manager's `isReplicationSupported()` must not mask paused replication.
Fence asynchronous source initialization and replacement callbacks so old work cannot change the
replacement manager's state.

A routed `add()` is insufficient. `DefaultConfigManager.update()` is synchronized, and
[IndexActions](../src/main/java/com/xgen/mongot/config/manager/IndexActions.java) waits for all drop
futures during deletion and replacement. Split logical retirement from physical cleanup: persist
retirement, remove query/write eligibility, retain source ownership, then drain and clean up outside
the shared config-manager monitor. A slow remote drop must not block subsequent local create/update
or promotion. Apply this to explicit drop, staged replacement, and phasing-out cleanup.

`IndexActions.dropIndex()` currently proceeds with physical cleanup after an exceptional replication
drop future. In coexistence, a timeout or failure is not proof that a writer or lease has drained.
Retain and fence the affected generation until safe cleanup succeeds. Do not manufacture successful
futures to unblock reconciliation. Complete cleanup callbacks against the exact generation and
retirement operation, and persist progress before removing ownership tombstones.

Standalone statistics and push-to-push joins retain their existing local resource coordination.
Because added APIs do not acquire remote readers or create cross-source joins, this release does
not require a new public read-epoch protocol or standalone leases inside native replication's
clear/close/drop paths. Preserve those native paths and their checkpoint/cursor semantics. The
new asynchronous retirement layer must still honor the native backend's completion contract.

Each backend owns its commits. API commits using `EncodedUserData.EMPTY` must never touch remote
generations; replication owns its resume tokens and commit metadata. Preserve standalone write
concerns and native initial-sync/steady-state behavior. Local mapping changes still create empty
generations; native updates retain main's rebuild and promotion semantics.

## Cursor and index visibility boundaries

The shared engine's cursor manager needs origin-scoped facades. Native getMore/kill commands use
cursor IDs directly; they must not read or cancel a standalone cursor. Standalone continuation
retains its principal/index checks and cannot operate on a native cursor. Record origin internally
at creation and enforce it in every continuation, timer/explain lookup, cancellation, and abandoned
stream cleanup. Preserve native cursor ID encoding and native error behavior; no new cursor fields
or metadata are required from mongod. Lifecycle invalidation remains privileged across origins.

Native index lookup and query factories see only remote generations. Standalone queries, mapping
export, statistics, and join name resolution see only local generations. Keep index names and
revision conventions scoped to their existing protocols. Do not augment native list/status replies
with standalone indexes or require old clients to understand an ownership enum.

## Storage, outages, and shutdown

Mounting an existing upstream replicated mongot directory must work directly with its normal
source configuration. Open its existing journal and Lucene generations, preserve index IDs,
definition versions, resume/checkpoint metadata and `serverId.txt`, and continue native recovery
and replication. Enabling standalone commands must not force a fresh initial sync, reindex, empty
replacement directory, manual conversion, or backup script. Upstream recovery may still resync an
index for its ordinary reasons, such as unusable resume history.

Prefer additive ownership/catalog metadata over rewriting native definitions or commit data.
Before the first local create, atomically establish a coexistence marker and ownership sidecar.
For a recognized legacy native directory, adopt journaled entries as MongoDB-owned, including
recoverable quarantined identities. This follows verified directory/launch provenance, not a
namespace heuristic. Preserve the native journal representation where possible. Once the marker
exists, missing/conflicting ownership metadata is an error, not permission to reinterpret an API
index as replicated. Persist new local ownership before generation creation and retain it until
all retiring generations are cleaned up.

Use one lock, engine journal, factory, routed lifecycle facade, and underlying cursor manager.
The facade composes source-scoped native lifecycle instances so source restart cannot stop local
ingestion. Retain owner-scoped desired state through startup, staged builds, quarantine, and drop.
Atomic catalog revisions, native journal/generation identities, and monotonic ownership records
recover whether a transition was applied without adding fields to the native journal. Keep
MongoDB's remote catalog schema unchanged. Test first activation and crashes between sidecar
persistence, journal updates, generation creation, retirement, deletion, and tombstone removal.
Existing pure standalone directories retain their current recovery behavior. Backup automation and
reverse-format/downgrade tooling are outside scope; compatibility with old mongod clients does not
promise that older mongot binaries understand the added standalone state.

A source outage leaves local catalog/ingestion/query operations available. Preserve main's native
stale-query and recovery behavior; do not impose a new freshness policy on old mongod calls.
Local diagnostics can report source failure separately. Unsupported/corrupt source state and
identity mismatch are not permission to reinterpret remote indexes as local.

Source identity and topology checks retain the native bootstrap's existing behavior. This release
adds no identity probe, privilege, source enrollment protocol, or independent source-repointing
workflow. Use the original source configuration when mounting an existing directory. Native resume
token validation and ordinary resync rules continue to apply on source recovery.

One instance is expected. Stop the former process before mounting its directory in its replacement;
concurrent clones are unsupported. There is no user-facing stop-replication, detach, conversion,
or ownership-transfer workflow. Replicated indexes are removed with native deletion and standalone
indexes with standalone deletion. Outages and operational pauses do not change either index type.

Disk and memory budgets account for both producers' queued work, local/remote indexes, staged
builds, retained readers, and merges. Use the shared scheduler's native priorities plus the API
admission budget above; preserve native scheduling when no API work is present. An actual shared-disk or JVM failure can affect both owners. A paused
replication source must not suppress independent disk-pressure checks.

On shutdown, stop admission and snapshot producers, drain accepted work, and let each backend
checkpoint its own generations. Close replication clients early when needed to unblock synchronous
reads, as the existing replication shutdown does. Drain local statistics/query leases before
physical close; finish best-effort native server-state updates, close remaining clients, and release
the directory lock last. Shared indexing schedulers stop only after both producers and their
barriers/leases drain. Register every source monitor and partial-startup resource with this owner.

## Keeping upstream merges manageable

Place fork-specific behavior under the existing `standalone` packages or a small coexistence
composition package: ownership sidecars, combined desired-state coordination, filtered catalog
facades, API batch policy, RPC cancellation/admission, and service registration. Shared engine code
must not depend on generated standalone protobuf types, gRPC status codes, bearer principals, or
standalone write-concern enums. Translate those at the API boundary into engine-level operations.

Keep the upstream extension surface explicit and limited:

- **Scheduler construction:** an additive factory overload accepts externally owned scheduler
  resources. Existing constructors/factories retain their allocation and shutdown semantics.
- **Batch execution/completion:** preserve the current `schedule(...)` signature and native
  parallel/checkpoint path. Add an overload with a small execution/completion policy whose default
  delegates to the current upstream implementation. The API adapter supplies ordering, cancellation,
  and its pre-release barrier. Do not replace the scheduler run loop, copy it into a subclass, or
  introduce a general callback framework. Failed-batch settlement needs an exact, tested contract.
- **Writer policy:** implement the existing `DocumentIndexer` interface on the API side. Keep
  native commit metadata, error policy, codecs, and `DefaultDocumentIndexer` logic upstream-owned.
- **Composition:** attach extra service definitions and owner-filtered dependencies at bootstrap
  boundaries. Keep native protocol descriptors and source producers untouched. Cursor-origin
  enforcement belongs in transport-facing facades with privileged engine cleanup retained.
- **Retirement:** isolate deferred physical cleanup behind a small lifecycle/retirement interface.
  This is the highest merge-risk change because current reconciliation synchronously waits for
  drops. Keep it separate from scheduler work, retain native behavior by default, and cover its
  journal/drain invariants before relying on it. An empty completion hook must never pretend an
  unfinished physical drop succeeded.

Prefer a few ordinary, named interfaces/overloads to scattered `if (standalone)` branches. Reuse
native scheduling priorities and keep ownership in additive local metadata. Do not move/rename
upstream classes, reformat their files, or bundle unrelated cleanup with these changes. Do not
freeze a copy of upstream logic in a fork package: upstream fixes must continue reaching the path
both producers actually use.

Implement generic extension hooks, fork adapters, and any required lifecycle refactor in separate
focused commits. Record each shared-code modification's purpose, default behavior, invariants, and
regression tests alongside the pinned upstream SHA. This gives a concrete list to inspect after
each upstream merge rather than relying on conflict markers alone.

For every upstream refresh, diff that extension surface against the new main and run native tests
with the extension disabled, shared-scheduler tests with both producers, cancellation/commit/drop
races, and existing-directory restart coverage. Reuse an upstream extension point if one appears.
A textual merge succeeding is not proof that execution or finalization ordering is still correct.
The objective is a small, reviewable maintenance surface; conflict-free future merges cannot be
guaranteed, especially around lifecycle retirement.

## Implementation sequence

1. Pin the main-branch reference and capture native service/command/response behavior. Inventory
   the minimal shared-code extension surface above; preserve existing launch/configuration contracts
   and add the optional standalone block.
2. Extract shared runtime and service construction. Register additive standalone namespaces,
   service-scoped interceptors, origin-scoped cursors, and compatible health handling.
3. Add automatic adoption of existing native directories, additive ownership metadata, filtered
   catalog facades, combined reconciliation, and owner-scoped backend registration/restart.
4. Move indexing scheduler ownership to the runtime. Route API events through the shared queue with
   ordered execution, completion barriers, cancellation settlement, and work-lifetime admission.
   Preserve native producers, strategies, writer/checkpoint policies, and relative priorities.
5. Implement asynchronous retirement and crash recovery, including queued API writes and shared
   scheduler drain, before claiming local availability during native lifecycle work.
6. Package both launch modes and run upstream compatibility plus coexistence tests. Document the
   additive extension; no backup tooling, ownership conversion, or new mongod behavior is needed.

## Compatibility and acceptance tests

Resolve and record the latest `mongodb/mongot` main SHA for the implementation/release comparison.
Use upstream's supported configurations and compatibility tests as the reference. Test the same
native requests against reference and candidate with standalone services disabled and enabled.
Concrete mongod binaries are regression fixtures, not a separate product-version decision or a
prerequisite for this design. Compare normalized BSON/results and error codes rather than unstable
IDs/timings. Existing [compatibility tooling](../scripts/tests/community/compatibility_test.py) can
be reused and aligned with upstream; do not create a separate support floor for this fork.

Required checks include:

- Every existing gRPC service/method name, marshaller, streaming shape, wire-version header, reserved
  header check, native TLS mode, and optional legacy field behaves as the pinned main baseline does.
  No standalone token, capability call, revision field, or new command is required from mongod.
- Native create/list/update/drop, initial sync, inserts/updates/deletes, search/vector search,
  getMore/killCursors, legacy query forms, and error paths work with old mongod. Native definitions
  retain their original IDs/revisions and all supported feature/configuration behavior.
- Existing standalone clients create, manage, ingest, query, page, join, and inspect local indexes
  while native replication and queries run. Equivalent names across owners do not collide. Local
  IDs cannot be used through native lookup, and remote IDs cannot be mutated through standalone RPCs.
- Both native wire-stream and BSON-service paths remain registered; the extension does not change
  native transport limits or health. A shared listener registers health only once. Native callers
  never hit the standalone interceptor; standalone callers never require MongoDB protocol headers.
- Cursor IDs supplied through the wrong origin cannot read or cancel the other protocol's cursors.
  Native cursor representation and errors stay compatible. Local principal grants still apply.
- Lose MongoDB at startup and during steady state. Local management/writes/queries recover and keep
  working; neither catalog polling nor metadata publishing touches push indexes. Native health and
  recovery retain their source-specific semantics, and failed local indexes do not hold native readiness.
- Hold or fail a remote drop future while local reconciliation continues. Verify no physical deletion
  before producers/leases drain, no old callback updates a replacement, and retirement survives restart.
- Crash at every catalog/journal/cleanup boundary. Recover both ownership sets without a local-only
  startup filter deleting remote data. Exercise quarantined journal entries, first-activation interruption,
  inconsistent restores, native checkpoints, and committed local writes. Mount an unchanged existing
  replicated directory with the extension enabled and verify automatic adoption preserves data,
  IDs, journal state, and valid checkpoints without forced rebuilding or manual migration.
- Prove both producers submit ordinary Lucene work to the same scheduler/queue/executor instance.
  Restart or pause only replication while API work is queued; API work must finish normally.
- Preserve API ordering with repeated IDs, ordered deletes, partial-prefix failure, concurrent
  requests per generation, and a failed request followed by another queued request. Native parallel
  batching and checkpoint behavior must remain unchanged.
- Repeatedly cancel queued/running API RPCs. Verify credits stay held until work drains, cancellation
  affects only the requested operation, other requests progress, and no writer closes during
  application/refresh/commit. Test all write concerns and coalesced background commits.
- Verify FIFO across API/native work sharing the existing steady-state priority, native priority ordering,
  bounded retained API payloads, and source-attributed metrics. API writer failure must not erase
  native checkpoints or change native error handling.
- Exercise the unchanged native constructors and `schedule(...)` calls with the extension disabled;
  defaults must preserve native batching, finalization, cancellation, resource ownership, and metrics.
  Keep fork-adapter tests separate from these native regression checks for future upstream merges.
- Saturate one workload and verify bounded queues plus reserved capacity for the other. Exercise
  shared-disk pause/resume, backfill/build admission, and shutdown with a hung source socket.

The feature is complete when existing mongod clients continue to work unchanged and additional
standalone commands operate on independent indexes in the same process, including during ordinary
source outages under the stated local-resource assumptions.

## Implementation notes

- `standalone/coexistence` composes scoped catalogs, native config/cursor facades, and a routed
  lifecycle without copying the native config manager or replication manager.
- Native factory overloads accept an externally owned scheduler factory. Source manager shutdown
  drains its producers while the engine keeps the scheduler alive for local generations.
- API producer operations use the existing generation FIFO, priority and executor, with sequential
  document application and refresh/commit barriers inside the scheduled operation. Native batch
  finalization/checkpoint behavior retains its original defaults.
- Deferred retirement is enabled only for coexistence. Logical removal is journaled, physical drain
  runs outside reconciliation, and exceptional drain retains the writer and recovery entry.
- The marker and ownership sidecar are additive. Ownership records remain after deletion; this
  implementation deliberately has no tombstone compaction or downgrade/conversion command.
- Shared queue gauges include both producers. Native enqueue/dequeue and batch timers retain their
  native policy; API admission and completion are reported under `standalone.*` metrics.
- Source reconfiguration runs separately from reconciliation and preserves native materialized-view
  source updates and disk gates. Recheck that adapter when upstream restart policy changes.
- Live acceptance coverage is in `scripts/tests/community/coexistence_test.py`. The existing native
  and standalone suites remain required; this is not a claim that every case in the broader
  adversarial/crash checklist has been exhaustively exercised.
