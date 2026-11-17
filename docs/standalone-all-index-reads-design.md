# Direct API reads across all index ownership types

Status: Implemented in this worktree. Source baseline: `5d2b055a`, 2026-11-16.

Allow the standalone gRPC API to discover, inspect, and query every search and vector index
known to this mongot process, whether populated through direct ingestion or MongoDB replication.
Introduce a shared read catalog and generation leases, while retaining the existing ownership
boundaries for index management and document writes. A replicated index must never enter the
standalone ingestion manager merely to become readable.

This extension supersedes the standalone-only **read** restrictions in the
[coexistence design](coexistence-mode-design.md) and
[operating guide](coexistence-mode.md). Their single-writer, native protocol,
recovery, and ownership requirements continue to apply. The operating guide and clients now
describe the expanded read surface.

## Scope and observable behavior

“All indexes” means all logical search and vector indexes in the process's accepted catalog,
including indexes still building, failed, recovering, or quarantined. Discovery does not require
a usable reader. Execution requires a queryable active generation and an operation supported by
that index type. Internal materialized indexes and retiring generations are implementation
resources, not additional public logical indexes.

Execution support covers the Lucene engines available in this build. Vectorlite and its
`indexNamespace` query option are outside this extension's scope; the community factory does not
initialize that engine.

The API remains local to one mongot process. It does not discover ordinary MongoDB B-tree indexes,
query another mongot, merge results across shards, fetch source documents from MongoDB, or execute
arbitrary aggregation pipelines. Results and statistics describe this instance's indexed data.
Existing per-index partition reduction still applies. Direct results retain the current hit and
metadata format; full source documents are available only through configured stored source.

| Direct operation | Standalone-owned index | Replicated index |
| --- | --- | --- |
| ListIndexes and GetIndex | Discover definitions and state | Same, including namespace and ownership |
| GetIndexMappings | Inspect active search mappings or vector fields | Same, preserving native definition features |
| GetIndexStats and GetIndexFieldStats | Inspect a pinned active generation | Same, with capability-specific unavailable measurements |
| Search and SearchMeta | Existing engine semantics | Same engine semantics and effective feature policy |
| Autocomplete through Search | Requires an autocomplete field mapping | Same requirement |
| VectorSearch | Query supported vector representations | Same, including native query materialization where required |
| Explain, GetMore, and KillCursor | Existing direct API contracts | Same direct API contracts |
| Join predicates | Existing search-index joins | Permit either owner on each side, with explicit target bindings |
| GetStatus | Include authorized logical indexes | Include both owners; retain service health semantics |
| CreateIndex | Creates standalone-owned indexes | No replicated-create operation |
| UpdateIndex and DropIndex | Existing revision and ownership rules | Reject through the standalone mutation path |
| Upsert, Delete, and DeleteByQuery | Existing write rules | Reject, including DeleteByQuery dry runs |

Pure standalone deployments use the same read abstraction with only a standalone source.
Existing native MongoDB commands retain their replicated-only namespace view and wire behavior.
No index ownership conversion, reingestion, journal migration, or new listener is required.

## Constraints before this extension

The restriction is enforced at several independent layers. Changing only the catalog passed to
`StandaloneRuntime` would leave reads broken and weaken the separation used by writes.

| Component | Current constraint and implication |
| --- | --- |
| [CoexistenceRuntime](../src/main/java/com/xgen/mongot/standalone/coexistence/CoexistenceRuntime.java) | Passes `localIndexes`, an ownership-filtered engine catalog, into the direct runtime. The native side receives a separate remote view. |
| [StandaloneRuntime](../src/main/java/com/xgen/mongot/standalone/runtime/StandaloneRuntime.java) | Descriptions start in `LocalIndexCatalog`; mappings and query snapshots also require API-ingestion readiness. Mapping export assumes active and desired revisions agree. |
| [StandaloneQueryService](../src/main/java/com/xgen/mongot/standalone/server/StandaloneQueryService.java) | Search resolves the local desired catalog before the live generation. Cursor leases recheck local existence on GetMore. |
| [StandaloneSearchMeta](../src/main/java/com/xgen/mongot/standalone/server/StandaloneSearchMeta.java) | Selects a local index by query name. `index_id` and `generation_id` are fences, not selectors. Missing unfenced indexes use empty-query semantics. |
| [StandaloneJoinPlanner](../src/main/java/com/xgen/mongot/standalone/server/StandaloneJoinPlanner.java) | Foreign names resolve locally; all dependencies use API-ingestion leases and local existence checks. |
| [StandaloneIndexStatistics](../src/main/java/com/xgen/mongot/standalone/runtime/StandaloneIndexStatistics.java) | Resolution, collection leases, cancellation, and publication checks depend on API ingestion. |
| [StandaloneDefinitionValidator](../src/main/java/com/xgen/mongot/standalone/catalog/StandaloneDefinitionValidator.java) | Rejects native-only creation features; its normalization removes synonyms. It cannot serve as the replicated definition exporter. |
| [MongotCursorManagerImpl](../src/main/java/com/xgen/mongot/cursor/MongotCursorManagerImpl.java) | Already supports exact-generation direct cursors over the shared engine and tracks direct transport origin separately from index ownership. Reuse this path. |

The existing [IndexStatus](../src/main/java/com/xgen/mongot/index/status/IndexStatus.java)
predicate `canServiceQueries()` permits `STEADY`, `STALE`, `RECOVERING_TRANSIENT`, and
`RECOVERING_NON_TRANSIENT`. API ingestion currently accepts only its registered `STEADY`
generations. Applying that check to replicated indexes would incorrectly reject useful reads
during source outages and rebuilds.

## Shared read catalog

Add `DirectReadCatalog` in the standalone runtime package. It is a read-only facade over
accepted metadata and engine state; it is neither a second persistent catalog nor a reconciler.
Keep `LocalIndexCatalog`, ownership-scoped engine views, and `ApiIngestionManager` as the only
dependencies of direct mutations. Prefer explicit `reads()` and `localWrites()` dependencies to
an accessor whose meaning changes according to its caller.

The implementation exposes `DirectReadCatalog.snapshot()`, `describe(indexId)`,
`resolve(indexId, generationFence)`, and `acquire(indexId, generationFence)`. Descriptors are
immutable `IndexDescription` messages. `GenerationReadLeases.Lease` retains both the logical
generation and its initialized execution resource, which can differ for auto-embedding indexes.
Its `definition()` returns the logical definition, `index()` supplies the pinned initialized
resource, and `retired()` checks invalidation and both logical and physical readiness. `close()`
is idempotent. These handles remain internal; no writer is exposed through a read RPC.

The descriptor carries ownership, logical ID, name, type, namespace, desired definition,
active and staged generation identities, their versions and statuses, queryability, and supported
read operations. A desired definition and an active definition are separate values. Capture
mutable definitions as immutable copies; replication can change observed collection names.

Single-ID discovery uses keyed lookups in the accepted remote definitions and quarantine maps.
Only ListIndexes enumerates the catalog. Execution resolution, lease acquisition, GetMore, and
join dependency checks do not render descriptors or serialize definitions. Active generations
resolve under the config-manager monitor without consulting the coordinator's metadata provider;
only an absent active generation needs a targeted metadata lookup to distinguish pending,
quarantined, and unknown IDs. That lookup releases the manager monitor before taking the
coordinator monitor and rechecks the active generation afterward.

Compose two metadata providers:

1. A standalone provider backed by `LocalIndexCatalog`, including its existing dropped-ID and
   revision semantics.
2. A replicated provider backed by the last accepted remote definitions in
   `CoexistenceConfigManager`, enriched with live, staged, and quarantined engine entries. Add a
   read-only snapshot accessor; do not broaden the native `getLiveIndexes()` facade or fetch
   MongoDB during a direct RPC.

Enumerate logical identities from accepted desired state and recovery/quarantine metadata, not
just `InitializedIndexCatalog`. Collapse live and staged entries into one descriptor. Preserve
unreadable known IDs with the available identity and a sanitized reason; entries without a usable
active generation remain unqueryable. Do not publish raw journal BSON. Failed entries remain
discoverable. Removed replicated indexes disappear after the accepted catalog deletion;
retired-only generations are never listed.
Retain existing standalone tombstone behavior for explicit GetIndex calls.

If desired metadata becomes unreadable while native reconciliation retains a parsed live or staged
generation, use that generation for identity and active/build inspection. Omit the unknown desired
definition, revision, and version, report a sanitized incompatibility reason, and retain the active
generation's queryability. Include these retained identities in discovery without including
phasing-out generations or restoring deleted standalone entries.

Ownership comes from persisted local ownership metadata and accepted replicated identities.
Unknown IDs do not become replicated simply because `local.owns(id)` is false. Reject identity
collisions through the existing coordinator rather than choosing a preferred owner.

Capture immutable descriptors while holding the coordinator, config-manager, and local-catalog
monitors in that order. The coordinator's snapshot accessor copies accepted definitions; this
keeps source changes and reconciliation outside the snapshot interval. No Lucene reads or network
calls run while building the projection. Pure standalone mode uses the same projection without
a replicated metadata provider. Acquisition revalidates current live-generation identity and
readiness while registering its lease under the config-manager monitor. No persisted epoch or
second catalog is needed; pagination retains immutable values, and execution uses exact generation
fences. Never call the coordinator provider while already holding the config-manager monitor.

## Public metadata and selection

Keep the `mongot.standalone.v1` namespace and existing RPCs and field numbers. The following are
additive fields in [standalone.proto](../src/proto/standalone/v1/standalone.proto).
They describe the wire contract; implementation must regenerate all clients together.

| Message | Additive fields |
| --- | --- |
| IndexDescription | `ownership = 10`, `namespace = 11`, `queryable = 12`, `active_definition_bson = 13`, `read_capabilities = 14`, `active_version = 15`, `desired_version = 16`, `build = 17`, `source_status = 18` |
| ListIndexesRequest | `ownership = 1`, `page_size = 2`, `page_token = 3` |
| ListIndexesResponse | `next_page_token = 2` |
| SearchMetaRequest | `target_index_id = 5`, `join_indexes = 6` |
| QueryRequest | `join_indexes = 6` |
| GetIndexMappingsResponse | `index_type = 6`, `vector_fields_json = 7`, `active_version = 8` |
| IndexStats | `active_version = 17` |

New clients must not silently use selection-changing fields against an old server: protobuf
unknown-field handling could otherwise turn an explicit replicated target into a local name lookup.
When using new selectors, join bindings, ownership filters, or pagination, send the
`mongot-direct-read-contract: all-index-reads-v1` gRPC metadata header. The upgraded interceptor
validates it and echoes it in response headers; clients require that acknowledgment before
accepting results. An absent acknowledgment means the server is incompatible, with no automatic
fallback to a legacy query. Unsupported contract values fail with `FAILED_PRECONDITION` on an
upgraded server. Existing clients need not send the header. Apply this only to standalone services,
and test both old-client/new-server and new-client/old-server combinations.

When rejecting an otherwise successful Search or VectorSearch reply for a missing or incorrect
acknowledgment, attempt KillCursor for any nonzero returned cursor ID using the same authenticated
connection. Use a fresh cleanup deadline capped at five seconds and preserve the contract error
if cleanup fails. Never discard an accepted cursor or retry the query using legacy selection.

Define `IndexOwnership` values `UNSPECIFIED = 0`, `STANDALONE = 1`, and `REPLICATED = 2`.
Unspecified in the list filter means both owners. An unknown enum value is invalid. A namespace
contains database, collection UUID, last observed collection name, and optional view name.
Collection UUID and index ID establish identity; a display name is not an identity fence.
For standalone entries, omit the public source namespace rather than suggesting the synthetic
internal namespace is a MongoDB collection.

`IndexVersion` is a presence-bearing message with an optional definition version and a version
domain (`STANDALONE_REVISION` or `NATIVE_DEFINITION_VERSION`). Preserve native version zero and
missing versions; never add one or substitute `orElse(1L)` on a replicated read. Existing numeric
revision fields retain standalone values. For replicated entries they carry the native value
when present and protobuf zero otherwise; the new version message distinguishes zero from
unknown. They confer no permission to call UpdateIndex. Generation IDs remain opaque exact
identifiers and are the authoritative execution fences.

`build` describes a staged generation separately, including its definition, version, and state.
`source_status` exposes the native status code; quarantine failures use a sanitized reason. Preserve existing standalone
`lifecycle_state` values. Replicated lifecycle summaries distinguish pending, building, ready,
recovering, stale, failed, and quarantined; `queryable` is independent of the summary. An index
may be building a replacement and still serve its active generation. A failed staged build must
not make a healthy active generation appear unqueryable.

A quarantined replacement remains visible in `build` with state `QUARANTINED` and its known
generation ID. Its unreadable definition and unknown definition version are omitted. With a
parsed active generation, the summary is `FAILED`, `source_status` still describes the active
generation, and `queryable` retains the active generation's readiness. A sanitized failure message
identifies the incompatible replacement. If no active generation can be parsed, the summary is
`QUARANTINED` and execution fails with `FAILED_PRECONDITION`.

Read capabilities describe operation support, not current readiness or the caller's mutation
privileges. Include search, search metadata, vector search, expanded search mappings, vector
definition inspection, index statistics, field statistics, and joins where supported. Derive
them from the reader/definition adapter, not ownership. Unsupported measurement components keep
the existing explicit unavailable/partial statistics representation.

### Discovery and pagination

ListIndexes defaults to all authorized logical indexes. Sort deterministically by canonical ID.
An ownership filter lets existing management tools request standalone entries explicitly.
Authorization filtering happens before page construction and token creation.

For compatibility, an empty request retains unpaged behavior within the configured response byte
limit. Return `RESOURCE_EXHAUSTED` with a pagination hint if it cannot fit; never silently omit
indexes. A positive `page_size` opts into bounded pages, with a maximum of 500 entries
and the byte limit taking precedence. Tokens reference a bounded, expiring immutable metadata
snapshot and bind the principal, filters, page size, and server incarnation. Recheck current index
grants and logical deletion on every page; restart pagination if those invalidate its snapshot.
Expired tokens return `FAILED_PRECONDITION`. A list page is an observation, not an execution fence.

GetIndex returns both desired and active definitions when available. It can succeed while reads
are unavailable. Large definitions and list snapshots remain subject to explicit byte/memory
limits. Metadata response limits use `maxBatchBytes + 64 KiB`, matching the transport envelope
allowance; cursor and statistics payloads keep their existing bounds. GetStatus includes descriptions for both owners under its existing administrator grant
requirement; it is a bounded summary, with ListIndexes as the paginated discovery endpoint.
If the status response cannot fit, fail explicitly rather than truncate it without notice.

### Query targets and duplicate names

Search and VectorSearch already select by `index_id`. Resolve that ID across both sources and
retain the optional generation fence. An explicit BSON `index` name must agree with the selected
definition. Do not search a global name map: replicated indexes in different collections can
share a name, and a standalone index can share it too.

Preserve the existing SearchMeta behavior when `target_index_id` is absent: the BSON stage's
`index` name, defaulting to `default`, resolves in the standalone catalog, and `index_id` remains
an identity fence. A missing unfenced local name retains the existing empty-result semantics.
Do not fall back to a replicated match.

With `target_index_id`, select either ownership type directly. If BSON supplies `index`, validate
it against the selected name; if omitted, insert that selected name before engine parsing. Any
legacy `index_id` supplied alongside the target must match it. A missing explicit target is
`NOT_FOUND`, not an empty query. This opt-in selector avoids silently changing old SearchMeta
requests and supports any replicated namespace without adding namespace syntax to query BSON.

`JoinIndexBinding` contains `alias`, `index_id`, and optional `generation_id`. The repeated
`join_indexes` envelope field binds the existing join `from` string to an exact target. For
example, `from: "inventory"` can bind to a replicated index ID in another collection. Bindings
take precedence over legacy name lookup; omitted `from` still means self-join. Unbound names
retain standalone lookup. Reject duplicate/unused aliases and conflicting fences. Authorize all
targets before acquiring readers or executing foreign predicates. This keeps the shared
`JoinOperator` BSON grammar unchanged while enabling all four ownership combinations.

## Query readiness and generation lifetime

Separate three decisions: whether an index exists, whether its active generation can serve this
operation, and whether that generation is still safe to access. A Java reference obtained from
the initialized catalog establishes none of the latter lifetime guarantees.

| Active state | Discoverable | New reader-based operation |
| --- | --- | --- |
| Pending, unknown, not started, initial sync, or uninitialized | Yes | `UNAVAILABLE` |
| Standalone steady and admitted by its lifecycle | Yes | Allowed |
| Replicated steady | Yes | Allowed |
| Replicated stale or either recovering state | Yes | Allowed if initialized and `canServiceQueries()` remains true |
| Failed or source collection missing | Yes while still cataloged | `UNAVAILABLE` |
| Quarantined definition with no usable active generation | Yes | `FAILED_PRECONDITION` with a sanitized incompatibility reason |
| Logically deleted | No new replicated descriptor | `NOT_FOUND` |

Do not gate replicated reads on API-ingestion registration, source connectivity, or desired/active
version equality. During a replacement build, mappings, queries, and statistics use the active
generation and its definition. They must never label an old reader with the new desired mapping.
Source status indicates stale/recovering behavior; a successful query does not promise replication
is caught up. Refresh makes indexed data visible, not committed or synchronized with MongoDB.

Add `GenerationReadLeases` to protect direct read resources across both lifecycles.
It records exact-generation admission, invalidation, outstanding leases, and a drain future.
The internal handle supplies the initialized resource and logical definition. Statistics sample
writer heap as a read-only diagnostic; RPCs do not expose ingestion or commit methods.
Keep existing write leases in `ApiIngestionManager`; compose their drain with direct-read drain
for standalone retirement. Reads must no longer hold API-ingestion leases as their common API.

Acquisition checks logical existence, capability, the requested fence, live-generation identity,
initialization, and owner-specific readiness, and registers its lease atomically with respect to
retirement admission. Revalidate after taking the lease before opening a snapshot or cursor.
If the active generation changed, fail the resolved generation fence and let the caller retry. Never retry an already executed query or batch.

Integrate admission closure and invalidation with catalog removal, active-generation replacement,
physical close/drop, and shutdown. A callback only in the direct DropIndex RPC is insufficient:
replicated changes arrive through native reconciliation. Cover
[IndexActions](../src/main/java/com/xgen/mongot/config/manager/IndexActions.java),
[StagedIndexesSwapper](../src/main/java/com/xgen/mongot/config/manager/StagedIndexesSwapper.java),
and the shutdown path. Use small additive hooks with no-op native defaults. A lifecycle wrapper
alone cannot cover paths that close resources without normal replication lifecycle participation.

On logical removal or active replacement:

1. Close admission for the affected generation as part of publishing the catalog transition.
2. Mark its direct leases invalid, then notify direct cursor, join, and statistics owners outside
   catalog locks. Cancel pending collection jobs and release cached continuations.
3. Let in-flight work observe cancellation and close its snapshots. Combine its drain future with
   the existing producer/lifecycle drain before removing or closing initialized resources.
4. Close and delete through the existing lifecycle once drains finish. Never force-close resources
   that a direct lease still uses merely because a timeout elapsed; retain the retirement entry
   and surface the failure through the existing deferred-retirement mechanism.

Keep catalog transition locks short. Do not perform Lucene reads, embedding calls, cursor kills,
lease callbacks, or blocking waits under the config-manager or registry lock. Finish drain
futures outside locks because their continuations may close engine resources. Tests must cover
lock ordering and every close path, including recovery, quarantine, and source reconfiguration.

In deferred mode, attach an asynchronous continuation to the direct-read drain using the configured
retirement executor. The worker begins producer teardown only after the drain completes; closing
the last lease must not execute teardown on a query worker or the shared notification thread.
Retain the recovery entry and open resources if the executor rejects that continuation.

## Cursor and snapshot contracts

Reuse the generation-based `MongotCursorManagerImpl.newCursor` path. Cursor transport origin is
independent of index ownership: a direct query over a replicated index creates a **direct** cursor.
[NativeCursorManager](../src/main/java/com/xgen/mongot/standalone/coexistence/NativeCursorManager.java)
must continue denying native GetMore/kill access to those IDs. Direct GetMore and KillCursor must
require a lease owned by the authenticated principal and current grants, even for a native index.

A direct cursor lease retains its index ID, generation ID, principal, generation read lease, and
all join dependencies. Every batch checks invalidation and current authorization before use;
every completed batch checks invalidation again before publication. Never re-resolve GetMore to
a newer generation. A request already admitted before a catalog transition may finish before
that transition is observed, but no subsequent batch can begin on the invalidated lease.

The direct API will invalidate old-generation cursors on a replicated swap, matching the explicit
invalidation already performed on standalone schema updates. This choice bounds resource retention
and gives joins one consistent rule. Native cursors retain their existing phase-out behavior:
direct invalidation must kill only direct cursor IDs, not call `killIndexCursors` on a still
phasing-out generation that has native cursors. Physical deletion continues to close all cursors
when the native lifecycle requires it.

Register a direct lease and its invalidation callback before publishing a cursor ID. If deletion
or replacement races registration, detect the invalid generation, kill the new cursor, and
release its capacity. Cleanup is idempotent across exhaustion, cancellation, response abandonment,
KillCursor, idle expiry, engine-side cursor termination, index invalidation, and shutdown.
Transfer cleanup ownership when inserting the cursor lease, before registering callbacks. Register
callbacks and release the cursor under the same lease monitor so retirement cannot close a join
plan during registration. Registration failures use the registered lease's idempotent cleanup.
Preserve the existing cursor capacity and idle expiry. Add terminal notification or reconciliation
for engine-side removal so API capacity and generation leases cannot leak until another client call.

Unary SearchMeta, mappings, and statistics use short leases and close them before returning.
Statistics continuations retain immutable values, never Lucene readers. Retain existing generation,
principal, filter, and snapshot fences for field-statistics tokens; invalidate them on replacement
or deletion. Joins pin every dependency and invalidate the entire plan if any generation changes.
Snapshots across different indexes are independent; cross-source joins do not establish a common
MongoDB transaction time or globally consistent replication point. Asynchronous statistics jobs
retain leases only while collecting, release them on cancellation or completion, and publish
cached values only after a final invalidation check.

## Mappings and native query features

For search indexes, reuse `LuceneIndexMappingExporter` against a pinned active generation, including
all its partitions. Preserve current configured-field expansion, observed dynamic types, and
`dynamic: false` output semantics. Autocomplete remains an ordinary Search query and succeeds only
where that active mapping supports it. No separate autocomplete endpoint or source-document scan
is needed.

Export definitions through an owner-neutral public-definition mapper based on `IndexMapper`.
Do not run replicated definitions through standalone creation validation or its synonym-stripping
normalizer. Preserve analyzers, synonyms, view-related semantics, stored source, vector settings,
and supported native options. If the public definition format omits source namespace/view identity,
carry that identity in the descriptor instead of silently losing it. For materialized indexes,
distinguish the user definition from a derived execution definition; internal fields must not
appear as user mappings.

Extend GetIndexMappings for vector indexes: set `index_type`, return the active public vector
definition in `definition_json` and its field array in `vector_fields_json`, and leave
`mappings_json` empty. Search responses preserve their existing JSON shape and leave vector fields
empty. This makes exploration available for both types without inventing a search `mappings`
object for a vector definition. Update the current protobuf comment that promises every exported
definition can be passed to standalone CreateIndex/UpdateIndex: native-only definitions are
inspectable but may require adaptation before standalone creation.

Share the native parser, feature policy, and execution adapters. Ownership must not select a reduced
query language. Existing replicated synonyms and views use their initialized engine resources;
direct reads must not create another replication/materialization pipeline or reapply a view pipeline
to already indexed data.

The current direct VectorSearch requires `queryVector`, whereas
[VectorSearchCommand](../src/main/java/com/xgen/mongot/server/command/search/VectorSearchCommand.java)
supports query materialization and specialized nested/materialized readers. Extract reusable
query preparation/execution adapters from that native path and inject the configured embedding
service when needed. Preserve deadlines, feature gates, model validation, and explain semantics.
Revalidate the generation after an external embedding call and before execution. Explicit-vector
queries retain their current path. Native-supported auto-embedding and nested index variants must
be covered before claiming this extension serves all queryable index variants; merely listing
them while rejecting every query is not completion.

Compatible standalone schema updates keep the physical generation and existing direct cursors.
A direct lease captures the immutable definition and logical reader together under the config
manager monitor. Queries, join snapshots, mapping exports, and statistics use that captured reader;
new leases observe the newly published schema. Rebuilds and drops invalidate leases and defer
physical retirement until those leases drain. Statistics caches additionally key by schema revision,
and collectors discard results if their captured revision is replaced during collection.

Statistics use the same active-generation selection and capability adapters. Preserve the existing
storage ledger, snapshot timestamps, cache bounds, and distinction between measured, estimated,
and unavailable costs. A native reader that cannot supply one field measurement should report it
as unavailable rather than hide its index. Use the existing metered and multipart Lucene reader snapshot adapters, including the derived
Lucene resource behind materialized indexes. Count that physical resource once; do not add the
remote materialized collection to local storage totals. Field representations retain physical
names for diagnostics, while configured fields and version metadata use the logical definition.
Capture the logical-to-physical field mapping from the pinned generation, including unresolved
offline auto-embedding generations. Apply its inverse before grouping field measurements,
assigning storage and memory owners, or filtering requested paths. This combines lexical and
derived vector costs under their public field path without counting physical resources twice.
Do not change replication checkpoints or commit writers to collect diagnostics.

## Authorization and errors

Keep the existing role division: catalog for discovery/inspection, query for execution and cursor
operations, and write for document mutation. Check explicit index grants before resolving an ID
or returning its metadata. Filter discovery by grants; do not include denied IDs in counts,
pagination tokens, diagnostics, or errors. Join planning checks every dependency before work.

Index grants apply to both ownership types. The existing `*` grant consequently includes replicated
reads after this change, including the implicit loopback principal. Document this access expansion
in release notes and deployment guidance. Standalone bearer authorization is independent of MongoDB
database roles; a direct reader does not inherit a caller's MongoDB collection permissions. Retain
the current TLS, bearer, and shared-listener restrictions without changing native authentication.

Writes still resolve only through the local mutation catalog. For a replicated ID, preserve the
existing `NOT_FOUND` mutation response after the normal authorization check; do not route based
on a client-supplied ownership flag. This applies to DeleteByQuery before its scan starts and to
dry runs. Read capabilities never authorize mutations.

| Condition | Direct response |
| --- | --- |
| Malformed ID, query, binding, enum, or wrong query index type | `INVALID_ARGUMENT` |
| Missing required role or index grant | `PERMISSION_DENIED` without index metadata |
| Unknown/deleted explicit read target | `NOT_FOUND` |
| Known but temporarily unqueryable generation | `UNAVAILABLE` |
| Valid but mismatched generation fence or quarantined definition | `FAILED_PRECONDITION` |
| Unsupported reader capability | `FAILED_PRECONDITION` with a capability reason |
| Cursor absent, expired, invalidated, native-origin, or owned by another principal | `NOT_FOUND` |
| Admission, collection, response, or snapshot budget exceeded | `RESOURCE_EXHAUSTED` |
| Request cancellation or deadline | Existing `CANCELLED` or `DEADLINE_EXCEEDED` behavior |

The legacy SearchMeta unfenced-missing and fenced-mismatch rules remain as described above.
Keep mapping-specific type errors compatible where already defined. Sanitize lifecycle and
quarantine reasons before exposing them; do not return connection strings or raw exception dumps.

## Service health and operational behavior

Management readiness continues to describe direct service recovery and admission, not whether
every replicated index is healthy or MongoDB is reachable. Per-index queryability and source state
report those conditions. Preserve native health and its replicated-only config view.

Keep existing request, cursor, join-work, and statistics limits across both owners so expanding
discovery does not create a separate unbounded workload. Expose bounded `standalone.reads.resolution{status}`, `standalone.reads.active`,
`standalone.reads.invalidations`, `standalone.reads.drains`, and
`standalone.reads.drain.total_nanos` metrics. Existing RPC metrics supply operation/status timing. Preserve GetStatus's existing filtering of raw native metrics; typed index discovery
does not authorize unrestricted registry export. Use existing index labels only in already-scoped
diagnostics, and never label metrics with bearer credentials or raw query text.

On shutdown, stop admission, cancel direct operations, invalidate direct cursors and cached jobs,
and drain direct leases before closing the shared engine. A failure to drain must be observable;
it must not silently permit use-after-close. No new state is persisted for read catalog snapshots,
tokens, or leases. Restart invalidates them, as it does direct cursors today.

## Implementation sequence

1. **Introduce the read model and metadata providers.** Add descriptor projection, ownership and
   namespace fields, revision presence, capability adapters, and paginated discovery. Wire pure
   standalone and coexistence providers without altering write dependencies. Expose accepted
   remote metadata through a dedicated immutable accessor.
2. **Establish generation lifetime protection.** Add the direct read registry and resource adapters;
   integrate transition, retirement, and shutdown hooks with no-op defaults. Prove acquisition,
   invalidation, and close ordering before routing replicated reader operations to it.
3. **Move all inspection operations.** Refactor GetIndex, mappings, statistics, field pages, and
   status to the read catalog. Implement vector mapping responses, lossless native definition
   export, and native reader diagnostics adapters. Remove read-only dependence on API ingestion.
4. **Move all query operations.** Refactor Search, VectorSearch, SearchMeta, explain, and cursor
   continuation; implement the explicit SearchMeta target and join bindings. Add native vector
   preparation adapters and keep direct/native cursor origin isolation intact.
5. **Update clients and documentation.** Regenerate Java, Python, Node, and Rust bindings through
   the existing client generation/synchronization workflow. Add helpers and CLI options for owner
   filters, pagination, SearchMeta targets, and join bindings; display ownership, namespace, state,
   and queryability. Update the gRPC guide, coexistence guide, client examples, and release notes.
6. **Release after compatibility and race coverage.** Enable both owners in the existing direct
   API by default once the full acceptance suite passes. Intermediate commits can retain the
   standalone-only provider while hooks are incomplete; do not ship a union catalog ahead of safe
   lifetime handling. No public enablement flag or ownership migration is needed.

Implementation will touch the standalone runtime/server/catalog composition, coexistence
metadata adapter, shared lifecycle hooks, protobuf, client helpers, and tests. Prefer extraction
of native vector preparation over copying `VectorSearchCommand`. Keep native command envelopes,
namespace lookup restrictions, writer selection, catalog journals, and revision validation intact.

## Validation and completion criteria

The matrix below is the regression plan; it is broader than a claim that every timing
interleaving or deployment size has been exercised. The implementation record at the end lists
checks actually run.

Extend existing standalone RPC, statistics, join, ownership-lifecycle, native cursor, config-manager,
and end-to-end suites. Add focused read-catalog/lease tests. Use a live MongoDB coexistence fixture
for replication and compare direct reads with native reads over the same index and indexed data;
separately populated indexes do not establish replicated-read correctness.

| Area | Required cases |
| --- | --- |
| Discovery | Both owners, duplicate names in different collections, standalone/native name collision, staged-only metadata where present, pending/failed/quarantined entries, rename, deletion, ID collision rejection, owner filters, bounded pagination, denied-entry filtering |
| Inspection | Static and dynamic mappings, autocomplete, synonyms retained in exported definitions, vector fields, view identity, native zero/absent versions, active/desired mismatch, multipart statistics, partial measurements, field token invalidation |
| Query | Search hits, autocomplete, counts/facets/SearchMeta, explain, vectors, stored source, empty indexes, native synonyms/views, supported auto-embedding and nested variants; all under effective feature gates |
| Selection | Explicit IDs, mismatched BSON names and generation fences, legacy SearchMeta default/missing semantics, explicit SearchMeta replicated target, all four join ownership combinations, ambiguous names resolved through bindings |
| Availability | Initial sync, failed active generation, healthy active with failed/rebuilding staged generation, stale/recovering reads, disconnected source, resumed replication, process recovery |
| Cursors | First batch/GetMore/exhaustion/kill, idle expiry, wrong principal, revoked grants, native/direct ID isolation on the same replicated index, capacity restored after engine-side removal |
| Races | Drop/swap during resolution, snapshot acquisition, cursor registration, first batch, GetMore, mapping export, embedding call, join planning, and statistics capture/publication; shutdown with in-flight work |
| Mutations | Every direct mutation rejects replicated IDs, including DeleteByQuery dry run; no writes, commits, journal edits, or replication checkpoint changes result from read access |
| Compatibility | Existing standalone tests and old-client fixtures, native cursor phase-out and drop tests, native transport/health regression tests, all four clients and CLIs |

Use deterministic barriers for lifecycle races rather than timing sleeps. Assert resource closure,
zero leaked leases/cursor slots, no reader access after close, and no cross-generation batches.
Exercise deletion through native reconciliation as well as direct mutation. Test that an idle
direct cursor cannot pin a retired generation indefinitely and that invalidating it leaves a
native cursor on the same phasing-out generation usable under native rules.

Run the relevant Bazel unit/integration targets and client verification scripts when implementing.
Extend the existing [SearchMeta parity harness](../clients/tests/search_meta_parity.py) and
[client end-to-end harness](../clients/tests/e2e.py) to include direct reads of replicated IDs.
Measure discovery size, lease contention, and expensive field-statistics work under simultaneous
replication and direct reads; compare native throughput and latency with a same-build baseline.

The change is complete when a granted client can list a replicated index, inspect its current
definition/mappings/statistics, execute its supported queries and autocomplete, page or close its
direct cursors, and observe coherent behavior through rebuilds and source outages—while every
write and native compatibility boundary above continues to hold.

## Alternatives considered

Passing the unfiltered engine catalog directly to `StandaloneRuntime` is insufficient: desired
metadata, readiness, leases, names, exports, and cursor continuation remain local-only. Reusing
`LocalIndexCatalog` for replicated metadata would also mix ownership and revision rules.

Routing reads through MongoDB commands would introduce an external dependency for operations the
local engine can already execute and would not supply the direct inspection or cursor contract.
A new v2 service would avoid some additive selector fields but duplicate the current read surface
and require a broader client migration. The additive v1 design preserves existing standalone
calls and makes replicated selection explicit where the old name semantics cannot identify it.

Letting all direct cursors survive generation swaps would reduce client retries but require
longer retention and more complex joins and token rules. This design chooses bounded direct
invalidation while preserving native cursor behavior separately.

## Implementation record

- Shared discovery and all direct read RPCs resolve both owners. Native mutations and direct
  mutations retain their source-specific catalog. SearchMeta adds an explicit selector while
  preserving legacy local-name lookup; joins add exact-ID alias bindings.
- Query preparation is shared with native search/vector commands, including auto-embedding field
  remapping and vector encoding. Public mapping export uses the logical generation definition,
  preserving native synonyms and auto-embedding fields.
- Direct leases invalidate on native swap/drop and drain before physical close. Cursor origin is
  independent of ownership. A deterministic test verifies native continuation remains usable
  after direct invalidation on the same generation; retirement and shutdown tests verify readers
  drain before producer/physical closure.
- Java, Node, Python, and Rust clients require the contract acknowledgment when using new
  selectors, bindings, ownership filters, or pagination. The JSON CLIs expose these protobuf
  fields without separate flags.
- Initial implementation validation passed on 2026-11-16: 80 Bazel test targets spanning standalone, config-manager,
  cursor, and native search commands; all four client builds; 11 Python tests; all four
  client/CLI end-to-end scenarios over TLS, including authentication, pagination, explicit
  SearchMeta selection, and crash restart; and 15 live coexistence checks against MongoDB 8.3.17
  in `clients/tests/all_index_reads.py`. The live checks include replicated views/synonyms,
  cross-owner joins, source outage, offline mongot restart, resumed replication, rebuild/drop
  invalidation, and rejection of every direct mutation against replicated IDs.
- Built the deployable archive with `bazel build //deploy:mongot-standalone`.
  The client suite ran with `clients/python/.venv/bin/python clients/tests/e2e.py --launcher
  <extracted-package>/mongot-standalone`; the coexistence invocation is documented in
  [clients/README.md](../clients/README.md#verify-reads-across-ownership-types).
- Post-review fixes replace catalog-wide single-ID resolution with keyed metadata lookups and
  metadata-free active reader resolution, and preserve quarantined replacements in discovery.
  All 40 affected standalone/config-manager Bazel test targets passed, including regressions for
  lookup isolation, generation arrival during metadata lookup, active/quarantined generation
  reporting, defensive metadata copies, and accepted catalog deletion. The client/live runs and
  packaged archive above predate these fixes.
- Second-review fixes serialize cursor callback registration with cleanup and transfer resource
  ownership before registration, preventing capacity over-release when retirement races a failure.
  SearchMeta resolves readiness before checking the active definition's type, preserving the
  incompatibility error for quarantined targets. Five added regressions cover registration races
  and failures, quarantined targets, healthy active generations with quarantined replacements,
  and vector-type rejection without leaking leases. All 40 affected standalone/config-manager
  Bazel targets passed on 2026-11-17, including style checks. The client/live runs and packaged
  archive above have not been repeated for these fixes.
- Third-review fixes keep retained live/staged replicated generations discoverable when desired
  metadata becomes unreadable, without presenting an old definition as the desired one or exposing
  phasing-out generations. Five added catalog regressions cover unreadable metadata, recovery,
  deletion, and retained replacements. Node normalizes the string default ownership enum before
  deciding whether to require a contract acknowledgement. All 40 affected standalone/config-manager
  Bazel targets and eight Node gRPC compatibility tests passed on 2026-11-17. The packaged archive
  and live coexistence suite have not been rebuilt or rerun for these fixes.
- Fourth-review fixes share the automatic-candidate feature check across native VectorSearch,
  search operators, and the direct VectorSearch API. Disabled automatic selection rejects omitted
  `numCandidates`; explicit candidates and enabled automatic selection remain supported. All four
  client wrappers attempt bounded cursor cleanup before rejecting an unacknowledged reply,
  preserving the contract error when cleanup fails. All 47 affected standalone/config-manager,
  feature-policy, and native search Bazel targets passed, as did all client builds, eight Node
  tests, 11 Python tests, and 40 gRPC compatibility scenarios across the four clients. The archive
  and live coexistence suite have not been rebuilt or rerun for these fixes.
- Fifth-review fixes dispatch deferred producer teardown on the retirement executor after direct
  leases drain. The last reader and shared notification thread no longer run blocking producer
  teardown. Three added regressions cover stalled teardown during reader close, notification
  progress, and retention of resources/recovery state when the executor rejects work. All 44
  affected standalone, config-manager, and lifecycle Bazel targets passed, including style checks.
  The packaged archive and live coexistence suite have not been rebuilt or rerun for this fix.
- Sixth-review fixes attribute auto-embedding field statistics to public paths using the mapping
  captured from the pinned generation. Field selection, disk and memory ownership use logical
  names; representation diagnostics retain physical names. Regressions use real derived Lucene
  indexes for resolved and offline replicated generations, including partitioned readers, and
  verify merged lexical/vector costs across both supported codec versions and compound-file
  modes. Storage totals still reconcile without double counting. All 46 affected standalone,
  config-manager, statistics, and native search Bazel targets passed on 2026-11-17, including style
  checks. These tests make no embedding-provider calls; the packaged archive and live coexistence
  suite have not been rebuilt or rerun for this fix.
- Integrated with the coupled-join implementation on current `main`: direct alias resolution and
  generation leases adapt the shared join planner, while native adapters keep their lifecycle.
  Added regressions for conflicting alias fences on one dependency and a fenced alias selecting
  the query's root index, and retained callback-registration race coverage. All 67 affected
  standalone, config-manager, statistics, native query, cursor, executor, and join-operator Bazel
  targets passed on 2026-11-17. The final adversarial review also passed all 40 cross-language gRPC
  contract scenarios before this integration. Live coexistence, provider-backed embedding, and
  the packaged archive have not been rerun or rebuilt after the latest changes.
- Live embedding-provider credentials and production-scale latency/load measurements are outside
  this validation run. Native auto-embedding and offline-serving command regressions exercise the
  shared preparation logic; the live coexistence suite uses real replicated search/vector indexes.
