# In-place standalone index schema updates

Compatible mapping updates change ingestion and query configuration while retaining the same
physical Lucene index. They do not allocate a generation, copy a snapshot, reopen the writer,
backfill documents, or force a document commit. This applies to gRPC-owned indexes in standalone
and MongoDB coexistence modes. MongoDB-owned indexes retain their existing lifecycle.

## Contract and compatibility

`UpdateIndex` checks `expected_revision` and classifies the complete parsed public definition.
Unknown changes require rebuilding; compatibility is never inferred from the engine's broad
`isCompatibleWith` check. Semantic normalization preserves ordered index-sort specifications.

| Change | Result |
| --- | --- |
| Equivalent definition, including explicit defaults | No-op; same schema revision, physical generation, and cursors |
| New static string, token, boolean, date, number, objectId, or uuid field | In-place |
| New static document subtree containing only supported additions | In-place |
| New children under an existing static document | In-place |
| New `string.multi` representation | In-place |
| Root, string, or existing multi-field `searchAnalyzer` change | In-place |
| Append custom analyzer definitions without modifying existing definitions | In-place |
| Remove/change an existing indexing representation, analyzer, normalizer, norms, or similarity | Rebuild required |
| Add an explicit mapping where it overrides implicit dynamic indexing | Rebuild required |
| Change dynamic rules, stored source, sort order, partitions, or other global indexing settings | Rebuild required |
| Add/change embedded documents, autocomplete, geo, facet, or vector representations | Rebuild required in this release |
| Changed mapping involving a dotted-name alias | Rebuild required conservatively |

Unchanged embedded, vector, sorted, and dynamic portions may coexist with a supported addition.
A static subtree underneath a dynamic root may receive additions if that subtree's indexing rules
remain static. A newly introduced field can select its own supported analyzer and similarity;
existing fields must retain their physical encodings and indexing semantics.

`AUTO` chooses in-place publication for supported changes and retains the existing empty-rebuild
behavior for incompatible changes. `REQUIRE_PRESERVE` rejects an incompatible change with
`FAILED_PRECONDITION` without changing the catalog. `REBUILD_EMPTY` explicitly creates new empty
storage, including when the supplied definition is equivalent. An in-place publication failure
never falls back to an empty rebuild.

New fields are not backfilled, even when old stored source contains their values. Complete-document
upserts populate the new representations. Query-only analyzer changes immediately affect new
queries against existing indexed terms. Breaking reindex from stored source into a new physical
generation is a separate future feature, not part of this protocol.

## Physical identity and schema identity

The physical `GenerationId` continues to identify directories, initialized resources, write fences,
and cursor dependencies. The definition revision identifies the ingestion/query schema. An in-place
update advances only the latter. New requests carrying the old physical generation fence continue
to work after publication. A physical rebuild/drop still retires those fences and cursors.

The shared configuration planner ordinarily treats definition changes as a reason to allocate a
new generation. The local owner now supplies the active definition while an accepted schema change
is being applied, and explicitly journals the new definition on the same live generation when it
publishes. It does not stage or swap a replacement. Lifecycle resource ownership remains attached
to the existing initialized index.

## Runtime schema publication

`LiveSearchSchema` exists only for locally owned search indexes. It contains an immutable view of
the definition, analyzer registry, indexing analyzer, field resolver, and similarity. The factory
opts in using local ownership; native replicated indexes retain fixed schemas.

Preparation builds the following without holding the configuration-manager or ingestion monitors:

- A validated analyzer registry and immutable schema view.
- A replacement indexing policy for every physical writer partition.
- New logical query readers using the existing searcher managers and the new immutable query
  configuration. This also updates query factories, highlighting, and per-field query analysis.

The existing Lucene writer receives stable delegating analyzer and similarity objects when it is
first created. Their delegates follow the published schema. Analyzer reuse delegates to the active
analyzer, so components cached by a prior schema cannot silently tokenize new fields incorrectly.
Existing field similarities cannot change under the compatibility rules. Searchers likewise use a
stable similarity delegate so a newly added field has the correct scoring model before the next
physical reader refresh.

Publication installs the prepared partition policies, logical readers, and immutable schema view.
It creates no physical reader manager or directory and performs no document flush, commit, or copy.
Preparation failures leave desired metadata and the active schema unchanged.

## Writes and lock order

Each ingestion state has a fair read/write schema gate. An executing mutation batch holds the
shared side through indexing and its requested refresh/commit barrier. Explicit write completion
and background commits use the same side. Preparation does not block these operations.

Publication takes the exclusive side before acquiring the configuration-manager and ingestion
monitors. It therefore waits for executing batches, while later queued batches wait behind the
publication. A batch uses one coherent ingestion schema. Queued writes may use the newly published
schema even if they were admitted earlier; their supported preexisting mappings remain compatible.

Under the brief publication gate, the coordinator rechecks the expected revision, exact active
schema, physical identity, readiness, and absence of a staged transition. It then persists metadata
and installs prepared runtime state. Two updates prepared against the same revision cannot both
publish: the loser receives `ABORTED`. Drop or rebuild during preparation also prevents publication.
A read lease pins the initialized index throughout preparation and publication.

Normal successful in-place updates do not close write admission or return a transition-related
`UNAVAILABLE` to writes. The metadata writes and executing-batch drain can add latency; this is not
a zero-latency guarantee. No extra index capacity or long copy pause is needed.

`INDEXED`, `VISIBLE`, and `COMMITTED` retain their existing meanings. Schema publication is not a
refresh/commit barrier. Uncommitted documents remain subject to normal crash loss, and a query
immediately after an `INDEXED` write may still see the prior searchable state.

## Queries, cursors, joins, and diagnostics

New queries obtain the current logical reader. Existing queries/cursors retain their immutable
query view and ordinary pinned Lucene readers. They are not invalidated by a schema-only update.
Old logical readers own no searcher managers or directories and need no physical retirement queue.
Foreign-join dependencies retain the same physical identity and their captured readers.

Statistics jobs/cache slots include both physical generation and schema revision. A stale job
cannot publish over a newer revision's cache. Completed field-statistics page tokens remain usable
as immutable snapshots of their reported revision while the physical index remains live.
Field-type telemetry publishes its resolver, analyzer classification, cache, and refresh interval
with the schema. Each schema owns its cached counts, so an asynchronous refresh from a prior
schema cannot overwrite the new result. The feature flag is checked when metrics are requested.

Delete-by-query captures one logical scan reader and revision. Subsequent delete batches use the
current desired revision while retaining the scan's physical generation fence. A compatible update
therefore does not interrupt the operation or alter its selection; rebuild/drop still fences it.
The response reports the captured scan revision.

Generation-retirement notifications invalidate only resources tied to the retired physical
identity, including delayed notifications after a replacement has become ready. In-place
publication does not emit a retirement notification.

## Durable metadata and crash recovery

Local catalog version 4 adds an `IndexSchemaUpdate` binding containing complete source and target
bound definitions. Both endpoints must share the same physical generation, index identity,
namespace, collection UUID, view, and overridden analyzer bindings. The target revision must be
exactly source plus one, and classification must be `PRESERVE_CONTENT`. It never authorizes empty
initialization. Only the latest binding for each index is retained.

Publication order:

1. Prepare runtime objects and obtain the exclusive schema gate.
2. Revalidate source/revision and atomically save the desired target plus source/target binding in
   `indexes.json`.
3. Atomically replace the same live physical generation's definition in the shared journal.
4. Install the prepared runtime schema and replace the live catalog entry on that same index.
5. Return `READY` with the new active/desired revision and unchanged physical generation ID.

The binding makes the local catalog and shared journal recoverable without a cross-file
transaction. Before generic lifecycle startup, recovery validates all local ownership metadata and
replays the accepted target onto the same journal entry. That entry must exactly match either
endpoint; an unrelated definition, staged conflict, changed physical identity, or missing journal
fails closed. Replay does not create data or invent a generation.

| Interruption | Recovery |
| --- | --- |
| During preparation, before durable acceptance | Old schema remains; caller may submit a new update |
| After local acceptance, before shared journal publication | Replay target on the same physical index |
| After shared journal publication, before runtime publication | Open the existing physical index using the target schema |
| After runtime publication, before the response | Target is accepted; inspect current revision before retrying |
| Missing/corrupt Lucene commits after prior readiness | Fail the affected index; never recreate empty content |

During a running-process metadata failure, the accepted binding remains pending. Reconciliation
resumes it; an identical request with the current desired revision can also resume without another
revision increment. Until completion the shared planner follows the source schema. A newer,
different update cannot bypass the pending publication. Explicit rebuild/drop remain separate
operations and remove the in-place binding when they supersede its desired definition.

Versions 1–3 remain readable. Version 4 prevents older binaries, which do not understand schema
bindings, from interpreting a revision-only change as authorization to replace storage. Older
binaries must reject the catalog before opening index files; downgrade requires a matching stopped
backup, not manually removing the version field.

## Empty initialization and existing-data protection

Creation and explicit empty rebuild use a separate durable authorization bound to their exact
physical generation. Every partition receives a valid initial commit before that authorization is
recorded `READY` and API writes are admitted. Interrupted `INITIALIZING` work can resume only its
bound generation. Previously ready data and legacy data without authorization reopen strictly.
Missing commits cannot regain empty-creation permission.

Generic recovery and swapping are owner-aware: a local initialization failure cannot cause the
native recovery planner to allocate an empty retry generation. Failed staged initialization is
reported as `FAILED` with its cause. An identical no-op cannot reauthorize it; repairing the cause
and restarting resumes its reservation, while explicit rebuild starts a new authorized generation.

Unreadable local live/staged journal entries stop startup before native quarantine cleanup can
delete files. This check includes quarantined entries, not only successfully parsed definitions.
Native-owned quarantine and recovery policy remains unchanged.

## Legacy copy migrations

Recovery code for previously accepted catalog-v2/v3 copy migrations is retained. It uses durable
source/target generation reservations, mutation quiescence, committed-file snapshots, manifests,
strict target opening, and generation-specific retirement. New public compatible updates do not
create those records or invoke that protocol. Copy-specific unit fixtures use an explicit test
entry point to exercise upgrades and interrupted legacy operations.

An already accepted legacy migration may still require temporary disk space and write quiescence.
Its failure is surfaced without clearing active content; retry or explicit rebuild/drop remains
available. A completed legacy migration can subsequently receive in-place schema changes.
Snapshot support is scoped to locally owned search writers for this compatibility path; it is not
required for new schema publication or imposed on native/vector writers.

## Validation

The regression coverage includes real multipart Lucene writers, analyzer/multi-field additions,
custom similarities, unchanged sorted/embedded content, query-only changes, no backfill, existing
cursors, statistics revisions, same-generation fences, and restart persistence. Concurrency tests
cover batches queued during publication, executing commit barriers, competing updates, and drop
during preparation. Process-crash tests halt at preparation, durable acceptance, shared journal,
and publication boundaries and verify stable physical identity and committed data after restart.

Legacy migration, initialization authorization, quarantine, failure isolation, native configuration
manager, Lucene writer/factory/searcher, standalone server, and join suites remain regression gates.
Packaged validation exercises all 17 RPCs through Python, Java, TypeScript, and Rust and verifies
MongoDB coexistence, local changes during source outage, restart, and resumed native replication.

Validation completed on 2026-11-17: 457 Java cases across 25 suites and six Python unit tests pass.
The built package passes all 17 RPCs and crash restart through all four clients over TLS, plus the
live MongoDB coexistence suite. A separate packaged stress run completes 400 concurrent committed
writes during ten schema updates without changing physical identity or invalidating an existing
cursor. After SIGKILL, all 403 documents and the new field remain queryable. The prior catalog-v3
binary refuses the resulting v4 catalog without modifying any of its 87 files. All temporary test
databases and indexes were removed after their processes stopped.
