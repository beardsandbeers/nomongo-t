# Join queries in standalone and coupled architectures

Status: Initial same-database, non-sharded implementation added. See [coupled join usage](coupled-joins.md)
for the implemented contract and validation. The proposal below preserves the design rationale and
broader adversarial acceptance targets.

The [adversarial review](join-architectures-adversarial-review.md) records source-backed gaps
found in the initial draft. Focused unit and real-MongoDB validation now exercise the implementation;
comprehensive fault-injection and saturation validation remains additional work. Foreign-collection
authorization for coupled joins is an accepted known caveat, tracked in [issue #3](https://github.com/beardsandbeers/nomongo-t/issues/3) as a deferred
follow-up, not a blocker for this implementation.

Use one shared join planner with architecture-specific index resolution and request adapters.
Integrate coupled joins through the existing native cursor wrapper and pinned-reader path.
Keep join execution in the existing Lucene implementation, and confine additional changes to
upstream-owned code to small, additive cursor and bootstrap seams.

This proposal extends the existing [standalone join design](join-design-guide.md). Coupled mode
means MongoDB `$search` and `$searchMeta` over replicated indexes. A process can host both
architectures under the existing [coexistence model](coexistence-mode.md).

## Implementation decisions

`JoinSource`, `extensions.join.JoinPlanner`, and `CoupledJoinService` implement the source contract
and shared algorithm. `NativeCursorManager` prepares coupled queries even when standalone ingestion
is not enabled. `CursorQuery.Search` carries the expected generation; `CursorFactory` checks it.
`BulkheadCommandExecutor` carries a transport-neutral cancellation/deadline scope across both pools.
No new command BSON, Lucene algorithm, replication format, or mongod change is needed.

Metadata uses a separate four-connection pool with targeted lookups, a two-second operation bound,
a 30-second preparation cap, and four fail-fast admission permits. Lookup timeouts also respect the
remaining RPC deadline. Resource ownership transfers through a managed snapshot/producer, with
idempotent cleanup and native join cursor markers removed on close.

The first implementation uses the configured community source connection. A volatile source
snapshot and monotonic epoch in `ConfigState` invalidate plans on logical source updates without
taking configuration locks from cursor execution. The epoch tracks the cluster metadata connection
(including credentials/TLS) and sharded topology configuration. Replication host reselection,
temporary loss of a replication host, and replication restarts preserve it; the published snapshot
still carries the current replication configuration. It conservatively requires a restart when the
metadata connection configuration changes; it does not implement the proposed general reconfigurable metadata
provider. Native preparation failures currently use native query errors with descriptive messages;
a richer native resource-exhaustion taxonomy remains follow-up work.

## Requirements and proposed scope

Required behavior:

- Support joins through both the standalone API and the coupled MongoDB query path.
- Accept `from: { collection: "some_coll", index: "default" }` for coupled sources. An omitted
  `index` in this object means `"default"`.
- Preserve legacy `from: "index_name"` for standalone indexes.
- Preserve omitted `from` as a self-join against the exact current index.
- Preserve mongod's existing outer-collection authorization. Defer enforcement of the original
  caller's foreign-collection permissions for coupled inline joins and document that gap explicitly.
  Do not add an authorization-based rejection gate or require a shared-access opt-in for this work.
- Keep the standalone API directly queryable with its existing controls. Additional granular
  standalone authorization is deferred and is not a prerequisite for this feature.
- Minimize the integration surface in upstream mongot code and preserve ordinary query behavior.

The proposed first release supports same-database coupled joins where complete local and foreign
indexes reside in one mongot process in a non-sharded deployment. Different Lucene partition
counts are supported. Distributed MongoDB shard joins require a separate execution design.

Existing equality-semijoin semantics remain: return each matching local document once, without
foreign-document projection or score propagation. Preserve supported string token, ObjectId,
integer, mixed-type, array, exclusion, count, and facet behavior. `_id` requires an appropriate
explicit mapping. Existing feature flags continue to govern aggregation features.

Retain existing restrictions on nested joins, vector joins, embedded-document combinations, and
`returnScope`. Initially exclude views, cross-database sources, and joins between standalone-owned
and replicated indexes. These are proposed scope boundaries, not limitations of the new parser.

## Baseline implementation and available seams

The following observations describe the inspected repository implementation:

- [JoinOperator](../src/main/java/com/xgen/mongot/index/query/operators/JoinOperator.java) stores
  `from` as `Optional<String>`. It parses the foreign predicate as an operator, not a full search
  request.
- [StandaloneJoinPlanner](../src/main/java/com/xgen/mongot/standalone/server/StandaloneJoinPlanner.java)
  validates placement, resolves standalone names, authorizes dependencies, pins generations and
  readers, collects typed keys, and rewrites the query into materialized membership operators.
  It also owns memory admission and dependency checks, with direct dependencies on standalone
  runtime services and gRPC status/cancellation.
- [SearchIndexReader.QuerySnapshot](../src/main/java/com/xgen/mongot/index/SearchIndexReader.java)
  already exposes join-field inspection, key collection, query execution, and cancellation.
  Single-reader and multi-reader implementations support pinned planning. Successful execution
  transfers reader ownership to batch producers.
- [CursorQuery.Search](../src/main/java/com/xgen/mongot/cursor/CursorQuery.java) already carries an
  optional snapshot, which [CursorFactory](../src/main/java/com/xgen/mongot/cursor/CursorFactory.java)
  uses instead of acquiring fresh readers on its ordinary search path.
- [NativeCursorManager](../src/main/java/com/xgen/mongot/standalone/coexistence/NativeCursorManager.java)
  is a fork-owned decorator already installed by
  [CoexistenceRuntime](../src/main/java/com/xgen/mongot/standalone/coexistence/CoexistenceRuntime.java).
  It delegates native cursor operations and prevents access to standalone direct-API cursors.
- [IndexCatalog](../src/main/java/com/xgen/mongot/catalog/IndexCatalog.java) resolves indexes by
  database, collection UUID, optional view name, and index name. It does not resolve a collection
  name to a UUID.
- [LuceneSearchQueryFactoryDistributor](../src/main/java/com/xgen/mongot/index/lucene/query/LuceneSearchQueryFactoryDistributor.java)
  already executes materialized joins, but rejects unresolved joins with
  `join requires standalone query planning`.

The missing coupled functionality is primarily orchestration and source resolution. It does not
require a second Lucene join algorithm or an index-format migration.

## Public query contract

### Coupled source

```javascript
db.contacts.aggregate([
  {
    $search: {
      index: "contacts_search",
      compound: {
        filter: [{
          join: {
            from: {
              collection: "accounts",
              index: "default" // Optional; defaults to "default".
            },
            localField: "accountId",
            foreignField: "_id",
            operator: {
              equals: { path: "active", value: true }
            }
          }
        }]
      }
    }
  }
]);
```

The foreign database is the outer request's database. `collection` is a literal collection name;
it is not parsed as a database-qualified namespace. The outer query's index and the foreign
object's index are independent.

### Standalone source and self-joins

Existing standalone expressions remain valid:

```javascript
{
  compound: {
    filter: [{
      join: {
        from: "accounts_search",
        localField: "accountId",
        foreignField: "_id",
        operator: { equals: { path: "active", value: true } }
      }
    }]
  }
}
```

Omitting `from` in either architecture selects the exact outer index and its pinned readers.
It does not select the current collection's `default` index. Explicitly naming another index
on the same collection remains a cross-index join; explicitly resolving to the same index and
generation reuses its snapshot.

The initial adapters reject a collection object on the standalone API and a standalone-name
string on the coupled API. A string never silently becomes a replicated index lookup, and an
object never falls back to a standalone index with a coincidentally matching name. Supporting
cross-ownership joins later requires an explicit access policy.

### Parsing and serialization

Introduce a small `JoinSource` type beside `JoinOperator`:

```java
// Illustrative types; final parser integration follows the repository's BSON conventions.
sealed interface JoinSource {
  record StandaloneIndex(String name) implements JoinSource {}
  record CollectionIndex(String collection, String index) implements JoinSource {}
}
// JoinOperator changes from Optional<String> to Optional<JoinSource>.
```

Keep the current standalone string validation. For objects, require a nonblank collection name;
validate an explicit index name using the native index-name constraints. Reject nulls, other BSON
types, and unknown object fields, including unsupported `db` or `indexId` selectors. Default the
index only when absent, not when blank or null. Validate collection names using existing MongoDB
namespace conventions rather than copying the standalone index-name length limit.

Serialization preserves string versus object sources and omitted `from`. An object with omitted
`index` may serialize canonically with `index: "default"`. Parsed round trips must be semantically
equal. Materialized joins continue to serialize the original logical predicate without internal
keys, resolved IDs, or authorization data. The existing raw BSON request envelopes need no new
protobuf fields or SDK transport methods.

## Shared planner and adapters

Create a fork-owned package such as `com.xgen.mongot.extensions.join`. Extract the algorithm from
`StandaloneJoinPlanner`, keeping the existing class as a thin standalone adapter while callers
and tests migrate.

The shared planner owns query traversal, placement validation, mapping compatibility, key
materialization, typed-domain composition, dependency deduplication, and resource accounting.
Its dependencies are explicit and small:

- A resolver/environment that resolves targets, applies the architecture's existing access checks,
  acquires snapshots for exact generations, and verifies dependency validity. Coupled foreign-user
  authorization is deferred; standalone dependency grants remain enforced.
- A request execution context that supplies cancellation/deadline checks.
- A process-owned memory budget and a metrics interface.

Return a `PreparedJoin` containing the rewritten `SearchQuery`, local snapshot, immutable
dependency identities, and closeable resource state. Use typed planner failures that adapters
translate to existing gRPC or native query errors. The shared package must not depend on
`StandaloneRuntime`, standalone authentication, gRPC statuses, or native command implementations.

The standalone adapter retains `LocalIndexCatalog` name resolution, index-ID grants, ingestion
readiness checks, and existing continuation authorization. The coupled adapter uses replicated-only
catalogs, native readiness rules, namespace resolution, and existing transport/ownership boundaries.
It does not claim to check the original MongoDB user's foreign-collection permissions.

The preparation sequence is:

1. Validate placement, supported query forms, and topology before collecting any keys.
2. Resolve dependency identities and apply existing access checks. Standalone checks every index
   grant; coupled execution relies on mongod's outer check and retains the documented foreign gap.
3. Pin the selected local and foreign generations and acquire their snapshots. Deduplicate by
   stable identity, not display name; explicitly resolved self-joins reuse the local snapshot.
4. Collect all qualifying keys from all foreign Lucene partitions, under the existing bounds.
5. Rewrite the local query with the existing `MaterializedJoinOperator` implementation.
6. Release foreign snapshots once immutable keys own their bytes. Retain the local snapshot and
   memory reservation for execution, and verify dependencies before publishing results.

Preserve the existing suppression of foreign explain profiling and the local count/facet execution
hints. Do not expand keys into public BSON `in` queries or filter already-produced result batches.

## Coupled collection and index resolution

Resolve `(request database, source collection name)` to the current collection UUID, then call:

```java
replicatedCatalog.getIndex(database, collectionUuid, Optional.empty(), source.index());
```

Use the ownership-filtered catalog already supplied by coexistence composition. Require a search
index, native query availability, and an initialized generation. Preserve quarantine reporting
where the resolved UUID/index name identifies a quarantined index. Never select an arbitrary
candidate if identity is ambiguous.

Do not equate successful `throwIfUnavailableForQuerying()` with an existing foreign index:
`LuceneSearchIndex` deliberately allows `DOES_NOT_EXIST`, and ordinary `CursorFactory` handles
that state separately by returning an empty cursor. The foreign snapshot path bypasses this
factory. Explicitly reject `DOES_NOT_EXIST` before acquiring a foreign snapshot and in dependency
checks, even when index ID and generation have not changed. Continue to allow native serviceable
`STALE` and recovering states under the documented indexed-content consistency model.

An empty view selector is not a no-view constraint: `DefaultIndexCatalog.getIndex` only checks
the view when the request supplies a view name. Reject `getDefinition().getView().isPresent()`
on both resolved sides, as well as an explicit outer view request. Reject a namespace lookup
that identifies a view. These checks are required even for omitted-source self-joins and legacy
requests against a view's source collection.

### Collection identity and metadata access

The recommended resolver obtains the UUID through a bounded metadata lookup against the configured
MongoDB source. Deduplicate lookups within a request; initially avoid a persistent name cache and
its invalidation machinery. Use an explicit source-client/provider dependency supplied by
composition. Queries borrow it; they do not close the replication client's resources. If sharing
the existing client is unsuitable, an adapter-owned client must follow source reconfiguration and
shutdown, without changing replication behavior.

Do not put database-wide metadata scans or unbounded connection-pool waits on this query path.
The existing `MongoDbMetadataClient.resolveCollectionInfos` enumerates collections by database,
and its non-replication client defaults are intended for background metadata work. Use a targeted
name-filtered lookup with a bounded result, and an explicitly sized, isolated query-metadata pool
unless resource isolation can be demonstrated for a shared client. Bound pool acquisition,
server selection, socket I/O, and the complete operation by the remaining preparation deadline;
checking cancellation only before and after a blocking driver call is insufficient.

Acquire a process-owned preparation admission permit before metadata I/O or reader acquisition.
Use a bounded wait or rejection policy so unavailable MongoDB metadata cannot fill native command
workers indefinitely. The retained-key memory limit alone does not bound these pending requests.
Choose the permit count and timeout defaults in the integration phase and test ordinary search
and replication availability under saturated join preparation.

`lastObservedCollectionName` is useful diagnostic information, but not authoritative identity.
Replication updates it asynchronously after renames. Scanning this field alone can target an old
collection after a rename and recreation of the original name. The existing
[DefaultNamespaceResolver](../src/main/java/com/xgen/mongot/replication/mongodb/common/DefaultNamespaceResolver.java)
illustrates UUID/name validation, but exposes the reverse lookup and mutates the observed name;
it is not directly the new name-to-UUID API.

Bind each prepared query to the resolved UUID, index ID, generation, and source identity. Check
that this resolution is still valid before the initial response; reject a detected rename,
replacement, source switch, or generation race. These checks do not create a transactional lock
against concurrent MongoDB DDL. Existing cursors remain bound to their original identities and
must never retarget by name. Foreign source outages do not by themselves change a pinned identity.

Represent source identity with a locally tracked configuration epoch paired with the provider's
client/topology snapshot. Capture it before resolution and verify it after preparation and during
continuation. A logical source configuration change invalidates old prepared plans even if a catalog
generation has not changed yet. Compare epochs, not snapshot object identity: host-only updates
publish new configuration snapshots with the same epoch. Advance the epoch and publish an
unavailable source before applying a logical change, so failed changes also invalidate old plans. Do not substitute a cached replica-set name for this epoch, and
do not add network metadata lookups to every `getMore`.

A new named coupled join fails with an availability error if required metadata cannot be resolved.
This is an explicit availability tradeoff: ordinary queries and already-prepared cursors retain
their existing behavior, while a new named join does not guess from stale names. Omitted-source
self-joins can use the already-resolved outer UUID without an extra name lookup.

## Native cursor integration

Extend `NativeCursorManager` or compose a join-aware decorator beside it. The wrapper receives the
database, collection UUID, view, query, cursor options, optimization flags, and routing metadata
without changes to `SearchCommand`.

For ordinary queries, delegate unchanged. For supported join queries, resolve the outer index,
prepare the shared plan, and invoke the existing namespace-based native `newCursor` with the
rewritten query and a managed snapshot. Perform potentially expensive metadata and key collection
outside cursor-manager locks. Preserve native routing decisions, including requests routed away
from this shard, before opening foreign readers.

For explained queries, compare the already-active `ExplainQueryState.getNumPartitions()` with
the chosen local initialized index before acquiring its snapshot. `SearchCommand` sets up explain
from an earlier catalog lookup; a generation swap can change partition count before the decorator
runs. Reject that mismatch as a preparation race. The later expected-generation check cannot
protect snapshot acquisition from an explain context built for a different partition layout.

Do not call the generation-based standalone `newCursor` overload. It records direct-API cursor
IDs, which native `getMore` and kill paths intentionally cannot access.

### Bind the prepared snapshot to the selected generation

Add an optional expected `GenerationId` to `CursorQuery.Search`, retaining convenience constructors
with empty defaults. In `CursorFactory`, verify it against the selected initialized index before
consuming the supplied snapshot. This is an additive safeguard for prepared searches; ordinary
searches keep their existing path.

The check prevents a cursor planned against generation A from being registered under generation B
after a concurrent swap. Revalidate dependencies after registration and kill the newly created
cursor if that validation fails.

Missing outer indexes and routed-away requests should retain native empty-result behavior when
identified before preparation. A disappearance after preparing a real join is a dependency race.
The delegate may return an empty cursor without consuming the snapshot through its null-index
path; the wrapper must detect that case, close the unused plan, kill the new cursor, and report
the race. Do not assume that a returned cursor implies snapshot ownership was transferred.

### Native cancellation propagation

The native executor currently checks stream cancellation when regular work leaves the queue.
It does not attach the originating gRPC context around `command.run()`, and guaranteed-pool
commands such as `getMore` take a separate dispatch branch. The cursor wrapper cannot obtain
reliable request cancellation by simply calling `io.grpc.Context.current()` on those workers.

Add a small generic execution-context seam at native dispatch: capture the originating RPC
deadline/cancellation and stream-cancel supplier before dispatch, install a scoped context around
command execution on both pools, and restore prior thread state in `finally`. Provide an empty
default for callers without transport context. The planner receives a transport-neutral callback
captured from that scope. Each `getMore` captures its current invocation; Lucene worker callbacks
must reference this captured state, not consult their own thread-local transport context.

Use existing RPC deadlines when present and define a finite server preparation timeout when absent.
The search command definition does not itself carry MongoDB `maxTimeMS`; do not promise independent
enforcement of a client timeout that mongod has not propagated or signaled by cancellation. Do not
close readers asynchronously from the transport listener while query workers may still use them.

Activate cancellation on the local snapshot before calling its `query(...)`, then check dependencies
both before and after that call. Initial local search and count/facet calculation execute inside
`query(...)` before it returns a producer. Installing checks only in `BatchProducer.execute` leaves
this phase unprotected, particularly for `$searchMeta` and total-count queries.

### Cursor lifetime and resource ownership

Wrap the local `QuerySnapshot` in a fork-owned decorator. Its query method wraps the returned
`BatchProducer` so the existing cursor close path releases the join's membership and budget.

Before transfer, preparation owns all acquired resources. After successful producer creation,
the managed producer owns the local reader and retained membership. If registration or subsequent
validation fails, close the producer or kill its registered cursor. Cleanup is idempotent, covers
partial acquisition, and releases reservations exactly once. Keep any pending producer handle
reachable until registration succeeds so a failure after reader transfer cannot leak it.

On each batch, the managed producer installs the current invocation's cancellation context and
checks dependencies before work and before returning buffered results. It closes retained
resources on terminal dependency/cancellation failure; the wrapper also removes the registered
cursor on such failures. Native execution must not reuse an expired initial request context for
later `getMore` calls.

Also cover failures outside the producer, such as explain serialization in `MongotCursor`.
The native join wrapper must kill the registered join cursor when a batch attempt fails, including
I/O and explain-size failures, while leaving ordinary cursor behavior unchanged. If identifying
join cursor IDs requires a small registration map, let producer close remove its marker so idle
expiry cannot leak entries. Coordinate marker installation with producer close so a close racing
registration cannot be followed by insertion of an orphan marker. Resource ownership remains
with the producer, not the map.

Exhaustion, explicit kill, idle expiry, local-index retirement, and shutdown already close native
batch producers. Foreign retirement is detected when a batch is attempted; an idle cursor releases
its reservation through ordinary idle expiry. This avoids requiring new foreign-dependency hooks
throughout upstream lifecycle management. If immediate idle invalidation becomes necessary, add
an adapter-owned dependency registry with explicit cleanup rather than another cursor manager.

Keep standalone principal/lease validation intact during extraction. Its resource cleanup and the
managed producer must share idempotent prepared-plan ownership; two independent memory counters
must not decrement the same reservation.

## Consistency, limits, and errors

Materialize foreign membership once per cursor. `getMore` does not execute the foreign predicate
again. Ordinary refreshes and subsequent document changes affect new queries; existing cursors
retain their selected membership and local readers. Dropped or retired dependency generations
invalidate continuation when observed by the adapter. Reused names never resurrect a cursor.

Each index has its own reader view and replication progress. This feature provides no atomic
cross-collection snapshot or MongoDB transaction/read-concern guarantee for indexed content.

Distinguish cursor continuation from sequence-token pagination. `getMore` uses the prepared plan;
a new `$search` with `searchAfter` or `searchBefore` resolves dependencies and collects fresh
membership. Sequence tokens do not retain the previous join snapshot. Preserve this existing
fresh-query behavior and test/document possible membership changes between independently issued
pages rather than promising snapshot stability across them.

Retain the existing limits initially: eight join predicates, 100,000 keys, 32 MiB estimated query
memory, 256 MiB estimated concurrent retained memory, and 10,000,000 foreign-work checkpoints.
Use one process-owned concurrent budget across both architectures. Reservations remain charged
through cursor lifetime, including foreign-key materialization, partition copies, and facet work.
Preserve existing standalone metric names; expose coupled/shared metrics without counting the
same reservation twice or using unbounded collection names as labels.

Distinguish invalid syntax or mappings, unsupported topology, unauthorized dependencies, missing
foreign indexes, unavailable generations, resource exhaustion, and cancellation. Translate them
at the adapter boundary using existing transport error conventions. Fail the query without
partial results when a limit is exceeded. Only a successfully queried foreign index with no
matching keys produces empty membership; a missing foreign index is an error.

## Authorization caveat and topology constraints

### Known caveat: coupled foreign collection authorization

Tracking: [Enforce MongoDB user permissions on foreign collections in coupled joins (#3)](https://github.com/beardsandbeers/nomongo-t/issues/3).

Decision: proceed with coupled cross-collection joins without enforcing the original caller's
foreign-collection permissions in this implementation. This gap is deferred follow-up work, not
a release gate. Preserve existing outer-collection authorization, native transport/cursor isolation,
ownership boundaries, and standalone access checks.

A user allowed to search `contacts`, but not `private_accounts`, may infer information from
`private_accounts` by using it as a join source and observing matching local documents, exclusions,
or counts. Returning no foreign documents does not remove this information disclosure. Document
this limitation alongside coupled join examples and release notes; do not advertise per-user
foreign-collection isolation. Same-database placement and allowlists do not close the gap.

Standalone joins continue requiring access to every dependency before foreign execution and on
continuation. An existence predicate reveals foreign information even when it returns no foreign
documents.

The inspected
[SearchCommandDefinition](../src/main/java/com/xgen/mongot/server/command/search/definition/request/SearchCommandDefinition.java)
contains the outer namespace and query but no end-user authorization proof for the foreign
collection. Transport authentication and the replication client's privileges do not establish
that the original MongoDB user can read the foreign collection.

Upstream source inspection confirms that stock search authorization does not discover this foreign
dependency. In both the inspected
[MongoDB v8.3 search lite parser](https://github.com/mongodb/mongo/blob/v8.3/src/mongo/db/pipeline/search/lite_parsed_search.h)
and [master implementation](https://github.com/mongodb/mongo/blob/master/src/mongo/db/pipeline/search/lite_parsed_search.h),
`getInvolvedNamespaces()` returns an empty set and `requiredPrivileges()` requires `find` only on
the outer namespace. Both `$search` and `$searchMeta` use this shared base. These are source
observations, not a runtime test of every MongoDB release.

The inspected v8.3
[search request builder](https://github.com/mongodb/mongo/blob/v8.3/src/mongo/db/query/search/mongot_cursor.cpp)
does not add the end-user identity or privileges. Its
[search executor](https://github.com/mongodb/mongo/blob/v8.3/src/mongo/db/query/search/search_task_executors.cpp)
also supplies no egress metadata hook to convey authorization; the gRPC factory's hook defaults
to null. The local native headers and search-envoy metadata provide no authenticated per-user
identity either. Therefore the proposed inline join cannot transparently enforce the original
MongoDB caller's foreign permissions purely on the search side with this unmodified protocol.
Reading user/role definitions does not supply the missing binding between request and user.
Caller-supplied usernames or comments must not be treated as that binding; a callback using the
replication account would check the service account's permissions instead.

Resolving this caveat later requires an authorization integration outside this mongot planner.
The direct candidate is a mongod change that recognizes supported join sources during search
lite parsing, declares the foreign namespaces, and requires `find` on each before dispatching any
search work. Apply it to both search stages and every supported join placement. Keep permission
decisions in mongod; mongot needs no duplicate MongoDB user/role database for this model. Ensure all
trusted ingress uses the authorizing implementation; unpatched mongod callers must not be able to
bypass it. A trusted application gateway would change the query path and is outside a purely
search-side solution; it must not be described as transparent enforcement for arbitrary direct
mongod callers.

For that follow-up, test a MongoDB user authorized only for the outer collection, including count-only
and explain queries, and require denial before foreign execution. A collection allowlist, a
replication service account's privileges, and transport authentication do not provide per-user
foreign authorization. Ordinary same-collection self-joins need no additional collection grant
when views and cross-ownership access are excluded as specified above.

Do not reject coupled cross-collection inline joins solely because this deferred enforcement is
absent. No new shared-access configuration or mongod modification is required for the current
implementation. The standalone API remains independent and directly queryable; additional granular
controls are separate future work. This deferral does not remove any existing access controls.

If mongod must remain unmodified, expressing the relationship through MongoDB's native `$lookup`
is an alternative query design, with foreign-namespace authorization owned by MongoDB. It is not
the same inline search operator or execution plan and needs separate semantics/performance review.

### Complete foreign membership

The first release rejects joins in sharded deployments and intermediate cursor execution. Use
deployment/source topology as well as request metadata; absence of an intermediate flag alone
does not prove that a foreign index is complete. Unknown topology must not silently qualify.

Reuse the existing `MongodTopologyMonitor` and `CatalogAccessGuard` wiring rather than building a
second topology monitor. A matching topology alone is insufficient: a correctly configured
sharded deployment passes `requireTopologyMatch()`. The join adapter must additionally require
the verified non-sharded case and associate the result with its source configuration epoch.

The current intermediate cursor path does not carry the prepared snapshot through `CursorQuery`.
Rejecting it explicitly avoids both incomplete foreign membership and accidentally reacquiring
different readers. Normal intermediate queries remain untouched.

Future distributed support needs global foreign membership collection, bounded transfer and
deduplication, routing, cancellation, and generation semantics across servers. It must also carry
prepared state through intermediate search/meta cursors and share resource ownership until both
close. Per-shard materialization of shard-local foreign data is insufficient.

## Upstream maintenance surface

Keep the feature implementation in new fork-owned classes plus the existing join and coexistence
extension classes. The expected additional integration surface is:

- `JoinOperator` and its adjacent source parser: extend the fork's existing join syntax.
- `CursorQuery` and `CursorFactory`: optional expected-generation binding with unchanged defaults.
- `ConfigState`: additive lock-free source snapshot/epoch publication, avoiding cursor/config lock inversion.
- `OnPremMongotBootstrapper`, only as needed: inject source metadata access, topology, and the
  process-owned join services through existing composition.
- `ServerCallHandler` and `BulkheadCommandExecutor`, as required by the dispatch implementation:
  propagate an optional execution context on both command pools, with a small shared context type.
  This additional seam is required by the cancellation review; the original cursor-only estimate
  was incomplete.
- `LuceneSearchQueryFactoryDistributor`: make the existing unresolved-join error architecture-neutral.
- Corresponding BUILD dependencies and focused regression tests.

Keep shared planner dependencies below transport composition to avoid BUILD dependency cycles.
Do not copy native cursor managers or command handlers into the extension. No new native command
schema, protobuf envelope, on-disk field, ingestion handler, replication algorithm, or Lucene join
algorithm is planned. Existing BSON-capable clients can submit the new expression.

Commit parser changes, planner extraction, upstream seams, and coupled adapters separately.
During an upstream merge, review the optional generation guard, snapshot ownership contract,
native null-index behavior, and decorator delegation. Re-run focused join/native tests and the
existing Docker replacement regression before claiming compatibility.

## Implementation sequence and acceptance tests

1. **Establish the coupled contract.** Run a small real-MongoDB integration spike to verify that
   `$search` and `$searchMeta` forward the proposed syntax. Test the outer-only user authorization
   case to characterize and document the accepted caveat; foreign-access denial is a follow-up
   acceptance criterion, not a prerequisite for enabling joins. Confirm source metadata access,
   source-switch behavior, topology detection, and native cancellation propagation.
   Test cancellation after work has started on both regular and guaranteed pools, and a stalled
   metadata lookup; queue-time cancellation coverage alone is insufficient.
2. **Add source parsing.** Test legacy strings, both object forms, custom index names, omitted
   `from`, malformed objects, explicit null/blank values, and serialization. Preserve rejection
   of unsupported `{ indexId: ... }` syntax and internal-key serialization.
3. **Extract the planner without changing standalone behavior.** Run the existing
   `JoinOperatorTest`, `StandaloneJoinsTest`, and Lucene join suites. Preserve standalone errors,
   grant checks, typed equality, limits, metrics, and generation behavior. Test that concurrent
   adapters would share the same memory budget.
4. **Implement coupled resolution and composition.** Test default/custom foreign indexes, the
   same index name in different collections/databases, ownership filtering, missing/vector/
   quarantined/unavailable indexes, self-join snapshot reuse, and metadata lookup failures.
   Exercise rename, drop/recreate, and source reconfiguration with deterministic race points.
   Include unchanged-generation transitions to `DOES_NOT_EXIST`, implicit view resolution with an
   empty view selector, source-epoch changes before catalog replacement, and metadata admission
   saturation while ordinary queries continue.
5. **Implement native prepared cursors.** Add expected-generation binding and producer ownership.
   Test a swap between planning and registration, the unconsumed-snapshot empty-cursor path,
   partial acquisition failure, cancellation, dependency retirement, and registration failure
   after reader transfer. Verify memory returns to baseline after exhaustion, kill, idle expiry,
   errors, and shutdown. Verify native and standalone cursor IDs remain isolated.
   Test cancellation during initial local count/facet execution before producer creation, explain
   partition-count changes before snapshot acquisition, explain-size failures after registration,
   and cleanup of any join-registration markers through idle expiry.
6. **Validate end to end and document the supported contract.** Query replicated collections
   through actual MongoDB `$search` and `$searchMeta`; test pagination, explain, sorting, counts,
   facets, arrays, exclusions, all existing key domains, and mismatched Lucene partition counts.
   Compare results with a MongoDB reference pipeline constrained to the same supported types and
   normalization semantics after replication catches up. Verify ordinary native and standalone
   operations remain compatible using coexistence and Docker replacement regressions.

Acceptance requires rejection tests for unsupported views, vector/nested joins, intermediate and
sharded paths, and unauthorized standalone index access. Preserve mongod's outer-collection checks;
coupled foreign-collection denial is deferred. New searches must observe fresh membership after
updates while existing cursors preserve their initial membership. Exact integer tests must retain
values above `2^53` and BSON type distinctions. Performance validation should measure preparation
and retained memory separately from local execution; ordinary queries must bypass metadata lookup
and materialization entirely.

Include distinct mutation tests for `getMore` and for fresh searches using `searchAfter` or
`searchBefore`. Only the former must preserve the original membership. Test preparation admission
independently of retained-key memory limits, including slow metadata with zero collected keys.

## Alternatives and decisions still open

Duplicating the standalone planner would create two implementations of mapping validation, limits,
and lifecycle correctness. Calling standalone services from native commands would couple transports
and authorization domains. Moving source lookup into Lucene would bring catalog and request policy
into the engine. These approaches increase maintenance cost without using the seams already present.

A catalog-only lookup by `lastObservedCollectionName` would avoid metadata round trips and improve
outage availability, but would make source selection depend on asynchronous rename observation.
The proposal prefers authoritative UUID resolution; changing that choice requires an explicit
weaker namespace-consistency contract and corresponding tests.

The following decisions remain open before implementation is enabled:

- Whether the initial same-database, non-sharded, collection-only scope meets deployment needs.
- The source-client injection/lifecycle mechanism and the bounded metadata lookup timeout policy.
- Whether operators accept the new-search availability tradeoff during source metadata outages.

Distributed execution and cross-ownership joins remain follow-up designs. They should not expand
the initial upstream patch implicitly. Coupled foreign-collection authorization is also deferred;
the eventual enforcement mechanism does not block this implementation.
