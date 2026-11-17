# Joins through MongoDB

Coupled search indexes support the same equality semijoin as the standalone API. A join filters
local documents by keys selected from a foreign search index. It does not return foreign documents
or propagate their scores. No mongod code changes are required.

```javascript
db.contacts.aggregate([
  {$search: {
    index: "contacts_search",
    compound: {filter: [{join: {
      from: {collection: "accounts", index: "default"},
      localField: "accountId",
      foreignField: "_id",
      operator: {equals: {path: "active", value: true}}
    }}]}
  }}
]);
```

`index` in the source object defaults to `"default"` when omitted. The collection is in the
outer query's database. Omitting `from` selects the exact outer index, including a non-default
index. The standalone API continues accepting `from: "index_name"`; collection objects are
coupled-only, and string sources are standalone-only. Neither form crosses index ownership.

Map both fields to compatible token, ObjectId, or integer types. Token normalizers must match.
Integer matching preserves exact int32/int64 equality, including values above `2^53`, and does not
match doubles or numeric strings. Arrays and mixed mappings retain the standalone semantics.
Place joins in compound filter or exclusion positions. `$searchMeta`, counts, facets, sorting,
explain, and normal cursor pagination use the same prepared membership.

## Known authorization caveat

Mongod still enforces access to the outer collection. **The inline join does not enforce the
original user's permission to read the foreign collection.** Matching documents, exclusions,
counts, or facets can reveal foreign information, even without returning foreign documents.
This is accepted deferred work tracked in [issue #3](https://github.com/beardsandbeers/nomongo-t/issues/3).
It is not a per-user security boundary between collections. The standalone API retains its
existing dependency grants and direct query access.

## Scope and consistency

- Both indexes must be replicated search indexes in the same mongot process and database, on a
  verified non-sharded deployment. Different Lucene partition counts are supported.
- Views, cross-database or cross-ownership joins, intermediate/sharded execution, nested joins,
  vector/embedded-document combinations, and `returnScope` are unsupported.
- Source names resolve using targeted MongoDB collection metadata and current UUIDs. Missing or
  unavailable foreign dependencies fail the query instead of becoming empty membership. A renamed
  or recreated collection never resolves by the old index's asynchronously observed name.
- Membership and local readers are pinned once per cursor. `getMore` keeps that membership;
  a fresh query with `searchAfter`/`searchBefore` prepares new membership. There is no atomic
  cross-collection or MongoDB transaction snapshot guarantee.
- Retiring a dependency invalidates continuation. Logical source configuration changes invalidate
  prepared plans; changing the metadata connection configuration requires restarting mongot.
  Ordinary source outages and replication host reselection do not invalidate already-pinned plans,
  but new named joins need metadata access.

## Bounds and implementation

Both APIs share the planner and one retained-memory budget. Defaults are eight predicates,
100,000 distinct keys, 32 MiB estimated memory per query, 256 MiB estimated retained memory per
process, and 10,000,000 foreign-work checkpoints. Reservations live until cursor close,
exhaustion, expiry, or failure.

Configure memory at startup with `standalone.querying.joins.maxQueryBytes` and
`standalone.querying.joins.maxConcurrentBytes` in YAML, or override them with
`MONGOT_JOIN_MAX_QUERY_BYTES` and `MONGOT_JOIN_MAX_CONCURRENT_BYTES`. Values are positive integer
byte counts. See [limits and errors](join-design-guide.md#limits-and-errors) for details.

Coupled preparation admits four requests at a time and rejects overload. Its separate four-connection
metadata pool does not use replication connections. Each lookup has a two-second operation timeout,
limited further by the remaining request/preparation deadline; preparation has a 30-second cap.
Topology discovery uses the existing monitor and may wait up to its two-second probe limit.
Transport cancellation is checked during key collection and local execution, including initial
count/facet work and subsequent batches. Metadata cancellation can take until its bounded driver
operation returns. Mongod must propagate cancellation or an RPC deadline for its client timeout
to be visible to search.

The shared implementation is in `extensions/join`; the coupled adapter and resource ownership are
in `standalone/coexistence`. Native cursor creation uses an optional generation guard. Other native
queries bypass join preparation. Coupled metrics use the `coupled.joins` prefix; the retained-byte
gauge observes the shared budget rather than a separately charged reservation.

## Validation

Focused unit suites cover parser round trips and malformed sources, standalone behavior, generation
binding, cancellation dispatch on both executor pools, native cursor behavior, and prepared-resource
cleanup. The live fixture `scripts/tests/community/integer_join_e2e_test.py` compares actual MongoDB
8.3 queries with a MongoDB-only `$lookup` oracle and standalone results. It covers default/custom
indexes, unequal partitions, self joins, missing/view/string-source rejection, exact and mixed keys,
counts, exclusions, explain, pagination, mutations, restart, and source outage recovery. With
MongoDB authorization enabled, it characterizes the accepted foreign-access gap using an outer-only
user. The design review also identifies additional fault-injection and saturation cases; these
checks do not constitute a comprehensive performance or race certification.

The continuation fixture uses 4,096 matching documents and asserts a live mongot cursor and a
retained join reservation before changing foreign membership. It drains that cursor after a fresh
query observes the mutation, tests foreign-index retirement across `getMore`, and cancels a native
wire-message gRPC stream after continuation. Each case requires the cursor count and retained bytes
to return to their baseline. MongoDB's `batchSize: 1` alone is not evidence of mongot continuation.
Source epoch tests cover replication host loss/recovery, replication restarts, and logical cluster
changes; real coupled-adapter unit tests cover continuation and cancellation-context rebinding.

The implementation passed six focused suites (`ConfigStateTest`, `CoupledJoinLifecycleTest`,
`CoupledJoinServiceTest`, `BulkheadCommandExecutorTest`, `StandaloneJoinsTest`, and
`CursorFactoryTest`), relevant Checkstyle/SpotBugs checks, the standalone package build, and all
21 live checks with MongoDB 8.3.17. To reproduce the live checks, build the package and supply a
compatible mongod binary and a locally available Linux Docker image:

```sh
bazel build //deploy:mongot-standalone
mkdir -p /tmp/coupled-join-package
tar -xzf bazel-bin/deploy/mongot-standalone.tgz -C /tmp/coupled-join-package
PYTHONPATH=clients/python:scripts/tests/community python3 \
  scripts/tests/community/integer_join_e2e_test.py \
  --mongod /path/to/mongod-8.3 \
  --mongot /tmp/coupled-join-package/mongot-standalone/mongot \
  --runtime-image mongo:8.3.17-noble \
  --output /tmp/coupled-join-results
```

The output directory must not already exist. The fixture requires `pymongo`, `grpcio`, `protobuf`,
and `pyyaml`; its runtime image must already be available locally. It removes its containers,
volumes, indexes, and temporary credentials, retaining logs and `results.json` in the output
directory. Inspect `passed` and `cleanup_errors` in that file.
