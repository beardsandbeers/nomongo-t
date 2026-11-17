# Index joins for standalone mongot

**Standalone search-index joins were introduced in server 0.6.0. Native ObjectId joins
are available in 0.8.0. Integer joins, mixed-type joins, and numeric cancellation fixes
are available in 0.9.0.**

This guide adapts the original two-Lucene-index example to this repository's
standalone push server, BSON search expressions, mappings, and cursor lifecycle.
It covers joins across indexes and self-joins within one index.

For joins through mongod over replicated indexes, see [coupled joins](coupled-joins.md).
That entry point uses `from: {collection, index}` and has a documented foreign-collection
authorization caveat. The standalone string-source syntax and dependency access checks below
remain unchanged; both architectures use the shared planner in `extensions/join`.

The initial feature is an equality **semijoin**: return local documents
when at least one qualifying foreign document has a matching key. Returning
foreign documents, expanding rows, and combining relevance scores are separate
features. Use server 0.6.0 or later with the existing BSON SDK calls.

## 1. Project baseline and scope

The standalone server exposes gRPC without a MongoDB aggregation executor.
`QueryRequest.index_id` identifies the index whose documents are returned;
`query_bson` contains a mongot search expression. Keep that entry point.

Each index owns its documents and receives writes directly through `Upsert` and
`Delete`. `CreateIndex` takes a name, type, and mapping; names are instance-wide.
There is no shared source document store, automatic write fan-out, or replay log.
Applications write related content to each index themselves. `UpdateIndex`
creates an empty generation that callers must refill from their own source.

| Area | Existing behavior | Consequence for joins |
| --- | --- | --- |
| Runtime/build | Java 21 and Bazel | Uses the existing build, without a separate Maven application. |
| Lucene | Upstream modules at 10.4.0; core and join overridden by fork 10.4.0-5 | Verify against the pinned fork, rather than the original guide's 10.6.0 examples. |
| Catalog | `LocalIndexCatalog`, instance-wide index names, index ObjectIds, live generations | Resolve a foreign name once, then pin its stable index/generation identity. |
| Ingestion | Writes target one index; mapping updates create empty generations | Joins search indexed content; they do not fan out writes or trigger automatic backfill. |
| Search | `SearchQuery`, `JoinOperator`, and standalone join planning | Parses the foreign predicate separately from executing it against another index. |
| Token fields | Exact postings and `SortedSetDocValues` | Reuse these for string join keys, including scalars and arrays. |
| Identity | Internal `$meta/_id` uses BSON-encoded bytes | Token keys are not directly compatible with internal ID bytes. |
| Readers | Managed searcher references; optional local index partitions | Cover every partition and retain the correct snapshots. |
| Cursors | Snapshot-pinning results and index-scoped leases | Materialize membership once and preserve it through `GetMore`. |
| Authorization | QUERY role and index-ID allowlist | Require access to every participating index, including on continuation. |

References: [API schema](../src/proto/standalone/v1/standalone.proto),
[query service](../src/main/java/com/xgen/mongot/standalone/server/StandaloneQueryService.java),
[index catalog](../src/main/java/com/xgen/mongot/standalone/catalog/LocalIndexCatalog.java),
[dependency pins](../bazel/java/maven_search_query.MODULE.bazel), and
[client guide](../clients/README.md).

Supported scope:

- Equality membership between string fields indexed as `token`, native `objectId` fields,
  or BSON int32/int64 fields indexed as integer-only `number`.
- Native `_id` joins when `_id` has an explicit matching token, objectId, or integer mapping.
- Cross-index joins and same-index self-joins in one standalone server.
- Any-shared-value semantics for arrays, without duplicate local results.
- Foreign predicates using supported search operators, with scores ignored.
- Filters and exclusions; ordinary local predicates retain their scoring.
- Multiple independent join predicates within server resource limits.
- Local scoring, sorting, counts, and facets through the existing query paths
  and feature flags. Existing drill-sideways defaults remain unchanged.

Defer foreign-document projection, outer/full joins, score propagation,
correlated expressions, composite keys, nested/recursive index joins,
remote servers, vector joins, and double/decimal join keys. Internal BSON metadata `_id`
bytes are not used as a join field; explicitly map `_id` to its native type.

## 2. Semantics and example data

For local document `d`, foreign predicate `Q`, and sets of indexed key values `K`:

```text
join(d) = exists foreign document f:
            Q(f) AND intersection(K(d.localField), K(f.foreignField)) is nonempty
```

Only local documents are returned. Foreign keys need not be unique: this also
supports many-to-many membership. One parent per contact is a data convention,
not an algorithm requirement.

```text
accounts_search index:
  { _id: "a1", accountKey: "a1", region: "bay area", industry: "aerospace" }
  { _id: "a2", accountKey: "a2", region: "new york", industry: "aerospace" }
  { _id: "a3", accountKey: "a3", region: "bay area", industry: "finance" }

contacts_search index:
  { _id: "c1", accountKey: "a1", title: "Principal Software Architect" }
  { _id: "c2", accountKey: "a2", title: "Solutions Architect" }
  { _id: "c3", accountKey: "a1", title: "Aerospace Engineer" }
  { _id: "c4", accountKey: "a3", title: "Cloud Architect" }
```

Search contacts for `architect` whose account is in `bay area` and `aerospace`.
Only `c1` qualifies. Conceptually this is SQL `WHERE EXISTS`, without returning
contact/account pairs.

## 3. BSON syntax

Use `join` inside `compound.filter`, with familiar equality-join field names and
the existing `operator` convention for a nested search predicate.

### Cross-index search

The outer request selects the contacts index. Its `query_bson` is:

```json
{
  "compound": {
    "must": [
      { "text": { "path": "title", "query": "architect" } }
    ],
    "filter": [
      {
        "join": {
          "from": "accounts_search",
          "localField": "accountKey",
          "foreignField": "accountKey",
          "operator": {
            "compound": {
              "filter": [
                { "equals": { "path": "region", "value": "bay area" } },
                { "equals": { "path": "industry", "value": "aerospace" } }
              ]
            }
          }
        }
      }
    ]
  }
}
```

`accounts_search` is the instance-wide name of the foreign search index. The
outer request identifies `contacts_search` by its index ID. The existing SDK
request shape carries the join expression:

```python
# `query` is the expression above; use your configured index names and IDs.
query["compound"]["filter"][0]["join"]["from"] = accounts_index_name
response = client.search(api.QueryRequest(
    index_id=contacts_index_id,
    query_bson=encode_bson(query),
    batch_size=10,
))
```

No new RPC or protobuf field is needed. Protobuf request fields keep `snake_case`, while
fields inside BSON use the existing `camelCase` convention.

| Field | Contract |
| --- | --- |
| `from` | Optional, nonblank index-name string. Omitted means the exact current index and its pinned generation/readers. |
| `localField` | Required literal field path resolved against the outer index. |
| `foreignField` | Required literal field path resolved against the foreign index. No implicit `_id` default. |
| `operator` | Required foreign search operator subtree; paths and analyzers belong to the foreign index. |

Resolve an explicit `from` by exact, case-sensitive name in `LocalIndexCatalog`.
Names are unique across the standalone instance, so no namespace qualifier or
ambiguity rule is needed. A missing name is `NOT_FOUND`; a target that is not a
search index is `INVALID_ARGUMENT` in the first release.

Resolve the name once during planning, authorize its index ID, and retain its
stable index/generation identity. Cursor continuation must not resolve the name
again or switch to a dropped-and-recreated index with the same name. Name lookup
is internal query planning; it must not require the caller to invoke the catalog
RPCs or acquire the CATALOG role.

Require a predicate initially, rather than silently defaulting to a full foreign
scan. It can be a `compound`. It is not a complete `SearchQuery`: reject foreign
`index`, sort, pagination, counts, collectors, highlights, stored-source options,
and nested index joins. Collect **all** qualifying keys; a foreign top-k or
`batch_size` limit would change the result incorrectly.

Initially accept joins in `compound.filter` and `compound.mustNot`, including
non-scoring compound subtrees under those clauses. Reject scoring `must`/`should`
placements and a bare root `join`. A join-only search uses a root `compound` with
only `filter`. Reject `score` on `join`; foreign operator scores are ignored.

### Same-index self-join

Find employees whose manager works in Berlin. Omit `from` to use the current
employees index on both sides:

```json
{
  "compound": {
    "filter": [
      {
        "join": {
          "localField": "managerKey",
          "foreignField": "employeeKey",
          "operator": {
            "equals": { "path": "office", "value": "Berlin" }
          }
        }
      }
    ]
  }
}
```

Map both keys and `office` as tokens. An explicit `from` string can also name
the outer index. Both forms reuse exactly the same reader snapshots. Naming
another index performs a cross-index join, even if the application writes similar
documents to both; the indexes have independent documents, mappings, and visibility.

The local and foreign field names may be identical. A document may match itself
when its keys and foreign predicate match; there is no implicit self-exclusion.
This is one-hop existence, not recursive manager traversal.

### Reverse direction and exclusion

To return accounts having architect contacts, query the accounts index and join
to the contacts index. Use `accountKey` on both sides and a foreign `text`
operator over `title`. There is no separate parent versus child join syntax.

Putting the same `join` object inside `compound.mustNot` means no qualifying
foreign match exists. This includes documents with missing keys or orphaned
references. Add a local existence predicate when missing keys should be excluded.

## 4. Mappings and key equality

The example uses explicit `accountKey` fields to stay within the existing token
representation. These are existing mapping shapes.

Accounts search index definition:

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "accountKey": { "type": "token", "normalizer": "none" },
      "region": { "type": "token", "normalizer": "none" },
      "industry": { "type": "token", "normalizer": "none" }
    }
  }
}
```

Contacts search index definition:

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "accountKey": { "type": "token", "normalizer": "none" },
      "title": { "type": "string" }
    }
  }
}
```

`dynamic: true` alone does not establish the token-key contract. Resolve both
paths to their shared supported equality domains: token, objectId, and integer. Each shared
mapping contributes its own membership set; a match in any domain qualifies the local document
once. A field mapped with all three types can therefore join a collection with mixed `_id`
types. Fields with no shared supported type are rejected. Token normalizers must match when
both fields support token keys. An ordinary lossy number mapping alongside token/ObjectId
mappings contributes no integer keys; use the integer-only options below to enable that domain. Use
`FieldName.TypeField.TOKEN.getLuceneFieldName(path, Optional.empty())`; do not
pass raw BSON paths to Lucene or assume token and internal ID field names match.

`IndexableFieldFactory` writes tokens and ObjectIds to exact postings and
`SortedSetDocValuesField`, even for scalars. ObjectIds use 12 raw bytes, never
UTF-8 or hex conversion. ObjectId joins require an index capability that provides
ObjectId DocValues; older indexes must be rebuilt. The original guide's single-valued
`SortedDocValues` collector and `JoinUtil(..., false, ...)` do not apply here.

Equality rules:

- Compare indexed values within the same equality domain. BSON int32 and int64 share the integer
  domain and compare by numeric value. Require matching token normalizers; recommend `none`
  for identifiers. Lowercase normalization deliberately makes keys
  case-insensitive. Do not normalize collected bytes a second time.
- Scalars contribute one key; arrays contribute their indexed values in every shared type.
  Any shared value qualifies, with one result per local document.
- Missing, null, empty arrays, and other BSON types contribute no key.
  A missing supported mapping is a query error, not an empty join.
- An empty string is an ordinary key when present in the token index.
- No implicit number/ObjectId/string conversion, uniqueness enforcement,
  referential integrity, or cascading deletion.

**Length caveat:** token ingestion truncates long strings in
`addSortableStringFieldIfNeeded`. Indexed-token joins inherit that behavior:
distinct overlong source values can collapse to one key. Applications using
tokens as identity keys must bound their canonical keys below the ingestion
limit. Lossless raw-value equality would need a mapping/ingestion contract that
rejects overlong join keys and accounts for existing truncated data. Collecting
keys at query time cannot recover that information.

**Native `_id` joins use a mapped field.** The internal `$meta/_id` is BSON-encoded
using `LuceneDocumentIdEncoder`; it is not the ordinary mapped `_id` field. Map
`_id` as `objectId` on the parent index and the reference as `objectId` on the
local index to use the same native binary representation:

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "_id": { "type": "objectId" },
      "office": { "type": "token" }
    }
  }
}
```

The local mapping contains `"accountId": { "type": "objectId" }`. Use
`localField: "accountId", foreignField: "_id"` with the existing `join` syntax.
For string IDs, explicitly map both sides as `token`. Adding a mapping requires
reingesting documents into the new generation. No application shadow key or
change to the internal update/delete ID encoding is needed. A hexadecimal string never matches
an ObjectId.

### Mixed ID types

Map `_id` on the foreign index and the reference field on the local index with all required types:

```json
[
  {"type": "token", "normalizer": "none"},
  {"type": "objectId"},
  {"type": "number", "representation": "int64", "indexIntegers": true, "indexDoubles": false}
]
```

For example, use this array as the value of `mappings.fields._id` on the parent and
`mappings.fields.accountId` on the child. One unchanged `join` predicate then matches string
references to string IDs, ObjectIds to ObjectIds, and integer references to either integer width.
`"42"` never matches integer `42`, and an ObjectId's hexadecimal string never matches that ObjectId.
Mixed arrays match if any same-type value matches, with one result per local document. If only
one side maps all three types, only types shared with the other side participate. `mustNot`
excludes a document when any shared type matches. Existing compatible columns are reused.

### Integer join keys

Server 0.9.0 supports exact integer joins using existing numeric columns. Map both
the foreign `_id` (or other key) and local reference with these options:

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "_id": {
        "type": "number",
        "representation": "int64",
        "indexIntegers": true,
        "indexDoubles": false
      },
      "office": { "type": "token" }
    }
  }
}
```

On the local index, map `accountId` with the same numeric options and query with
`localField: "accountId", foreignField: "_id"`. The `join` request syntax is unchanged.
For example, this foreign ID and local reference match:

```json
{"_id": {"$numberInt": "42"}, "office": "Berlin"}
{"_id": "contact", "accountId": {"$numberLong": "42"}}
```

All signed 64-bit values compare exactly, including adjacent values above `2^53`. Arrays
contribute all indexed integers; missing, null, strings, doubles, and decimals contribute none.
Integer join planning rejects the default double representation, `indexIntegers: false`, and
`indexDoubles: true`: double representation can round large integers, while int64 representation
with doubles enabled can truncate fractional inputs into integer keys. Ordinary numeric search
mappings keep their existing behavior.

Foreign collection reads `SortedNumericDocValues` from `NUMBER_INT64_V2`, encodes longs into
eight-byte membership keys, and deduplicates them under the existing query budgets. Local queries
use `NumericJoinQuery` on that same column, choosing numeric points or DocValues using Lucene's
numeric cost rule. Both point traversal and per-document value scans check cancellation. The
DocValues branch reuses bounded binary-hash membership, including for adversarial integer keys. No additional on-disk join fields or index-format changes are needed.
Indexes already using this mapping can be reused. Changing a mapping creates a new generation
that must be reingested.

Join equality is numeric across integer widths. Upsert/delete identity still preserves the
original BSON type, so `Int32(42)` and `Int64(42)` are separate stored IDs that both qualify
for an integer reference to 42. Use a consistent BSON width for document identity. Clients
must also preserve int64 input: use `bson.Int64` in Python and BSON `Long.fromString(...)` in
JavaScript for large values, rather than passing them through JavaScript numbers.

References: [field construction](../src/main/java/com/xgen/mongot/index/lucene/document/single/IndexableFieldFactory.java),
[field naming](../src/main/java/com/xgen/mongot/index/lucene/field/FieldName.java),
[token definitions](../src/main/java/com/xgen/mongot/index/definition/TokenFieldDefinition.java), and
[ID encoding](../src/main/java/com/xgen/mongot/index/lucene/util/LuceneDocumentIdEncoder.java).

## 5. Integration and execution

`StandaloneJoinPlanner` handles foreign index access outside BSON parsing and
local query factories. `SearchIndexReader.QuerySnapshot` provides managed
reader acquisition, binary-key collection, and execution on the same snapshots.

```text
StandaloneQueryService.search(QueryRequest)
  parse and validate join placement / supported query forms
  resolve foreign names to stable index IDs; authorize indexes; validate mappings
  pin live generations and managed partition searchers
  StandaloneJoinPlanner, for each join:
    compile foreign operator using the foreign index's query factory
    collect all matching keys across every foreign partition
    union into one immutable, bounded membership set
  open local cursor with the already pinned local searchers and internal plan
    compile membership against every local partition
    apply it before hit, count, and supported collector processing
  GetMore continues the initial local snapshots and materialized membership
```

The service parses `SearchQuery`, prepares membership, and passes the pinned
local snapshot through `CursorQuery.Search` to `runtime.cursors().newCursor`.
`CursorFactory` uses that snapshot instead of acquiring another local reader.
Ordinary searches retain the existing reader-acquisition path.

`LocalIndexCatalog.getIndexByName` resolves names under the catalog lock. The
planner retains IDs before acquiring readers. A dropped dependency causes failure
instead of a second lookup against a replacement with the same name.

`JoinOperator` describes the wire expression; `MaterializedJoinOperator` holds
immutable pooled binary membership and count/exhaustiveness hints internally. Serialization retains the original
expression without serializing membership. Local optimizer and scope visitors
treat a join as a leaf; its foreign predicate uses its own query factory and
mapping context. Membership is never expanded into a public BSON `in` expression.

`MultiLuceneSearchIndexReader` is an important boundary: union foreign keys once
across **all** foreign partitions, then apply that set to every local partition.
Pairing partition 0 with partition 0 misses valid matches. For self-joins, reuse
the same pinned partition snapshot set; matching documents need not share a
partition or segment.

Initially reject `returnScope`, embedded-document join placement, and foreign
`embeddedDocument`/`hasRoot`/`hasAncestor` predicates. These existing block-join
operators describe relationships within one BSON document. Reject index
joins in vector entry points/operators until separately supported.

## 6. Adaptive execution and term statistics

Foreign preparation searches every qualifying foreign document and collects all
its indexed keys from pinned readers. For token and ObjectId fields, it deduplicates segment-local
ordinals before looking up bytes. Integer fields read numeric values directly. It unions owned
keys across all partitions in a bounded `JoinKeySet`. Local top-k or lower-bound counting does **not** justify
truncating this foreign membership: an omitted parent could change the best hit.

`AdaptiveJoinQuery` chooses between two exact local execution paths for both
strings and ObjectIds:

- Postings: a lazily constructed `TermInSetQuery`.
- Candidate probing: `SortedSetDocValues` plus native binary hash membership in
  a `TwoPhaseIterator`. A fixed-size, execution-local ordinal memo avoids repeated
  decoding without allocating an array proportional to the entire dictionary.

Integer joins use cancellable numeric point/DocValues membership instead of the term-statistics
sampling below. The two numeric paths have identical membership semantics; term-postings
benchmark results below do not measure integer-join performance.

Sets of at most 16 keys retain Lucene's optimized small-term postings query,
without sampling or probe setup. For larger sets, each leaf samples at most 16
membership terms using `TermsEnum.docFreq()`. The
sample estimates total postings work and match density. `Terms.getDocCount()` and
`getSumDocFreq()` estimate documents with keys and multivalue fan-out. Lucene's
`ScorerSupplier.get(leadCost)` supplies the estimated work of sibling filters.

In relative work units, the model compares:

```text
postings work = 64 * distinct foreign keys + estimated sum of local doc frequencies
probe work    = 64 * expected candidate documents * average keys per document
```

The constants are heuristics, not measured nanoseconds. Term statistics do not
capture correlation with other predicates, stale deleted-document frequencies,
cache locality, or the number of candidates ranking will inspect. Sampling can
miss matching keys. It never removes keys or suppresses matches: zero sampled
frequency means an empty join only when every key was checked.

For `count.type: "total"` or any collector query (including facets and
aggregations), the model estimates exhaustive consumption. Otherwise it can
estimate a shorter scan from the lower-bound count threshold and join density.
These flags must come from `SearchQuery`: Lucene passes `COMPLETE_NO_SCORES` to
filter clauses even when the outer collector is doing top-k search.

An optimistic probe is not a result limit. When its actual document checks reach
the estimated break-even cost, its iterator switches to postings at the next
unvisited document. It neither rescans collected documents nor stops collecting.
This preserves ranking, requested lower-bound thresholds, pagination, total
counts, and facets when estimates are wrong. A lower-bound request can still
require exhaustive work; the collector alone decides when it can stop.

Postings construction, key sorting, and rewrite are deferred until a leaf chooses
postings or a probe switches. This avoids the expensive setup in the existing
`SortedSetDocValuesField.newSlowSetQuery`/`IndexOrDocValuesQuery` combination for a
large membership set and a very selective local predicate. Empty membership
becomes `MatchNoDocsQuery`.

No shared global ordinal map is required. Ordinals belong to individual segment
field dictionaries and must never be compared directly across indexes or leaves.
The foreign collector transfers values, and the local probe memo is private to
one scorer and its reader. Cross-request dictionaries/caches are separate future
work, requiring explicit snapshot identity and memory accounting.

References: [adaptive query](../src/main/java/com/xgen/mongot/index/lucene/query/AdaptiveJoinQuery.java),
[binary membership](../src/main/java/com/xgen/mongot/index/query/operators/JoinKeySet.java),
[Lucene reader](../src/main/java/com/xgen/mongot/index/lucene/LuceneSearchIndexReader.java),
and [partitioned reader](../src/main/java/com/xgen/mongot/index/lucene/MultiLuceneSearchIndexReader.java).

### Join explain

Set `explain_verbosity` on the standalone `Search` request, outside `query_bson`, as described
in the [client guide](../clients/README.md#explain-and-trace-queries). Inline joins add a `joins`
section to the existing explain output. Preparation appears once at the top level; local execution
appears inside each `indexPartitionExplain` entry, or at the top level for a single-partition index.

`joins.preparation` describes the foreign membership collected before local search:

| Field | Meaning |
| --- | --- |
| `localPartitions`, `keyCount`, `keyBytes` | Local partition count and totals of distinct keys and their encoded bytes, summed across join equality domains. |
| `reservedBytes`, `limits` | Conservative cursor-lifetime admission estimate and the existing key, per-query memory, concurrent memory and foreign-work limits. Reserved bytes are not measured JVM heap usage. |
| `timing`, `dependencySetup` | Total preparation and the included authorization, dependency resolution and snapshot-acquisition phase, in `millisElapsed`. |
| `foreignWorkUnits` | Existing admission-work checks. These include timeout checks, segment visits, document visits and value visits; this is not a document count. |
| `sources[]` | One record per prepared equality domain, with a stable query-local `id`, optional `from`, resolved foreign index/generation, `selfJoin`, field names, `keyType`, normalizer, foreign partition count, key counts/bytes and memory estimate. `collection` times that domain's foreign collection; `foreignWorkUnits` records its work. |

Mixed token, ObjectId and integer mappings produce separate source records. Key bytes are the
owned native key payload, while reservations also account conservatively for supporting structures
and local query copies. Self-joins retain their existing snapshot semantics. Foreign collection
does not enter the local scorer tree or local collector statistics.

`joins.execution[]` links back to those source records through `id`. It includes the internal local
field, equality type, key count, exhaustive/count-threshold hints, `strategy` and `selectionReason`:

| Strategy | Meaning |
| --- | --- |
| `empty` | Foreign selection produced no keys (`noForeignKeys`). |
| `termInSet` | A small binary key set uses Lucene's direct term-set path (`smallKeySet`). Its normal query-tree timings remain available; no custom segment counters are recorded. |
| `adaptive` | Binary membership chooses between DocValues probing and postings using sampled costs (`sampledCost`). |
| `numeric` | Integer membership chooses points or DocValues using point cost and the lead iterator's cost (`pointCostVsLeadCost`). This path does not switch mid-scan. |

At execution verbosity, `segments[]` reuses one record for each pinned local segment ordinal:

- `estimates` contains the binary path's Lucene documents-with-field, sampled postings, values per document
  and sampled-key count, or the numeric path's point cost. These are planning estimates, not actual
  hits. `minLeadCost` and `maxLeadCost` record the lead-cost range across scorer creations;
  `Long.MAX_VALUE` is Lucene's unbounded lead-cost hint.
- `strategyCounts` counts initial scorer choices: `docValuesProbe`, `postings`, and `points`.
- `probeBudget` is the binary path's per-scorer break-even estimate. `counters` accumulates `probes`,
  `probeMatches`, `valuesExamined`, `dictionaryLookups` and `fallbacks`. Dictionary lookups apply to
  binary membership's ordinal cache; they are zero for numeric probing.
  Probe matches count membership confirmations, including repeated count/facet work, not returned rows.
- A binary probe that switches to postings increments `fallbacks` and reports
  `fallbackReason: "probeBudgetExceeded"`. This transition preserves exact membership. Initial
  strategy counts and fallback counts measure different events.
- `costEstimation` and `indexSetup` time cost estimation and postings/point scorer construction.
  Numeric `weightSetup` additionally measures numeric key conversion and point-query weight setup.

Each timing reports `millisElapsed` and an invocation count. Preparation timings are wall time;
dependency setup and foreign collection are included within total preparation. Segment timings are
summed across their invocations and may overlap across parallel segments. They also overlap the
existing query-tree context timings, so adding these sections together does not give request latency.
These counters do not measure every internal postings/BKD visit.

`queryPlanner` reports identities, keys, limits and strategy metadata, while omitting timing and
execution-counter fields. Planning still materializes foreign membership. Execution snapshots are
cumulative across cursor batches, including count/facet work and work already prefetched; do not sum successive snapshots.
Segment records do not grow with the number of `GetMore` calls. Queries without joins omit this
section. Existing explain-response limits and cursor cleanup apply.

## 7. Consistency, permissions, and resource bounds

### Snapshots and cursors

- Pin a live generation and managed reader snapshot set per dependency. Reuse
  local snapshots exactly when the foreign index is the outer index.
- Different indexes own independent documents and reader views. Successful
  `VISIBLE` writes apply to one index; separately writing/refreshing both sides
  does not create an atomic cross-index transaction snapshot.
- Mapping updates create empty generations. Fresh joins use query-ready live
  generations and see only the content the application has ingested into them;
  readiness does not imply that application-level reingestion is complete.
- Materialize keys once. Release foreign searchers after collection if the plan
  owns only copied bytes. Keep membership and local readers for the cursor.
- `GetMore` never reruns the foreign predicate. Foreign changes affect fresh
  searches; existing cursors retain their initial membership.
- Record every dependency's index and generation IDs in the lease. Invalidate a
  join cursor when any dependency is dropped or its generation is retired by a
  mapping update. Extend the existing outer-index drop/generation cleanup to
  foreign dependencies, including checks for invalidation races before serving
  a batch. Normal reader refreshes preserve pinned results; never retarget a
  cursor to a replacement generation or rerun its foreign predicate.
- `QueryResponse.generation_id` describes the outer index only. Foreign fences
  or dependency metadata would be separate additions if needed.
- Error, cancellation, kill, expiry, and shutdown release readers, key memory,
  and admission permits, including after partial acquisition.

Scoring, sorting, highlighting, and stored-source output describe local
documents. Apply membership before top-k and counts; filtering returned batches
would produce incorrect rankings and totals. Every supported facet/aggregation
path must consume the same plan. Explicitly reject combinations not yet wired
through and tested.

### Authorization

Require the QUERY role and access to every dependency index before running
foreign predicates. `StandaloneAuthInterceptor.requireIndex` and
`StandaloneConfig.Principal.allows` enforce the existing index-ID grants. The
query service and planner check all dependencies for joins, including on
`GetMore` after membership has been materialized. A grant for one index does not
authorize a join to another.

Foreign matches reveal information even when foreign documents are not returned.
The foreign index therefore requires authorization too. A field called
`tenantId` is not automatically enforced by the existing index allowlist;
do not assume document-level tenant isolation.

### Limits and errors

The server applies these admission limits by default:

| Limit | Value |
| --- | --- |
| Independent predicates per query | 8 |
| Distinct hash keys across predicates | 100,000 |
| Distinct ordinal keys across predicates | 1,000,000 |
| Estimated build/retained memory per query | One eighth of the effective shared join pool |
| Estimated memory across concurrent joins and their cursors | One eighth of the JVM maximum heap |
| Join preparation work checkpoints across predicates | 10,000,000 |

The pool is derived once at startup from the JVM maximum heap, leaving most of the heap
for other query work, indexing, caches, and runtime overhead. These limits govern join
admission; they are not a global memory budget for every search/indexing allocation.
The default per-query allowance follows the effective pool, including an explicit pool
override. Integer divisions round down with a minimum of one byte. Instantaneous free
heap does not change admission capacity; outstanding reservations determine availability.

Optionally override the memory limits at startup in standalone YAML:

```yaml
querying:
  joins:
    # Omit these settings to use automatic heap-relative limits.
    maxQueryBytes: 33554432
    maxConcurrentBytes: 268435456
```

With a MongoDB `syncSource`, put the same `querying.joins` block under `standalone`
(`standalone.querying.joins`). Both APIs then use these limits and share the process
reservation counter. `maxConcurrentBytes` includes membership and execution allowances
reserved by idle cursors.

`MONGOT_JOIN_MAX_QUERY_BYTES` and `MONGOT_JOIN_MAX_CONCURRENT_BYTES` override their
respective YAML values. Each setting is optional and must be a positive integer
byte count within the signed 64-bit range. An omitted `maxConcurrentBytes` uses the
heap-derived pool; an omitted `maxQueryBytes` uses one eighth of the effective pool after
YAML and environment overrides. An explicit per-query setting remains independent; the
shared pool is always an additional ceiling. Key-count and work limits remain independent
of heap size.
Changes require a restart. Explain reports the effective limits in
`joins.preparation.limits`; memory-limit errors include the limit, currently
reserved bytes, and the requested additional reservation.

Admission estimates account for query metadata and prepared membership. The hash
path includes pooled keys, foreign deduplication, probe state, and term-query
copies per partition and facet. The ordinal path reserves local bitmaps and a conservative
dictionary-buffer/scorer allowance for every local leaf and collector copy for the cursor's
lifetime. DocValues opens these dictionary buffers even when the scorer only reads ordinals.
Foreign dictionaries, foreign bitmaps, and extra translation scratch are released from the
reservation after preparation. Estimates describe admitted allocations, rather than measured
JVM heap or RSS. Cursor-held reservations are released on exhaustion, kill, invalidation,
expiry, or shutdown. Deadline
and cancellation checks run during foreign collection and local Lucene execution;
each `GetMore` installs its current invocation's cancellation context.

`GetStatus` exposes `standalone.joins.reservedBytes`, `standalone.joins.keys`,
`standalone.joins.foreign`, and `standalone.joins.prepare`. These track reserved
memory, successful key counts, foreign collection time, and complete planning
time. Existing query/RPC timers cover local execution and overall request time.

Never truncate the foreign result/key set to fit a budget. Fail before returning
a partial answer.

| Condition | gRPC status |
| --- | --- |
| Invalid syntax, placement, mapping, normalizer mismatch, target index type, or unsupported combination | `INVALID_ARGUMENT` |
| Missing/dropped dependency, subject to normal authorization handling | `NOT_FOUND` |
| Index access denied | `PERMISSION_DENIED` |
| No query-ready generation | `UNAVAILABLE` |
| Explicit generation fence no longer satisfied | `FAILED_PRECONDITION` |
| Key, memory, or work limit exceeded | `RESOURCE_EXHAUSTED` |
| Deadline or caller cancellation | `DEADLINE_EXCEEDED` / `CANCELLED` |

Foreign key sets and local membership are not cached across requests. Pinned local
searchers disable Lucene query caching so membership terms retain the cursor's
bounded lifetime. Later key-set caches must bind to foreign
reader identities, generation, predicate, field/type/normalizer semantics, and
authorization context. Ordinal caches also need local reader/leaf and membership
identity; integer ordinals are not stable across refreshes/merges.

## 8. Validation and algorithm comparison

Implementation entry points:

1. Typed parsing/serialization: `JoinOperator` and `MaterializedJoinOperator`.
2. Name resolution, authorization, budgets, and immutable membership:
   `LocalIndexCatalog` and `StandaloneJoinPlanner`.
3. Managed readers and partition-wide execution: `SearchIndexReader.QuerySnapshot`,
   `LuceneSearchIndexReader`, and `MultiLuceneSearchIndexReader`.
4. Cursor transfer, paging, cancellation, and dependency invalidation:
   `CursorQuery.Search`, `CursorFactory`, and `StandaloneQueryService`.
5. Usage: existing `QueryRequest.query_bson`; no generated schema changes.
6. Local membership: `JoinQueryFactory` creates `AdaptiveJoinQuery`; the A/B/C
   results below describe the earlier postings-only baseline.

Relevant checks from the repository root:

```bash
bazel test --test_output=errors \
  //src/test/unit/java/com/xgen/mongot/standalone/... \
  //src/test/unit/java/com/xgen/mongot/index/query/... \
  //src/test/unit/java/com/xgen/mongot/cursor/... \
  //src/test/unit/java/com/xgen/mongot/index/lucene/query/... \
  //src/test/unit/java/com/xgen/mongot/index:QueryMetricsRecorderTest \
  //src/test/unit/java/com/xgen/mongot/index/lucene:LuceneSearchIndexReaderTest \
  //src/test/unit/java/com/xgen/mongot/index/lucene:MultiLuceneSearchIndexReaderTest
bazel build \
  //src/main/java/com/xgen/mongot/standalone:MongotStandalone \
  //src/main/java/com/xgen/mongot/community:MongotCommunity
```

Development verification on 14 November 2026 passed all 285 test targets above,
including 24 join behavior/reader tests and 10 join parser/serialization tests.
Both executables built successfully. A separate launched standalone process also
returned the guide's expected result over gRPC, observed a foreign-document
update, and paged a self-join. Live MongoDB ingestion alongside push ingestion
was not exercised by these checks.

Regression coverage and cases to preserve:

- The contact example returns only `c1`; reverse joins return accounts once.
- Same-index self-joins and cross-index joins, including independently written
  documents in different segments and local partitions.
- Instance-wide index-name lookup, case-sensitive matching, missing names,
  unsupported target types, and name reuse after drop without cursor retargeting.
- Scalar/array keys, duplicates, missing/null/wrong types, empty strings, empty
  foreign results, normalizers, Unicode, and overlong-key collisions.
- Boolean composition, exclusions, multiple predicates, and rejected nested,
  scoring, embedded, and vector forms.
- Local relevance, pre-top-k filtering, counts, and supported collectors.
- Foreign document upserts/deletes between pages, refreshes, merges, mapping
  updates that create empty generations, explicit reingestion, dependency
  drops/generation retirement, cursor expiry, and kill.
- Access denied on the foreign side, permission changes before `GetMore`,
  budget/deadline failures, cancellation, and cleanup after partial acquisition.

Measure foreign search/key collection, membership construction/rewrite, local
execution, and total time separately. Vary key cardinality, local selectivity,
fan-out, segments/partitions, key length, concurrency, and warm/cold readers.
Report p50/p95 latency, peak/retained memory, allocations, and cancellation latency.
Include foreign preparation in every comparison.

### Original postings-only development comparison (14 November 2026)

A Lucene-only warm-reader comparison used the pinned `10.4.0-5` core, Java 21.0.4,
10,000 accounts in one segment, and 100,000 contacts in ten segments. Each
strategy used the same pinned readers and included foreign search/key collection,
membership construction/rewrite, and local top-10 search. Counts were checked
for equivalence before timing. There were 20 warmups and 40 measured runs per
case; query caches were disabled. This excludes gRPC, authorization, and server
admission bookkeeping and is not a server latency guarantee.

Times below are total p50 / p95 milliseconds. Broad local predicates match
100,000 candidates; selective predicates match 100.

| Foreign keys / local predicate | A: postings | B: DocValues | C: adaptive |
| --- | --- | --- | --- |
| 10 / broad | 2.510 / 2.888 | 3.170 / 3.606 | 2.124 / 2.418 |
| 10 / selective | 1.887 / 2.201 | 0.910 / 1.105 | 1.915 / 2.218 |
| 5,000 / broad | 11.385 / 13.967 | 53.925 / 56.175 | 12.041 / 13.602 |
| 5,000 / selective | 11.343 / 12.458 | 53.327 / 55.506 | 53.648 / 55.226 |

For 5,000 keys and selective local candidates, median thread allocation was
about 4.91 MiB for A, 18.60 MiB for B, and 18.79 MiB for C. These cases support
postings as the initial baseline while showing a small-key selective case where
DocValues wins. Rebenchmark with production data before changing the strategy.

### Adaptive binary-key comparison (14 November 2026)

A subsequent Lucene-only run exercised the production `AdaptiveJoinQuery` and
`JoinKeySet` with the same native foreign collection on both arms. It used the
pinned 10.4.0-5 fork, Java 21.0.11 with the Vector API, 10,000 foreign documents
in four segments, and 100,000 local documents in ten segments. Readers were warm
and in memory; query caches were disabled. Each case had 20 warmups and 40
measurements with shuffled strategy order. Counts were checked against expected
results before timing. Foreign collection, query construction/rewrite, and local
execution are all included. Measurements were taken without concurrent builds.

Times are total p50 / p95 milliseconds. Every row below collects 5,000 foreign
keys. Selective predicates leave 100 local candidates; broad predicates leave
100,000. Top-10 uses the normal 1,000-hit lower-bound threshold; full count
consumes all matches.

| Key / local request | Postings | Adaptive |
| --- | --- | --- |
| ObjectId / selective top-10 | 15.366 / 17.680 | 3.416 / 3.872 |
| String / selective top-10 | 11.218 / 11.793 | 1.303 / 1.553 |
| ObjectId / broad top-10 | 18.529 / 19.487 | 5.472 / 5.933 |
| String / broad top-10 | 12.311 / 14.353 | 1.801 / 2.215 |
| ObjectId / broad full count | 14.774 / 19.027 | 14.200 / 18.078 |
| String / broad full count | 11.668 / 12.881 | 10.749 / 12.017 |

The 10-key cases retain the original postings path: broad string p50 was
0.688 ms in both arms, and broad ObjectId p50 was 2.432 / 2.443 ms. For 5,000-key
selective joins, median thread allocations were 1,540 / 1,035 KiB for ObjectIds
and 1,501 / 1,112 KiB for strings (postings / adaptive).

These synthetic results support the initial heuristic, not a claim of universal
optimality or server latency. They omit gRPC, disk I/O, concurrent load, and server
admission. Tests additionally check multivalued keys, missing values, deleted
documents, unsampled matches, score ordering across parallel leaves, pagination,
probe-to-postings switching, native mapped `_id`, type validation, and collector
hints. Total-count and facet requests remain exact.

### Ordinal bitmap prototype (17 November 2026)

The initial `OrdinalJoinQuery` experiment replaced copied foreign keys with
local ordinal bitmaps. The measurements below used the isolated Lucene
benchmark before production integration.
It supports the existing sorted-set columns for strings and ObjectIds, not the
numeric columns used by integer joins.

Preparation evaluates the foreign predicate and marks selected ordinals in each
foreign segment. It merges those selected dictionary streams by binary key,
deduplicates across segments and partitions, and intersects the stream with each
local dictionary. The retained query owns only local membership bitmaps and
reader cache identities. Foreign readers can close after preparation. Local
execution uses a `TwoPhaseIterator` to check candidate document ordinals, preserving
ordinary document traversal and ranking. Executing against an unrelated local
snapshot fails rather than reusing invalid ordinal numbers.

This first implementation uses dense `LongBitSet` storage on both sides. Bitmap
payload grows with the sum of per-segment dictionary cardinalities, approximately
one bit per dictionary entry, rather than the byte lengths of selected keys.
Repeated dictionaries in different segments require separate bitmaps. Sparse
ordinal representations and a production strategy selector are future work.

The baseline compares production `JoinKeySet` collection and `AdaptiveJoinQuery`
with the prototype's default seek-first merge. It uses Java 21.0.11, the pinned
Lucene 10.4.0-5 fork, `-Xms768m -Xmx1536m`, warm `FSDirectory` readers, no query
caches, and a single query thread. Each case has two fresh JVMs, 15 warmups and
30 measured operations per strategy and request shape in each JVM. Strategy
order is shuffled. The 2,880 raw measured operations include foreign collection,
membership preparation, and local search. Index creation, transport, authorization,
and production admission are excluded. Temporary indexes are removed after each
JVM run; build and dependency caches remain on `/hdd`.

The default layout has four foreign and eight local segments. Most cases have
100,000 foreign keys, 1,000,000 local documents, and 100,000 unique local keys.
The million-key case has 1,000,000 foreign keys and 2,000,000 local documents.
The sparse case selects 10,000 of 1,000,000 foreign keys, also with 2,000,000 local
documents. Its matching foreign documents are clustered in the first foreign
segment. The low-overlap case has 1,000,000 unique local keys, giving 10% overlap.
The small case selects 10 of 10,000 foreign keys and has 100,000 local documents.
Local key numbers follow a deterministic permutation; selective predicates match
every thousandth local document. This is a synthetic comparison, not a production
latency or capacity guarantee.

Pooled median end-to-end milliseconds, **current hash path → ordinal prototype**:

| Case | Selective top-10 | Broad top-10 | Exact full count |
| --- | ---: | ---: | ---: |
| 10 ObjectId keys | 3.0 → 1.4 | 2.4 → 3.3 | 2.0 → 2.2 |
| 100,000 ObjectId keys | 51.0 → 88.1 | 51.2 → 100.7 | 209.9 → 110.5 |
| 1,000,000 ObjectId keys | 728.6 → 359.3 | 725.4 → 412.1 | 2156.1 → 424.4 |
| 100,000 string keys, 64 bytes | 293.7 → 121.6 | 294.3 → 132.8 | 546.3 → 143.8 |
| 100,000 string keys, 256 bytes | 464.2 → 168.4 | 459.1 → 184.4 | 778.5 → 194.2 |
| Sparse foreign matches | 8.7 → 102.9 | 32.5 → 105.5 | 32.3 → 119.6 |
| 32 foreign / 32 local segments | 54.4 → 214.1 | 53.8 → 210.9 | 497.1 → 223.0 |
| 10% local overlap | 51.3 → 588.7 | 88.6 → 596.9 | 171.5 → 608.1 |

Memory measures deliberately separate retained query state from allocation and
preparation structures. Retained membership below is the prepared query object
graph estimated with Lucene's `RamUsageTester`, excluding index readers and local
execution weights. Allocation is the main thread's `ThreadMXBean` allocation
delta over preparation plus full-count execution; it is not retained heap.

| Case | Retained membership, KiB | Allocated per full count, MiB |
| --- | ---: | ---: |
| 10 ObjectId keys | 32.6 → 10.5 | 0.68 → 0.22 |
| 100,000 ObjectId keys | 2701.6 → 98.3 | 18.65 → 0.35 |
| 1,000,000 ObjectId keys | 25086.0 → 244.8 | 97.44 → 0.61 |
| 100,000 string keys, 64 bytes | 7792.8 → 98.3 | 73.80 → 0.43 |
| 100,000 string keys, 256 bytes | 26652.3 → 98.3 | 129.79 → 0.76 |
| Sparse foreign matches | 298.7 → 244.8 | 2.21 → 0.58 |
| 32 foreign / 32 local segments | 2701.6 → 124.4 | 11.94 → 1.28 |
| 10% local overlap | 2701.6 → 122.8 | 18.66 → 0.37 |

For one million ObjectId keys, peak owned preparation structures were approximately
28.50 MiB for the current hash path versus 0.36 MiB of foreign and local ordinal
bitmaps. The hash estimate samples the key hash plus current foreign ordinal
deduplication set at leaf boundaries and every 4,096 documents. These figures
exclude iterator objects, temporary reallocation overlap, local query rewrite,
and reader memory. They are **not** JVM heap high-water marks or process RSS.
Cold-reader I/O and concurrent request behavior were not benchmarked.

The low-overlap regression exposed an avoidable cost: seeking a DocValues
dictionary for every short gap. A separate follow-up advances at most four terms
sequentially before falling back to `seekCeil`. This mode is selected with
`--scan-before-seek 4`; the default remains zero so the full baseline is
reproducible. Both modes pass the same correctness oracles. The follow-up uses
two fresh JVMs, 10 warmups and 20 measured operations per strategy and shape per
JVM, adding 320 raw measured operations. Retained membership is unchanged.

| Case / request | Initial seek-first, ms | Four sequential steps, ms | Hash rerun, ms |
| --- | ---: | ---: | ---: |
| 10% overlap / selective top-10 | 588.7 | 112.5 | 52.0 |
| 10% overlap / exact full count | 608.1 | 122.0 | 172.0 |
| Sparse / selective top-10 | 102.9 | 107.2 | 9.1 |
| Sparse / exact full count | 119.6 | 122.8 | 33.1 |

These are medians from separate experiments, with an interleaved hash baseline
in each. Sequential advancement fixes much of the low-overlap setup overhead and
makes its full count faster than the hash rerun. It does not solve the sparse
case. [Follow-up raw samples](benchmarks/ordinal-joins/scan-before-seek/samples.csv),
[summary](benchmarks/ordinal-joins/scan-before-seek/summary.json), and
[metadata](benchmarks/ordinal-joins/scan-before-seek/metadata.json) preserve the
comparison separately from the full baseline.

The results support an additional strategy for large membership and long keys.
They do not support replacing the current path unconditionally: eager local
dictionary preparation is expensive for selective requests, sparse foreign
matches, low overlap, and many segments. A selector needs to consider selected
key bytes, the sum of local dictionary sizes, segment count, local candidate
cost, and the available memory budget. Production integration also needs
admission before bitmap allocation, cancellation/work checks in all preparation
phases, and a strategy-specific policy for the existing key cap.

The focused oracle tests cover binary and string keys, multivalued and missing
fields, deletions, union across partitions, exclusions, parallel scoring,
pagination, self-joins, foreign-reader release, snapshot mismatch, and preparation
cancellation. Benchmark fixtures independently verify the expected distinct-key
and full-match counts before timing.

Sources: [ordinal query](../src/main/java/com/xgen/mongot/index/lucene/query/OrdinalJoinQuery.java),
[benchmark](../src/test/bench/java/com/xgen/mongot/index/lucene/query/OrdinalJoinBenchmark.java),
[runner](../scripts/perf/ordinal_join_benchmark.py),
[raw samples](benchmarks/ordinal-joins/baseline/samples.csv),
[summary including p95 and individual JVM medians](benchmarks/ordinal-joins/baseline/summary.json),
and [measurement metadata](benchmarks/ordinal-joins/baseline/metadata.json).

Reproduce the full baseline into a fresh results directory:

```bash
python3 scripts/perf/ordinal_join_benchmark.py \
  --output-dir /tmp/ordinal-join-results-new \
  --forks 2 --warmups 15 --samples 30 --scan-before-seek 0
```

Reproduce the targeted traversal follow-up:

```bash
python3 scripts/perf/ordinal_join_benchmark.py \
  --output-dir /tmp/ordinal-join-scan4-new \
  --forks 2 --warmups 10 --samples 20 --scan-before-seek 4 \
  --shape selectiveTop10 --shape broadCount \
  --case low-overlap --case sparse-objectid
```

The runner checks that `/hdd` is mounted, configures persistent Bazel caches there,
and stops its build server before timing. Rebuild the comparison from preserved
samples without rerunning queries:

```bash
python3 scripts/perf/ordinal_join_benchmark.py \
  --output-dir docs/benchmarks/ordinal-joins/baseline --summarize-only \
  --html-output docs/benchmarks/ordinal-joins/ordinal-join-comparison.html
```

### Adaptive choice during join preparation

The shared planner chooses a representation per string or ObjectId equality
domain. Hash membership uses the existing adaptive postings and DocValues
query. Ordinal membership uses the pinned local segment dictionaries and
matches the resulting bitmaps through the ordinary collector.

Automatic selection starts with foreign `exists` on the join key. The planner
reads dictionary sizes from the pinned snapshots. Repeated values across
segments and keys on deleted documents can raise these estimates. Foreign
selection and the exact join still use the normal predicate and root filter.

| Request | Minimum sum of foreign dictionary sizes to consider ordinals |
| --- | ---: |
| Exact count or metadata collector | 50,000 |
| Field sort or explicit pagination with a lower-bound count | 100,000 |
| First relevance page with a lower-bound count | 250,000 |

A single foreign dictionary above the 100,000 hash cap also qualifies for
consideration. Very large local dictionaries can rule out ordinals because
each participating dictionary adds traversal and bitmap work. The exact
thresholds are initial heuristics. The benchmark covered relevance and full
counts. The field-sort rule conservatively gives less credit for early
termination until representative sorted workloads can be measured.

Each prepared snapshot may expose its join partitions. The metered snapshot
forwards the capability and the partitioned reader composes it. Readers without
the capability use hash. The selection stays in the shared planner and the
query factory creates a scorer for the prepared representation. Existing
collectors consume either scorer for filtering, sorting and pagination.

Before ordinal preparation, reserve local bitmaps, dictionary buffers for each leaf and
collector copy, and scorer overhead, plus foreign dictionary/bitmap and translation scratch.
The local execution allowance also covers the local dictionary merge during preparation.
If the reservation cannot be made, try hash. Release preparation-only scratch on success
and keep local membership and execution buffers reserved until cursor close, including idle
cursors and repeated `GetMore` batches. Release the entire plan on failure. The shared work limit
charges bitmap preparation as well as hash collection. Ordinal domains have a
separate cumulative distinct-key cap. Both strategies preserve exact matches.
Explain reports `ordinalBitmap` and the selection reason for ordinal queries.

#### Follow-up design

**Request intent.** Treat exact total counts and collectors requiring all matches
(including facets) as exhaustive, even if the request returns only ten sorted hits.
For a hits request with a lower-bound count, estimate the work to fill the page
*and* satisfy the count threshold. Track first page and continuation separately.
A page boundary alone gives no upper bound on candidates examined. Reuse the
prepared membership for the lifetime of a cursor.

**Sort.** For relevance order, the collector may skip noncompetitive scores.
For explicit field order, discount local work only when the collector can exploit
the actual index sort, including field sequence, directions and missing-value
semantics. With an unrelated sort, finding the best ten may require visiting
every candidate. Treat unknown collector capabilities conservatively. A sort
hint influences the cost estimate only. Keep scoring, tie-breaking, search-after,
exclusion, and termination in the existing collector.

**Cheap inputs.** Read segment counts and per-leaf dictionary sizes from pinned
DocValues. Sum rounded bitmap sizes across all local and foreign leaves for a
conservative dense-bitmap bound. Obtain foreign predicate cost from the scorer
supplier where cheap. That cost estimates documents, while distinct key count
also depends on duplication and multivalued fields. If the decision is close,
take a bounded sample across partitions and leaves to estimate key width,
duplication and local dictionary overlap. Cap documents, values, dictionary
seeks and elapsed work. Skip sampling for tiny joins. Treat clustered or
inconclusive samples as low confidence. Never use a sample to omit a key or
declare a join empty.

**Selection rule.** First rule out plans that cannot fit their preparation peak
and retained state within the configured budgets. For feasible choices compare
estimated preparation time plus local execution time. Hash preparation depends
on distinct selected keys and their byte lengths. Ordinal preparation depends on
selected ordinal visits, dictionary entries traversed, overlap and segment count.
Local work depends on expected candidate visits and whether the request is
exhaustive. Prefer hash when estimates are close for a first page. Favor ordinals
when wide or numerous keys make hashing expensive, or exhaustive local work
repays bitmap setup. Use the four-step advance before seeking as an initial
candidate for calibration. The measured sparse full-count case still favors hash,
so exhaustive intent alone must never force ordinals.

**Admission and fallback.** If hash growth crosses its reservation, consider one
restart on the same pinned snapshots only when the ordinal bound fits. Release discarded buffers and their
reservation before rebuilding, and charge both passes to the same work limit.
Propagate cancellation immediately. Reject when neither plan fits. Finish preparation before publishing any hits.

**Validation and rollout.** Start with forced hash and ordinal modes for tests,
then record the proposed choice in shadow mode. Explain should include request
intent, sort eligibility, sample size and confidence, estimated bytes and cost
for both plans, the selected strategy, actual preparation work and any restart.
Calibrate on held-out workloads covering relevance, aligned and unaligned field
sorts, first and later pages, selective siblings, sparse dictionaries, skew and
concurrent cursors. The existing benchmark measured relevance top-10 and exact
counts. Field-sort and continuation performance still need measurement. Compare
ordered hits, scores, counts and facets against forced hash, and measure p50/p95
first-page latency, preparation peak, retained bytes and cancellation latency.
Evaluate broader automatic selection behind a rollout flag after the selector
preserves correctness and stays within agreed latency and memory bounds.

## 9. Agreed initial syntax and scope

| Decision | Initial choice | Rationale / consequence |
| --- | --- | --- |
| Result semantics | Existence filter returning local documents | Enrichment or joined rows require an output/projection design. |
| Operator name | `join` | `hasMatch` / `existsIn` emphasize existence; `lookup` suggests enrichment. |
| Foreign target | `from: "indexName"` | Resolve the instance-wide name once, then retain stable index/generation identities. |
| Self-join | Omit `from` | Reuses the current index and snapshots without a name lookup. Its explicit name also works. |
| Key names | `localField` / `foreignField` | `path` / `from.path` uses search vocabulary but makes direction less familiar. |
| Foreign predicate | Required `operator` subtree | `query` suggests a full request; omission would implicitly scan all foreign documents. |
| Keys | Token strings, native ObjectIds, and BSON int32/int64 | `_id` requires a matching explicit mapping; integers require an integer-only int64 number mapping. Double and decimal join keys are unsupported. |
| Placement | Filter/exclusion positions first | Scoring positions need defined constant-score or foreign-score behavior. |

String references such as `accountKey` and native ObjectId references use the
same adaptive engine. `foreignField: "_id"` uses an explicit native mapping as
described in section 4; it does not read the BSON metadata ID field. Integer references use
the existing numeric columns and cancellable numeric membership.
