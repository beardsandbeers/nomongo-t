# NoMongo-T 0.9.0

This release adds exact integer joins and joins over mixed string, ObjectId, and integer
fields. It also fixes delayed cancellation during numeric join execution. The existing
BSON query API works with all four clients; no new RPC or wire-format change is required.

## Integer and mixed ID joins

Integer joins reuse the existing numeric index columns. Map both the parent key and the
local reference as follows, including an explicit mapping when the parent key is `_id`:

```json
{"type": "number", "representation": "int64", "indexIntegers": true, "indexDoubles": false}
```

BSON int32 and int64 keys compare by numeric value across the full signed 64-bit range,
including adjacent values above `2^53`. Doubles and decimals are not integer join keys.
Default numeric mappings are unsuitable because double representation can round integers
and indexing doubles into an int64 column can truncate fractional inputs.

For a collection containing different `_id` types, map both the parent `_id` and the
local reference using this list:

```json
[
  {"type": "token", "normalizer": "none"},
  {"type": "objectId"},
  {"type": "number", "representation": "int64", "indexIntegers": true, "indexDoubles": false}
]
```

The same query matches every shared mapped type:

```json
{
  "compound": {
    "filter": [{
      "join": {
        "from": "accounts",
        "localField": "accountId",
        "foreignField": "_id",
        "operator": {"equals": {"path": "office", "value": "Berlin"}}
      }
    }]
  }
}
```

Strings match strings, ObjectIds match ObjectIds, and integer widths share numeric
equality. The string `"42"` does not match integer `42`, and an ObjectId does not match
its hexadecimal string. Arrays match any shared value and return each local document
once. Exclusion with `compound.mustNot` excludes a document if any shared type matches.
When the two mappings differ, only their shared supported types participate.

See the [join guide](../join-design-guide.md#mixed-id-types) for complete examples and
resource limits. Standalone joins return local documents; MongoDB aggregation `$lookup`
remains the way to attach parent documents in MongoDB-backed pipelines.

## Cancellation and explain

Numeric DocValues scans check cancellation inside large per-document arrays. Numeric
point traversal also checks cancellation while visiting fully accepted subtrees, including
bulk document visits. This closes a path that could continue scanning after cancellation.
Numeric join explain output includes the field path and materialized key count.

## Upgrade

- Use the `0.9.0` standalone Docker image or `community-0.9.0` official-container
  replacement. Both retain automatic standalone/dual ingestion selection.
- Existing indexes with the required mappings can reuse their columns. No new on-disk
  join field is introduced. Changing a mapping creates a new generation that must be
  reingested; an upgrade does not add missing mappings automatically.
- Upsert/delete identity still preserves BSON types. Standalone `Int32(42)` and `Int64(42)`
  IDs are distinct stored identities even though both match an integer join reference.
  Use a consistent BSON width for document identity and preserve int64 values in clients.
- Install matching 0.9.0 clients. Python is distributed through PyPI and Rust through
  crates.io; Java and TypeScript packages are downloadable release assets. The TypeScript
  package remains private and is installed from its tarball.
- Upgrades from before 0.7.0 must account for the `INDEXED` default write concern. Request
  `VISIBLE` for read-after-write search or `COMMITTED` for durability, using updated clients.

## Validation

Regression tests cover mixed mappings, integer widths and boundaries, arrays, exclusions,
paging, counts, and cancellation in both numeric execution paths. An independent Lucene
oracle exercised 514 comparisons across segments, deletions, and serial/parallel searches.

The [live Mongo fixture](../../scripts/tests/community/integer_join_e2e_test.py) passed
14 checks with MongoDB 8.3.0, comparing `$search` followed by `$lookup` on the parent
collection with a MongoDB-only oracle while standalone ingestion ran in the same process.
It covers typed and mixed IDs, mutations, cursor snapshots, restart, and replication
outage/recovery. The fixture removes its containers and data after each run.

Release verification also installs the server and all four client packages outside the
checkout and exercises TLS and plaintext RPCs. Docker verification includes an image-only
replacement of official mongot with the same configuration and data. Published verification
reports and `SHA256SUMS` accompany the
[release downloads](https://github.com/beardsandbeers/nomongo-t/releases/tag/standalone-v0.9.0).
