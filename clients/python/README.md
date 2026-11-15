# mongot python push client

See [the client guide](https://github.com/beardsandbeers/nomongo-t/blob/main/clients/README.md) for build instructions, examples, TLS, deadlines and write
semantics. This client targets indexes directly using the schema bundled with this checkout.
Release 0.7.0 uses this push API and is incompatible with the older collection-backed 0.2.x API. Use the matching server.

Upsert replaces BSON documents by `_id`; Delete sends BSON `_id` wrappers. No source documents,
request ledger or collection catalog are maintained outside Lucene. Reingest after mapping changes.

Use `GetIndexMappings` to export configured fields and observed dynamic field types as a static
mapping, including mixed types per field. The response also includes a complete reusable index
definition with analyzers and other options preserved. See the client guide for an example.

Version 0.6.0 supports cross-index and self-join predicates in Search BSON. Use
`from: "indexName"`, explicit `localField`/`foreignField`, and a foreign `operator`
inside `compound.filter` or `compound.mustNot`. Both key fields must be strings
indexed as `token` with matching normalizers; arrays use any-shared-value semantics.
Native typed `_id` keys and foreign-document enrichment are outside this scope.
See the [join examples](https://github.com/beardsandbeers/nomongo-t/blob/main/clients/README.md#join-searches-across-indexes).

Release 0.7.0 defaults writes to `INDEXED`, without waiting for reader refresh or durable commit.
Background refresh and commits default to one second. Request `VISIBLE` for read-after-write search
or `COMMITTED` for durability. Concurrent batches can use the existing server workers against the
same or different indexes; each individual batch still targets one index, and documents within
that batch remain ordered. Sequence overlapping writes and retries when their order matters.

Rebuild older generated bindings to send an explicit zero-valued `VISIBLE` concern. Older bindings
omit that value on the wire, which the new server treats as the default `INDEXED`.

```python
client.upsert_many(index_id, documents)  # INDEXED; suitable for bulk ingestion
client.upsert_many(index_id, documents, concern="visible")  # searchable on return
client.upsert_many(index_id, documents, concern="committed")  # durable on return
```
