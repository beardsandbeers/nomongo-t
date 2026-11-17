# NoMongo-T 0.10.0

This release adds direct gRPC reads of MongoDB-replicated indexes, standalone SearchMeta,
schema-preserving index updates, and expanded join and explain support. Python, Rust,
Java, and TypeScript clients use the matching 0.10.0 API.

## Changes

- Discover, query, and inspect standalone and replicated indexes through the direct API.
  Ownership filters, pagination, explicit index targets, and join bindings disambiguate
  indexes with the same name. Replicated indexes remain read-only through this API.
- Run SearchMeta counts and facets without returning document hits.
- Preserve existing Lucene content for supported compatible schema updates. Incompatible
  changes still require an explicit rebuild or reingestion; see the
  [index lifecycle guide](../standalone-mongot.md#mapping-changes-and-index-lifecycle).
- Use inline joins through MongoDB-backed search, with join planning diagnostics and
  analyzed and rewritten Lucene queries in explain. Coupled joins do not enforce the
  caller's foreign-collection permissions; see the [coupled join guide](../coupled-joins.md).
- Use parallel exhaustive delete-by-query, with cancellation and coordinated cleanup.
- Fix direct-read cursor lifecycle, cancellation, and query caching.

## Upgrade and rollback

Use `mybyte/nomongo-t:0.10.0` for standalone deployment or
`mybyte/nomongo-t:community-0.10.0` as the official-image replacement. Both images target
Linux amd64. Install `mongot-standalone-client==0.10.0` from PyPI or version `0.10.0` of
the Rust crate. Rebuild Java and TypeScript clients from the matching source.

Back up persistent state before upgrading. Catalog version 4 records compatible schema
bindings and explicit authorization to initialize generations. Older binaries cannot
open a version-4 catalog; rollback requires restoring a backup from before the upgrade.
Keep the generated TLS identity and bearer token with the data volume.

Use matching clients when requesting the new direct-read selectors. Clients check the
server's read-contract acknowledgment and reject servers that would ignore those fields.
The existing `INDEXED` default write concern remains; request `VISIBLE` for immediate
search visibility or `COMMITTED` for durability.

## Validation

The source-built Docker deployment passed all 17 RPCs through each of the four clients
over TLS, bearer-token rejection, forced-crash recovery, and container recreation with
persistent credentials and committed text, vectors, joins, and BSON integer values.
Client validation passed 21 unit tests and 40 cross-language read-contract scenarios.

The replicated-read suite passed 15 checks, including source outages, offline process
restart, replication recovery, and cursor invalidation after rebuild and drop. The
official-image replacement suite passed eight checks covering text and vector indexes,
index identity, replication catch-up, and restart. Those MongoDB fixtures used MongoDB
8.3.17 and test-specific storage settings to accommodate host kernel and disk conditions.
