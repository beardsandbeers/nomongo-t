# Standalone recovery, RPC deadlines and readiness

For 0.11, fix three behaviors that an operator can encounter on the current server. Opening an
incomplete data volume can delete surviving indexes. The server's RPC timeout can expire while
the transport still holds an admission slot. The default Docker probe can report healthy while
document writes return `UNAVAILABLE`. The proposed fixes need no data migration or changes to
client packages. The review also identified separate compatibility, scaling and authorization work.

## Refuse destructive recovery when the local catalog is missing

The local catalog supplies the desired index IDs. The ownership ledger remembers IDs even after
a drop. The journal records which physical generations were live, staged or already being
deleted. Today an absent `indexes.json` produces an empty desired set. The native initializer
then journals surviving generations as dropped and deletes their files. This matters on a
partially restored volume because standalone cannot replay pushed documents.
[Catalog loading](../src/main/java/com/xgen/mongot/standalone/catalog/LocalIndexCatalog.java) ·
[Native startup](../src/main/java/com/xgen/mongot/config/manager/ConfigInitializer.java)

Add the check while `LocalIndexCatalog` holds the directory lock, after reading ownership and
before its first call to `persistOwnership()`. Distinguish a missing file from a read or parse
error. On the missing-file path, read the existing journal through `ConfigJournalV1`. Inspect
the identity of every live and staged entry, including quarantined entries. If any ID is locally
owned, throw an `IOException` identifying the missing catalog and advise restoring the matching
stopped backup. This ordering protects both launchers before schema recovery, reconciliation or
native cleanup can write. Keep the native initializer's deletion semantics for its existing callers.
[Journal reader](../src/main/java/com/xgen/mongot/config/backup/ConfigJournalV1.java) ·
[Coexistence startup](../src/main/java/com/xgen/mongot/standalone/coexistence/CoexistenceRuntime.java)

| State at open | Expected outcome |
| --- | --- |
| Missing catalog and an owned live or staged journal entry | Stop. Preserve journal, ownership and generation files. Reopening after restoring the catalog must return the committed documents. |
| Fresh directory, or an ID reserved before the first create became durable, with no owned live or staged entry | Allow normal startup. The ledger alone is insufficient evidence of data loss. |
| Existing valid empty catalog after a deliberate drop | Honor the recorded drop, including a restart before physical cleanup. |
| Missing catalog with only already-deleted local entries, or only replicated entries in a valid mixed volume | Continue ordinary retirement or replicated recovery, subject to existing ownership checks. |

Exercise those cases in the catalog and recovery suites. For the refusal case, use a disposable
fixture with committed data, check the files before and after the refused open, restore its
catalog and query the same generation. Cover standalone and coexistence startup, with a
replicated index present in the mixed fixture.

## Enforce the deadline on the gRPC transport

`StandaloneAuthInterceptor` currently attaches the server timer to a child context. gRPC closes
the stream when its own call context reaches a deadline. Move the effective deadline to that
context so a terminal transport callback returns the permit even when the application handler
has yet to start. The effective timeout is the earlier of the client's timeout and
`maxRpcDurationMillis`. [Admission interceptor](../src/main/java/com/xgen/mongot/standalone/server/StandaloneAuthInterceptor.java)

Register a method-scoped `ServerStreamTracer.Factory` on the actual Netty builders. In
`newServerStreamTracer`, clamp the incoming `grpc-timeout` metadata before gRPC creates its
call context. Supply the configured timeout when the client omits it. Preserve a shorter client
timeout, including zero. Use the pinned gRPC Java timeout key and marshaller in one adapter so
all six protocol time units and overflow handling follow the transport. Keep malformed input on
gRPC's error path. The pinned Netty implementation passes the same metadata from the tracer
factory to the transport listener. Cover that ordering with an integration test when upgrading
the dependency. [gRPC Java pin](../bazel/java/java.MODULE.bazel) ·
[Dedicated builder](../src/main/java/com/xgen/mongot/standalone/server/StandaloneGrpcServer.java) ·
[Shared builder](../src/main/java/com/xgen/mongot/server/grpc/GrpcStreamingServer.java)

Select methods from the registered service descriptors. On the dedicated listener include the
four standalone services and its health service. On a shared listener scope the policy to the
four extension services. Native commands and native health keep their own deadlines. The
interceptor attaches the principal and operation with `Context.withValue`. gRPC owns cancellation
and timer cleanup. The terminal callbacks and handler-setup exception path release the transport
permit exactly once. The dispatcher continues to charge request bytes and worker capacity until
work actually finishes, including an in-progress Lucene write.
[Dispatch accounting](../src/main/java/com/xgen/mongot/standalone/server/RpcDispatcher.java)

Test timeout selection and permit accounting with a fake clock and call listener. Cover normal
completion, an earlier client cancellation, server expiry before dispatch and simultaneous
completion and expiry. Assert one terminal outcome, one permit returned and no dispatch after
expiry. On both listeners, verify a normal slow test handler observes the effective deadline
and the next call succeeds. Include a native RPC as a control on the shared listener.

## Separate cursor expiry from readiness publication

Run `expireIdle()` on its own single maintenance executor. A `GetMore` holds the lease monitor
while fetching a batch. An expiry pass can wait for that monitor when the cursor crosses the
two-minute idle threshold. Keep the existing under-lock recheck of the last access time and
generation before releasing a lease. Keep readiness publication on its own short periodic task.
During shutdown, cancel the periodic tasks, allow in-flight calls to drain, and join the sweeper
before closing its cursor resources. Use a latch around a batch in the regression test and
verify an unrelated deadline and a readiness update both progress while cleanup is waiting.
[Cursor lifecycle](../src/main/java/com/xgen/mongot/standalone/server/StandaloneQueryService.java)

Sample `managementReady()` once per health publication and apply that value to the aggregate
and all four named services on the dedicated listener. Publish the initial value before serving,
publish both failure and recovery, and keep terminal shutdown final. Reuse the named-service
publisher in coexistence, where the native health manager owns the aggregate. Docker's
`mongotctl health` checks the named Operations service. Fixing the server covers that probe and
other clients using the standard gRPC health API.
[Readiness and write admission](../src/main/java/com/xgen/mongot/standalone/runtime/StandaloneRuntime.java) ·
[Docker probe](../deploy/standalone/StandaloneControl.java)

Acceptance requires the aggregate and each dedicated service to report
`SERVING → NOT_SERVING → SERVING` across a reconciliation failure and recovery. At the failed
state, assert the Operations probe used by Docker reports failure and writes return
`UNAVAILABLE`. On the shared listener assert the four extension names follow recovery while
the native aggregate follows its own manager. Check the terminal state after a late publication.

## Delivery and rollback

Land the recovery guard independently. The transport change and cursor scheduling change can
ship together, followed by the health publisher and Docker probe regression. Exercise the
dedicated and shared listeners before building the release image. All three changes use the
existing catalog, journal and wire formats. If a new binary refuses an incomplete volume,
restore its matching stopped backup before restarting. Rolling back the executable on that
volume would reintroduce the deletion path.

## Follow-up work

### Preservation-required updates against 0.9

Introduce a distinct catalog RPC for preservation-required updates. Have it invoke the existing
revision and compatibility checks with `REQUIRE_PRESERVE` enforced by the server. Route the four
SDKs and their CLIs through it for that mode. Treat `UNIMPLEMENTED` as terminal so an older server
rejects the operation before mutation. Roll out server support first. A new client using the new
method against 0.10 will also receive `UNIMPLEMENTED`, although 0.10 understands the existing
field. Test both older versions and confirm the revision and generation stay intact on rejection.
[Current wire contract](../src/proto/standalone/v1/standalone.proto) · [0.10 behavior](releases/standalone-0.10.0.md)

### Cost of paginated discovery

Measure allocation and catalog-lock time for small pages on local and mixed catalogs, including
principals with access to a single index. If material, filter visible IDs and ownership before
encoding definitions, and store a bounded immutable snapshot with offsets. For continuations,
recheck grants and logical existence using IDs without serializing definitions again. Preserve
snapshot ordering, token replay, expiry, byte limits and failure on lost visibility. Verify a
continuation encodes zero fresh definitions and benchmark reconciliation latency during listing.
[Pagination implementation](../src/main/java/com/xgen/mongot/standalone/server/DirectIndexPages.java) ·
[Discovery contract](standalone-all-index-reads-design.md#discovery-and-pagination)

### Foreign-collection authorization for coupled joins

For deployments requiring collection-level isolation, issue #3 needs an enforceable restriction
on cross-collection coupled joins until mongod can supply a trusted authorization decision for the
caller and the resolved foreign namespace. An interim restriction can allow same-collection joins
by verified database and collection UUID. For full support, bind authorization to the resolved
collection identity and enforce it throughout the caller's cursor lifetime. Test an outer-only
reader, a reader authorized on both collections, and a collection recreated under the same name.
Counts, facets and ordinary hits must enforce the same decision.
[Coupled join contract](coupled-joins.md#known-authorization-caveat)

## Standalone index capacity

The first standalone catalog allowed 64 collections and 64 active indexes and documented both
under “Admission, status, and capacity.” The 0.3 push-mode rewrite removed collections and
carried the index count forward. The recorded rationale goes as far as bounding instance
capacity. Neither change gives a benchmark or resource calculation for choosing **64**.
[Original capacity documentation](https://github.com/beardsandbeers/nomongo-t/blob/d7a41d21f818d6005f2d1b1585305478da978bd9/docs/standalone-mongot.md#admission-status-and-capacity) ·
[Push-mode rewrite](https://github.com/beardsandbeers/nomongo-t/commit/8bfc7a6e736a2f1c4ffd8f3e56ff6f77a2c58d54)

Standalone now admits new local index definitions without a count limit, including in coexistence
mode. The catalog format already supports larger inventories. Size deployments using heap, disk,
open file descriptors and commit/refresh latency at representative partition counts.
[Catalog implementation](../src/main/java/com/xgen/mongot/standalone/catalog/LocalIndexCatalog.java) ·
[Operator guide](standalone-mongot.md)
