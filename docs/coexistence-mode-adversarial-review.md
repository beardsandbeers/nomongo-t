# Coexistence mode review and shared ingestion findings

Historical design/review notes. The [operating guide](coexistence-mode.md) supersedes the
explicit mode/enablement settings and generation-serial API scheduling described below.

Status: Pre-implementation review of the [design](coexistence-mode-design.md), incorporating the
confirmed requirements and code investigation. The findings below describe the baseline and the
required adaptations. The [operating guide](coexistence-mode.md) records the implemented contract
and validation.

## Requirements settled

- Use the latest commit on `mongodb/mongot` main as the compatibility reference. Upstream determines
  the mongod support contract; concrete binaries are regression fixtures, not another user decision.
  The upstream head verified here is
  [`70116297003f2423f9dfcf3eeb984c6f8d70c82b`](https://github.com/mongodb/mongot/commit/70116297003f2423f9dfcf3eeb984c6f8d70c82b).
- Share the native gRPC listener by default, adding standalone service namespaces. Preserve native
  methods, framing, feature support, authentication settings, configuration, and replies.
- Mount an existing replicated mongot directory and continue its indexes/checkpoints. No manual
  migration, forced rebuild, backup scripts, or new-directory requirement is acceptable.
- Replicated indexes and manually maintained indexes remain separate. Use each type's normal
  deletion path; no user-facing stop-replication, detach, or ownership conversion is needed.
- One mongot instance is expected. There is no mixed-mongot writer/deployment coordination problem.
- Both producers should use the same indexing scheduler/queue and document-application path.
- Keep upstream changes easy to merge through a small additive extension surface with native
  defaults preserved. Fork adapters must not become copies of upstream scheduling/indexing logic.

The previous replicated public CRUD, revision-offset, replication-admin, native feature-allowlist,
backup-automation, and mode-conversion proposals are not part of the design. Added Catalog/Document/
Query services resolve only local indexes; native commands retain replicated index management.

## Shared ingestion is feasible at an existing boundary

Both initial scans and change-stream decoding reach
[IndexingWorkScheduler.schedule](../src/main/java/com/xgen/mongot/replication/mongodb/common/IndexingWorkScheduler.java)
with a `List<DocumentEvent>` and a generation-bound `DocumentIndexer`. API BSON can be validated
and converted into the same update/delete events before entering that scheduler. Its completion
future is suitable for asynchronous gRPC completion once source-specific semantics are preserved.

Before implementation, the scheduler/queue, default indexer, replication manager, native method
descriptors/interceptor, and journal files inspected matched the verified upstream commit. The
integration point therefore exists upstream as well; it is not an artifact of the standalone fork.

Do not use `InitialSyncQueue` or fake change-stream events for API input. Those layers require
MongoDB sessions, collection metadata, lifecycle events, and checkpoint state. Share the normalized
indexing stage and its resources while retaining the existing source adapters.

### The changes required for correct sharing

1. **Scheduler ownership:** the replication manager currently creates and destroys the scheduler
   factory. Move shared ownership to the runtime so a source restart cannot close the API queue.
   Cancel/drain by owned generations; stop the executor only after all producer work has drained.
2. **Ordering:** the default scheduler applies batch documents concurrently, whereas API requests
   allow repeated IDs and promise ordered partial-prefix behavior. Add ordered API execution to
   the same scheduler and leave native batching parallel. Do not silently deduplicate API input.
3. **Commit/error policies:** `DefaultDocumentIndexer` owns replication metadata and process-fatal
   IO handling. Reuse common event application with source policies; preserve API write errors and
   empty commit metadata instead of accidentally adopting MongoDB checkpoint semantics.
4. **Completion barriers:** refresh/commit for API write concerns must settle before releasing the
   generation lane or writer lease. Today's scheduler finalizes its queue entry before completing
   the future, so an external completion callback alone cannot provide that ordering. Background
   API commits need the same lane and must be coalesced.
5. **Cancellation:** queue cancellation currently operates on a generation, not an individual
   request. API deadlines must cancel/skip only their work, settle the failed queue head, and let
   subsequent requests proceed. Retirement waits for actual worker completion, not an RPC future
   that was cancelled while indexing still runs.
6. **Admission:** `SchedulerQueue` is unbounded; the scheduler's two-active-batch limit is not an
   admission limit. `RpcDispatcher.async` currently releases credits after cancelling its future.
   Track response completion separately from work drain and retain credits for queued/active API
   payloads until they are really released.
7. **Priority/metrics:** reuse the existing steady-state priority and track API origin separately.
   This avoids changes to the priority enum/comparator while preserving FIFO and native priority
   ordering. Keep native replication meters source-specific and add API/combined metrics through
   a narrow hook instead of silently inflating replication activity.

These are meaningful adaptations to an existing common queue, not reasons to build a parallel
standalone indexing pipeline. The full contract and acceptance cases are in the main design.

## Other findings that still apply

### Service registration must remain additive

Standalone bearer authentication and request limits must be scoped to added methods, never to
all native calls. Keep native transport limits and global engine configuration. A shared listener
has one TLS policy and one health service; preserve native empty-service health and add named
standalone health. A dedicated listener is retained for pure standalone launch; coexistence currently uses only the
shared native listener and its transport policy.

### Shared reconciliation must not wait on slow retirement

[DefaultConfigManager.update](../src/main/java/com/xgen/mongot/config/manager/DefaultConfigManager.java)
holds a monitor while [IndexActions](../src/main/java/com/xgen/mongot/config/manager/IndexActions.java)
waits for drop futures. Physical cleanup currently proceeds even after a backend drop future fails.
Use journaled logical retirement with asynchronous cleanup, and retain a fenced generation when
producer/lease drain is uncertain. A remote drop must not stall local catalog work or close a writer
still used by the shared queue.

### Existing directories need additive ownership recovery

Read existing upstream journals and commit metadata as-is. Automatically adopt recognized legacy
replicated entries as remote-owned before registering either producer. Add durable local ownership
metadata before local generation creation; retain it through staged, quarantined, and retiring state.
After activation, missing ownership metadata must not fall back to guessing. Test interrupted first
activation and mixed recovery without changing existing native IDs or forcing reingestion.

Use one combined desired-state coordinator and filtered owner views for native metadata publishing,
collection checks, optime/count helpers, readiness, and query lookup. Missing MongoDB connectivity
must not become an empty desired catalog. New local indexes must not affect native readiness or
appear in native catalog replies. Origin-scope cursor continuation/cancellation internally without
changing the IDs or protocol expected by mongod.

## Upstream maintenance constraint

Use additive factory/schedule overloads with upstream defaults, an API-side `DocumentIndexer`
adapter, and composition-time service/catalog facades. Keep standalone protobufs, authentication,
and write-concern interpretation outside shared replication code. Preserve the native scheduling
loop, decoding pipeline, commit policy, codecs, and journal representation wherever possible.

Deferred retirement remains the highest merge-risk part: it changes an existing synchronization
boundary, so it needs a small explicit interface, a separate focused change, and native regression
coverage. Do not hide that change inside the queue adapter or copy the config manager to avoid a
textual conflict. Record and recheck the extension surface on every upstream update, including
behavioral tests after a clean merge. This reduces maintenance cost without claiming future main
changes can never conflict.

## Validation is implementation work

Compare the candidate with the latest pinned upstream main behavior, reusing upstream-compatible
test configurations and native command coverage. No separately chosen mongod version floor is
needed. Preserve native freshness behavior and checkpoint recovery while exercising both producers.

The key new tests are shared scheduler identity; source restart with queued API work; ordered
repeated-ID batches; all write concerns; cancelled RPCs retaining admission until work drains;
failed work followed by successful queued work; safe drop during indexing; and reuse of an unchanged
existing replicated data directory. Verify native priority/metrics semantics when API input is absent
and bounded mixed behavior when it is present. No automated backup or conversion workflow is required.
