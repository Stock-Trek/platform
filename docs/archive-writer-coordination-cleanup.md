# Archive writer coordination cleanup

Status: implementation complete, real-infrastructure verification remaining.
This document records the gaps between the plan in
[archive-writer-coordination.md](../crates/data-plane/archive-writer-coordination.md)
and the current code. It does not replace the plan or re-open completed work.

The coordination primitives, claim-index maintenance, lease authorization,
polling instrumentation, and metadata cleanup are implemented. The remaining
work is evidence against real infrastructure and a small number of fault
injection tests that cannot be covered by in-memory fakes.

## Status at a glance

| Area | Current state | Remaining |
| --- | --- | --- |
| P0-1 source claims | Properties are serialized in the snapshot summary, reloaded from metadata, and validated for version, count, and digest. Fake commit tests cover ambiguous responses and damaged claim metadata. | No real REST-catalog round-trip that applies the serialized request and reloads the claim set. |
| P0-2 lease generations | Release writes a tombstone in both stores. Acquisition advances the token by one. In-memory tests cover release, reacquire, stale writes, and checkpoint handover. | The NATS suite is opt-in and not part of a required gate. |
| P1-1 claim index | Batch-keyed format v4, point lookups by source, output, and archive batch, digest cache, atomic head update, lease-gated scheduled consolidation, live-batch retirement, reachable-metadata sweep, and a configurable safety window. Indexed commits also install finite retention properties in the same conditional update. | The sweep and expired-snapshot retention still need S3-backed tests. |
| P1-2 paged recovery | Paginated listing, bounded registration pages, deadline checks, page metrics, conflict remainder handling, and restart skipping are implemented. | No S3-backed pagination, deadline, restart, or cross-page conflict test. |
| P1-3 indexed polling | Inventory is incremental with a periodic full reconcile. Listing, index-read, cache, poll-duration, fallback-scan, and backlog metrics are emitted. The inventory-gap path is covered by a deterministic listing fake. | None. |
| P1-4 lease-gated retries | Every file commit and property update authorizes before submit and after backoff. Consolidation requires an authorized commit context. | Property-contention and successor-during-backoff tests still rely on fakes. |
| P1-5 operational hardening | Clock-skew margins, conditional property updates, bucket history and size limits, startup validation, and readiness-after-validation are implemented. The compactor removes unreachable metadata behind a safety window, and an unreferenced index page is proven to survive until that window elapses. | No real end-to-end suite. |
| P2 metadata hygiene | Test-only lease helpers are gated, the archive-batch key is queried through the batch index during recovery, consolidation has a production caller, and the batch-index lookup helpers are part of the production API. | None. |

## Remaining evidence

### P0-1: real REST-catalog round-trip

Add an integration test that commits a source-bearing compacted batch through a
real REST catalog, reloads the table, and asserts that the live data files and
processed-source set match the registration. The same test should drop the
successful commit response and prove the retry resolves as `AlreadyApplied`.
Keep the existing damaged-claim metadata tests: a missing `source-file:`, a
wrong `source-claim-count`, or a wrong `archive-claim-version` must fail loading
rather than appear unclaimed.

### P0-2: NATS handover in the rollout gate

Run the NATS lease suite in CI or a required integration environment. It must
cover acquire, release, acquire with exactly `previous + 1`, and a checkpoint
handover where writer A saves, releases, and writer B acquires a greater token
and saves successfully. Keep the stale-release and expired-takeover cases
alongside it.

### P1-1: S3-backed retention and sweep

Exercise the maintenance path against S3 listing behavior with more than one
continuation token. Prove that a live compacted output still blocks
recompaction after old snapshots expire, that retained batches keep their
source/output claims, and that old pages are deleted only after the safety
window. The implementation uses `COMPACTOR_MAINTENANCE_SAFETY_SECS`, which
defaults to 24 hours and is validated as positive.

### P1-2: S3-backed recovery pages

Exercise `recover_table_pages` against a real or emulated S3 listing with
multiple continuation tokens. Inject a conflict into one page and prove later
pages register only the remainder. Force the soft deadline between pages and
prove no further commit occurs and the lease is released. Restart after the
deadline and prove already-live paths are skipped and the remainder completes.

### P1-3: inventory gap

Covered by `an_inventory_gap_is_reconciled_without_skipping_or_duplicating_a_source`:
the fake listing omits a new object from incremental polls, the periodic
reconciliation discovers it, and the completed source is not compacted again.

### P1-5: end-to-end and ambiguous orphan cleanup

Add a suite using a real REST catalog, S3-compatible storage, and NATS that
covers archiver flush, compactor claim, reload, ambiguous response, graceful
handover, and recovery. The in-memory cleanup path is covered by
`an_unreferenced_index_page_waits_for_the_safety_window`; real manifest cleanup
still needs the end-to-end environment.

## Suggested order

1. Close P0-1 and P0-2 with real infrastructure tests so the correctness
   boundary is proven.
2. Add the P1-1 and P1-2 S3-backed evidence.
3. Add the P1-3 inventory-gap test and the P1-5 end-to-end and orphan-cleanup
   tests.
4. Keep the existing rollout gate: no production compaction until P0 and the
   P1 scale, fault-injection, and orphan-cleanup tests pass.
