# Archive writer coordination cleanup

Status: investigation complete, implementation partially complete.
This document is the remaining-work list for the plan in
[archive-writer-coordination.md](../crates/data-plane/archive-writer-coordination.md).
It records the gaps between the plan and the current code at commit `fbc15ec`.
It does not replace the plan or re-open completed work.

The coordination primitives are largely present, but the plan is not
production-ready. P0 work still lacks real-infrastructure proof, and P1 work
still lacks index maintenance, fallback metering, complete commit
authorization, and orphan cleanup.

## Status at a glance

| Area | Current state | Remaining |
| --- | --- | --- |
| P0-1 source claims | Properties are serialized in the snapshot summary and read back by metadata loading. Fake commit tests cover ambiguous responses. | No real REST-catalog round-trip that applies the serialized request and reloads the claim set. |
| P0-2 lease generations | Release writes a tombstone in both stores. Acquisition adds one to the previous token. | Real NATS test is opt-in. No explicit release, reacquire, then aggregator checkpoint with the successor token. |
| P1-1 claim index | Immutable pages and blocks, source/output point lookups, digest cache, atomic head update, finite retention after the head exists. | No archive-batch key, no scheduled consolidation, no claim retirement, no reachability cleanup, no old-page sweep. |
| P1-2 paged recovery | Paginated listing, bounded registration pages, deadline checks, page metrics. | No S3-backed pagination, deadline, restart, or cross-page conflict test. |
| P1-3 indexed polling | Commit evaluation uses source/output point queries. Inventory is incremental with a periodic full reconcile. | Missing listing, index-read, cache, poll-duration, fallback-scan, and backlog metrics. No inventory-gap test. |
| P1-4 lease-gated retries | Conditional file commits authorize before every submit and after backoff. | Table property updates still commit outside the lease authorizer. |
| P1-5 operational hardening | Clock-skew margin, conditional property updates, bucket history and max-age validation. | No max-message/max-byte validation, no validation-backed readiness, no real end-to-end suite, no orphan cleanup. |
| P2 metadata hygiene | Unpartitioned tables, schema/version checks, and configuration documentation are enforced. | Test-only lease helpers and unused coordination surface remain. |

## P0-1: prove source claims against the real catalog

The claim builder, snapshot serialization, and compacted-metadata reader are
implemented. The missing closure evidence is a real catalog round-trip:

- commit a source-bearing compacted batch through the REST catalog;
- reload the table from the catalog;
- assert the live data files and processed-source set match the original
  registration;
- drop or lose the successful commit response and assert the reload resolves
  the retry as `AlreadyApplied`;
- assert a damaged claim block (missing `source-file:`, wrong
  `source-claim-count`, or wrong `archive-claim-version`) fails metadata
  loading rather than appearing unclaimed.

The current catalog test is a hand-written TCP stub and the lost-response test
uses `FakeTarget`. Both are useful unit tests but do not close P0-1.

## P0-2: prove graceful handover

The in-memory store and the NATS store implement release tombstones and
`previous + 1` acquisition. The NATS test is skipped unless
`PLATFORM_TEST_NATS_URL` is set, so it is not a release gate today.

Remaining:

- run the NATS lease suite in CI or a required integration environment;
- add an explicit acquire, release, acquire test against NATS and assert the
  token is exactly one greater;
- add an aggregator checkpoint test where writer A checkpoints, releases, and
  writer B acquires with a greater token and successfully saves a checkpoint;
- keep the stale-release and expired-takeover tests alongside that handover
  test.

## P1-1: finish claim-index maintenance

Present: immutable pages and blocks, source/output point lookups, digest-based
cache, head properties written with the snapshot, and finite retention once
`archive.index-head` exists.

Remaining:

1. **Archive-batch lookup.** `archive_batch` is computed from a page's output
   paths in `claim_index_page.rs`, but pages are never queried by it. In
   consolidated pages the output list is empty, so every consolidated page gets
   the fingerprint of an empty set and the per-commit batch identity is lost.
   Either add commit/batch grouping with an archive-batch lookup key, or remove
   the claim that this index supports batch-digest queries. This requires an
   index format version change.
2. **Scheduled consolidation.** `consolidate_claim_index` in
   `table_contract.rs` has no caller. The only compaction is inline when the
   chain reaches depth 64 in `claim_index.rs`, which loads the complete claim
   state during a writer commit. Move consolidation to a maintenance runner or
   scheduled job, authorize it against a lease, and swap the head with one
   conditional metadata-only update.
3. **Claim retirement.** Claims are append-only. There is no batch-to-output
   grouping, no rule that retires an entry only after every output for that
   batch is absent from the live table, and no safety window before old pages
   become unreachable. This needs a maintenance sweep, not a writer-path rule.
4. **Reachability cleanup.** Index pages and blocks written before a failed
   commit, and pages left behind by consolidation, are never removed. Add a
   sweep from the current head that deletes only unreachable pages older than
   the safety window. Never use age alone.

Closure tests: expire old snapshots and prove a live compacted output still
blocks recompaction of its sources; consolidate a long chain and prove the
source set is unchanged and the head advances atomically; retain old pages
until the safety window elapses; query by source, output, and batch digest
without loading unrelated blocks.

## P1-2: add S3-backed recovery evidence

The paged recovery path is implemented: each listing page gets a lease guard,
registration pages honor file and byte limits, conflicts retain only the
remaining files, and page metrics are emitted.

Remaining is verification, not redesign:

- exercise `recover_table_pages` against a real or emulated S3 listing with more
  than one continuation token;
- inject a conflict in one page and prove later pages still register only the
  remainder;
- force the soft deadline between pages and prove no further commit occurs and
  the lease is released;
- restart after the deadline abort and prove already-live paths are skipped and
  the remainder completes.

## P1-3: meter polling and repair paths

Present: conditional evaluation uses source/output point queries when the claim
head exists, and the compactor inventory advances by listing watermark with a
periodic full reconcile.

Remaining:

- record listing pages and objects examined per poll;
- record index pages and blocks read, cache hits, and cache misses;
- record poll duration and inventory backlog age or growth;
- count every fallback to a full table scan when no claim head exists, and log or
  alert on it;
- add an inventory-gap test: create an object, omit it from an incremental
  listing, force reconciliation, and prove it is neither skipped nor compacted
  twice;
- add an alert signal for a candidate backlog that grows faster than the poll
  interval.

## P1-4: authorize property commits too

`ConditionalCommit` authorizes before each submit and after backoff, and the
recovery path supplies a lease authorizer. The gap is the property-update path:
`ensure_required_properties` in `table_properties.rs` submits a conditional
`set-properties` request with no `CommitContext` and no authorizer. It is called
from `ensure_table`, which is used while loading state before every file commit.
A worker that has lost its lease can therefore still mutate table properties.

Remaining:

- pass the lease context and authorizer through property convergence, or move
  property convergence to a startup or maintenance path that holds a lease;
- authorize before every property submit and after every backoff;
- require an authorized `CommitContext` for `consolidate_claim_index` as well;
- add a fake-target test that a successor takes the lease during backoff and no
  property or snapshot request is submitted by the old owner.

The current authorizer test counts callbacks but does not exercise an actual
lease change.

## P1-5: harden startup, integration, and cleanup

Present: clock-skew margins are configurable and validated, property updates
assert table UUID and the current `main` ref, and lease buckets are rejected for
zero history or non-zero key age.

Remaining:

1. **Bucket eviction policy.** `NatsLeaseStore` checks history and max age but
   not `max_messages`, `max_messages_per_subject`, or `max_bytes`. Any of these
   can evict lease tombstones and reset a fencing generation. Expose those
   limits through `platform_nats::KeyValue` and reject unsafe buckets at startup.
2. **Readiness.** Both service entry points call `health.mark_ready()`
   immediately after constructing the lease store. The validation result is not
   exposed. Mark ready only after the lease bucket and table contract have been
   validated, and keep a clear failure reason for readiness probes.
3. **End-to-end suite.** There is no real REST catalog, S3-compatible storage,
   or NATS suite covering archiver flush, compactor claim, reload, ambiguous
   response, graceful handover, and recovery. Add one and run it in the rollout
   gate.
4. **Orphan cleanup.** Failed commits can leave manifests, manifest lists, index
   pages, and index blocks. There is no cleanup component. Implement a sweep
   that reloads the table, proves the commit was not applied, and only then
   deletes unreferenced metadata. Ambiguous outcomes must be left to an
   age-based safety window, never deleted immediately.
5. **Missing tests.** Add property-update contention, bucket count/size
   rejection, real lost-response resolution, and orphan-cleanup tests.

## P2: remove dead and test-only surface

The partition, schema, format-version, and documentation requirements are
implemented. Remaining cleanup:

- `InMemoryLeaseStore::acquire_attempt_count` and `wait_for_acquire_attempt` are
  public production APIs used only by tests; move them behind a test helper or
  `cfg(test)`;
- `IcebergCatalog::consolidate_claim_index` is unused;
- `ClaimIndexPage::archive_batch` is written and validated but never read by a
  lookup path;
- remove or integrate these APIs before declaring the coordination surface
  closed.

## Suggested order

1. Close P0-1 and P0-2 with real infrastructure tests so the correctness
   boundary is proven.
2. Close P1-4 and the P1-5 bucket and readiness gaps so no mutation can outlive
   its lease.
3. Build P1-1 maintenance and P1-3 instrumentation before enabling compaction at
   scale.
4. Add the P1-2 S3-backed tests and the P2 cleanup.
5. Keep the existing rollout gate: no production compaction until P0 and the
   P1 scale, fault-injection, and orphan-cleanup tests pass.
