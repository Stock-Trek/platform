# Archive writer coordination

## Scope

This document defines the writer-coordination contract for every process that appends to archive Iceberg tables:

- the compactor, when claiming raw source objects and adding compacted files;
- the archiver, when registering flushed raw files;
- the archiver's startup recovery, when registering objects left uncommitted by an earlier process.

The contract has two independent layers:

- A fenced NATS KV lease prevents concurrent workers from doing the same work.
- An Iceberg compare-and-swap (CAS) commit on the table snapshot is the correctness boundary that prevents duplicate claims and duplicate data.

The lease is never sufficient on its own. A process can stall past lease expiry and resume, so every file registration must still pass the conditional commit.

## Invariants

1. A raw source object is claimed by at most one compacted snapshot.
2. Compacted data files and their source-object claims become visible in the same Iceberg snapshot. A reader never observes one without the other.
3. A data-file path is present at most once in the live files of an archive table.
4. A commit based on snapshot `S` is accepted only if the table UUID matches and the table's `main` reference still points to `S`.
5. A failed or ambiguous commit is resolved by reloading table metadata. A transport error is neither success nor failure until the reload proves one of those outcomes.
6. If any source object in a batch is already claimed, the batch is not committed. If every source object and every expected output file is already present, the batch is treated as already applied.
7. A present source set is non-empty and duplicate-free. An empty source set is rejected rather than being collapsed into a shared empty-batch digest.
8. There is no fallback registration path that appends without the conditional commit. A worker that loses its lease or passes its soft work deadline aborts before registration, and lease expiry is a crash backstop, never authorization to write.

## Conditional Iceberg commit

All archive appends go through a conditional commit primitive exposed by `platform-archive`:

```rust
struct ConflictDetails {
    claimed_sources: Vec<String>,
    live_outputs: Vec<String>,
}

enum RegisterOutcome {
    Applied,
    AlreadyApplied,
    Conflict(ConflictDetails),
}
```

The primitive loads one table state and performs one commit attempt against that state. The caller may retry with a newly loaded state, but the primitive must not silently refresh the table and re-apply an append action. `ConflictDetails` must name the overlapping source URIs and output paths so callers can log and metric them without a second metadata read.

### Commit algorithm

1. Load the target table and capture its UUID and current `main` snapshot ID.
2. Read the live data-file paths and the source-object claims from that exact table state.
3. Validate the request: reject a present but empty `source_files` list, deduplicate the source list, and normalize the expected output paths.
4. Evaluate the request:
   - If `source_files` is present and every source is claimed and every expected output path is live, return `AlreadyApplied`.
   - If `source_files` is present and any source is already claimed, return `Conflict` with the claimed sources and live output paths in `ConflictDetails`, even when other sources in the batch are unclaimed.
   - If `source_files` is absent and every expected output path is live, return `AlreadyApplied`.
   - If `source_files` is absent and only some expected output paths are live, return `Conflict` with those live paths in `ConflictDetails`. Archive appends are atomic, so a partial set means the path set belongs to a different request or an earlier crashed writer.
   - Otherwise prepare the append against the loaded table state.
5. Submit exactly one conditional table update containing:
   - `assert-table-uuid` for the loaded table UUID;
   - `assert-ref-snapshot-id` for `main` and the loaded snapshot ID, or the absence of `main` when the table has no snapshot;
   - the new snapshot, its data files, and its source claims.
6. On success, return `Applied`.
7. On a failed requirement, reload the table and repeat step 4. Return `AlreadyApplied` or `Conflict` when the reloaded state proves one of those outcomes. Never retry the old update payload.
8. On a transport or unknown outcome, reload the table and apply step 4 before deciding whether to retry:
   - If every expected output path is live and every source claim is present, return `AlreadyApplied`. A successful CAS whose response was lost is indistinguishable from a transport failure at the client, so the reloaded table is the only source of truth.
   - If any source claim is present or only some expected output paths are live, return `Conflict` with the details.
   - If no source claim and no expected output path is live, a bounded retry with jittered backoff may rebuild the commit from the newly loaded snapshot.
   - A transport error is never reported as a permanent failure without this revalidation.

The outer retry count must be bounded and use jittered backoff. A conflict with an unrelated batch may be retried after revalidation. A conflict caused by any overlapping source claim must return `Conflict` without retrying that batch.

The generic Iceberg `Transaction::commit` API must not be used for archive appends: it refreshes a stale base table and re-applies the append action before submitting, which defeats the source-claim precondition. The commit primitive must construct the snapshot update and submit the conditional update directly. If the pinned Iceberg client cannot issue a one-shot conditional commit, add that capability by upgrading or patching the client before implementing this contract. Archive tables must also set `commit.retry.num-retries` to `0` so a future use of the generic transaction API cannot reintroduce silent retries.

The conditional primitive must be the only public mutation API for archive tables. A repository check (a disallowed-method lint or an equivalent CI grep gate) must fail if `Transaction::fast_append`, `Transaction::commit`, or any other append helper is used outside `platform-archive`.

## Snapshot claims

A compacted commit serializes these properties into the new snapshot summary:

| Property | Value |
| --- | --- |
| `source-file:{source-uri}` | `1` for each raw object in the batch |
| `source-claim-count` | Decimal count of `source-file:` properties in this snapshot |
| `source-batch:{digest}` | Lowercase SHA-256 of the sorted, deduplicated, NUL-separated source URIs |
| `archive-batch:{digest}` | Fingerprint of the sorted expected compacted output paths |
| `archive-claim-version` | Claim encoding version, `1` |

The commit assembles the snapshot metrics and these archive properties into one map before constructing `Summary.additional_properties`. `SnapshotSummaryCollector::build()` only returns metrics; the archive properties must be merged into its result explicitly.

The source list is sorted and deduplicated before the commit, and the batch digest is never computed over an empty set. A compacted snapshot whose `source-claim-count` does not equal its number of `source-file:` properties is corrupt and fails the commit evaluation rather than being treated as unclaimed. A singleton batch is valid and produces a digest over its one URI. A digest collision between two different source sets is treated as a conflict, never as already applied, because the output fingerprint would differ. The SHA-256 collision probability is assumed negligible for this claim scheme.

Claims are append-only for the lifetime of their compacted outputs: no lease expiry, writer failure, or snapshot retention removes a claim while any output from its batch remains live. The processed-source set is the union of claim records reachable from the current table state, so an older snapshot cannot hide a claim. Inline summary keys and indexed claim pages are two encodings of the same rules.

Output keys are deterministic for a given source set and compaction result: they encode the source-batch digest, partition, content hash, and length, never an attempt ID, process ID, or random value. Retrying the same batch therefore targets the same objects. Raw source objects are not deleted by this contract.

## Fenced lease

Extract the lease contract into a shared foundation crate, `platform-lease`, and use it from the archive writers. Move the aggregator onto the same crate so the repository has one fenced lease implementation; archive scopes must remain separate from aggregator shard scopes. The crate must contain the value type, lease store trait, NATS KV implementation, in-memory test implementation, and tests. It must not expose a lease operation that ignores the fencing token or KV revision.

A lease contains `scope` (the resource being serialized), `owner_id` (the process or worker identity), `fencing_token` (a monotonically increasing generation for the scope), and `expires_at` (the absolute expiry time).

The NATS KV key is derived from a SHA-256 digest of the scope, not from a raw S3 prefix, so long or invalid object keys cannot create invalid KV keys. The stored value contains the readable scope, owner, token, expiry, and whether the lease is released. The first acquisition of a scope uses the configured initial token, which defaults to `1`; any configured initial token must be a positive integer, and a zero or negative value fails startup. The initial token is only the first generation, never a per-acquisition reset.

Acquire creates a missing key at the initial token or replaces an expired or released key at token `previous + 1` using the observed revision. Renew succeeds only for the current owner and token and only before expiry. Release writes a released tombstone for the current owner, token, and revision. It must not delete the key or erase the generation, so a stale worker cannot release its successor's lease and a graceful handover still advances the fencing token. A revision race is retried by re-reading the key, never by overwriting it unconditionally. Lease keys and tombstones are never removed by bucket age or size limits.

Lease scopes include the archive deployment identity (bucket and archive prefix) as well as the table or raw hourly prefix. Deployments that share a lease bucket therefore do not contend for each other's work.

The lease TTL is configurable per archive kind. Each kind also has a soft work deadline that is shorter than the TTL, so a large compaction cannot renew forever and still commit after the deployment has moved on. A worker renews at one third of the TTL. A failed renewal or an expired soft deadline marks the lease as lost: the worker stops adding new work and must not commit. Immediately before every conditional commit attempt, including retries, the worker renews and verifies the token again. If the process stalls after that check, the Iceberg CAS still prevents duplicate claims.

## Compactor flow

1. `pending_batches()` produces candidates only. It may use a stale view because it is not a correctness boundary.
2. For each candidate, acquire a lease scoped to `compact:{bucket}:{output-prefix}:{kind}:{sha256(raw-hour-prefix)}`. If the lease is held elsewhere, skip the candidate and continue with the next one. A lease per raw hourly prefix allows different hours and archive kinds to be compacted in parallel while serializing the work that would otherwise collide.
3. Under the lease, reload the compacted table and its processed-source set. If any source in the candidate is now claimed, release the lease and abandon the whole candidate. The next poll will select a fresh set.
4. Compact, write all output objects, and keep the lease renewed throughout the work. Renew and verify the lease immediately before registration; abort before registration if the soft work deadline expires or renewal fails.
5. Register the output files and source list through the conditional commit. Treat `Applied` and `AlreadyApplied` as success. Treat `Conflict` as a skipped batch, log the overlapping sources and paths, and do not retry that source set in the same attempt.
6. Release the lease on every success, conflict, error, and cancellation path. Lease expiry is the backstop for a crashed process.

## Archiver flow

**Normal flush.** Every raw archive append uses the conditional commit primitive with `source_files = None`. The expected output paths are the Parquet objects written by the flush. A concurrent writer registering the same paths receives `AlreadyApplied` or `Conflict`; it never appends the paths a second time. Normal flushes do not need a lease because path-based CAS is sufficient.

**Startup recovery.**

1. Before listing recovery objects, acquire a lease scoped to `archive-recovery:{bucket}:{raw-prefix}:{raw-table}`. If another process holds the lease, wait for it to release or expire, then retry the recovery scan.
2. Under the lease, list the configured raw prefix and reload the raw table's live data-file paths.
3. Discard every object whose path is already live and deduplicate the remaining paths.
4. Read row counts and sizes for the remaining objects, then register them page by page through the conditional commit.
5. If the result is `Conflict`, reload the live paths, discard newly registered paths, log the overlapping paths, and retry only the remainder. Abort before registration if the recovery soft deadline expires or lease renewal fails, then release the lease after recovery completes or fails.

Recovery runs before the archiver starts consuming new messages. This ensures an orphaned object cannot remain unrecovered merely because another process held the recovery lease for the entire startup window.

## Configuration

The archive writers fail startup when coordination configuration is missing or invalid. There is no in-process or unconditional-commit fallback.

| Variable | Default | Purpose |
| --- | --- | --- |
| `COMPACTOR_NATS_URL` | required | NATS KV endpoint for compactor leases. |
| `COMPACTOR_LEASE_BUCKET` | required | KV bucket for compactor leases. Must already exist with a history limit of at least `1`. |
| `COMPACTOR_LEASE_TTL_SECS` | required | Default lease TTL for compaction. Positive and long enough for a positive renewal interval. Per-kind override: `COMPACTOR_LEASE_TTL_SECS_{KIND}`. |
| `COMPACTOR_MAX_WORK_SECS` | required | Soft upper bound on one compaction attempt. Positive and strictly less than the effective TTL. |
| `COMPACTOR_MAINTENANCE_SAFETY_SECS` | `86400` | Minimum age before unreachable Iceberg manifests, claim-index pages, and claim-index blocks are deleted. Positive. |
| `COMPACTOR_LEASE_INITIAL_TOKEN` | `1` | First fencing generation for a new compactor scope. Positive. Later generations always use `previous + 1`. |
| `COMPACTOR_LEASE_CLOCK_SKEW_SECS` | `5` | Safety margin added to lease expiry to tolerate wall-clock skew. Non-negative and compatible with the TTL and renewal interval. |
| `ARCHIVER_LEASE_BUCKET` | required | KV bucket for archiver recovery leases. Must already exist with a history limit of at least `1` and no key-age expiry. |
| `ARCHIVER_LEASE_TTL_SECS` | required | Lease TTL for archiver recovery. Positive and long enough for a positive renewal interval. |
| `ARCHIVER_MAX_WORK_SECS` | required | Soft upper bound on one recovery scan or page. Positive and strictly less than the TTL. |
| `ARCHIVER_LEASE_INITIAL_TOKEN` | `1` | First fencing generation for a new recovery scope. Positive. Later generations always use `previous + 1`. |
| `ARCHIVER_LEASE_CLOCK_SKEW_SECS` | `5` | Safety margin added to lease expiry to tolerate wall-clock skew. Non-negative and compatible with the TTL and renewal interval. |
| `ARCHIVER_RECOVERY_PAGE_FILES` | required | Maximum candidate files in one recovery registration page. Positive. |
| `ARCHIVER_RECOVERY_PAGE_BYTES` | required | Maximum candidate bytes in one recovery registration page. Positive. |

`ARCHIVER_NATS_URL` already supplies the archiver's NATS endpoint. The renewal interval is derived as `TTL / 3`; configuration validation must reject a TTL that cannot produce a positive renewal interval. A shared lease bucket may be used by both services as long as the scope prefixes prevent key collisions. The lease bucket must already exist with a history limit of at least `1`. Deployment automation may create it, but creation must set a history limit of at least `1` and read the configuration back; the service itself must never auto-create a bucket with unlimited history. A missing or invalid bucket fails startup rather than silently running without fencing. These variables and the services that require them must also be added to `docs/environment-variables.md`.

## Observability

Every lease acquisition, contention, loss, commit conflict, and already-applied outcome is logged with the table, scope, owner, fencing token, and source-batch digest. A `Conflict` log includes the overlapping source URIs and the overlapping output paths from `ConflictDetails`, not just a count. Metrics cover lease acquisitions, contentions, and losses; conditional commit outcomes by table and outcome; compaction batches skipped because of an overlapping claim; and recovery objects registered, skipped as already live, and deferred by conflict.

Lease loss and source-claim overlap are warnings, not silent retries.

## Acceptance criteria

### Commit behavior

- A commit built from snapshot `S` fails when `main` no longer points to `S`.
- A commit whose `assert-table-uuid` does not match the loaded table is rejected by the catalog.
- The failed commit is not retried against the old base, and its append action is not re-applied automatically.
- A commit whose output paths and source claims are all already live returns `AlreadyApplied`.
- A compacted batch with any source already claimed returns `Conflict`, even if other sources in the batch are unclaimed.
- A raw batch with only some output paths live returns `Conflict` and reports those paths.
- A present empty source set is rejected before any commit attempt.
- A successful compacted commit exposes data files and source claims in one snapshot.
- An ambiguous transport failure is resolved by reloading the table and checking both expected paths and source claims; a lost-success response returns `AlreadyApplied` rather than a permanent failure.
- No repository code path outside `platform-archive` constructs an append transaction.

### Lease behavior

- Two workers cannot acquire an unexpired lease for the same scope.
- After expiry, exactly one contender acquires the lease and increments the fencing token.
- An expired owner cannot renew.
- A stale owner cannot release its successor's lease.
- A worker that loses its lease or exceeds its soft work deadline aborts before commit.
- A zero or negative initial fencing token is rejected at startup.
- A graceful release retains the generation, and the next acquisition uses a strictly greater fencing token.
- A missing lease bucket, a bucket without a positive history limit, or a bucket that expires lease keys fails startup.

### Compactor behavior

- Two compactors selecting the same batch produce one `Applied` result and one `Conflict` or skip result.
- A compacted scan contains each source record once.
- A partial source overlap aborts the whole candidate batch, and a later poll compacts only the unclaimed objects.
- A crash after output upload but before commit leaves no source claims; a later run can claim those sources.
- A crash after commit but before lease release leaves no duplicate and does not require manual cleanup.
- Different raw hourly prefixes can be compacted concurrently.
- A compaction that exceeds `COMPACTOR_MAX_WORK_SECS` aborts without registering.

### Archiver behavior

- Concurrent recovery scans register each uncommitted object once.
- Concurrent normal flushes of the same object path append it once.
- Recovery filters paths that become live during a CAS retry and registers only the remainder.
- A stalled recovery owner cannot register after a successor has taken the lease and committed the same paths.
- A recovery scan that exceeds `ARCHIVER_MAX_WORK_SECS` aborts without registering.

### Tests

- Use an in-memory lease store, a deterministic conditional-commit fake, and barriers rather than sleeps to exercise both interleavings: writer A commits before writer B checks, and both writers check before either commits.
- Cover exact-match retries, partial source overlap, partial path overlap, ambiguous commit outcomes, lease expiry, fencing-token increments, stale release, renewal failure, and soft-deadline expiry.
- Cover source-batch digest construction for empty, singleton, duplicate, and reordered inputs, and verify that a conflict exposes the overlapping sources and paths.
- Add a catalog-level test that the real Iceberg commit request contains `assert-table-uuid` and `assert-ref-snapshot-id`, that a mismatched table UUID is rejected, and that no retry path calls the generic transaction commit.
- Add configuration tests for invalid TTL to renewal-interval ratios, zero or negative initial fencing tokens, and missing or history-less lease buckets.
- Add a serialized-commit test that asserts every compacted registration contains `archive-batch`, `source-batch`, `source-claim-count`, `archive-claim-version`, and one `source-file:` entry per source in the `add-snapshot` summary.
- Add a graceful-release test that acquires, releases, acquires again, and saves an aggregator checkpoint with the successor's fencing token.

## Remediation plan

This plan closes every finding from the production-readiness review. It is ordered by risk. P0 items block deployment; P1 items block production scale and graceful handover; P2 items are required hardening and cleanup. A workstream is complete only when its verification gates pass.

The remaining work after the current implementation is tracked in
[archive-writer-coordination-cleanup.md](../../docs/archive-writer-coordination-cleanup.md).

| Priority | Workstream | Closure condition |
| --- | --- | --- |
| P0 | Serialized source claims | A source-bearing compacted commit round-trips through the real catalog with every claim visible after reload. |
| P0 | Monotonic lease generations | A graceful release is followed by a strictly greater fencing token, and aggregator checkpoints remain writable. |
| P1 | Bounded claim index and retention | Commit and poll cost are bounded by the candidate batch and live metadata, not total history. |
| P1 | Paged recovery | Recovery registers arbitrary backlogs in bounded pages without exceeding a lease window. |
| P1 | Indexed compactor polling | Polling uses point lookups and incremental S3 discovery instead of full table scans. |
| P1 | Lease-gated retries | Every conditional commit attempt is authorized against the current lease immediately before submission. |
| P1 | Operational hardening | Clock skew, property CAS, real-catalog coverage, and orphan cleanup are specified and tested. |
| P2 | Metadata hygiene | Test-only and dead APIs are removed, and the unpartitioned-table assumption is enforced. |

### P0-1: Persist source claims through serialization

**Required end state.** A compacted `add-snapshot` request contains its archive properties in the serialized summary. Reloading the committed table yields exactly the same processed-source set, so an ambiguous success resolves to `AlreadyApplied` and a completed batch is never selected again.

**Implementation.**

1. Assemble the metrics map first, then extend that map with the properties returned by `snapshot_properties`. Do not use `SnapshotSummaryCollector::set` as the property carrier, because `build()` does not include collector properties that were not merged through `merge()`.
2. Keep source validation, sorting, deduplication, digest construction, claim-count construction, and version stamping in one claim-property builder. Both the commit path and the metadata reader use that builder's rules.
3. Change the metadata reader to return an error for a malformed compacted snapshot: a snapshot carrying `source-batch` must carry `archive-claim-version`, a matching `source-claim-count`, and exactly that many `source-file:` properties. A missing claim block is a hard fault, not an unclaimed batch.
4. Add the claim version and claim count to every compacted snapshot so the reader can distinguish an intentionally empty raw snapshot from a damaged compacted snapshot.
5. Keep the UUID and `main`-ref requirements and the snapshot/ref updates in the same conditional request. The archive properties are part of that request, not a second metadata update.

**Verification.**

- Build a source-bearing commit and assert against the serialized request JSON, not an intermediate map. Assert the `add-snapshot` summary contains every `source-file:` key, `source-batch`, `archive-batch`, `source-claim-count`, and `archive-claim-version`.
- Apply that serialized request to an in-memory table state, reload it, and assert `processed_source_files()` equals the original sorted source set.
- Add a REST-catalog round-trip test using the real serialized request. Commit a compacted batch, reload the table from the catalog, and verify both the data files and the processed-source set.
- Add a fault-injection test that drops the successful commit response and verifies the reload returns `AlreadyApplied`, not a repeated compaction.
- Add a regression test that runs the compactor twice for the same raw batch and asserts the second run observes the claims and writes no new output.
- Add a corruption test that removes one `source-file:` property or changes `source-claim-count`; metadata loading must fail loudly.

### P0-2: Preserve fencing generations across graceful release

**Required end state.** The fencing token is monotonic for the lifetime of a scope. The first acquisition uses the configured initial token; every later acquisition, including after a graceful release, uses `previous + 1`. Release never removes the generation.

**Implementation.**

1. Represent release as a tombstone record written with the observed KV revision. Keep the scope, previous owner, previous token, and released state in the record. Do not delete the key.
2. Make both the NATS and in-memory stores treat a released record exactly like an expired record for acquisition: the next owner receives `previous + 1`. `current()` may return the tombstone, while `is_current()` must report false for it.
3. Wake watchers on release so a waiting owner can acquire immediately. A stale owner must still be unable to renew or overwrite the tombstone.
4. Validate the configured initial token as a first-generation floor only. Do not apply it when a tombstone or any earlier generation exists.
5. Configure the lease bucket without key-age or key-count expiry. If a bucket policy can remove tombstones, startup must fail rather than risk resetting a generation.

**Verification.**

- Test acquire, release, acquire: the second token is exactly one greater than the first.
- Test release, acquire, release, acquire over several generations and assert strict monotonicity.
- Test that a stale release cannot remove or roll back the successor's record.
- Test a crash-expiry takeover and a graceful handover with the same assertions.
- Test the aggregator checkpoint path: writer A checkpoints at token `n`, releases gracefully, writer B acquires at a token greater than `n`, and `CheckpointRecord::is_writable_by` accepts writer B's save.
- Run the NATS-backed variant in the integration environment, not only against the in-memory store.

### P1-1: Bound claim metadata and index the processed-source lookup

**Required end state.** The per-commit and per-poll cost depends on the candidate batch and the current live output set, not on the total number of historical claims. Snapshot retention can be finite without losing claims for live compacted outputs.

**Implementation.**

1. Introduce an immutable commit index page written before the conditional commit. Each page records its parent page, the sorted source URIs, the archive-batch digest, and the output paths and fingerprints for that commit. Raw commits write output-only pages; compacted commits write source and output pages.
2. Reference the new page from table properties updated in the same conditional commit as `AddSnapshot` and `SetSnapshotRef`: `archive.index-head`, `archive.index-head-digest`, and `archive.index-format-version`. A commit that succeeds makes its data files and index page visible atomically; a failed commit leaves an unreferenced page that cleanup can ignore.
3. Store the index as a blocked, sorted structure with separate lookup keys for source URI, archive-batch digest, and output path. A point query reads only the relevant blocks. A full enumeration is used only by maintenance, reconciliation, and audit.
4. Cache decoded pages by digest in each process. The cache is keyed by immutable page digest, so a stale table load can never produce a mixed source set.
5. Add a consolidation job that writes one page containing all current claims and resets the parent pointer. The job swaps `archive.index-head` atomically with a conditional metadata-only update. Trigger consolidation by chain length, chain age, or page count.
6. Replace the pinned unlimited snapshot retention with finite, configurable retention once the index head is authoritative. Set finite `history.expire.min-snapshots-to-keep` and `history.expire.max-snapshot-age-ms` values and finite branch retention.
7. Retire an index entry only when every output path for its batch is absent from the live table. Never retire a claim merely because an old snapshot expired. Keep raw source objects untouched.
8. Protect index pages from orphan cleanup while any table state references their chain. Cleanup uses a reachability sweep from the current index head, not an age-only rule.

**Verification.**

- Build a table with many commits and assert that the serialized commit request size is bounded by the candidate batch, not total claims.
- Assert that a processed-source lookup for a candidate batch reads only the relevant index blocks, with no full snapshot or manifest scan.
- Expire old snapshots and verify that a compacted output that remains live still blocks recompaction of its raw sources.
- Consolidate a long chain and verify that the source set is unchanged, the head advances atomically, and old pages become unreachable only after the safety window.
- Add a scale test with a large synthetic claim set that records commit request bytes, lookup latency, and pages read.

### P1-2: Page recovery into bounded commits

**Required end state.** Recovery can register an arbitrarily large uncommitted backlog without one giant commit, without unbounded memory, and without exceeding the recovery lease window.

**Implementation.**

1. Replace the whole-prefix object list with a paginated S3 listing. Process each listing page as a bounded recovery page and keep only the current page's candidate objects in memory.
2. Add `ARCHIVER_RECOVERY_PAGE_FILES` and `ARCHIVER_RECOVERY_PAGE_BYTES`. A page closes on either limit. Validate both as positive at startup.
3. For each page, filter paths that are already live, read row counts and sizes, and submit one conditional registration. Apply conflict handling to the page before moving to the next page.
4. Check `ARCHIVER_MAX_WORK_SECS` before listing, before each page commit, and before each conflict retry. If the deadline is reached, release the lease and fail startup with a retryable error. The next startup resumes from the live-path index.
5. Recovery may release and reacquire the recovery lease between pages. It must not start consuming new messages until a complete pass finds no remaining uncommitted objects.
6. Record page counts, registered files, skipped files, bytes, and remaining candidates in metrics and logs.

**Verification.**

- Recover a backlog larger than one page and assert that each object is registered exactly once across pages.
- Inject a page conflict and assert that only the remainder is registered.
- Force the soft deadline between pages and assert that no further page commits, the lease is released, and startup fails without consuming messages.
- Restart after a deadline abort and assert that already-live paths are skipped and the remainder completes.
- Assert that page memory and request size remain within the configured bounds.

### P1-3: Replace full-table polling with indexed discovery

**Required end state.** A compactor poll does not load every live data file and every historical snapshot. Discovery is incremental, and claim/output checks are point queries against the commit index.

**Implementation.**

1. Add catalog methods to query claimed sources and registered output paths for a candidate set. The default implementation uses the commit index; a manifest scan is a repair-only fallback and is metered.
2. Change conditional-commit evaluation to use those point queries. `Registration` is applied, already applied, or conflicted without materializing the complete live-path and processed-source sets.
3. Change `pending_batches` to use a cached S3 inventory updated by listing watermark or continuation token. Periodically run a full reconciliation pass to catch missed notifications or overwritten objects.
4. Use exact batch digests and deterministic output paths so `AlreadyApplied` is proven from the index rather than from a table-wide data-file scan.
5. Instrument listing pages, index blocks read, cache hits, and poll duration. Alert when a poll falls back to a full scan or when the candidate backlog grows faster than the poll interval.

**Verification.**

- Build a table with many historical snapshots and files, then assert a poll reads only new listings and index blocks for its candidates.
- Assert that a full scan is not executed on the normal path.
- Cover an inventory gap followed by reconciliation, and verify that no source is skipped or compacted twice.
- Record latency and bytes read for a large synthetic table and compare against the bounded-cost target.

### P1-4: Authorize every commit attempt against the current lease

**Required end state.** A worker cannot submit a conditional commit after losing its lease or soft deadline, including during a retry that started after the initial lease check.

**Implementation.**

1. Give the conditional-commit primitive an authorization callback or guard that is invoked immediately before every `submit`, and again after every reload before a retry.
2. Authorize after the reload and after any backoff, not only at the start of registration. A lease renewal failure, token change, or deadline expiry returns an error before the HTTP commit request is sent.
3. Cap backoff to the remaining lease time and soft deadline. Do not sleep past the point where commit is no longer authorized.
4. Use a no-op authorizer only for operations that have no lease, such as normal raw flushes. Compaction and recovery always supply the lease guard.
5. Keep the Iceberg CAS as the final safety boundary; authorization reduces wasted work and enforces the contract but does not replace the source-claim conflict check.

**Verification.**

- Expire a lease between the first evaluation and the first retry and assert that no commit is submitted.
- Let a successor take the lease during backoff and assert that the old worker aborts while the successor commits.
- Fail a renewal inside the retry loop and assert that the conditional request is not sent.
- Verify with a fake target that authorization is called before every submit attempt.

### P1-5: Harden operational assumptions

**Required end state.** Startup fails closed for unsafe coordination configuration, and the real-catalog path is covered by integration tests.

**Implementation.**

1. Add a configurable clock-skew margin to lease expiry. Treat a lease as expired only after `expires_at + skew_margin`, and require NTP synchronization. Validate that the TTL can accommodate the renewal interval plus the margin.
2. Deploy the lease bucket with a positive history limit and no age-based key expiry. Verify these properties at startup and expose the result through health and readiness checks.
3. Update required table properties through a conditional request containing both `assert-table-uuid` and the current `main` ref requirement, with bounded jittered retries. Fail startup if the properties cannot be converged, rather than issuing a UUID-only update.
4. Add an end-to-end suite with a real REST catalog, S3-compatible storage, and NATS. Cover archiver flush, compactor claim, reload, ambiguous commit response, graceful lease handover, and recovery.
5. Clean up manifest and manifest-list objects only after a reload proves the commit was not applied. Never delete objects after an ambiguous outcome; leave them to age-based orphan cleanup.
6. Add tests for clock skew, bucket policy rejection, property-update contention, lost responses, and orphan cleanup.

### P2: Enforce constraints and remove dead surface

**Required end state.** The code and schema assumptions are explicit, and no test-only or unused production API remains.

**Implementation.**

1. Remove `batch_registered_in_metadata` or make it part of the production batch-evaluation path. Remove any other API that is only exercised by tests.
2. Enforce that archive tables use an unpartitioned default spec at table creation and startup. If partitioned archive tables are required later, build data files from the actual partition spec and add round-trip tests before enabling them.
3. Add a startup check that the archive table schema and format version match the supported claim/index version.
4. Document every new setting in `docs/environment-variables.md` and validate all coordination settings together at startup.

**Verification.**

- Startup rejects a partitioned archive table and an unsupported claim/index version.
- The repository has no test-only production API and no unused coordination method.
- Configuration tests cover every new positive, zero, negative, and cross-field invalid case.

### Rollout gates

1. Land P0-1 and P0-2 together with their serialized-commit, real-catalog, and graceful-handover tests.
2. Do not enable compaction in production until P1-1, P1-2, P1-3, and P1-4 pass their scale and fault-injection tests.
3. Run the end-to-end suite against the deployment's REST catalog, S3-compatible storage, and NATS configuration, not only local fakes.
4. Require `cargo fmt --all -- --check`, `cargo clippy` with warnings denied, and the archive, lease, archiver, compactor, and aggregator test suites before each rollout.
5. Monitor repeated compaction of the same sources, index fallback scans, lease token regressions, and recovery pages that cannot finish within the lease window. These are release-blocking signals, not silent retries.
