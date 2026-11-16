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

A compacted commit writes these properties into the new snapshot summary:

| Property | Value |
| --- | --- |
| `source-file:{source-uri}` | `1` for each raw object in the batch |
| `source-batch:{digest}` | Lowercase SHA-256 of the sorted, deduplicated, NUL-separated source URIs |
| `archive-batch:{digest}` | Fingerprint of the sorted expected compacted output paths |

The source list is sorted and deduplicated before the commit, and the batch digest is never computed over an empty set. A singleton batch is valid and produces a digest over its one URI. A digest collision between two different source sets is treated as a conflict, never as already applied, because the output fingerprint would differ. The SHA-256 collision probability is assumed negligible for this claim scheme.

Claims are append-only: no lease expiry, snapshot retention, or writer failure removes a claim. The processed-source set is the union of `source-file:` keys across all snapshots, so an older snapshot cannot hide a claim.

Output keys are deterministic for a given source set and compaction result: they encode the source-batch digest, partition, content hash, and length, never an attempt ID, process ID, or random value. Retrying the same batch therefore targets the same objects. Raw source objects are not deleted by this contract.

## Fenced lease

Extract the lease contract into a shared foundation crate, `platform-lease`, and use it from the archive writers. Move the aggregator onto the same crate so the repository has one fenced lease implementation; archive scopes must remain separate from aggregator shard scopes. The crate must contain the value type, lease store trait, NATS KV implementation, in-memory test implementation, and tests. It must not expose a lease operation that ignores the fencing token or KV revision.

A lease contains `scope` (the resource being serialized), `owner_id` (the process or worker identity), `fencing_token` (a monotonically increasing generation for the scope), and `expires_at` (the absolute expiry time).

The NATS KV key is derived from a SHA-256 digest of the scope, not from a raw S3 prefix, so long or invalid object keys cannot create invalid KV keys. The stored value contains the readable scope, owner, token, and expiry. The initial fencing token is fixed at `1`; any configured initial token must be a positive integer, and a zero or negative value fails startup.

Acquire creates a missing key at token `1` or replaces an expired key at token `previous + 1` using the observed revision. Renew succeeds only for the current owner and token and only before expiry. Release deletes only for the current owner, token, and revision, so a stale worker cannot release its successor's lease. A revision race is retried by re-reading the key, never by overwriting it unconditionally.

Lease scopes include the archive deployment identity (bucket and archive prefix) as well as the table or raw hourly prefix. Deployments that share a lease bucket therefore do not contend for each other's work.

The lease TTL is configurable per archive kind. Each kind also has a soft work deadline that is shorter than the TTL, so a large compaction cannot renew forever and still commit after the deployment has moved on. A worker renews at one third of the TTL. A failed renewal or an expired soft deadline marks the lease as lost: the worker stops adding new work and must not commit. Immediately before the conditional commit, the worker renews and verifies the token again. If the process stalls after that check, the Iceberg CAS still prevents duplicate claims.

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
4. Read row counts and sizes for the remaining objects, then register the whole set through the conditional commit.
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
| `ARCHIVER_LEASE_BUCKET` | required | KV bucket for archiver recovery leases. Must already exist with a history limit of at least `1`. |
| `ARCHIVER_LEASE_TTL_SECS` | required | Lease TTL for archiver recovery. Positive and long enough for a positive renewal interval. |
| `ARCHIVER_MAX_WORK_SECS` | required | Soft upper bound on one recovery scan. Positive and strictly less than the TTL. |

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
- A missing lease bucket or a bucket without a positive history limit fails startup.

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
