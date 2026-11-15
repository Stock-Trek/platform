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
4. A commit based on snapshot `S` is accepted only if the table's `main` reference still points to `S`.
5. A failed or ambiguous commit is resolved by reloading table metadata. A transport error is not treated as either success or failure.
6. If any source object in a batch is already claimed, the batch is not committed. If every source object and every expected output file is already present, the batch is treated as already applied.
7. There is no fallback registration path that appends without the conditional commit.

## Conditional Iceberg commit

All archive appends go through a conditional commit primitive exposed by `platform-archive`, for example:

```rust
enum RegisterOutcome {
    Applied,
    AlreadyApplied,
    Conflict,
}
```

The primitive loads one table state and performs one commit attempt against that state. The caller may retry with a newly loaded state, but the primitive must not silently refresh the table and re-apply an append action.

### Commit algorithm

1. Load the target table and capture its UUID and current `main` snapshot ID.
2. Read the live data-file paths and the source-object claims from that exact table state.
3. Evaluate the request:
   - If `source_files` is present and every source is claimed and every expected output path is live, return `AlreadyApplied`.
   - If `source_files` is present and any source is already claimed, return `Conflict`.
   - If `source_files` is absent and every expected output path is live, return `AlreadyApplied`.
   - If `source_files` is absent and only some expected output paths are live, return `Conflict`; archive appends are atomic, so a partial set means the path set belongs to a different request.
   - Otherwise prepare the append against the loaded table state.
4. Submit exactly one conditional table update containing:
   - `assert-table-uuid` for the loaded table UUID;
   - `assert-ref-snapshot-id` for `main` and the loaded snapshot ID, or the absence of `main` when the table has no snapshot;
   - the new snapshot and its data files.
5. On success, return `Applied`.
6. On a failed requirement, reload the table and repeat step 3. Return `AlreadyApplied` or `Conflict` when the reloaded state proves one of those outcomes.
7. On a transport or unknown outcome, reload the table and apply step 3 before deciding whether to retry. A retry must rebuild the commit from the new snapshot, never replay the old update payload.

The outer retry count must be bounded and use jittered backoff. A conflict with an unrelated batch may be retried after revalidation. A conflict caused by any overlapping source claim must return `Conflict` without retrying that batch.

The generic Iceberg `Transaction::commit` API must not be used for archive appends: it refreshes a stale base table and re-applies the append action before submitting, which defeats the source-claim precondition. The commit primitive must construct the snapshot update and submit the conditional update directly. If the pinned Iceberg client cannot issue a one-shot conditional commit, add that capability by upgrading or patching the client before implementing this contract. Archive tables must also set `commit.retry.num-retries` to `0` so a future use of the generic transaction API cannot reintroduce silent retries.

## Snapshot claims

A compacted commit writes these properties into the new snapshot summary:

| Property | Value |
| --- | --- |
| `source-file:{source-uri}` | `1` for each raw object in the batch |
| `source-batch:{digest}` | Lowercase SHA-256 of the sorted, NUL-separated source URIs |
| `archive-batch:{digest}` | Fingerprint of the expected compacted output paths |

The source list is sorted and deduplicated before the commit. Claims are append-only: no lease expiry, snapshot retention, or writer failure removes a claim. The processed-source set is the union of `source-file:` keys across all snapshots, so an older snapshot cannot hide a claim.

Output keys are deterministic for a given source set and compaction result: they encode the source-batch digest, partition, content hash, and length, never an attempt ID, process ID, or random value. Retrying the same batch therefore targets the same objects. Raw source objects are not deleted by this contract.

## Fenced lease

Extract the lease contract into a shared foundation crate, `platform-lease`, and use it from the archive writers. Move the aggregator onto the same crate so the repository has one fenced lease implementation; archive scopes must remain separate from aggregator shard scopes. The crate must contain the value type, lease store trait, NATS KV implementation, in-memory test implementation, and tests. It must not expose a lease operation that ignores the fencing token or KV revision.

A lease contains:

- `scope`: the resource being serialized;
- `owner_id`: the process or worker identity;
- `fencing_token`: a monotonically increasing generation for the scope;
- `expires_at`: the absolute expiry time.

The NATS KV key is derived from a SHA-256 digest of the scope, not from a raw S3 prefix, so long or invalid object keys cannot create invalid KV keys. The stored value contains the readable scope, owner, token, and expiry.

Acquire, renew, and release use KV revision checks:

- Acquire creates a missing key at token `1`, or replaces an expired key at token `previous + 1` using the observed revision.
- Renew succeeds only for the current owner and token and only before expiry.
- Release deletes only for the current owner, token, and revision, so a stale worker cannot release its successor's lease.
- A revision race is retried by re-reading the key, never by overwriting it unconditionally.

A worker renews at one third of the TTL. A failed renewal marks the lease as lost: the worker stops adding new work and must not commit. Immediately before the conditional commit, the worker renews and verifies the token again. If the process stalls after that check, the Iceberg CAS still prevents duplicate claims.

## Compactor flow

1. `pending_batches()` produces candidates only. It may use a stale view because it is not a correctness boundary.
2. For each candidate, acquire a lease scoped to `compact:{kind}:{sha256(raw-hour-prefix)}`. If the lease is held elsewhere, skip the candidate and continue with the next one.
3. Under the lease, reload the compacted table and its processed-source set. If any source in the candidate is now claimed, release the lease and abandon the whole candidate. The next poll will select a fresh set.
4. Compact, write all output objects, and keep the lease renewed throughout the work.
5. Renew and verify the lease immediately before registration.
6. Register the output files and source list through the conditional commit. Treat `Applied` and `AlreadyApplied` as success. Treat `Conflict` as a skipped batch and do not retry that source set in the same attempt.
7. Release the lease on every success, conflict, error, and cancellation path. Lease expiry is the backstop for a crashed process.

A lease per raw hourly prefix allows different hours and archive kinds to be compacted in parallel while serializing the work that would otherwise collide.

## Archiver flow

### Normal flush

Every raw archive append uses the conditional commit primitive with `source_files = None`. The expected output paths are the Parquet objects written by the flush. A concurrent writer registering the same paths receives `AlreadyApplied` or `Conflict`; it never appends the paths a second time. Normal flushes do not need a lease because path-based CAS is sufficient.

### Startup recovery

1. Before listing recovery objects, acquire a lease scoped to `archive-recovery:{raw-table}`. If another process holds the lease, wait for it to release or expire, then retry the recovery scan.
2. Under the lease, list the configured raw prefix and reload the raw table's live data-file paths.
3. Discard every object whose path is already live and deduplicate the remaining paths.
4. Read row counts and sizes for the remaining objects, then register the whole set through the conditional commit.
5. If the result is `Conflict`, reload the live paths, discard newly registered paths, and retry only the remainder.
6. Release the lease after recovery completes or fails.

Recovery runs before the archiver starts consuming new messages. This ensures an orphaned object cannot remain unrecovered merely because another process held the recovery lease for the entire startup window.

## Configuration

The archive writers fail startup when coordination configuration is missing or invalid. There is no in-process or unconditional-commit fallback.

| Variable | Default | Purpose |
| --- | --- | --- |
| `COMPACTOR_NATS_URL` | required | NATS KV endpoint for compactor leases. |
| `COMPACTOR_LEASE_BUCKET` | required | KV bucket for compactor leases. |
| `COMPACTOR_LEASE_TTL_SECS` | required | Lease TTL. Must be positive. |
| `ARCHIVER_LEASE_BUCKET` | required | KV bucket for archiver recovery leases. |
| `ARCHIVER_LEASE_TTL_SECS` | required | Lease TTL. Must be positive. |

`ARCHIVER_NATS_URL` already supplies the archiver's NATS endpoint. The renewal interval is derived as `TTL / 3`; configuration validation must reject a TTL that cannot produce a positive renewal interval. A shared lease bucket may be used by both services as long as the scope prefixes prevent key collisions. These variables and the services that require them must also be added to `docs/environment-variables.md`.

## Observability

Every lease acquisition, contention, loss, commit conflict, and already-applied outcome is logged with the table, scope, owner, fencing token, and source-batch digest. Metrics cover at least:

- lease acquisitions, contentions, and losses;
- conditional commit outcomes by table and outcome;
- compaction batches skipped because of an overlapping claim;
- recovery objects registered, skipped as already live, and deferred by conflict.

Lease loss and source-claim overlap are warnings, not silent retries.

## Acceptance criteria

### Commit behavior

- A commit built from snapshot `S` fails when `main` no longer points to `S`.
- The failed commit is not retried against the old base, and its append action is not re-applied automatically.
- A commit whose output paths and source claims are all already live returns `AlreadyApplied`.
- A compacted batch with any source already claimed returns `Conflict`, even if other sources in the batch are unclaimed.
- A raw batch with only some output paths live returns `Conflict`.
- A successful compacted commit exposes data files and source claims in one snapshot.
- An ambiguous transport failure is resolved by reloading the table and checking both expected paths and source claims.

### Lease behavior

- Two workers cannot acquire an unexpired lease for the same scope.
- After expiry, exactly one contender acquires the lease and increments the fencing token.
- An expired owner cannot renew.
- A stale owner cannot release its successor's lease.
- A worker that loses its lease aborts before commit.

### Compactor behavior

- Two compactors selecting the same batch produce one `Applied` result and one `Conflict` or skip result.
- A compacted scan contains each source record once.
- A partial source overlap aborts the whole candidate batch, and a later poll compacts only the unclaimed objects.
- A crash after output upload but before commit leaves no source claims; a later run can claim those sources.
- A crash after commit but before lease release leaves no duplicate and does not require manual cleanup.
- Different raw hourly prefixes can be compacted concurrently.

### Archiver behavior

- Concurrent recovery scans register each uncommitted object once.
- Concurrent normal flushes of the same object path append it once.
- Recovery filters paths that become live during a CAS retry and registers only the remainder.
- A stalled recovery owner cannot register after a successor has taken the lease and committed the same paths.

### Tests

- Use an in-memory lease store and a deterministic conditional-commit fake to exercise both interleavings: writer A commits before writer B checks, and both writers check before either commits.
- Use barriers rather than sleeps to force the race windows.
- Cover exact-match retries, partial source overlap, partial path overlap, ambiguous commit outcomes, lease expiry, fencing-token increments, stale release, and renewal failure.
- Add a catalog-level test that the real Iceberg commit request contains `assert-table-uuid` and `assert-ref-snapshot-id`, and that no retry path calls the generic transaction commit.
