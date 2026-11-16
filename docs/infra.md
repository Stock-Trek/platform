# Infrastructure runbook

Use this document to provision, validate, tear down, and recover the platform. Full variable
definitions are in [environment-variables.md](environment-variables.md), probes in
[health-checks.md](health-checks.md), and archive coordination in
[archive-writer-coordination.md](../crates/data-plane/archive-writer-coordination.md).

## Service and dependency map

| Area | Services | Required infrastructure |
| --- | --- | --- |
| Market data | Ingester, Backfiller, Aggregator, Archiver, Compactor, Rebuilder | NATS JetStream and KV, S3-compatible storage, Iceberg REST catalog and warehouse |
| Portfolio | Portfolio service, Portfolio registry, Exchange gateway | NATS JetStream and KV, DynamoDB, Valkey, Vault, SQS, exchange REST/WebSocket endpoints |
| Auth | Auth service, Auth relay | Valkey, Vault, SQS, shared Ed25519 key and JWKS endpoint |
| Build | Build orchestrator, Git webhook, Build agent | Lambda, S3, SQS, GitHub App credentials, SSM Parameter Store |
| Control | Run invoker, Run store | Lambda, DynamoDB, S3 artifact and log buckets |
| Sandbox | CWASM invoker, Runner | S3 artifacts, auth-service and portfolio order endpoints |

All hosts need AWS region and credential chains, NTP synchronisation, TLS for every `https`/`wss`
endpoint, and network access to every configured exchange.

## Prerequisites

### Platform services

- NATS must provide persistent streams, durable consumers, KV buckets, and enough file storage.
  Stream replicas must stay below the NATS cluster size.
- S3 must provide the raw archive, compacted archive, build artifact, run artifact, run log, and
  rebuilt-candle locations. The Iceberg REST catalog must support conditional updates containing
  `assert-table-uuid` and `assert-ref-snapshot-id`.
- Archive tables must use an unpartitioned default spec, the supported schema/format and claim-index
  version, and `commit.retry.num-retries=0`.
- Vault stores auth-service tokens and exchange credentials. Valkey provides replay protection and
  the token-usage queue. DynamoDB stores portfolio ownership and scheduled runs. SQS carries build
  requests and auth audit events.
- Lambda functions must exist for the build orchestrator, build agent, run invoker, and runner.
  GitHub App credentials and the webhook secret must be available. `github.com` is always allowed;
  extra hosts require `GITHUB_ENTERPRISE_ALLOWED_HOSTS`.

### Configuration validation

Every process validates configuration at startup and fails closed on a missing, empty, malformed, or
unsafe value. Values come from the process environment except GitHub webhook and backfiller secrets,
which load from `SECRETS_SSM_PATH`.

- Every required variable is present and `HEALTH_BIND_ADDR` or the service-specific
  `<SERVICE>_HEALTH_BIND_ADDR` is set.
- Iceberg URI, warehouse, and namespace are set wherever archive access or recovery is enabled.
  The aggregator requires them for `checkpoint` and `archive` replay policies.
- `AGGREGATOR_SHARD_COUNT` is positive and no greater than the archive routing bucket count.
  `AGGREGATOR_CHECKPOINT_INTERVAL_SECS` is lower than `AGGREGATOR_LEASE_TTL_SECS`.
- Every lease TTL leaves a positive renewal interval after clock-skew margin, and soft work
  deadlines are shorter than the skew-adjusted TTL.
- The aggregator pair ceiling, shard count, per-interval closed-candle caps, and memory budget pass
  the worst-case startup memory check. Rebuilder spill storage and record limits fit available disk
  and memory.
- Portfolio service and registry use identical ownership/update stream settings. Auth service,
  portfolio service, and auth relay use matching Valkey queue settings.

### NATS buckets and streams

The archiver and compactor lease buckets must exist before startup with a positive history limit of
at least `1`, no key-age expiry, and no key-count expiry. Services must never auto-create an
unlimited-history bucket. Lease keys and release tombstones must never be removed by bucket age or
size limits.

Portfolio streams are provisioned idempotently with file storage, limits retention, and three
replicas by default. Set replicas explicitly below cluster size. Retention must exceed the maximum
consumer outage plus the disaster-recovery replay window.

| Stream | Default retention | Default capacity |
| --- | --- | --- |
| `portfolio.updates.v1` | 7 days | 10 GiB / 100 million messages |
| `portfolio.ownership.changed.v1` | 30 days | 1 GiB / 10 million messages |

### Capacity and safety

- The aggregator scales by shard, never by sharing a shard between writers. Changing
  `AGGREGATOR_SHARD_COUNT` changes candle ownership and requires a rebuild, not a rolling change.
- Closed-candle memory is bounded by the static exchange/asset pair ceiling, shard count, and the
  sum of per-interval caps. Correction reach is the shorter of the event-time cutoff and the newest
  `N_i` windows for each interval.
- The ingester must not subscribe to both `@trade` and `@aggTrade` for the same symbol.
- Archive writers use fenced leases. A worker that loses its lease or exceeds its soft deadline must
  abort before the next conditional commit. Do not delete lease keys, tombstones, or claim records
  while related outputs remain live.

## Provisioning process

1. Provision NATS, object storage, the Iceberg catalog and warehouse, Vault, Valkey, DynamoDB, SQS,
   SSM parameters, Lambda functions, IAM roles, networking, and TLS.
2. Create the compactor and archiver lease buckets with the required history and expiry policy.
   Provision portfolio streams and KV buckets. Confirm storage, retention, replicas, and limits with
   `nats stream info`.
3. Register GitHub App credentials, webhook secrets, exchange API credentials, SigV4 callers, and
   the shared auth-service JWT private key. Every auth replica must use the same key so tokens
   signed by one replica verify against another replica's JWKS.
4. Deploy the auth service, then the auth relay on a short cron schedule. Deploy the portfolio
   registry before the portfolio service so ownership events populate the owner/account registry
   before orders are accepted.
5. Deploy the archiver and compactor before enabling archive-backed aggregator replay. Select the
   replay policy: `checkpoint` resumes a compatible checkpoint or rebuilds from the archive and raw
   tail; `archive` ignores checkpoints and rebuilds from the archive and raw tail; `earliest`
   replays the live source from the beginning; `latest` processes only messages published after
   startup.
6. Deploy the build, control, and sandbox planes, then enable ingestion and order traffic. Verify
   readiness for every service; readiness becomes true only after dependencies connect, startup
   recovery completes, and production listeners bind.

## Operating checks

### Health

Use `GET /healthz` for liveness and `GET /readyz` for readiness. Both return `204` when healthy;
`/readyz` returns `503` during startup, recovery, or shutdown. A failed health listener is fatal.
During graceful shutdown, rely on connection draining as well as readiness.

### Pipeline checks

- Confirm ingester sequence progress is persisted in NATS KV and gap markers appear after restart.
- Confirm aggregator checkpoints advance and exactly one writer holds each shard lease.
- Confirm archiver recovery runs before new message consumption and registers raw objects once.
- Confirm compactor candidates become `Applied` or `AlreadyApplied`; `Conflict` means skip that
  source set, not retry the same batch.
- Confirm portfolio streams report the expected replicas, retention, durable consumers, and lag.
- If the read model is lost or rebuilt, reset the `portfolio-updates` and `portfolio-ownership`
  checkpoints before starting portfolio-service, or the empty read model will not rebuild from
  historical events.

### Monitoring

Dashboard connector connectivity, subscriptions, reconnect rate, update age, consumer lag, apply
failures, ownership lag, and order rejections. Page on disconnected connectors, stale feeds, lag
above threshold, any apply failure, and `not_owned` or `stale` order-rejection spikes.

Aggregator metrics named `market_data_aggregator_*` cover checkpoint age and failures, watermark
lag, replay progress, restore duration, lease acquisition, duplicate events, incomplete windows,
retained and evicted candles, and out-of-contract corrections.

Archive writers log every lease acquisition, contention, loss, commit conflict, and already-applied
outcome with table, scope, owner, fencing token, and source-batch digest. Conflict logs include the
overlapping source URIs and output paths. Treat repeated compaction of the same sources, index
fallback scans, lease-token regressions, and recovery pages that cannot finish inside the lease
window as release-blocking.

### Backup

Back up both portfolio streams and the read-model KV bucket on the same schedule as the rest of the
NATS data. A stream backup includes durable consumers and checkpoints. Back up Iceberg catalog
metadata and the warehouse under the catalog/object-store policy, retaining enough history to cover
stream retention and replay.

```sh
nats stream backup portfolio.updates.v1 /backups/portfolio-updates
nats stream backup portfolio.ownership.changed.v1 /backups/portfolio-ownership
```

### Credential rotation

Rotate exchange credentials by starting a replacement connector with the new version, waiting for
connected status and non-zero subscriptions, then draining the old connector. Revoke the old
credential only after the replacement is healthy. If rotation fails, restore the previous Vault
version and restart the connector. Auth token mutations invalidate or refresh the bounded token
cache; usage and audit records drain through the auth relay.

## Teardown process

1. Stop new order traffic, disable portfolio connectors, and drain order listeners.
2. Stop ingestion and backfilling, then stop the aggregator after it checkpoints. Stop the
   compactor, archiver, and rebuilder after in-flight work completes and leases are released.
3. Run the auth relay until usage and audit queues drain. Stop auth and control services only after
   consumers and scheduled runs are idle.
4. Back up NATS streams, checkpoints, and the read-model KV bucket before deleting or recreating
   them. Never reset a fencing generation or delete a lease tombstone during a normal restart.
5. Remove S3 objects only under an explicit retention plan. Do not use age-only lifecycle rules on
   raw objects before their compacted representation and processed-source record are durable and
   their raw-table entries are handled.
6. Remove Lambda functions, SQS queues, DynamoDB tables, Vault entries, Valkey keys, and exchange
   credentials only after traffic is stopped and backups are verified.

The current archive model is append-only. Do not expire compacted snapshots or delete raw objects
without preserving the processed-source set. The recommended retention path is a durable compaction
ledger with source manifests, crash-safe publish ordering, and a separate maintenance runner for
snapshot expiry, orphan cleanup, compacted-file rewriting, and raw deletion. Any maintenance runner
must use an ordered sequence, a fencing lease, partition-level coordination with compaction, a dry
run, metrics, throughput limits, and back-pressure.

## Disaster recovery

### Portfolio streams and read model

Restore NATS stream backups, restore the read-model KV bucket, start portfolio-service, and verify
ownership and portfolio counts before enabling order traffic. Confirm consumer lag returns to
normal. If the read-model bucket is lost but the streams survive, reset both consumer checkpoints
first so the read model rebuilds from history. If Vault is unavailable, token signing fails closed
after the maximum stale window; recover Vault or wait for the circuit breaker to close.

### Aggregator and archive recovery

Archive rebuilds replay raw events through the deterministic candle processor. Recovery reads
compacted event files plus only raw objects not represented by the compacted snapshot. Both layers
are grouped by a fixed routing bucket so every archived event is replayed by exactly one worker.
Events at or before the rebuilt watermark only advance the source position, preventing double
application. Recovery continues the highest recorded state epoch.

A retryable checkpoint read failure is surfaced to the supervisor, not silently converted to an
archive replay. If archive replay finds no events, the shard starts with explicit empty state and
does not report successful archive recovery. Use the rebuilder with a new configured epoch after an
aggregator configuration or code change.

### Partial failures and lease handover

After a failed or ambiguous Iceberg commit, reload the table and evaluate both expected output paths
and source claims. A lost successful response returns `AlreadyApplied`; a partial overlap returns
`Conflict` with details. Never delete objects after an ambiguous outcome and never retry the old
update payload. A crash after upload but before commit leaves no claims, so a later run can claim
those sources. A crash after commit but before lease release leaves no duplicate and needs no manual
cleanup.

After lease expiry, exactly one contender acquires the scope and increments the fencing token. An
expired owner cannot renew, and a stale owner cannot release its successor's lease. A graceful
release writes a tombstone and preserves the generation; the next acquisition uses `previous + 1`.
Archiver recovery uses a dedicated recovery lease and pages registrations in bounded batches so a
large backlog cannot exceed the recovery window. If a page deadline expires, release the lease, fail
startup with a retryable error, and resume from the live-path index on the next startup.

After raw expiry, recovery must use the compacted layer plus the processed-source ledger as the sole
authority, record that choice, and never silently fall back to missing raw objects.

## Test requirements

### Startup and configuration

- Missing, empty, malformed, zero, negative, and cross-field-invalid values fail startup.
- Invalid lease TTL/renewal/skew combinations and zero or negative initial fencing tokens fail.
- A missing lease bucket, a bucket without a positive history limit, or a bucket that expires lease
  keys fails startup.
- Archive startup rejects a partitioned table and an unsupported claim/index version.
- Aggregator startup rejects a configuration whose worst-case closed-candle memory exceeds budget.
- Readiness stays false until dependencies connect, recovery completes, and listeners bind.

### Archive and lease

- A source-bearing compacted commit survives a real REST-catalog round trip, reloads the same live
  files and processed-source set, and returns `AlreadyApplied` when the response is lost. Damaged
  claim metadata fails loading instead of appearing unclaimed.
- The required NATS lease suite proves acquire/release/reacquire with `previous + 1`, stale release,
  expired takeover, renewal failure, and aggregator checkpoint handover.
- S3-backed retention tests use multiple continuation tokens, prove a live output still blocks
  recompaction after old snapshots expire, and delete only unreachable metadata after the safety
  window.
- Recovery tests page a large backlog, register each object once, handle a cross-page conflict, skip
  already-live paths on restart, and release the lease at the soft deadline.
- Inventory-gap reconciliation discovers an omitted object without skipping or duplicating it.
- An end-to-end suite against the real REST catalog, S3-compatible storage, and NATS covers
  archiver flush, compactor claim, reload, ambiguous response, graceful handover, recovery, and
  orphan cleanup.

### Data plane

- Ingester restart resumes from the last durable sequence and creates gap markers for missed events.
- Gap markers close candle windows, resolved gaps remove incompleteness, and late or backfilled
  trades revise retained candles deterministically.
- Checkpoint restore, archive rebuild, and rebuilder replay apply the same correction cutoff and
  per-interval caps. A checkpoint restored under smaller caps is pruned deterministically.
- Each replay policy behaves as specified, archive-required policies fail without a catalog, and a
  changed shard count is handled by a rebuild.
- Rebuilder dry run writes no output; normal rebuild produces bounded spill runs and no duplicate
  candle revisions.
- Maintenance and compaction concurrency tests prove they cannot race on the same partition.

### Portfolio and rollout gates

- Stream provisioning is idempotent and reports file storage, limits retention, and expected
  replicas. Backup and restore drills preserve consumers and rebuild the read model when its
  checkpoints are reset.
- Connector credential rotation, stale-feed recovery, stuck-consumer recovery, and order
  authorization fail closed as described by the portfolio runbook.
- Run `cargo fmt --all -- --check`, `cargo clippy` with warnings denied, and the archive, lease,
  archiver, compactor, and aggregator suites before rollout.
- Do not enable production compaction until P0 correctness gates and the P1 scale,
  fault-injection, and orphan-cleanup tests pass against real infrastructure.

## Related references

- [Environment variables](environment-variables.md)
- [Health checks](health-checks.md)
- [Portfolio hardening runbook](portfolio-hardening-runbook.md)
- [Archive retention and Iceberg maintenance options](archive-maintenance-options.md)
- [Archive writer coordination cleanup](archive-writer-coordination-cleanup.md)
- [Market data services](../crates/data-plane/README.md)
- [Archive writer coordination](../crates/data-plane/archive-writer-coordination.md)
