# Infrastructure runbook

Use this document to provision, validate, tear down, and recover the platform. Full variable
definitions are in [environment-variables.md](environment-variables.md) and probes in
[health-checks.md](health-checks.md).

## Service and dependency map

| Area | Services | Required infrastructure |
| --- | --- | --- |
| Market data | Ingester, Backfiller, Aggregator, Archiver, Compactor, Rebuilder, Maintainer | NATS JetStream and KV, S3-compatible storage, Iceberg REST catalog and warehouse |
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
- Archive tables must use an unpartitioned default spec, the supported schema/format and batch-keyed
  claim-index format v4, and `commit.retry.num-retries=0`.
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

- Every required variable is present and `HEALTH_BIND_ADDRESS` or the service-specific
  `<SERVICE>_HEALTH_BIND_ADDRESS` is set.
- Iceberg URI, warehouse, and namespace are set wherever archive access or recovery is enabled.
  The aggregator requires them for `checkpoint` and `archive` replay policies.
- `AGGREGATOR_SHARD_COUNT` is positive and no greater than the archive routing bucket count.
  `AGGREGATOR_CHECKPOINT_INTERVAL_SECS` is lower than `AGGREGATOR_LEASE_TTL_SECS`.
- Every lease TTL leaves a positive renewal interval after clock-skew margin, and soft work
  deadlines are shorter than the skew-adjusted TTL.
- Archive writers fail startup when coordination configuration is missing or invalid. There is no
  in-process or unconditional-commit fallback.
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

A lease record contains its scope, owner, fencing token, absolute expiry, and release state. Lease
keys are derived from a SHA-256 digest of the scope, so long or invalid object prefixes cannot
create invalid KV keys. The first acquisition of a scope uses the configured initial token, which
defaults to `1` and must be positive. Every later acquisition, including expired takeover and
graceful release, uses `previous + 1`. Lease scopes include the archive deployment identity (bucket
and archive prefix), so deployments sharing a lease bucket do not contend with each other. The
archiver and compactor may share a lease bucket as long as their scope prefixes prevent key
collisions. Lease updates use the observed KV revision; a revision race is retried by re-reading
the key, never by overwriting it unconditionally.

The portfolio update and ownership pipeline covers exchange gateway portfolio connectors,
`portfolio.updates.v1` and `portfolio.ownership.changed.v1` JetStream streams, the portfolio-service
update and ownership consumers, and the durable portfolio read model in the NATS KV bucket used for
checkpoints.

Portfolio streams are provisioned idempotently with file storage, limits retention, and three
replicas by default. Set replicas explicitly in production and keep them below the NATS cluster
size. Retention must exceed the maximum tolerated consumer outage plus the replay window required
for disaster recovery.

| Stream | Default retention | Default capacity | Replicas |
| --- | --- | --- | --- |
| `portfolio.updates.v1` | 7 days | 10 GiB / 100 million messages | 3 |
| `portfolio.ownership.changed.v1` | 30 days | 1 GiB / 10 million messages | 3 |

Each stream must report file storage, limits retention, the configured maximum age and size, and the
expected replica count.

```sh
nats stream info portfolio.updates.v1
nats stream info portfolio.ownership.changed.v1
```

### Capacity and safety

- The aggregator scales by shard, never by sharing a shard between writers. Changing
  `AGGREGATOR_SHARD_COUNT` changes candle ownership and requires a rebuild, not a rolling change.
- Closed-candle memory is bounded by the static exchange/asset pair ceiling, shard count, and the
  sum of per-interval caps. Correction reach is the shorter of the event-time cutoff and the newest
  `N_i` windows for each interval.
- The ingester must not subscribe to both `@trade` and `@aggTrade` for the same symbol.
- Source claims are append-only for the lifetime of their compacted outputs. Snapshot retention,
  lease expiry, or writer failure must not remove a claim while any output from its batch remains
  live. The processed-source set is the union of claims reachable from the current table state.
- The lease is never sufficient on its own. Every file registration still passes the conditional
  Iceberg commit, which is the correctness boundary for duplicate claims and duplicate data.
- Archive writers use fenced leases. Lease TTLs are configurable per archive kind, workers renew at
  one third of the TTL, and each kind has a shorter soft work deadline. A worker renews and verifies
  its token immediately before every conditional commit attempt, including retries. A worker that
  loses its lease or exceeds its soft deadline must abort before the next conditional commit. Do not
  delete lease keys, tombstones, or claim records while related outputs remain live.
- The compactor leases each raw hourly prefix, so different hours and archive kinds compact in
  parallel while work that would collide is serialized. Archiver recovery uses a separate lease for
  each raw prefix and raw table. Normal raw flushes do not need a lease because their path-based
  conditional commit is sufficient.

### Archive claim index and maintenance

Snapshot summaries serialize source claims with a version, count, and digest. Loading rejects
missing `source-file:`, wrong `source-claim-count`, or wrong `archive-claim-version` metadata rather
than treating damaged claims as unclaimed.

The claim index is batch-keyed format v4 and supports point lookups by source, output, and archive
batch. It uses a digest cache and atomic head updates. Consolidation is scheduled and lease-gated,
retires live batches, and sweeps only metadata unreachable from the current table state. Indexed
commits install finite retention properties in the same conditional update.

The compactor deletes unreachable archive metadata only after `COMPACTOR_MAINTENANCE_SAFETY_SECS`,
which defaults to 24 hours and must be positive. A live compacted output or retained batch keeps its
source and output claims and blocks recompaction even after older snapshots expire.

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
6. Deploy the maintainer (`platform-maintainer`) after the archiver and compactor. It validates its
   retention and budget configuration against the hard floors at startup, installs the S3 lifecycle
   backstop unless `MAINTAINER_S3_LIFECYCLE_ENABLED=false`, and runs snapshot expiry, orphan
   cleanup, compacted and ledger rewriting, and raw deletion in order on the configured interval.
   Run its first cycle with `MAINTAINER_DRY_RUN=true` and inspect the run report before enabling
   deletion.
7. Deploy the build, control, and sandbox planes, then enable ingestion and order traffic. Verify
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

#### Portfolio dashboards

Connector health:

```promql
portfolio_connector_connected
portfolio_connector_subscriptions
increase(portfolio_connector_reconnects_total[15m])
```

Update freshness:

```promql
portfolio_connector_last_update_age_seconds
histogram_quantile(0.95, sum by (le) (rate(portfolio_consumer_lag_seconds_bucket[5m])))
```

Consumer throughput and failures:

```promql
sum by (cex, outcome) (rate(portfolio_updates_total[5m]))
sum by (outcome) (rate(portfolio_ownership_outcomes_total[5m]))
sum by (stage, operation) (rate(portfolio_apply_failures_total[5m]))
```

Ownership and order authorization:

```promql
histogram_quantile(0.95, sum by (le) (rate(portfolio_ownership_lag_seconds_bucket[5m])))
sum by (reason) (rate(portfolio_order_rejections_total[5m]))
```

#### Portfolio alerts

Connector disconnected:

```promql
max by (cex) (portfolio_connector_connected) == 0
```

Fire after five minutes. Check exchange status, connector logs, network policy, and credential
validity. A planned connector shutdown must silence this alert.

Stale connector feed:

```promql
max by (cex) (portfolio_connector_last_update_age_seconds) > 120
```

Fire after five minutes. Confirm the connector is connected and subscribed before treating this as
an exchange-side outage.

Consumer lag:

```promql
histogram_quantile(0.95, sum by (le) (rate(portfolio_consumer_lag_seconds_bucket[10m]))) > 60
```

Fire after ten minutes. Inspect apply failures and NATS consumer state.

Ownership event lag:

```promql
histogram_quantile(0.95, sum by (le) (rate(portfolio_ownership_lag_seconds_bucket[10m]))) > 60
```

Fire after ten minutes. Ownership events gate order authorization, so page the on-call owner.

Apply failures:

```promql
sum by (stage, operation) (increase(portfolio_apply_failures_total[5m])) > 0
```

Fire immediately. The labels distinguish fetch, apply, checkpoint, acknowledge, and persist
failures.

Order rejection spike:

```promql
sum by (reason) (rate(portfolio_order_rejections_total[5m])) > 1
```

Treat `not_owned` and `stale` spikes as authorization incidents.

Aggregator metrics named `market_data_aggregator_*` cover checkpoint age and failures, watermark
lag, replay progress, restore duration, lease acquisition, duplicate events, incomplete windows,
retained and evicted candles, and out-of-contract corrections.

Archive writers log every lease acquisition, contention, loss, commit conflict, and already-applied
outcome with table, scope, owner, fencing token, and source-batch digest. Conflict logs include the
overlapping source URIs and output paths. Metrics cover lease acquisitions, contentions, and losses;
conditional commit outcomes by table and outcome; compaction batches skipped because of an
overlapping claim; and recovery objects registered, skipped as already live, and deferred by
conflict. Lease loss and source-claim overlap are warnings, not silent retries. Treat repeated
compaction of the same sources, index fallback scans, lease-token regressions, and recovery pages
that cannot finish inside the lease window as release-blocking.

Compactor inventory polling is incremental with a periodic full reconcile. It emits listing,
claim-index read, cache, poll-duration, fallback-scan, and backlog metrics.

#### Archive maintenance dashboards

The maintainer (`platform-maintainer`) runs snapshot expiry, orphan cleanup, compacted and ledger
rewriting, and raw deletion in order on a fixed interval. Run and stage outcomes, stage lag, and
partition file counts:

```promql
sum by (status) (increase(market_data_archive_maintenance_runs_total[24h]))
sum by (stage, status) (increase(market_data_archive_maintenance_stage_outcomes_total[6h]))
max by (stage) (market_data_archive_maintenance_stage_lag_seconds)
market_data_archive_maintenance_metadata_files
market_data_archive_maintenance_partition_files
```

Raw backlog age, recovery layer selections, and raw-expiry decisions:

```promql
max by (kind) (market_data_archive_maintenance_raw_backlog_seconds)
sum by (layer, kind) (increase(market_data_archive_recovery_layer_total[24h]))
sum by (decision, kind) (increase(market_data_archive_raw_expiry_decisions_total[24h]))
```

The raw backlog age is the age of the oldest processed ledger entry whose raw objects are still
live; it must trend down. Recovery layer selections record every decision, and
`compacted_plus_ledger` after raw expiry is the fail-closed path, not a fallback.

#### Archive maintenance alerts

Alert codes are carried by `market_data_archive_maintenance_alerts_total` with the blocking stage:

```promql
sum by (stage, code) (increase(market_data_archive_maintenance_alerts_total[6h])) > 0
```

Routing and paging rules:

- Page immediately on `partition_spec_changed`. Raw deletion is paused until an operator
  re-baselines after a ledger partition-spec change, and raw objects keep aging toward expiry while
  the pause holds.
- Page immediately on `source_manifest_quarantined`. A lost or corrupt source manifest quarantines
  its batch and needs investigation before its raw objects can be cleaned up.
- Page on `metadata_growth_exceeded` when it fires in consecutive cycles. Raw cleanup is paused
  until metadata is rewritten under the growth budget.
- Raise a ticket on `maintenance_budget_exhausted`. Back-pressure inside one hourly window is the
  budget working as designed; page only when stages stay blocked across consecutive windows.
- Raise a ticket on `partition_lease_contended`. Contention with the compactor for a partition
  lease is expected and retried on the next cycle.

Page when the raw backlog age exceeds one maintenance cycle or fails to fall over two consecutive
maintenance cycles, and when no maintenance run reports `completed` within 24 hours. A blocked
earlier stage stops later stages, so one blocked stage suppresses raw deletion until it clears.

### Backup

Back up both portfolio streams and the read-model KV bucket on the same schedule as the rest of the
NATS data. A stream backup includes durable consumers and checkpoints. Back up Iceberg catalog
metadata and the warehouse under the catalog/object-store policy, retaining enough history to cover
stream retention and replay.

```sh
nats stream backup portfolio.updates.v1 /backups/portfolio-updates
nats stream backup portfolio.ownership.changed.v1 /backups/portfolio-ownership
```

Restore in this order:

1. Restore NATS stream backups.
2. Restore the read-model KV bucket.
3. Start portfolio-service and verify the restored ownership count and portfolio count before
   enabling order traffic.
4. Confirm consumer lag returns to the normal range.

### Credential rotation

1. Confirm the connector reads the new secret version from Vault and validate exchange permissions
   and IP allowlists for the new credential.
2. Start a replacement connector with the new credential and wait for connected status and non-zero
   subscriptions.
3. Drain the old connector after the new subscriptions are active.
4. Revoke the old credential only after the replacement reports connected and has non-zero
   subscriptions.
5. If rotation fails, roll back to the previous Vault version and restart the connector. Verify
   update age recovers and no ownership events were lost.

Auth token mutations invalidate or refresh the bounded token cache; usage and audit records drain
through the auth relay.

## Portfolio runbooks

### Stuck consumer

1. Inspect the consumer:

   ```sh
   nats consumer info portfolio.updates.v1 portfolio-updates
   nats consumer info portfolio.ownership.changed.v1 portfolio-ownership
   ```

2. Check the failure operation:

   - `fetch`: NATS connectivity, stream existence, replica health, permissions.
   - `apply`: malformed events or a version conflict; inspect service logs.
   - `checkpoint` or `ack`: KV bucket availability or permissions.
   - `persist`: the read-model KV bucket is unavailable or full.

3. Verify the read-model bucket exists and is writable.
4. If the consumer is wedged on a single poison message, restore service after fixing the payload
   path; do not skip the message without recording the affected account and ownership version.
5. After recovery, confirm lag returns to baseline and both applied counters advance.

### Stale feed

1. Check `portfolio_connector_connected` and subscription count for the affected exchange.
2. Check connector reconnect rate and logs.
3. Confirm the exchange status page and rate-limit behavior.
4. Verify the exchange gateway has the expected account set from ownership events.
5. If the connector is connected with subscriptions but update age keeps rising, restart the
   connector and confirm new snapshots arrive.
6. Verify order traffic respects the staleness policy while the feed recovers.

## Teardown process

1. Stop new order traffic, disable portfolio connectors, and drain order listeners.
2. Stop ingestion and backfilling, then stop the aggregator after it checkpoints. Stop the
   compactor, archiver, and rebuilder after in-flight work completes and leases are released. Stop
   the maintainer in the same window and keep its lease bucket intact until the process is stopped;
   a stopped maintainer deletes nothing, so backups and teardown checks run against a frozen
   archive.
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
  claim metadata, including a missing `source-file:`, a wrong `source-claim-count`, or a wrong
  `archive-claim-version`, fails loading instead of appearing unclaimed.
- The NATS lease suite runs in CI or a required integration environment, not as an opt-in test. It
  proves acquire/release/reacquire with `previous + 1`, stale release, expired takeover, renewal
  failure, property contention, successor takeover during backoff, and aggregator checkpoint
  handover.
- S3-backed retention tests use multiple continuation tokens, prove a live output still blocks
  recompaction after old snapshots expire, and delete only unreachable metadata after the safety
  window. Use `metadata_sweep_paginates_and_deletes_only_unreachable_old_objects` to cover the
  paginated sweep and safety-window decision with an injected object store.
- Recovery tests exercise `recover_table_pages` against a real S3 listing with multiple
  continuation tokens. They page a large backlog, register each object once, handle a cross-page
  conflict by registering only the remainder, skip already-live paths on restart, and release the
  lease at the soft deadline. A recovery scan that exceeds `ARCHIVER_MAX_WORK_SECS` aborts without
  registering.
- A compaction that exceeds `COMPACTOR_MAX_WORK_SECS` aborts without registering, and concurrent
  normal flushes of the same object path append it once.
- Inventory-gap reconciliation discovers an omitted object without skipping or duplicating it. Use
  `an_inventory_gap_is_reconciled_without_skipping_or_duplicating_a_source` to prove that the fake
  incremental listing omits the object and the periodic full reconcile finds it.
- An end-to-end suite against the real REST catalog, S3-compatible storage, and NATS covers
  archiver flush, compactor claim, reload, ambiguous response, graceful handover, recovery, and
  orphan cleanup. `an_unreferenced_index_page_waits_for_the_safety_window` covers only an
  unreferenced index page; real manifest cleanup still needs the end-to-end environment.

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
  authorization fail closed as described in the portfolio runbooks and credential rotation steps.
- Run `cargo fmt --all -- --check`, `cargo clippy` with warnings denied, and the archive, lease,
  archiver, compactor, and aggregator suites before rollout.
- Keep the rollout order: close P0 correctness with real infrastructure tests, add the P1-1 and
  P1-2 S3-backed evidence, then add P1-3 inventory-gap and P1-5 end-to-end and orphan-cleanup tests.
  Do not enable production compaction until all P0 and P1 scale, fault-injection, and
  orphan-cleanup tests pass against real infrastructure.

## Related references

- [Environment variables](environment-variables.md)
- [Health checks](health-checks.md)
- [Archive retention and Iceberg maintenance plan](archive-maintenance-plan.md)
- [Market data services](market-data-services.md)
