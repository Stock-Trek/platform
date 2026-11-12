# Portfolio hardening runbook

## Scope

This runbook covers the portfolio update and ownership pipeline:

- exchange gateway portfolio connectors
- `portfolio.updates.v1` and `portfolio.ownership.changed.v1` JetStream streams
- portfolio-service update and ownership consumers
- durable portfolio read model in the NATS key-value bucket used for checkpoints

## Provisioning and recovery targets

Both streams are provisioned idempotently with file storage, bounded limits
retention, and three replicas by default.

| Stream | Default retention | Default size | Replicas |
| --- | --- | --- | --- |
| `portfolio.updates.v1` | 7 days | 10 GiB / 100M messages | 3 |
| `portfolio.ownership.changed.v1` | 30 days | 1 GiB / 10M messages | 3 |

Set the replica count explicitly in production and keep it below the NATS
cluster size. Retention must exceed the maximum tolerated consumer outage plus
the replay window required for disaster recovery.

Confirm provisioning:

```sh
nats stream info portfolio.updates.v1
nats stream info portfolio.ownership.changed.v1
```

Each stream must report file storage, limits retention, the configured maximum
age and size, and the expected replica count.

## Backup and disaster recovery

Back up both streams and the read-model key-value bucket on the same schedule as
the rest of the NATS data. A stream backup includes its durable consumers and
checkpoints.

```sh
nats stream backup portfolio.updates.v1 /backups/portfolio-updates
nats stream backup portfolio.ownership.changed.v1 /backups/portfolio-ownership
```

Restore in this order:

1. Restore NATS stream backups.
2. Restore the read-model key-value bucket.
3. Start portfolio-service and verify the restored ownership count and portfolio
   count before enabling order traffic.
4. Confirm consumer lag returns to the normal range.

If the read-model bucket is lost but the streams survive, delete the
`portfolio-updates` and `portfolio-ownership` consumer checkpoints before starting the service. Without
that reset the consumers resume after their last checkpoint and the empty read
model is never rebuilt from historical events.

## Dashboards

### Connector health

```promql
portfolio_connector_connected
portfolio_connector_subscriptions
increase(portfolio_connector_reconnects_total[15m])
```

### Update freshness

```promql
portfolio_connector_last_update_age_seconds
histogram_quantile(0.95, sum by (le) (rate(portfolio_consumer_lag_seconds_bucket[5m])))
```

### Consumer throughput and failures

```promql
sum by (cex, outcome) (rate(portfolio_updates_total[5m]))
sum by (outcome) (rate(portfolio_ownership_outcomes_total[5m]))
sum by (stage, operation) (rate(portfolio_apply_failures_total[5m]))
```

### Ownership and order authorization

```promql
histogram_quantile(0.95, sum by (le) (rate(portfolio_ownership_lag_seconds_bucket[5m])))
sum by (reason) (rate(portfolio_order_rejections_total[5m]))
```

## Alerts

### Connector disconnected

```promql
max by (cex) (portfolio_connector_connected) == 0
```

Fire after five minutes. Check exchange status, connector logs, network policy,
and credential validity. A planned connector shutdown must silence this alert.

### Stale connector feed

```promql
max by (cex) (portfolio_connector_last_update_age_seconds) > 120
```

Fire after five minutes. Confirm the connector is connected and subscribed
before treating this as an exchange-side outage.

### Consumer lag

```promql
histogram_quantile(0.95, sum by (le) (rate(portfolio_consumer_lag_seconds_bucket[10m]))) > 60
```

Fire after ten minutes. Inspect apply failures and NATS consumer state.

### Ownership event lag

```promql
histogram_quantile(0.95, sum by (le) (rate(portfolio_ownership_lag_seconds_bucket[10m]))) > 60
```

Fire after ten minutes. Ownership events gate order authorization, so page the
on-call owner.

### Apply failures

```promql
sum by (stage, operation) (increase(portfolio_apply_failures_total[5m])) > 0
```

Fire immediately. The labels distinguish fetch, apply, checkpoint, acknowledge,
and persist failures.

### Order rejection spike

```promql
sum by (reason) (rate(portfolio_order_rejections_total[5m])) > 1
```

Treat `not_owned` and `stale` spikes as authorization incidents.

## Runbook: stuck consumer

1. Inspect the consumer:

   ```sh
   nats consumer info portfolio.updates.v1 portfolio-updates
   nats consumer info portfolio.ownership.changed.v1 portfolio-ownership
   ```

2. Check the failure operation:

   - `fetch`: NATS connectivity, stream existence, replica health, permissions.
   - `apply`: malformed events or a version conflict; inspect service logs.
   - `checkpoint` or `ack`: key-value bucket availability or permissions.
   - `persist`: the read-model key-value bucket is unavailable or full.

3. Verify the read-model bucket exists and is writable.
4. If the consumer is wedged on a single poison message, restore service after
   fixing the payload path; do not skip the message without recording the
   affected account and ownership version.
5. After recovery, confirm lag returns to baseline and both applied counters
   advance.

## Runbook: stale feed

Steps:

1. Check `portfolio_connector_connected` and subscription count for the affected
   exchange.
2. Check connector reconnect rate and logs.
3. Confirm the exchange status page and rate-limit behavior.
4. Verify the exchange gateway has the expected account set from ownership
   events.
5. If the connector is connected with subscriptions but update age keeps rising,
   restart the connector and confirm new snapshots arrive.
6. Verify order traffic respects the staleness policy while the feed recovers.

## Runbook: failed credential rotation

1. Confirm the connector is reading the new secret version from Vault.
2. Validate exchange permissions and IP allowlists for the new credential.
3. Rotate by starting a replacement connector with the new credential, then
   drain the old connector after new subscriptions are active.
4. Revoke the old credential only after the replacement reports connected and
   has non-zero subscriptions.
5. If the rotation fails, roll back to the previous Vault version and restart the
   connector.
6. Verify update age recovers and no ownership events were lost.

