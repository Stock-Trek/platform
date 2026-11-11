# Portfolio/order path production plan

Status: investigation only. This document makes no code changes, as requested by
issue #569.

## Question

What would it take for the portfolio/order path to serve real traffic, and what
is the actionable plan for closing the gap?

## Current path

`PortfolioRunner::run` starts the service as follows:

1. Connect the Vault/Valkey-backed `PortfolioAuthService`.
2. Construct `PortfolioService::in_memory()`.
3. Wrap it in `OrderRequestService` and bind `OrderRequestServer`.
4. On each signed-order request, verify the caller's JWT and require the target
   CEX account to be tracked for that owner.

The relevant code is in
`crates/data-plane/portfolio-service/src/runner.rs:14-28` and
`src/order_request.rs:120-150`.

## Findings and evidence

| Gap | Evidence | Consequence |
| --- | --- | --- |
| No production feed | The runner uses `PortfolioService::in_memory()` (`runner.rs:17`), which installs `NoopPortfolioFeed` (`service.rs:25-27`). `NoopPortfolioFeed::set_accounts` returns `Ok(())` without contacting an exchange (`feed.rs:36-41`). | No portfolio data ever arrives. `account_portfolio` remains `None`. |
| No production connector | `PortfolioConnector` has no non-test implementation. `GroupingPortfolioFeed` is never constructed outside tests. | The connector abstraction exists, but there is nothing to plug into it. |
| No inbound update path | `PortfolioConnection` only exposes `update_accounts` and `disconnect` (`feed.rs:15-20`). It has no callback, stream, or channel that can deliver `PortfolioUpdate` values to `PortfolioService::apply_update`. | Implementing a connector alone would not make updates reach service state. The feed contract needs an inbound update sink. |
| No durable account state | `PortfolioState` is a `parking_lot::RwLock` owned by `PortfolioService` (`service.rs:12-15`). There is no load, snapshot, restore, or persistence interface. | Every restart starts with an empty tracked-account set and no portfolios. |
| No startup bootstrap | `PortfolioState::new()` is empty (`state.rs:8-18`) and `PortfolioConfig` has no account-source option (`config.rs:11-16`). | After startup no account is tracked for any owner, so every order reaches the ownership check at `order_request.rs:142` and is rejected. |
| No admin/read surface | `OrderRequestServer` serves only the signed-order submission path. `add_account`, `remove_account`, `add_user_account`, `remove_user_account`, `apply_portfolio`, `portfolios`, and `tracked_accounts` have callers only in `src/tests/*`; `add_user_account` and `remove_user_account` have no callers at all. | Operators cannot register an account, users cannot list their accounts or portfolios, and there is no supported way to repair state. |
| No multi-replica consistency | Each process owns a separate `RwLock<PortfolioState>`. Nothing synchronises account ownership or portfolios between replicas. | An order can be authorized on one replica and rejected on another. Sticky routing would be required, and restarts would still lose state. |
| No connector configuration | `PortfolioConfig` contains only logging, order-request, and auth configuration. There are no exchange URL, credential, subscription, persistence, or admin settings. | Even a finished connector could not be deployed without new configuration and secret wiring. |
| No end-to-end test | Tests exercise the service, state, feed grouping, and HTTP server separately. No test starts the runner with a configured account, receives an update, and submits an order through the server. | The empty-state failure is invisible to the current test suite. |

The existing proto `proto/portfolio_dto.proto` is used for cwasm backtest input
(`crates/sandbox-plane/runner/src/cwasm/backtest/portfolio.rs`). It is not a
read API for the portfolio service and does not change the findings above.

## Requirements

Functional: the account registry must be authoritative, durable, auditable, and
readable at startup; registration changes must go through a control-plane API,
not process memory; the service must subscribe to exchange streams for tracked
accounts; order authorization must remain owner-scoped; owners may read their
own accounts and portfolios while admins may mutate any mapping; state must
survive restarts and rolling deployments.

Operational: reconnects need backoff, health reporting, and alerting; updates
need versioning and duplicate handling; staleness policy must be explicit;
metrics must cover connections, subscription size, update age, apply failures,
consumer lag, and rejected orders; credentials must stay in the existing
secret-management path.

Correctness: ownership changes and subscriptions must not leave an authorizable
account without a feed or a feed without an authorizable account; removal must
stop the subscription and clear cached state; untracked updates must be ignored;
restore must not resurrect an account that was explicitly removed.

## Proposed design: control-plane registry plus dedicated connector

The control plane owns account ownership. An authoritative store holds
owner/account mappings; admin writes and owner-scoped reads go through its API.
Every mutation writes an audit record and a transactional outbox event, making
the control plane the only writer of ownership state.

A dedicated connector service owns exchange connectivity. Connectors are
partitioned by exchange, consume ownership events, load credentials from Vault,
open user-data streams, and publish normalised, versioned updates. They do not
serve orders or store ownership state.

The portfolio/order service owns only a local read model. It consumes ownership
and update streams, materialises tracked accounts and latest portfolios, and
authorises orders from that state. It holds no exchange credentials and no
runtime mutation API outside the event consumers.

NATS JetStream carries the contracts: `portfolio.ownership.changed.v1`
(owner/account add or remove with ownership version and audit id),
`portfolio.updates.v1` (normalised updates with account id, owner, exchange
sequence or version, and timestamp), and `portfolio.connector.health.v1`
(connection state, subscription count, and last-update age). Streams need
replay, duplicate detection, durable consumers, and checkpoints; updates are
idempotent and consumers ignore duplicates and older versions.

Failure semantics:

- Orders are rejected until the ownership event is materialised.
- Orders fail closed when portfolio data exceeds the configured staleness bound,
  unless a documented policy explicitly downgrades that behaviour.
- After restart, the service replays from its checkpoint and reports unready
  until caught up.
- Removing a mapping stops the subscription and clears the read-model entry;
  late updates are ignored by version and ownership check.
- Every replica consumes the same streams, so authorization is consistent
  without sticky routing.

## Actionable plan

### Phase 1: make account ownership authoritative

1. Define the ownership model and lifecycle: single-owner or shared accounts,
   removal semantics, and conflict rules.
2. Add the authoritative store with schema, migrations, and audit history.
3. Add a control-plane admin API for add/remove mapping and an owner-scoped read
   API for accounts. Put admin endpoints on a separate internal listener with
   its own authorization scope.
4. Publish ownership events through a transactional outbox. Include a monotonic
   ownership version so consumers can reject stale events.
5. Replace portfolio-service startup with an ownership-event consumer that
   builds the local registry, persists its checkpoint, and fails or reports
   degraded if it cannot reach the stream.
6. Remove runtime reliance on the in-memory mutation methods. Keep them only
   for tests; production writes come from the event consumer.
7. Add tests for add, remove, restart, replay, duplicate events, and owner
   isolation.

Exit criteria: an operator can add and remove a mapping through an audited API;
a restart preserves the mapping; an owner can submit an order for a tracked
account while another owner and an untracked account are rejected.

### Phase 2: add the connector and inbound update path

1. Define the versioned portfolio-update schema and the order path's staleness
   contract before implementing a connector.
2. Extend the feed contract with an inbound update sink so connector output can
   reach `PortfolioService::apply_update`.
3. Implement the first exchange connector:
   - credential loading and rotation from Vault;
   - subscribe/unsubscribe from ownership events;
   - reconnect and backoff with listen-key renewal where required;
   - exchange sequence handling, normalisation, and untracked-account filtering;
   - publishing to `portfolio.updates.v1` and health to
     `portfolio.connector.health.v1`.
4. Add a durable portfolio-service consumer with idempotent apply, checkpointing,
   duplicate/stale rejection, and consumer-lag metrics.
5. Add the order-path freshness gate and document whether stale data rejects
   orders or only degrades non-authorization features.
6. Persist or replay enough state that a rolling restart has a bounded recovery
   time; do not serve order traffic from an empty read model.
7. Add an integration test that registers an account, applies a real connector
   update, submits an order, restarts the service, and verifies authorization
   before and after recovery.

Exit criteria: real portfolio updates are applied for tracked accounts; update
age and consumer lag are observable; orders respect ownership and the staleness
policy; restart and replay are covered by tests.

### Phase 3: production hardening and rollout

1. Provision NATS JetStream with retention, replication, backup, and disaster
   recovery appropriate to the update replay window.
2. Define connector deployment topology: one connector per exchange, scaling,
   failover, and credential ownership.
3. Add dashboards and alerts for connector health, update age, consumer lag,
   apply failures, ownership-event failures, and rejected orders.
4. Run the new path in shadow/read-only mode beside the current service, compare
   outputs, then enable order authorization behind a feature flag.
5. Load-test reconnects, exchange rate limits, event floods, and stream replay.
6. Document runbooks for stuck consumers, stale feeds, failed credential
   rotation, and ownership rollback.

Exit criteria: the service survives connector and NATS restarts without losing
authorization state, alerts fire on stale or lagging data, and a documented
rollback path exists.

## Acceptance criteria for closing the issue

- Restarting the service preserves tracked accounts and does not silently drop
  portfolios.
- An operator can add and remove an owner/account mapping through a documented,
  audited API.
- A tracked owner can successfully submit an order end-to-end; another owner
  and an untracked account are rejected.
- A real connector applies portfolio updates for tracked accounts and exposes
  connection/update-age metrics.
- An integration test covers startup, account registration, update
  application, restart, and order authorization.

## Open questions

1. Which exchange is first, and does it provide a multi-account user-data
   stream, or only one listen key per API credential?
2. Are CEX accounts single-owner, shareable, or global? `PortfolioState`
   currently supports global and per-owner tracking, but the product model is
   not documented.
3. Is stale portfolio data acceptable for order authorization? Today the order
   path checks ownership only, not balances, so the answer may differ for
   authorization and strategy evaluation.
4. Should account registration be self-service, operator-only, or driven by an
   external control plane?
5. How should exchange credentials be associated with owners and accounts, and
   what rotation/revocation flow is required?
6. Are there existing production accounts to migrate, or can the first
   deployment start from an empty registry?
