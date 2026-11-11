# Portfolio/order path production options

Status: investigation only. This document makes no code changes, as requested by
issue #569. The issue had no comments when this investigation was performed.

## Question

What would it take for the portfolio/order path to serve real traffic, and what
are the viable ways to close the gap?

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

### Functional

- An account registry must be authoritative, durable, and readable at startup.
- New accounts must be registered and removed through a supported API or
  control-plane event, not by editing process memory.
- The service must subscribe to exchange portfolio streams for tracked
  accounts and apply updates to local state.
- Order authorization must remain owner-scoped: a caller may submit an order
  only for an account tracked for that owner.
- Operators and users need read access, subject to different authorization:
  - owner read: own accounts and portfolios only;
  - admin write: register/remove accounts for any owner.
- Account and portfolio state must survive restarts and rolling deployments.

### Operational

- Reconnects need backoff and a health signal.
- Portfolio updates need ordering/versioning and duplicate handling.
- Bounded staleness must be explicit; the order path must decide whether stale
  portfolio data is acceptable.
- The service must expose metrics for connected exchanges, subscription size,
  update age, apply failures, persistence failures, and rejected orders.
- Exchange credentials must live in the existing secret-management path, not
  in environment variables or source control.

### Correctness

- Account ownership changes and feed subscriptions must be applied atomically
  enough that a failed subscription does not create an authorizable account
  without a feed, or a feed without an authorizable account.
- Removing an account must stop its subscription and remove its cached
  portfolio without removing another owner's shared view.
- Updates for untracked accounts must be ignored, as they are today.
- State restore must not resurrect an account that was explicitly removed.

## Options

### Option A: Complete the in-process service

Keep the current single-service design and add the missing pieces:

- an account-registry trait with a durable implementation;
- an admin/read HTTP surface, preferably on a separate internal listener;
- a real connector for one exchange, plus an inbound update channel from the
  connection to `PortfolioService`;
- startup restore and write-through persistence;
- feed reconnect, health, metrics, and an end-to-end order test.

The account registry could use:
- Vault KV, which is already a dependency and supports compare-and-swap;
- a new control-plane database (PostgreSQL/DynamoDB) if account management is a
  product feature;
- Valkey for cached state plus an authoritative database, never Valkey alone.

**Advantages:** smallest number of moving parts; reuses the existing server and
auth stack; low update latency; easiest path to a first working deployment.

**Disadvantages:** exchange-specific protocol and credential handling live in
the order-serving process; every replica may open its own exchange connections;
a connector bug can affect order serving; multi-replica ordering and failover
need explicit design; adding each exchange expands the order service.

**Best for:** the first production exchange, a small deployment, or a
transitional step before a dedicated gateway.

### Option B: Dedicated portfolio-connector service or sidecar

Move exchange connectivity out of the order-serving process:

- a connector service connects to exchange account streams, owns credentials,
  and publishes versioned portfolio updates to NATS JetStream;
- the portfolio service consumes updates and maintains an in-memory read model;
- account subscription commands are published to the connector;
- the order server reads the local materialized state, with an explicit
  staleness policy.

One connector per exchange (or a connector deployment partitioned by exchange)
avoids duplicate exchange connections across order-service replicas.

**Advantages:** isolates exchange protocols and failures; one exchange
connection can serve several order-service replicas; JetStream provides replay
and recovery; the order service stays small; exchange connectors can be scaled
and deployed independently.

**Disadvantages:** more services and a NATS dependency; updates are eventually
consistent; needs idempotent apply, sequencing, consumer checkpoints, and
lag/health monitoring; account-registry events and subscription commands need
a clear ordering contract.

**Best for:** multiple exchanges, multiple order-service replicas, or any
deployment where exchange connectivity must not share a failure domain with
order serving.

### Option C: Control-plane-owned account registry

Make account ownership a control-plane concern rather than portfolio-service
state:

- store owner/account mappings in an authoritative control-plane store;
- publish account-added/account-removed events;
- portfolio-service consumers materialize the mapping locally;
- the connector reacts to the same events.

This can be combined with Option A or Option B. It is the cleanest answer to
"who owns this CEX account?" and gives one place to audit changes.

**Advantages:** one source of truth for ownership; supports user self-service
and operator tooling; avoids duplicating account administration in each
portfolio replica; natural fit for multi-tenant authorization.

**Disadvantages:** requires a product-level ownership model and a new
persistent store or an extension of the auth/control plane; event delivery and
migration need design; higher initial effort.

**Best for:** a real multi-tenant product, as opposed to a single-operator
deployment.

### Option D: Static bootstrap list plus polling

Load tracked accounts from configuration at startup and periodically poll the
exchange REST API for balances and open orders.

**Advantages:** no websocket protocol work; simple to operate for a small,
fixed set of accounts; useful as a fallback when an exchange has no suitable
account stream.

**Disadvantages:** configuration changes require a restart or a separate
reload path; polling has higher latency and rate-limit cost; no prompt order
updates; still needs durable ownership data and an update sink; does not scale
to self-service account registration.

**Best for:** a small pilot, a read-only portfolio view, or a fallback path.

### Option E: Track accounts on first order request

Accept an order for an unknown account and add it to the tracked set when the
request is first seen.

**Advantages:** trivial to implement and removes the startup rejection.

**Disadvantages:** it is not ownership verification. Any caller with a valid
token could claim an arbitrary CEX account, which breaks the authorization
model the portfolio service exists to enforce. It also cannot obtain a
portfolio without credentials for that account.

**Best for:** local development only. Not a production option.

## Comparison

| Option | Real portfolio updates | Durable ownership | Multi-replica safe | Exchange isolation | New infrastructure | Relative effort |
| --- | --- | --- | --- | --- | --- | --- |
| A. In-process completion | Yes | Yes, via chosen store | Needs explicit sync/leader design | No | Maybe a database | Medium |
| B. Connector service + read model | Yes | Depends on registry option | Yes, via JetStream | Yes | NATS, connector deployment | High |
| C. Control-plane registry | Not by itself | Yes | Yes | Depends on feed choice | Control-plane store | High |
| D. Static bootstrap + polling | Polled only | Static list only | Read-only replicas might share a DB | Partial | Scheduler/poller | Low to medium |
| E. Trust on first order | No | No | No | No | None | Very low (unsafe) |

## Recommendation

Use a phased approach. Do not treat a connector as the first task; without an
authoritative account registry and a supported admin path, a connector has
nothing to subscribe to and operators cannot repair state.

**Phase 1: make account ownership durable and observable.**

- Introduce an account-registry abstraction with an in-memory implementation
  for tests and a durable implementation for production. Reuse Vault if the
  deployment is small; prefer a control-plane store if ownership is a product
  feature.
- Add an internal admin/read HTTP surface:
  - owner-scoped `GET` for tracked accounts and portfolios;
  - admin-scoped add/remove for accounts and owner mappings;
  - explicit audit logging for every mutation.
- Load the registry at startup, restore portfolios if they are persisted, and
  fail startup if the configured account source cannot be read.
- Add an end-to-end test that registers an account, starts the server, submits
  an order, and verifies both acceptance for the owner and rejection for a
  different owner.

This phase alone fixes the empty-startup rejection and makes authorization
usable, but portfolios will still be empty or stale. It is not sufficient for a
real trading deployment.

**Phase 2: add the update path.**

- Extend the feed contract with an inbound `PortfolioUpdate` sink (channel or
  trait callback). A connector that only receives account sets cannot update
  the service today.
- Implement the first real exchange connector, including credential loading,
  reconnect/backoff, sequence or version handling, and untracked-account
  filtering.
- Persist portfolio snapshots with a bounded staleness marker, or rebuild them
  from the exchange on startup. Make the order path's staleness policy
  explicit.
- Add metrics and alerts for connection state, update age, apply failures, and
  persistence failures.

**Phase 3: choose the long-term topology.**

- For one exchange and a small deployment, keep Option A and add a single
  connector leader or partition accounts across replicas.
- For multiple exchanges or multiple order-service replicas, move to Option B
  and use NATS JetStream for replayable updates. Combine it with Option C if
  account ownership is user-facing.

The recommended target is **Option C for ownership + Option B for updates**,
with Option A as the transitional implementation. Option D is a useful
fallback or pilot, and Option E should be explicitly rejected.

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
5. Where should admin endpoints live: a separate internal listener, a
   service-to-service API, or the existing order-request listener with an
   additional authorization scope?
6. How should exchange credentials be associated with owners and accounts, and
   what rotation/revocation flow is required?
7. Are there existing production accounts to migrate, or can the first
   deployment start from an empty registry?

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
