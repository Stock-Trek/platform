# Front-end API

This document defines the API that the browser front-end speaks to. It covers conventions
shared by every resource group and the per-area endpoints:

- [Overview](#overview)
- [Versioning and base URL](#versioning-and-base-url)
- [Authentication and sessions](#authentication-and-sessions)
- [Conventions](#conventions)
- [Errors](#errors)
- [Streaming](#streaming)
- [Limits](#limits)
- [Accounts, portfolios, and orders](#accounts-portfolios-and-orders)
- [Algorithms and builds](#algorithms-and-builds)
- [Bots and runs](#bots-and-runs)
- [Backtesting](#backtesting)
- [Market data](#market-data)

## Overview

The front-end talks to exactly one platform component: the front-end API. The API is a new
service that composes the existing planes and hides their internals from the browser. It owns
the resources that no existing service owns today (bots, backtests) and exposes reads and
actions over the stores and streams that already exist.

```
Browser ⇄ Front-end API ⇄ Auth plane        (sessions, JWT verification)
                       ⇄ Portfolio registry (account ownership)
                       ⇄ Portfolio service  (read model, order signing)
                       ⇄ Build plane        (algorithms, build artifacts)
                       ⇄ Run store/invoker  (scheduled runs)
                       ⇄ Runner             (live runs, backtests)
                       ⇄ Aggregator/archive (candles)
```

The API introduces two new durable stores: a bot configuration store and a backtest store.
Everything else is a projection of existing state: the ownership registry, the portfolio read
model, build artifacts, the run store, and the candle archive.

## Versioning and base URL

- HTTPS only. All resources live under a fixed major version prefix: `/v1`.
- Changes within `/v1` are additive: new fields may appear in responses, new endpoints may
  appear. Removing or renaming fields, or changing semantics, requires `/v2`.
- Unknown fields in requests are rejected; unknown fields in responses are ignored by clients.

## Authentication and sessions

Every request carries `Authorization: Bearer <jwt>`. Tokens are short-lived EdDSA (Ed25519)
JWTs signed by the auth service; the API verifies them locally against the auth service JWKS
(`/.well-known/jwks.json`) with a bounded key cache. The API backend is a configured SigV4
caller of the auth service and mints owner-scoped tokens on behalf of authenticated users.

The JWT `owner` claim is the platform identity. Every resource in the API is scoped to the
owner: a user can only ever see and act on their own accounts, algorithms, bots, runs, and
backtests. Resource identifiers are not guessable, and every request re-checks ownership.

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/v1/session` | Exchange a platform login credential for a short-lived API token |
| `DELETE` | `/v1/session` | Invalidate the presented token |

`POST /v1/session` returns `201` with:

```json
{ "token": "eyJ...", "expiresAt": "2026-07-14T10:30:00.000Z", "owner": "usr_7gk2" }
```

Tokens are refreshed by calling `POST /v1/session` again before `expiresAt`. Tokens carry the
scopes granted by the owner's active order-write token; order placement requires the order-write
scope, everything else requires an authenticated session.

## Conventions

- Request and response bodies are UTF-8 JSON with `Content-Type: application/json`.
- Timestamps are RFC 3339 UTC with millisecond precision, for example
  `2026-07-14T09:30:00.000Z`. Durations are integer seconds unless a field name ends in
  `_millis`.
- Quantities and prices are decimal strings, for example `"1250.50"`, never JSON numbers.
- Identifiers are opaque strings prefixed by type: `usr_`, `alg_`, `bld_`, `bot_`, `run_`,
  `bt_`.
- Enum values are snake_case strings.
- Exchange and asset identifiers use the platform's canonical codes: `CexId` values such as
  `binance` and `AssetId` codes such as `btc` and `usdt`. Trading pairs are
  `{"base": "btc", "quote": "usdt"}`.
- List endpoints paginate with `limit` (default `50`, maximum `200`) and an opaque `cursor`.
  Responses wrap arrays as `{ "data": [...], "nextCursor": "..." | null }`.
- `POST` endpoints that create resources or trigger work accept an `Idempotency-Key` header.
  Retries with the same key return the original response instead of creating a duplicate.
- Mutable resources expose a `version` integer. Mutations may be conditioned with `If-Match:
  <version>` and fail with `409 conflict` when the version does not match.

## Errors

Errors use RFC 9457 problem details:

```json
{
  "type": "https://api.example.com/errors/not-owned",
  "title": "Resource is owned by another user",
  "status": 403,
  "detail": "Account binance:main is not linked to this owner",
  "code": "not_owned"
}
```

| `code` | Status | Meaning |
| --- | --- | --- |
| `validation_failed` | 400 | Request body or parameters are malformed |
| `unauthorized` | 401 | Missing, expired, or invalid token |
| `forbidden` | 403 | Token lacks the required scope |
| `not_owned` | 403 | Resource exists but belongs to another owner |
| `not_found` | 404 | Resource does not exist for this owner |
| `conflict` | 409 | Version mismatch or duplicate resource |
| `stale` | 409 | Portfolio data is too old to authorize the action |
| `out_of_contract` | 409 | Candle correction reach exceeded the retention contract |
| `payload_too_large` | 413 | Request or referenced artifact exceeds a size limit |
| `rate_limited` | 429 | Per-owner quota exhausted |
| `internal` | 500 | Unexpected server error |
| `unavailable` | 503 | Dependency is down; retry with backoff |

`stale` mirrors the portfolio service staleness policy (`allow`, `degrade`, `reject`); in
`degrade` mode accepted-but-stale actions are flagged in responses so the front-end can warn.

## Streaming

`GET /v1/stream` upgrades to a WebSocket after bearer authentication. The client subscribes to
channels, the server pushes JSON frames with a per-channel monotonically increasing `sequence`
so clients can detect gaps and resume:

```json
{ "action": "subscribe", "channel": "portfolio.binance.main", "lastSequence": 41 }
{ "channel": "portfolio.binance.main", "sequence": 42, "data": { "...": "..." } }
```

The server pings every 30 seconds and closes connections that miss two pongs. Channels:

| Channel | Frames |
| --- | --- |
| `portfolio.{cexId}.{accountId}` | Portfolio snapshots and order updates |
| `candles.{cexId}.{base}.{quote}.{interval}` | Open candle updates and closed candles |
| `builds.{algorithmId}` | Build state changes |
| `runs.{botId}` | Run state changes |
| `backtests.{backtestId}` | Backtest progress and partial results |

## Limits

| Limit | Value |
| --- | --- |
| Concurrent backtests per owner | 2 |
| Ad-hoc runs per hour per bot | 12 |
| Concurrent stream subscriptions per connection | 32 |
| Maximum request body size | 1 MiB |
| Referenced build artifact size | `S3_MAX_ARTIFACT_SIZE_BYTES` |

## Accounts, portfolios, and orders

### Ownership model

An owner (the JWT `owner` claim) controls zero or more exchange accounts. Ownership of
`(cexId, accountId)` is recorded by the portfolio registry and drives order authorization
everywhere: order requests for accounts an owner does not own are rejected with `not_owned`
and treated as security incidents. Ownership changes are audited and propagate through the
ownership stream to the portfolio service connectors.

Every linked account additionally needs exchange API credentials. Credentials are write-only:
they are stored in Vault, never returned by any endpoint, and rotated by replacing them.

### Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/v1/accounts` | List the owner's linked exchange accounts |
| `POST` | `/v1/accounts` | Link an exchange account and submit its credentials |
| `GET` | `/v1/accounts/{cexId}/{accountId}` | Account detail and connector status |
| `DELETE` | `/v1/accounts/{cexId}/{accountId}` | Unlink the account |
| `PUT` | `/v1/accounts/{cexId}/{accountId}/credentials` | Replace exchange credentials |
| `GET` | `/v1/accounts/{cexId}/{accountId}/portfolio` | Portfolio snapshot for the account |
| `POST` | `/v1/orders` | Submit an order for a linked account |

### Linking and listing accounts

`GET /v1/accounts` returns:

```json
{
  "data": [
    {
      "cexId": "binance",
      "accountId": "main",
      "active": true,
      "connector": "connected",
      "version": 7,
      "updatedAt": "2026-07-14T09:00:00.000Z"
    }
  ],
  "nextCursor": null
}
```

`connector` reports the portfolio connector state for the exchange (`connected`,
`disconnected`) so the front-end can show feed health.

`POST /v1/accounts` links a new account:

```json
{ "cexId": "binance", "accountId": "main", "apiKey": "...", "apiSecret": "..." }
```

The response is `201` with the account representation. Linking is asynchronous downstream:
the registry records the ownership mapping and the ownership stream wires the account into the
portfolio connector. `connector` stays `disconnected` until the first snapshot arrives.

`DELETE /v1/accounts/{cexId}/{accountId}` unlinks the account. The response is `204`. Bots
that still reference the account keep running but their order requests fail authorization, so
the API responds with `409 conflict` if any enabled bot references the account unless the
request carries `?force=true`.

`PUT .../credentials` replaces the stored credentials and triggers connector credential
rotation. The response is `204`. The body is identical to the credential fields of
`POST /v1/accounts`.

### Portfolio snapshots

`GET /v1/accounts/{cexId}/{accountId}/portfolio` returns the current read-model view for one
account. The read model is rebuilt from the portfolio update stream, so snapshots carry the
stream sequence they were built from:

```json
{
  "cexId": "binance",
  "accountId": "main",
  "asOf": "2026-07-14T09:59:41.120Z",
  "sequence": 9182,
  "assets": [
    { "asset": "btc", "total": "1.250000000" },
    { "asset": "usdt", "total": "4210.770000000" }
  ],
  "pendingOrders": [
    {
      "tag": "ord_9x2",
      "symbol": { "base": "btc", "quote": "usdt" },
      "side": "sell",
      "type": "limit",
      "quantity": "0.100000000",
      "price": "104250.000000000",
      "placedAt": "2026-07-14T09:45:02.000Z"
    }
  ]
}
```

`GET /v1/portfolios` returns the same shape for every linked account at once, for dashboards.

Live updates arrive on the `portfolio.{cexId}.{accountId}` stream channel. The channel frames
carry the same body as the snapshot endpoint with the stream `sequence` in the envelope, and
subscribing with `lastSequence` replays nothing (the read model is the resume point).

### Orders

Order submission reuses the platform order path: the front-end API accepts the order, wraps it
as an unsigned order request, and the portfolio service verifies the caller's JWT, checks
ownership and portfolio freshness, signs the request with the owner's auth-service token, and
forwards it to the exchange.

`POST /v1/orders`:

```json
{
  "cexId": "binance",
  "accountId": "main",
  "symbol": { "base": "btc", "quote": "usdt" },
  "side": "sell",
  "type": "limit",
  "quantity": "0.100000000",
  "price": "104250.000000000",
  "clientTag": "ord_9x2"
}
```

The response is `202` with `{ "tag": "ord_9x2", "status": "pending" }`. There is no order
store: accepted orders surface as pending orders in the portfolio snapshot and as exchange
order updates on the portfolio stream channel. `clientTag` is the idempotency handle the
exchange sees; omitting it lets the API generate one.

Failure semantics are the platform's:

- `not_owned` rejections mean the account is not linked to the caller; the API surfaces them
  as `403` and they are monitored as authorization incidents.
- `stale` rejections mean portfolio data exceeded `PORTFOLIO_MAX_UPDATE_AGE_SECS`; with the
  `degrade` policy the API accepts the order and flags it, with `reject` it returns `409`.
- Signature, replay, and availability failures fail closed and are surfaced as `503`.

Orders are never edited or cancelled through this API in `/v1`: cancellation happens on the
exchange, and the portfolio stream reflects the resulting state. A dedicated cancellation path
can be added to `/v2` once the order pipeline exposes one.

## Algorithms and builds

### Model

- An **algorithm** is a Git repository containing trading strategy code. The build plane
  watches the repository through the GitHub App; pushes to watched refs trigger builds.
- A **build** is one compile of a commit. It produces a compiled cwasm artifact in the
  artifact bucket together with its SHA-256, size, and the Wasmtime version required to run
  it. Artifact bytes are never proxied through the API; the runner fetches them directly.
- A **strategy** is a named algorithm inside a build. One build may expose several strategies.
  Strategy names are unique within a build; duplicate names fail the build.
- **Preferences** are the strategy's tunable parameters, described by a JSON schema the build
  exposes. Bots reference strategies by name and carry parameter values validated against
  this schema.

### Lifecycle

Algorithms and builds have independent lifecycles:

| Algorithm state | Meaning |
| --- | --- |
| `active` | Builds can be triggered and strategies can be used by bots |
| `archived` | Read-only; existing bots keep running, no new builds |

| Build state | Meaning |
| --- | --- |
| `queued` | Accepted, waiting for the build agent |
| `building` | Source fetched and compiling |
| `ready` | Artifact stored with SHA-256 and Wasmtime version |
| `failed` | Compilation or packaging failed; `error` carries the reason |

A build only becomes usable by bots when it is `ready`. Archiving an algorithm is a soft
delete: history stays queryable.

### Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/v1/algorithms` | List algorithms connected to the owner |
| `GET` | `/v1/algorithms/{algorithmId}` | Algorithm detail |
| `DELETE` | `/v1/algorithms/{algorithmId}` | Archive the algorithm |
| `GET` | `/v1/algorithms/{algorithmId}/builds` | List builds, newest first, filterable by `ref` and `status` |
| `POST` | `/v1/algorithms/{algorithmId}/builds` | Trigger a build for a ref or commit |
| `GET` | `/v1/algorithms/{algorithmId}/builds/{buildId}` | Build detail and status |
| `GET` | `/v1/algorithms/{algorithmId}/builds/{buildId}/strategies` | Strategies exposed by the build |
| `GET` | `/v1/algorithms/{algorithmId}/builds/{buildId}/strategies/{strategyName}/preferences` | Parameter schema |

Algorithm list items identify the connected repository:

```json
{
  "data": [
    {
      "algorithmId": "alg_5t1",
      "name": "momentum",
      "repository": { "provider": "github", "host": "github.com", "repo": "acme/momentum" },
      "defaultRef": "main",
      "state": "active",
      "latestReadyBuildId": "bld_882"
    }
  ],
  "nextCursor": null
}
```

### Triggering builds

`POST /v1/algorithms/{algorithmId}/builds` accepts:

```json
{ "ref": "heads/main", "commit": "e1f3a9b" }
```

`ref` uses the `heads/` or `tags/` form used by the build plane's source keys; `commit` pins an
exact revision and defaults to the ref head. The response is `202` with the build
representation. Builds are idempotent per `(ref, commit)`: re-triggering an existing build
returns that build.

`GET .../builds/{buildId}` returns:

```json
{
  "buildId": "bld_882",
  "algorithmId": "alg_5t1",
  "ref": "heads/main",
  "commit": "e1f3a9b",
  "state": "ready",
  "artifact": {
    "sha256": "9f2b…",
    "sizeBytes": 4211843,
    "wasmtimeVersion": "50.0.1"
  },
  "error": null,
  "createdAt": "2026-07-14T08:12:00.000Z",
  "completedAt": "2026-07-14T08:14:31.000Z"
}
```

Build state changes are pushed on the `builds.{algorithmId}` stream channel.

### Strategies and preferences

`GET .../strategies` enumerates the strategies compiled into a build by running the artifact's
metadata command in the sandbox:

```json
{ "data": [ { "name": "momentum_v2", "description": "Window-cross momentum" } ], "nextCursor": null }
```

`GET .../strategies/{strategyName}/preferences` returns the JSON schema bots must satisfy in
their `parameters` object. The schema is what the front-end renders as a parameter form. A
build that fails to report metadata is treated as `failed`.

```json
{
  "strategyName": "momentum_v2",
  "jsonSchema": {
    "type": "object",
    "properties": { "fast": { "type": "integer", "minimum": 2 }, "slow": { "type": "integer" } },
    "required": ["fast", "slow"]
  }
}
```

Bots pick the build and strategy they run; they may pin an exact `buildId` or track an
algorithm's `latestReadyBuildId`. Tracking upgrades happen only after the front-end has had a
chance to validate parameters against the new build's preference schema, so bots re-validate
on upgrade and fall back to the pinned build when validation fails.

## Bots and runs

### Model

A **bot** is a user's persistent trading configuration: which compiled strategy runs, with
which parameters, against which exchange accounts, and on what schedule. A **run** is one
execution of a bot. Scheduling works by materializing runs: every due schedule tick exists as
a run record with its own status, error, and log, so history is inspectable and retries are
explicit.

Bot configuration is stored by the front-end API. Scheduling writes run records to the run
store (DynamoDB); the run invoker claims due runs and invokes the runner, which downloads the
pinned cwasm artifact, executes the strategy, submits signed order requests, and uploads run
logs.

### Bot lifecycle

| State | Meaning |
| --- | --- |
| `enabled` | Schedule produces runs; the bot may place orders |
| `disabled` | Config kept, schedule paused, no new runs |
| `archived` | Read-only; hidden from default lists |

Creating a bot does not enable it implicitly: the first creation returns `disabled` and the
front-end enables explicitly once parameters are confirmed.

### Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/v1/bots` | Create a bot |
| `GET` | `/v1/bots` | List bots |
| `GET` | `/v1/bots/{botId}` | Bot detail |
| `PATCH` | `/v1/bots/{botId}` | Update configuration |
| `POST` | `/v1/bots/{botId}/enable` | Enable the schedule |
| `POST` | `/v1/bots/{botId}/disable` | Pause the schedule |
| `DELETE` | `/v1/bots/{botId}` | Archive the bot |
| `POST` | `/v1/bots/{botId}/runs` | Trigger an immediate ad-hoc run |
| `GET` | `/v1/runs` | List runs across bots, filterable |
| `GET` | `/v1/runs/{runId}` | Run detail |
| `GET` | `/v1/runs/{runId}/log` | Run log |

`POST /v1/bots` example:

```json
{
  "name": "momentum-main",
  "algorithmId": "alg_5t1",
  "buildId": "bld_882",
  "strategyName": "momentum_v2",
  "parameters": { "fast": 8, "slow": 34 },
  "accounts": [{ "cexId": "binance", "accountId": "main" }],
  "schedule": { "kind": "interval", "everySeconds": 60 }
}
```

The response is `201` with:

```json
{
  "botId": "bot_4fd",
  "owner": "usr_7gk2",
  "name": "momentum-main",
  "algorithmId": "alg_5t1",
  "buildId": "bld_882",
  "strategyName": "momentum_v2",
  "parameters": { "fast": 8, "slow": 34 },
  "accounts": [{ "cexId": "binance", "accountId": "main" }],
  "schedule": { "kind": "interval", "everySeconds": 60 },
  "state": "disabled",
  "version": 1,
  "createdAt": "2026-07-14T09:00:00.000Z"
}
```

Validation rules:

- `buildId` must belong to `algorithmId` and be in state `ready`.
- `strategyName` must exist in that build.
- `parameters` must satisfy the strategy's preference schema.
- Every referenced account must be a linked, active account of the owner.
- `schedule.kind` is `interval` (`everySeconds` ≥ 10) or `cron` (`expression` in RFC 9535
  five-field syntax plus `timezone`).

`PATCH /v1/bots/{botId}` accepts the same fields minus `algorithmId`; changing the build or
strategy re-validates `parameters` and returns `422 validation_failed` on mismatch. Mutations
require `If-Match` with the current `version`.

### Runs

Runs follow the run store lifecycle:

| State | Meaning |
| --- | --- |
| `pending` | Materialized, waiting for the invoker |
| `running` | Claimed and executing in the runner |
| `completed` | Finished successfully |
| `failed` | Finished with an error; `error` carries details |

`GET /v1/runs?botId=bot_4fd&status=failed&from=2026-07-13T00:00:00.000Z` returns:

```json
{
  "data": [
    {
      "runId": "run_b7c",
      "botId": "bot_4fd",
      "algorithmId": "alg_5t1",
      "strategyName": "momentum_v2",
      "scheduledAt": "2026-07-14T09:31:00.000Z",
      "status": "failed",
      "completedAt": "2026-07-14T09:31:44.000Z",
      "error": { "code": "strategy_not_found", "message": "strategy not found" }
    }
  ],
  "nextCursor": null
}
```

`error.code` distinguishes client errors (bad strategy name, undecodable context or portfolio,
invalid request) from server errors (timeouts, panics, resource limits, artifact download
failures) using the runner exit-code taxonomy, so the front-end can tell "fix your bot" from
"platform problem".

`GET /v1/runs/{runId}/log` returns `302` to a pre-signed log URL when the log exists and `404`
while the run is still pending or before upload completes. Pre-signed URLs expire with
`S3_SIGNED_URL_EXPIRY_SECS` and are fetched by the browser directly from object storage.

Run state changes are pushed on the `runs.{botId}` stream channel.

`POST /v1/bots/{botId}/runs` materializes a run scheduled now, subject to the ad-hoc run rate
limit, and returns `202` with the run representation.

## Backtesting

### Model

A **backtest** is an ad-hoc, resource-bounded experiment: it runs one strategy from one build
many times over archived market data with simulated execution friction and a seeded random
generator, streaming per-simulation results as they finish and finishing with a collated
result set. Backtests never touch live exchanges or real balances.

Market input comes from the candle archive, so backtests are subject to the archive's
correction contract: requested history must lie within the retention reach of the requested
intervals, otherwise creation fails with `out_of_contract` rather than silently producing
candles that later change.

Backtest definitions and results are stored by the front-end API. Execution uses the sandbox
runner's backtest subsystem, which already enforces worker limits, watchdog timeouts, and
memory limits.

### Lifecycle

| State | Meaning |
| --- | --- |
| `queued` | Accepted, waiting for a worker |
| `running` | Simulations executing; partial results may be streaming |
| `completed` | All simulations finished; final results available |
| `failed` | Rejected or aborted; `error` carries details |

### Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/v1/backtests` | Create and start a backtest |
| `GET` | `/v1/backtests` | List the owner's backtests |
| `GET` | `/v1/backtests/{backtestId}` | Backtest status |
| `DELETE` | `/v1/backtests/{backtestId}` | Cancel a queued or running backtest |
| `GET` | `/v1/backtests/{backtestId}/results` | Final collated results |
| `GET` | `/v1/backtests/{backtestId}/stream` | WebSocket of incremental results |

`POST /v1/backtests` example:

```json
{
  "algorithmId": "alg_5t1",
  "buildId": "bld_882",
  "strategyName": "momentum_v2",
  "parameters": { "fast": 8, "slow": 34 },
  "initialBalances": [{ "cexId": "binance", "accountId": "paper", "asset": "usdt", "total": "10000.000000000" }],
  "market": {
    "tradingPairs": [{ "base": "btc", "quote": "usdt" }],
    "windows": ["1m", "5m"],
    "historicalCandleCount": 500,
    "from": "2026-06-01T00:00:00.000Z",
    "to": "2026-07-01T00:00:00.000Z"
  },
  "simulation": {
    "count": 100,
    "maxConcurrent": 4,
    "randomSeed": 7,
    "candleLossChance": "0.01",
    "slippageChance": "0.02",
    "slippageProportion": "0.001",
    "lostOrderChance": "0.005"
  }
}
```

The response is `202` with `{ "backtestId": "bt_31a", "state": "queued" }`.

Validation mirrors the runner's spec checks and fails fast with `validation_failed`:

- `buildId` must be `ready`, and `strategyName` must exist in it; `parameters` must satisfy
  the strategy's preference schema.
- `tradingPairs` and `windows` must be non-empty and are de-duplicated; windows must be
  supported aligned intervals.
- `openCandleProgressMillis`, when given, must be smaller than the shortest requested window.
- Every chance field is a decimal string in `[0, 1]`.
- `count` and `maxConcurrent` are clamped to the runner's worker limits.
- At most one backtest may be `queued` or `running` per owner; a further creation returns
  `429 rate_limited`.

### Results and streaming

Per-simulation results are JSON objects produced by the strategy's simulation runtime. They
are delivered twice: incrementally on the stream as each simulation finishes, and collated as
an array at completion.

`GET /v1/backtests/{backtestId}/results` returns the collated array:

```json
{
  "backtestId": "bt_31a",
  "state": "completed",
  "simulationCount": 100,
  "results": [ { "simulation": 0, "finalEquity": "10412.500000000", "...": "..." } ]
}
```

`GET /v1/backtests/{backtestId}/stream` upgrades to a WebSocket. Frames are
`{ "sequence": 7, "simulation": 13, "result": { ... } }` with `sequence` increasing per
connection; a `complete` frame with no `simulation` field marks the collated result. The
stream ends when the backtest reaches a terminal state; clients that reconnect mid-run
receive only results not yet seen by their cursor.

`DELETE /v1/backtests/{backtestId}` cancels queued and running backtests (`204`) and is
idempotent on terminal backtests.

### Initial balances

`initialBalances` seeds the simulated portfolio. When omitted, the backtest starts from a
configurable default cash balance. Using a real linked account's current balances is a
front-end convenience: it copies the read-model snapshot into the request, never grants the
backtest access to the live account.

## Market data

### Model

The market data plane ingests exchange trades, aggregates them into candles at a fixed set of
aligned intervals, corrects candles when late or backfilled trades arrive, and archives
everything to Iceberg. The front-end reads two views:

- **Closed candles** from the archive, queryable history with revision tracking.
- **Live candles** over the stream: open candle updates as they tick and closed candle events.

Supported exchanges and assets are static platform enumerations (`CexId`, `AssetId`), so the
reference endpoints are cheap and highly cacheable. Candle intervals are the platform's
aligned windows (for example `1s`, `1m`, `5m`, `1h`, `1d`).

### Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/v1/exchanges` | Supported exchanges |
| `GET` | `/v1/exchanges/{cexId}/assets` | Assets tradable on the exchange |
| `GET` | `/v1/exchanges/{cexId}/pairs` | Trading pairs with candles available |
| `GET` | `/v1/candles` | Closed candles for one pair and interval |
| `GET` | `/v1/candles/{cexId}/{base}/{quote}/{interval}/latest` | Newest closed candle |

`GET /v1/candles` query parameters: `cexId`, `base`, `quote`, `interval` (all required),
plus `from`, `to` (RFC 3339, bounded by the retention contract), `limit` and `cursor`.
Candles are returned oldest-first within a page:

```json
{
  "data": [
    {
      "cexId": "binance",
      "symbol": { "base": "btc", "quote": "usdt" },
      "interval": "1m",
      "windowOpen": "2026-07-14T09:29:00.000Z",
      "windowClose": "2026-07-14T09:30:00.000Z",
      "open": "103912.010000000",
      "high": "103988.440000000",
      "low": "103901.000000000",
      "close": "103975.120000000",
      "volume": "18.402000000",
      "tradeCount": 512,
      "revision": 1,
      "complete": true
    }
  ],
  "nextCursor": null
}
```

### Corrections

Candles are deterministic aggregates that can be revised:

- A late or backfilled trade inside the correction contract republishes the candle with an
  increased `revision` and identical window bounds.
- A gap in source data marks affected candles `complete: false` until every missing source
  sequence has been backfilled, after which they are republished `complete: true`.
- Once a candle's window falls outside the correction reach (`min(correction cutoff,
  retained windows per interval)`), it is final: the API no longer serves new revisions for
  it. Requests for history older than the reach fail with `out_of_contract` rather than
  returning candles that could still change.

Front-ends must treat `(cexId, base, quote, interval, windowOpen)` as the identity and apply
revisions by replacing the stored candle, not by appending. The `latest` endpoint and the
stream both carry `revision` and `complete` for the same purpose.

### Candle streaming

The `candles.{cexId}.{base}.{quote}.{interval}` stream channel carries two frame types:

```json
{ "type": "open", "windowOpen": "2026-07-14T09:29:00.000Z", "progressMillis": 41250, "open": "...", "last": "..." }
{ "type": "closed", "windowClose": "2026-07-14T09:30:00.000Z", "revision": 1, "complete": true, "candle": { "...": "..." } }
```

Open frames update the in-progress candle; the closed frame is the authoritative candle and
may later be followed by higher-`revision` closed frames for the same window. Replay on
resubscribe is not provided for candles: clients recover by refetching `GET /v1/candles` for
the missed range and resuming the stream from the current window.

### Data quality

- `tradeCount` and `revision` let the front-end display confidence and update badges.
- Exchange connectivity is the platform's concern; the API surfaces it only through candle
  freshness (time since the last closed candle) and the `complete` flag, not through
  connector state.
- Candle values are decimal strings in the exchange's quote asset; `volume` is in the base
  asset.

## Related references

- [Infrastructure runbook](infra.md) for the service and dependency map
- [Market data services](market-data-services.md) for the aggregation and correction contract
- [Market data](market-data.md) for the ingest pipeline
- [Environment variables](environment-variables.md) for the portfolio service and artifact
  limits referenced above
