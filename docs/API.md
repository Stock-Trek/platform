# Front-end API

This document defines the API that the browser front-end speaks to.
It covers conventions shared by every resource group and the per-area endpoints:

- [Overview](#overview)
- [Versioning and base URL](#versioning-and-base-url)
- [Authentication and sessions](#authentication-and-sessions)
- [Conventions](#conventions)
- [Errors](#errors)
- [Streaming](#streaming)
- [Limits](#limits)
- [Accounts and portfolios](#accounts-and-portfolios)
- [Algorithms and builds](#algorithms-and-builds)
- [Bots and runs](#bots-and-runs)
- [Backtesting](#backtesting)

## Overview

The front-end talks to exactly one platform component: the front-end API.
The API is a new service that composes the existing planes and hides their internals from the browser.
It owns the resources that no existing service owns today (bots, backtests) and exposes reads and actions over the stores and streams that already exist.

```
Browser ⇄ Front-end API ⇄ Auth plane        (sessions, JWT verification)
                       ⇄ Portfolio registry (account ownership)
                       ⇄ Portfolio service  (read model)
                       ⇄ Build plane        (algorithms, build artifacts)
                       ⇄ Run store/invoker  (scheduled runs)
                       ⇄ Runner             (live runs, backtests)
```

The API introduces two new durable stores: a bot configuration store and a backtest store.
Everything else is a projection of existing state: the ownership registry, the portfolio read model, build artifacts, and the run store.

## Versioning and base URL

- HTTPS only.
  All resources live under a fixed major version prefix: `/v1`.
- Changes within `/v1` are additive: new fields may appear in responses, new endpoints may appear.
  Removing or renaming fields, or changing semantics, requires `/v2`.
- Clients build request bodies from the fields this document defines and never echo response representations back.
  Unknown fields in requests are rejected with `validation_failed`; unknown fields in responses are ignored by clients.

## Authentication and sessions

Every request carries `Authorization: Bearer <jwt>`.
Tokens are short-lived EdDSA (Ed25519) JWTs signed by the auth service; the API verifies them locally against the auth service JWKS (`/.well-known/jwks.json`) with a bounded key cache.
Token minting goes through the auth service, which only mints a token for an owner it can authenticate: every minting request must be signed with the login credential bound to the requested owner, and the API signs with the login credential the user presented, so it cannot mint a token for an owner it has not authenticated.
The request-signing scheme used between the API and the auth service is an internal platform detail and never reaches the browser.
Tokens carry a single scope that grants access to every endpoint in this document; `forbidden` means the presented token does not carry it.
`DELETE /v1/session` revokes the presented token server-side, and every authenticated request re-checks revocation, so locally verified tokens stop working immediately after invalidation.
Because the client is a browser, the API answers CORS preflights from an allowlist of origins, accepts the `Authorization` and `Idempotency-Key` request headers, and varies cached responses on `Origin`.

The JWT `owner` claim is the platform identity.
Every resource in the API is scoped to the owner: a user can only ever see and act on their own accounts, algorithms, bots, runs, and backtests.
Server-assigned identifiers (`alg_`, `bld_`, `bot_`, `run_`, `bt_`) are not guessable; account identifiers are chosen by the owner, so every request re-checks ownership.

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/v1/session` | Exchange a platform login credential for a short-lived API token |
| `DELETE` | `/v1/session` | Invalidate the presented token |

`POST /v1/session` takes the owner's platform login credential, an owner-bound credential pair issued by the platform when the owner is onboarded.

```json
{ "loginId": "lgn_9f2b", "loginSecret": "..." }
```

Invalid or unknown credentials fail with `401 unauthorized`, and failed attempts are rate limited per source address (see [Limits](#limits)).

`POST /v1/session` returns `201` with:

```json
{ "token": "eyJ...", "expiresAt": "2026-07-14T10:30:00.000Z", "owner": "usr_7gk2" }
```

Tokens are refreshed by calling `POST /v1/session` again before `expiresAt`.

## Conventions

- Request and response bodies are UTF-8 JSON with `Content-Type: application/json`.
- Timestamps are RFC 3339 UTC with millisecond precision, for example `2026-07-14T09:30:00.000Z`.
  Durations are integer seconds unless a field name ends in `_millis`.
- Quantities and prices are decimal strings, for example `"1250.50"`, never JSON numbers.
- Identifiers are opaque strings prefixed by type: `usr_`, `lgn_`, `alg_`, `bld_`, `bot_`, `run_`, `bt_`, `ord_`.
- Enum values are snake_case strings.
- Exchange and asset identifiers use the platform's canonical codes: `CexId` values such as `binance` and `AssetId` codes such as `btc` and `usdt`.
  Trading pairs are `{"base": "btc", "quote": "usdt"}`.
- List endpoints paginate with `limit` (default `50`, maximum `200`) and an opaque `cursor`.
  A `limit` above the maximum is clamped to the maximum.
  Responses wrap arrays as `{ "data": [...], "nextCursor": "..." | null }`.
  Lists are newest first by creation time unless an endpoint says otherwise.
- `POST` endpoints that create resources or trigger work accept an `Idempotency-Key` header.
  Retries with the same key return the original response instead of creating a duplicate.
  Reusing a key with a different request body fails with `409 conflict`.
  Keys are scoped to the owner and endpoint and are remembered for 24 hours; a retry after that window may create a duplicate.
- Mutable resources expose a `version` integer.
  Mutations may be conditioned with `If-Match: <version>` and fail with `409 conflict` when the version does not match; endpoints that require it say so below.
- Account identifiers may contain only lowercase letters, digits, `-`, and `_`, because they are embedded in stream channel names and URL paths.

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
| `conflict` | 409 | Version mismatch, duplicate resource, or resource state that forbids the action |
| `stale` | 409 | Portfolio data is too old to authorize the action |
| `out_of_contract` | 409 | Candle correction reach exceeded the retention contract |
| `payload_too_large` | 413 | Request, result, or referenced artifact exceeds a size limit |
| `rate_limited` | 429 | Rate limit or quota exhausted |
| `internal` | 500 | Unexpected server error |
| `unavailable` | 503 | Dependency is down; retry with backoff |

Every response carries an `X-Request-Id` header: the API echoes a client-supplied `X-Request-Id` or generates one, and problem details repeat it as `requestId` so front-end error reports can be matched to server logs.

`stale` mirrors the portfolio service staleness policy (`allow`, `degrade`, `reject`) and applies where an action authorizes against portfolio data: enabling a bot and triggering an ad-hoc run.
In `degrade` mode accepted-but-stale actions are flagged: the response carries `stale: true` with the `asOf` of the snapshot the action was authorized against, so the front-end can warn.
`rate_limited` responses carry a `Retry-After` header with the number of seconds to wait before retrying.

## Streaming

Browsers cannot set `Authorization` on WebSocket upgrades, so stream connections authenticate with a short-lived single-use ticket: `POST /v1/stream/ticket` requires a bearer token and returns `{ "ticket": "...", "expiresAt": "..." }`, and `GET /v1/stream?ticket=...` upgrades to a WebSocket after the ticket is redeemed.
Tickets are valid for one upgrade before `expiresAt`, which is 60 seconds after issue.
The client subscribes to channels with `subscribe` actions and leaves them with `unsubscribe` actions; the server pushes JSON frames with a per-channel monotonically increasing `sequence`:

```json
{ "action": "subscribe", "channel": "portfolio.binance.main", "lastSequence": 41 }
{ "channel": "portfolio.binance.main", "sequence": 42, "data": { "...": "..." } }
```

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/v1/stream/ticket` | Issue a single-use stream ticket |
| `GET` | `/v1/stream` | WebSocket for channel subscriptions |

The server never replays history on any channel.
A first subscribe omits `lastSequence` and receives only frames published after the server accepts it.
A `lastSequence` older than the channel's current head is answered with a single gap frame, `{ "channel": "portfolio.binance.main", "gap": { "afterSequence": 41 } }`, and live frames continue after it; the client recovers by re-fetching state over REST (a portfolio snapshot for `portfolio` channels, build and run detail for the others).
A subscribe that would exceed the per-connection subscription limit is rejected with an error frame and the connection stays open.
A subscribe to a channel the ticket's owner does not own is rejected the same way.
The server pings every 30 seconds and closes connections that miss two pongs.
Channels:

| Channel | Frames |
| --- | --- |
| `portfolio.{cexId}.{accountId}` | Portfolio snapshots and order updates |
| `builds.{algorithmId}` | Build state changes |
| `runs.{botId}` | Run state changes |

## Limits

| Limit | Value |
| --- | --- |
| Concurrent backtests per owner | 2 |
| Failed session attempts per source address | 10 per 15 minutes |
| Ad-hoc runs per hour per bot | 12 |
| API requests per owner | 120 per minute |
| Concurrent stream subscriptions per connection | 32 |
| Maximum request body size | 1 MiB |
| Referenced build artifact size | `S3_MAX_ARTIFACT_SIZE_BYTES` |
| Maximum backtest result size per simulation | 64 KiB |

## Accounts and portfolios

### Ownership model

An owner (the JWT `owner` claim) controls zero or more exchange accounts.
Ownership of `(cexId, accountId)` is recorded by the portfolio registry and drives authorization everywhere: requests for accounts an owner does not own are rejected with `not_owned` and treated as security incidents.
Ownership changes are audited and propagate through the ownership stream to the portfolio service connectors.

Every linked account additionally needs exchange API credentials.
Credentials are write-only: they are stored in Vault, never returned by any endpoint, and rotated by replacing them.

### Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/v1/accounts` | List the owner's linked exchange accounts |
| `POST` | `/v1/accounts` | Link an exchange account and submit its credentials |
| `GET` | `/v1/accounts/{cexId}/{accountId}` | Account detail and connector status |
| `DELETE` | `/v1/accounts/{cexId}/{accountId}` | Unlink the account |
| `PUT` | `/v1/accounts/{cexId}/{accountId}/credentials` | Replace exchange credentials |
| `GET` | `/v1/accounts/{cexId}/{accountId}/portfolio` | Portfolio snapshot for the account |
| `GET` | `/v1/portfolios` | Portfolio snapshots for every linked account |

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

`connector` reports the portfolio connector state for the exchange (`connected`, `disconnected`) so the front-end can show feed health.

`POST /v1/accounts` links a new account:

```json
{ "cexId": "binance", "accountId": "main", "apiKey": "...", "apiSecret": "..." }
```

The response is `201` with the account representation.
Linking is asynchronous downstream: the registry records the ownership mapping and the ownership stream wires the account into the portfolio connector.
`connector` stays `disconnected` until the first snapshot arrives; until then the portfolio endpoints return `asOf: null` with empty `assets` and `pendingOrders`, and the front-end shows feed health from `connector`.

`DELETE /v1/accounts/{cexId}/{accountId}` unlinks the account.
The response is `204`.
Bots that still reference the account keep running but their order requests fail authorization, so the API responds with `409 conflict` if any enabled bot references the account unless the request carries `?force=true`.

`PUT .../credentials` replaces the stored credentials and triggers connector credential rotation.
The response is `204`.
The body is identical to the credential fields of `POST /v1/accounts`.

### Portfolio snapshots

`GET /v1/accounts/{cexId}/{accountId}/portfolio` returns the current read-model view for one account.
The read model is rebuilt from the portfolio update stream, so snapshots carry the stream sequence they were built from:

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

`GET /v1/portfolios` returns `{ "data": [ ...account snapshots... ], "nextCursor": null }` with one account snapshot per linked account, for dashboards.

Live updates arrive on the `portfolio.{cexId}.{accountId}` stream channel.
The channel frames carry the same body as the snapshot endpoint with the stream `sequence` in the envelope, and the snapshot's `sequence` is the `lastSequence` to send on reconnect.

## Algorithms and builds

### Model

- An **algorithm** is a Git repository containing trading strategy code.
  The build plane watches the repository through the GitHub App; pushes to watched refs trigger builds.
  Owners connect a repository by granting the GitHub App access to it, so the API has no endpoint to create algorithms; they appear once the build plane registers the connection.
- A **build** is one compile of a commit.
  It produces a compiled cwasm artifact in the artifact bucket together with its SHA-256, size, and the Wasmtime version required to run it.
  Artifact bytes are never proxied through the API; the runner fetches them directly.
- A **strategy** is a named algorithm inside a build.
  One build may expose several strategies.
  Strategy names are unique within a build; duplicate names fail the build.
- **Preferences** are the strategy's tunable parameters, described by a JSON schema the build exposes.
  Bots reference strategies by name and carry parameter values validated against this schema.

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
| `ready` | Artifact stored with SHA-256 and Wasmtime version; strategies and preference schemas extracted |
| `failed` | Compilation or packaging failed; `error` carries the reason |

A build only becomes usable by bots when it is `ready`.
Archiving an algorithm is a soft delete: history stays queryable.

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
      "version": 3,
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

`ref` uses the `heads/` or `tags/` form used by the build plane's source keys and defaults to the algorithm's `defaultRef`; `commit` pins an exact revision and defaults to the ref head.
The response is `202` with the build representation.
Builds are idempotent per `(ref, commit)`: re-triggering an existing build returns that build, unless it is `failed`, in which case a new build is created so compilation can be retried.

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

`GET .../strategies` lists the strategies extracted from the build:

```json
{ "data": [ { "name": "momentum_v2", "description": "Window-cross momentum" } ], "nextCursor": null }
```

`GET .../strategies/{strategyName}/preferences` returns the JSON schema bots must satisfy in their `parameters` object.
The schema is what the front-end renders as a parameter form.
Metadata is extracted once when the build finishes, by running the artifact's metadata command in the sandbox; a build that fails to report strategies and preference schemas is treated as `failed`, and these endpoints serve the stored metadata.

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

Bots pick the build and strategy they run; they may pin an exact `buildId` or track an algorithm's `latestReadyBuildId`.
Tracking upgrades happen at run time, not at update time: the bot re-validates `parameters` against the new build's preference schema on every run and falls back to the previously used build when validation fails.

## Bots and runs

### Model

A **bot** is a user's persistent trading configuration: which compiled strategy runs, with which parameters, against which exchange accounts, and on what schedule.
A **run** is one execution of a bot.
Scheduling works by materializing runs: every due schedule tick exists as a run record with its own status, error, and log, so history is inspectable and retries are explicit.

Bot configuration is stored by the front-end API.
Scheduling writes run records to the run store (DynamoDB); the run invoker claims due runs and invokes the runner, which downloads the pinned cwasm artifact, executes the strategy, submits signed order requests, and uploads run logs.

### Bot lifecycle

| State | Meaning |
| --- | --- |
| `enabled` | Schedule produces runs; the bot may place orders |
| `disabled` | Config kept, schedule paused, no new runs |
| `archived` | Read-only; hidden from default lists |

Creating a bot does not enable it implicitly: the first creation returns `disabled` and the front-end enables explicitly once parameters are confirmed.

### Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/v1/bots` | Create a bot |
| `GET` | `/v1/bots` | List bots, filterable by `state` |
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

- `algorithmId` must reference an `active` algorithm when the bot is created.
- `buildId` must be `latest` or belong to `algorithmId` and be in state `ready`.
- `strategyName` must exist in that build.
- `parameters` must satisfy the strategy's preference schema.
- Every referenced account must be a linked, active account of the owner.
- `schedule.kind` is `interval` (`everySeconds` ≥ 10) or `cron` (`expression` in five-field cron syntax plus `timezone`, an IANA time-zone name such as `Europe/Zurich`).

`PATCH /v1/bots/{botId}` accepts the same fields minus `algorithmId`, applies only the fields present in the body, and re-validates `parameters` when the build or strategy changes, failing with `400 validation_failed` on mismatch.
Bot mutations — `PATCH`, `enable`, `disable` — require `If-Match` with the current `version` and return the updated bot representation.
Enabling re-checks the account references against current state and fails with `400 validation_failed` when a referenced account is no longer linked or active.

A `buildId` of `latest` tracks the algorithm's `latestReadyBuildId`: every run resolves the current latest ready build and re-validates `parameters` against its preference schema, falling back to the previous build when validation fails.
A concrete `buildId` pins the bot to that build.
Runs record the `buildId` they executed.

### Runs

Runs follow the run store lifecycle:

| State | Meaning |
| --- | --- |
| `pending` | Materialized, waiting for the invoker |
| `running` | Claimed and executing in the runner |
| `completed` | Finished successfully |
| `failed` | Finished with an error; `error` carries details |

`from` and `to` bound `scheduledAt`.

`GET /v1/runs?botId=bot_4fd&status=failed&from=2026-07-13T00:00:00.000Z&to=2026-07-14T00:00:00.000Z` returns:

```json
{
  "data": [
    {
      "runId": "run_b7c",
      "botId": "bot_4fd",
      "algorithmId": "alg_5t1",
      "buildId": "bld_882",
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

`error.code` distinguishes client errors (bad strategy name, undecodable context or portfolio, invalid request) from server errors (timeouts, panics, resource limits, artifact download failures) using the runner exit-code taxonomy, so the front-end can tell "fix your bot" from "platform problem".

`GET /v1/runs/{runId}/log` returns `302` to a pre-signed log URL when the log exists and `404` while the run is still pending or before upload completes.
Pre-signed URLs expire with `S3_SIGNED_URL_EXPIRY_SECS` and are fetched by the browser directly from object storage, so the log bucket allows cross-origin reads from the allowlisted front-end origins.

Run state changes are pushed on the `runs.{botId}` stream channel.

`POST /v1/bots/{botId}/runs` materializes a run scheduled now, subject to the ad-hoc run rate limit, and returns `202` with the run representation.
Disabled and archived bots answer `409 conflict`; ad-hoc runs never bypass a paused schedule.

Runs and their logs are retained for 90 days after a run reaches a terminal state and are deleted afterwards; a deleted run disappears from lists and answers `not_found`.
Retention is what bounds the run store, because scheduling materializes one record per tick.

## Backtesting

### Model

A **backtest** is an ad-hoc, resource-bounded experiment: it runs one strategy from one build many times over archived market data with simulated execution friction and a seeded random generator, streaming per-simulation results as they finish and finishing with a collated result set.
Backtests never touch live exchanges or real balances.

Market input comes from the candle archive, so backtests are subject to the archive's correction contract: requested history must lie within the retention reach of the requested intervals, otherwise creation fails with `out_of_contract` rather than silently producing candles that later change.

Backtest definitions and results are stored by the front-end API.
Execution uses the sandbox runner's backtest subsystem, which already enforces worker limits, watchdog timeouts, and memory limits.

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

- `buildId` must be `ready`, and `strategyName` must exist in it; `parameters` must satisfy the strategy's preference schema.
- `tradingPairs` and `windows` must be non-empty and are de-duplicated; windows must be supported aligned intervals.
- `openCandleProgressMillis`, when given, must be smaller than the shortest requested window.
- Every chance field is a decimal string in `[0, 1]`.
- `count` is at least 1 and at most 1000; `maxConcurrent` is clamped to the runner's worker limits.
- No more than the per-owner concurrent backtest limit in [Limits](#limits) may be `queued` or `running` at once; a further creation returns `429 rate_limited` with `Retry-After`.
- A simulation result larger than the per-simulation limit in [Limits](#limits) fails the backtest.

`from` and `to` bound the archived candles fetched for every requested pair and window, and each strategy sees the last `historicalCandleCount` closed candles before every simulation step.

### Results and streaming

Per-simulation results are JSON objects produced by the strategy's simulation runtime.
They are delivered twice: incrementally on the stream as each simulation finishes, and collated as an array at completion.

`GET /v1/backtests/{backtestId}/results` returns the collated array; `results` stays empty until the backtest reaches `completed`:

```json
{
  "backtestId": "bt_31a",
  "state": "completed",
  "simulationCount": 100,
  "results": [ { "simulation": 0, "finalEquity": "10412.500000000", "...": "..." } ]
}
```

`GET /v1/backtests/{backtestId}/stream?ticket=...` upgrades to a WebSocket, using a stream ticket as described in [Streaming](#streaming).
Frames are `{ "sequence": 7, "simulation": 13, "result": { ... } }` with `sequence` increasing monotonically for the backtest across connections; a `complete` frame with no `simulation` field marks completion and carries no results; clients then fetch the collated set over REST.
The stream ends when the backtest reaches a terminal state; reconnecting clients pass `?cursor=<sequence>` for the last frame they saw and receive the stored results after it, and clients that connect after the backtest finished receive the `complete` frame and the stream closes.

`DELETE /v1/backtests/{backtestId}` cancels queued and running backtests (`204`) and is idempotent on terminal backtests.
Backtests and their results are retained for 30 days after a backtest reaches a terminal state and are deleted afterwards; a deleted backtest disappears from lists and answers `not_found`.

### Initial balances

`initialBalances` seeds the simulated portfolio; its `cexId` and `accountId` are labels for the simulation and are not checked against linked accounts.
When omitted, the backtest starts from a configurable default cash balance.
Using a real linked account's current balances is a front-end convenience: it copies the read-model snapshot into the request, never grants the backtest access to the live account.

## Related references

- [Infrastructure runbook](infra.md) for the service and dependency map
- [Market data services](market-data-services.md) for the aggregation and correction contract
- [Market data](market-data.md) for the ingest pipeline
- [Environment variables](environment-variables.md) for the portfolio service and artifact limits referenced above
- [Health checks](health-checks.md) for the platform probe contract the API follows
