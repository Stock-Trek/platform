# Front-end API

This document is the contract for the API the browser front-end speaks to.
It covers conventions shared by every resource group and the per-area endpoints.
Implementation details that clients must not rely on [can be found in the design notes](./API-design-notes.md).

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
It exposes the resources a user manages (accounts, portfolios, algorithms, builds, bots, runs, and backtests) and hides the services behind them.

Everything before Design notes is contract.
The design notes explain how the API is built and may change without a version bump.

## Versioning and base URL

- HTTPS only.
  All resources live under a fixed major version prefix: `/v1`.
- Changes within `/v1` are additive: new fields, endpoints, enum values, error codes, and stream frame types may appear.
  Removing or renaming fields, or changing semantics, requires `/v2`.
- Clients build request bodies from the fields this document defines and never echo response representations back.
  Unknown fields in requests are rejected with `validation_failed`.
- Clients ignore unknown response fields and tolerate unknown enum values and error codes: an unknown value is displayed generically, and an unknown `code` is handled by its HTTP status.

## Authentication and sessions

Every request carries `Authorization: Bearer <jwt>`.
Access tokens are short-lived (15 minutes) EdDSA (Ed25519) JWTs.
The JWT `owner` claim is the platform identity, and every resource in the API is scoped to it.

The owner's platform login credential (`loginId` and `loginSecret`) is used only to log in and to step up.
The front-end never stores it.
Between those moments the session is kept alive with a rotating refresh token.

| Method   | Path                  | Description                                                   |
|----------|-----------------------|---------------------------------------------------------------|
| `POST`   | `/v1/session`         | Exchange the login credential for an access and refresh token |
| `POST`   | `/v1/session/refresh` | Rotate the refresh token and obtain a new access token        |
| `POST`   | `/v1/session/step-up` | Re-authenticate to obtain a short-lived elevated token        |
| `DELETE` | `/v1/session`         | End the session                                               |

`POST /v1/session` takes the login credential:

```json
{ "loginId": "login_9f2b", "loginSecret": "..." }
```

and returns `201`:

```json
{
  "token": "eyJ...",
  "expiresAt": "2026-07-14T10:30:00.000Z",
  "refreshToken": "rt_...",
  "refreshExpiresAt": "2026-07-14T21:30:00.000Z",
  "owner": "user_7gk2",
  "scope": ["read", "manage"]
}
```

Invalid or unknown credentials fail with `401 unauthorized`.
Failed attempts are throttled per source address and per `loginId` (see [Limits](#limits)), so distributed guessing against one login is also slowed.

### Refresh

`POST /v1/session/refresh` takes `{ "refreshToken": "rt_..." }`, needs no bearer token, and returns the same body as login with a new access token and a new refresh token.

- Refresh tokens are single-use and rotate on every call.
  The session has an absolute lifetime of 12 hours (`refreshExpiresAt`), after which the user logs in again.
- A token that was just rotated is accepted again for 10 seconds and returns the same response, so two browser tabs refreshing together do not break the session.
- Any later reuse of a rotated token is treated as theft: the whole session is revoked and the call fails with `401 unauthorized`.
- The front-end keeps the refresh token in memory only.

### Scopes and step-up

Tokens carry scopes.
A token missing the scope an endpoint needs is rejected with `403 forbidden`.

| Scope    | Grants                                                                                         |
|----------|------------------------------------------------------------------------------------------------|
| `read`   | Every `GET`, stream tickets, and stream subscriptions                                          |
| `manage` | Managing algorithms, builds, bots, runs, and backtests, plus the kill switch                   |
| `admin`  | Sensitive operations: link, unlink, or re-credential an account; enable a bot; promote a build |

Login issues `read` and `manage` only.
`admin` is available only through step-up.

`POST /v1/session/step-up` requires a bearer token and takes `{ "loginSecret": "..." }`.
It returns `201` with `{ "token": "...", "expiresAt": "..." }`: an access token that additionally carries `admin` for 5 minutes.
A sensitive operation called without `admin` fails with `403 step_up_required`, which tells the front-end to prompt for the password and retry.

### Revocation

`DELETE /v1/session` revokes the presented access token and the whole refresh-token family, and returns `204`.
Every authenticated request re-checks revocation, so locally verified tokens stop working immediately after invalidation.
Open stream connections of a revoked session are closed with WebSocket close code `4401`.

### Browser access (CORS)

The API answers CORS preflights from an allowlist of origins and varies cached responses on `Origin`.

- Allowed request headers: `Authorization`, `Content-Type`, `Idempotency-Key`, `If-Match`, `X-Request-Id`.
- Exposed response headers: `X-Request-Id`, `Retry-After`, `ETag`, `Location`.

### Owner scoping

A user can only ever see and act on their own accounts, algorithms, bots, runs, and backtests.
A resource that exists but belongs to another owner is indistinguishable from a missing one: both answer `404 not_found`.
Attempts to touch another owner's resources are logged and alerted on server-side but are not revealed to the caller.
All identifiers are server-assigned and unguessable.

## Conventions

- Request and response bodies are UTF-8 JSON with `Content-Type: application/json`.
- Timestamps are RFC 3339 UTC with millisecond precision, for example `2026-07-14T09:30:00.000Z`.
  Durations are integer seconds unless a field name ends in `Millis`.
- Quantities and prices are decimal strings, for example `"1250.50"`, never JSON numbers.
- Identifiers are opaque, server-assigned strings prefixed by type: `user_`, `login_`, `account_`, `algorithm_`, `build_`, `bot_`, `run_`, `backtest_`.
- Enum values are snake_case strings.
  The lifecycle field of every resource is called `state`, and filters on it use the same name.
- Exchange codes are lowercase, such as `binance`.
  Asset identifiers are the platform's canonical names, such as `bitcoin` and `tether_u_s_d`; they are defined by the platform, not derived from exchange tickers.
  Trading pairs are `{"base": "bitcoin", "quote": "tether_u_s_d"}`.
- List endpoints paginate with `limit` (default `50`, maximum `200`) and an opaque `cursor`.
  A `limit` above the maximum is clamped.
  Responses wrap arrays as `{ "data": [...], "nextCursor": "..." | null }`.
  Lists are newest first by creation time unless an endpoint says otherwise.
- `POST` endpoints that create resources or trigger work accept an `Idempotency-Key` header.
  Retries with the same key return the original response instead of creating a duplicate.
  Reusing a key with a different request body fails with `409 idempotency_conflict`.
  Keys are scoped to the owner and endpoint and are retained for 24 hours; after that an identical request is treated as new.
  Only a hash of the request body is retained, never the body itself, so credentials are not kept.
- Mutable resources expose a `version` integer and the same value as an `ETag` header (`ETag: "7"`).
  Endpoints that require `If-Match` say so.
  A missing header fails with `428 precondition_required` and a stale one with `412 precondition_failed`.
- `PATCH` uses JSON Merge Patch (`Content-Type: application/merge-patch+json`): fields present are applied, absent fields are left alone, and arrays are replaced.
  Tagged-union objects (`schedule`, `build`) are always replaced as a whole rather than merged.
  Required fields cannot be set to `null`.
- Commit identifiers in responses are always full object IDs.
  Requests may give an abbreviation of at least 7 characters, which the server resolves; an ambiguous or unknown one fails with `validation_failed`.
- Representations backed by a stream channel carry `streamSequence`: the channel's sequence at the moment of the read.
  It is the value to pass as `lastSequence` when subscribing (see [Streaming](#streaming)).

## Errors

Errors use RFC 9457 problem details:

```json
{
  "type": "https://api.example.com/errors/not-found",
  "title": "Resource not found",
  "status": 404,
  "detail": "Account account_3kq does not exist",
  "code": "not_found",
  "requestId": "req_01j..."
}
```

| `code`                  | Status | Meaning                                                                     |
|-------------------------|--------|-----------------------------------------------------------------------------|
| `validation_failed`     | 400    | Request body or parameters are malformed or invalid                         |
| `unauthorized`          | 401    | Missing, expired, revoked, or invalid token or credential                   |
| `forbidden`             | 403    | Token lacks the required scope                                              |
| `step_up_required`      | 403    | The operation needs a recent step-up (`admin` scope)                        |
| `quota_exceeded`        | 403    | A per-owner resource-count or schedule quota would be exceeded (see Limits) |
| `not_found`             | 404    | Resource does not exist for this owner                                      |
| `already_exists`        | 409    | A resource with the same unique attributes already exists for this owner    |
| `state_conflict`        | 409    | The resource's current state forbids the action                             |
| `idempotency_conflict`  | 409    | Idempotency key reused with a different request body                        |
| `precondition_failed`   | 412    | `If-Match` does not match the current version                               |
| `payload_too_large`     | 413    | Request exceeds a size limit                                                |
| `precondition_required` | 428    | The endpoint requires `If-Match` and it was not sent                        |
| `rate_limited`          | 429    | Rate limit exhausted                                                        |
| `internal`              | 500    | Unexpected server error                                                     |
| `unavailable`           | 503    | A dependency is down; retry with backoff                                    |

The API echoes a client-supplied `X-Request-Id` or generates one, returns it in the `X-Request-Id` header of every response, and repeats it in problem details as `requestId`, so front-end error reports can be matched to server logs.
`rate_limited` responses carry a `Retry-After` header with the number of seconds to wait.

Run failures are reported inside run representations, not as HTTP errors; see [Bots and runs](#bots-and-runs).

## Streaming

Browsers cannot set `Authorization` on WebSocket upgrades, so stream connections authenticate with a short-lived single-use ticket.
`POST /v1/stream/ticket` requires a `read` bearer token and returns `{ "ticket": "...", "expiresAt": "..." }`; `GET /v1/stream?ticket=...` upgrades to a WebSocket once the ticket is redeemed.
A ticket is valid for one upgrade and expires 60 seconds after issue.

| Method | Path                | Description                         |
|--------|---------------------|-------------------------------------|
| `POST` | `/v1/stream/ticket` | Issue a single-use stream ticket    |
| `GET`  | `/v1/stream`        | WebSocket for channel subscriptions |

The client subscribes with `subscribe` actions and leaves with `unsubscribe` actions.
The server pushes JSON frames carrying a per-channel, monotonically increasing `sequence`.
Every channel sequences its frames.

```json
{ "action": "subscribe", "channel": "portfolio.account_3kq", "lastSequence": 41 }
{ "channel": "portfolio.account_3kq", "sequence": 42, "data": { "...": "..." } }
```

### Channels

| Channel                  | Frames                                                | Replay                           |
|--------------------------|-------------------------------------------------------|----------------------------------|
| `portfolio.{accountId}`  | Portfolio snapshots and order updates                 | Bounded window                   |
| `builds.{algorithmId}`   | Build state changes and promotions                    | Bounded window                   |
| `runs.{botId}`           | Run state changes                                     | Bounded window                   |
| `backtests.{backtestId}` | One frame per finished simulation, then a final frame | Full, until the backtest expires |

### Resume and gaps

- A first subscribe omits `lastSequence`.
  On the first three channel types it receives only frames published after the server accepts it.
  On a `backtests` channel it receives the stored frames from the beginning.
- To subscribe without missing anything, a client reads state over REST and subscribes with `lastSequence` set to the `streamSequence` of that read.
  Each non-backtest channel keeps a bounded replay window (the most recent 256 frames); frames after `lastSequence` that are inside the window are replayed, closing the race between the read and the subscribe.
- If `lastSequence` is older than the window, or newer than the channel's head (for example after a sequencer reset), the server sends one gap frame and then continues live: `{ "channel": "portfolio.account_3kq", "gap": { "afterSequence": 41, "headSequence": 9300, "reason": "expired" } }`.
  `reason` is `expired` or `ahead_of_head`.
  The client recovers by re-reading state over REST.
- A subscribe that would exceed the per-connection subscription limit is answered with an error frame, `{ "error": { "code": "quota_exceeded", "channel": "..." } }`, and the connection stays open.
- A subscribe to a channel the ticket's owner does not own is answered the same way with `"code": "not_found"`.
- The server pings every 30 seconds and closes connections that miss two pongs.

## Limits

Every limit below that applies to an authenticated owner comes from the owner's tier, through a backend matrix of tier to limit values, and a tier can change at any time.
The login throttles apply before an owner is identified, so they are the same for everyone.
If a tier change lowers a limit below current usage, existing resources keep working and only new creations and enables are refused.

`GET /v1/limits` (scope `read`) returns the owner's effective limits, one entry per tier-based limit in the table below (a few are shown):

```json
{
  "linkedAccounts": { "limit": 20, "used": 2 },
  "algorithms": { "limit": 100, "used": 4 },
  "bots": { "limit": 50, "used": 3 },
  "scheduledRunsPerDay": { "limit": 20000, "used": 8640 },
  "concurrentBacktests": { "limit": 2, "used": 0 },
  "streamConnections": { "limit": 5, "used": 1 }
}
```

A `quota_exceeded` error names the quota in its problem details (`quota`, `limit`, `used`) so the front-end can explain it.

| Limit                                          | Source | When exceeded                          |
|------------------------------------------------|--------|----------------------------------------|
| API requests per owner, per minute             | Tier   | `429 rate_limited`                     |
| Failed session attempts per source address     | Fixed  | `429 rate_limited`                     |
| Failed session attempts per `loginId`          | Fixed  | `429 rate_limited`                     |
| Stream connections per owner                   | Tier   | `429 rate_limited`                     |
| Concurrent stream subscriptions per connection | Tier   | Error frame with `quota_exceeded`      |
| Linked accounts per owner                      | Tier   | `403 quota_exceeded`                   |
| Algorithms per owner                           | Tier   | `403 quota_exceeded`                   |
| Bots per owner (not archived)                  | Tier   | `403 quota_exceeded`                   |
| Scheduled runs per owner per day               | Tier   | `403 quota_exceeded`                   |
| Ad-hoc runs per hour per bot                   | Tier   | `429 rate_limited`                     |
| Concurrent backtests per owner                 | Tier   | `429 rate_limited`                     |
| Maximum request body size                      | Tier   | `413 payload_too_large`                |
| Maximum backtest result size per simulation    | Tier   | Backtest fails with `result_too_large` |

The scheduled-runs quota is computed from each enabled bot's schedule, so a bot with a 10-second interval uses 8,640 of the daily scheduled-runs quota.
Creating or enabling a bot that would exceed it fails with `403 quota_exceeded`.
Count limits also fail with `403 quota_exceeded`; request-rate limits fail with `429 rate_limited`.

## Accounts and portfolios

### Ownership model

An owner (the JWT `owner` claim) controls zero or more exchange accounts.
Each linked account has a server-assigned `accountId` and an owner-chosen `label`.
Because identifiers are server-assigned and owner-scoped, a label that another user has also used reveals nothing and cannot collide with theirs.

Every linked account additionally needs exchange API credentials.
Credentials are write-only: they are never returned by any endpoint and are rotated by replacing them.

### Account endpoints

| Method   | Path                                   | Scope   | Description                                         |
|----------|----------------------------------------|---------|-----------------------------------------------------|
| `GET`    | `/v1/accounts`                         | `read`  | List the owner's linked exchange accounts           |
| `POST`   | `/v1/accounts`                         | `admin` | Link an exchange account and submit its credentials |
| `GET`    | `/v1/accounts/{accountId}`             | `read`  | Account detail and connector status                 |
| `DELETE` | `/v1/accounts/{accountId}`             | `admin` | Unlink the account                                  |
| `PUT`    | `/v1/accounts/{accountId}/credentials` | `admin` | Replace exchange credentials                        |
| `GET`    | `/v1/accounts/{accountId}/portfolio`   | `read`  | Portfolio snapshot for the account                  |
| `GET`    | `/v1/portfolios`                       | `read`  | Portfolio snapshots for every linked account        |

### Linking and listing accounts

`GET /v1/accounts` returns:

```json
{
  "data": [
    {
      "accountId": "account_3kq",
      "cexId": "binance",
      "label": "main",
      "active": true,
      "connector": { "state": "connected", "reason": null, "since": "2026-07-14T09:01:12.000Z" },
      "version": 7,
      "updatedAt": "2026-07-14T09:00:00.000Z"
    }
  ],
  "nextCursor": null
}
```

`connector` reports the state of the feed for this account so the front-end can show its health:

| `connector.state` | Meaning                                                   |
|-------------------|-----------------------------------------------------------|
| `connecting`      | Linked or re-credentialed; waiting for the first snapshot |
| `connected`       | Receiving updates                                         |
| `disconnected`    | Feed interrupted; reconnecting                            |
| `error`           | Needs the owner's attention; `reason` says why            |

`reason` is `null` unless the state is `error` or `disconnected`; values include `invalid_credentials`, `insufficient_permissions`, `exchange_unavailable`, and `unknown`.
Clients must tolerate new values.

`POST /v1/accounts` links a new account:

```json
{ "cexId": "binance", "label": "main", "apiKey": "...", "apiSecret": "..." }
```

`label` is 1 to 64 characters and unique per owner and exchange; a duplicate fails with `409 already_exists`, which only ever refers to the caller's own accounts.
The response is `201` with the account representation.

Linking is asynchronous: the account starts as `connecting`, credentials are validated in the background, and a bad key surfaces as `connector.state: "error"` with `reason: "invalid_credentials"`.
Until the first snapshot arrives, the portfolio endpoints return `asOf: null` with empty `assets` and `pendingOrders`.

`DELETE /v1/accounts/{accountId}` unlinks the account and answers `204`.
If any enabled bot references the account it fails with `409 state_conflict`, because those bots' orders would fail authorization.
With `?force=true` the unlink goes ahead and the API first disables every bot referencing the account and cancels their pending runs; running runs receive a cancel request.
The response is then `200`:

```json
{ "disabledBotIds": ["bot_4fd"], "canceledRunIds": ["run_c01"] }
```

Disabled bots keep their reference but cannot be enabled until it is replaced.

`PUT /v1/accounts/{accountId}/credentials` replaces the stored credentials and answers `204`.
The body is `{ "apiKey": "...", "apiSecret": "..." }`.
The connector goes back to `connecting`.

### Portfolio snapshots

`GET /v1/accounts/{accountId}/portfolio` returns the current view for one account:

```json
{
  "accountId": "account_3kq",
  "cexId": "binance",
  "asOf": "2026-07-14T09:59:41.120Z",
  "streamSequence": 9182,
  "assets": [
    { "asset": "bitcoin", "total": "1.250000000" },
    { "asset": "tether_u_s_d", "total": "4210.770000000" }
  ],
  "pendingOrders": [
    {
      "tag": "order_9x2",
      "symbol": { "base": "bitcoin", "quote": "tether_u_s_d" },
      "side": "sell",
      "type": "limit",
      "quantity": "0.100000000",
      "price": "104250.000000000",
      "placedAt": "2026-07-14T09:45:02.000Z"
    }
  ]
}
```

`GET /v1/portfolios` returns `{ "data": [ ...account snapshots... ], "nextCursor": null }` with one snapshot per linked account, for dashboards.

Live updates arrive on the `portfolio.{accountId}` channel.
Frames carry the snapshot body without `streamSequence`, with `sequence` in the envelope.
The snapshot's `streamSequence` is the `lastSequence` to send when subscribing or reconnecting.

## Algorithms and builds

### Algorithm and build model

- An **algorithm** is a Git repository containing trading strategy code.
  Owners connect a repository by granting the GitHub App access to it, so the API has no endpoint to create algorithms; they appear once the connection is registered.
  Pushes to watched refs trigger builds.
- A **build** is one compile of a commit.
  It produces the executable artifact used by runs and backtests.
- A **strategy** is a named algorithm inside a build.
  One build may expose several strategies.
  Strategy names are unique within a build; duplicate names fail the build.
- The **promoted build** is the single build of an algorithm that tracking bots run.
  Builds appear when code is pushed, but nothing changes for live bots until an owner promotes a build explicitly.
  A push never deploys.

### Algorithm and build lifecycle

Algorithms and builds have independent lifecycles:

| Algorithm state | Meaning                                                                                                                                                               |
|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `active`        | Builds can be triggered and strategies can be used by bots                                                                                                            |
| `disconnected`  | GitHub access was revoked. No new builds. Existing builds stay usable: pinned bots and the promoted build keep working. Returns to `active` when access is re-granted |
| `archived`      | Read-only; existing bots keep running, no new builds                                                                                                                  |

| Build state | Meaning                                                     |
|-------------|-------------------------------------------------------------|
| `queued`    | Accepted, waiting for the build agent                       |
| `building`  | Source fetched and compiling                                |
| `ready`     | Compilation finished; strategies extracted                  |
| `failed`    | Compilation or packaging failed; `error` carries the reason |

A build only becomes usable by bots when it is `ready`.
Archiving an algorithm is a soft delete: history stays queryable.

### Algorithm endpoints

| Method   | Path                                                       | Scope    | Description                                                |
|----------|------------------------------------------------------------|----------|------------------------------------------------------------|
| `GET`    | `/v1/algorithms`                                           | `read`   | List algorithms connected to the owner                     |
| `GET`    | `/v1/algorithms/{algorithmId}`                             | `read`   | Algorithm detail                                           |
| `DELETE` | `/v1/algorithms/{algorithmId}`                             | `manage` | Archive the algorithm                                      |
| `POST`   | `/v1/algorithms/{algorithmId}/promote`                     | `admin`  | Promote a ready build for tracking bots                    |
| `GET`    | `/v1/algorithms/{algorithmId}/builds`                      | `read`   | List builds, newest first, filterable by `ref` and `state` |
| `POST`   | `/v1/algorithms/{algorithmId}/builds`                      | `manage` | Trigger a build for a ref or commit                        |
| `GET`    | `/v1/algorithms/{algorithmId}/builds/{buildId}`            | `read`   | Build detail and state                                     |
| `GET`    | `/v1/algorithms/{algorithmId}/builds/{buildId}/log`        | `read`   | Build log                                                  |
| `GET`    | `/v1/algorithms/{algorithmId}/builds/{buildId}/strategies` | `read`   | Strategies exposed by the build                            |

Algorithm list items identify the connected repository:

```json
{
  "data": [
    {
      "algorithmId": "algorithm_5t1",
      "name": "momentum",
      "repository": { "provider": "github", "host": "github.com", "repo": "acme/momentum" },
      "defaultRef": "main",
      "state": "active",
      "version": 3,
      "promotedBuildId": "build_870",
      "latestReadyBuildId": "build_882"
    }
  ],
  "nextCursor": null
}
```

`promotedBuildId` is `null` until the first promotion.
`latestReadyBuildId` is informational only: it is what a promotion would most likely target, not what bots run.

### Triggering builds

`POST /v1/algorithms/{algorithmId}/builds` accepts:

```json
{ "ref": "heads/main", "commit": "e1f3a9b" }
```

`ref` uses the `heads/` or `tags/` form and defaults to the algorithm's `defaultRef`; `commit` pins an exact revision (abbreviations are resolved, see [Conventions](#conventions)) and defaults to the ref head.
The response is `202` with the build representation.

Builds are idempotent per `(ref, full commit ID)`: re-triggering an existing build returns that build, unless it is `failed`, in which case a new build is created so compilation can be retried.

`GET .../builds/{buildId}` returns:

```json
{
  "buildId": "build_882",
  "algorithmId": "algorithm_5t1",
  "ref": "heads/main",
  "commit": "e1f3a9b7c20d4e8f6a1b3c5d7e9f0a2b4c6d8e0f",
  "state": "ready",
  "error": null,
  "logAvailable": true,
  "createdAt": "2026-07-14T08:12:00.000Z",
  "completedAt": "2026-07-14T08:14:31.000Z",
  "streamSequence": 18
}
```

Build state changes are pushed on the `builds.{algorithmId}` channel.

### Build and run logs

`GET .../builds/{buildId}/log` returns `200` with `{ "url": "https://...", "expiresAt": "..." }`, a short-lived pre-signed URL the browser fetches directly.
It returns `404 not_found` when no log exists.
`logAvailable` on the build tells the front-end whether to offer the link, so it does not need a failed request to find out.
The log is how a `failed` build is diagnosed.
Run logs work the same way (see [Bots and runs](#bots-and-runs)).

The storage behind the pre-signed URL allows cross-origin reads from the allowlisted front-end origins.
The API itself answers with `200` and a JSON body rather than a redirect, because browsers send an opaque `Origin` on cross-origin redirects.

### Strategies

`GET .../strategies` lists the strategies extracted from the build:

```json
{ "data": [ { "name": "momentum_v2", "description": "Window-cross momentum" } ], "nextCursor": null }
```

Strategies are extracted once when the build finishes; a build that fails to report its strategies is treated as `failed`.

### Promoting a build

`POST /v1/algorithms/{algorithmId}/promote` takes `{ "buildId": "build_882" }`, requires `admin` and `If-Match` with the algorithm's current `version`, and returns the updated algorithm with `200`.

- The build must belong to the algorithm and be `ready`.
- If any enabled bot that tracks the promoted build runs a strategy the new build does not expose, the promotion fails with `409 state_conflict` and the problem details list `affectedBotIds`.
  The owner updates or disables those bots first.
  Promotion never silently breaks a live bot.
- Tracking bots pick the new build up at their next run.
  Every run records the build it executed.
- Promotions are announced on the `builds.{algorithmId}` channel.

## Bots and runs

### Bot and run model

A **bot** is a user's persistent trading configuration: which compiled strategy runs, against which exchange accounts, and on what schedule.
A **run** is one execution of a bot.

Scheduling works by materializing runs: every due schedule tick exists as a run record with its own state, error, and log, so history is inspectable and retries are explicit.

### Bot lifecycle

| State      | Meaning                                          |
|------------|--------------------------------------------------|
| `enabled`  | Schedule produces runs; the bot may place orders |
| `disabled` | Config kept, schedule paused, no new runs        |
| `archived` | Read-only; hidden from default lists             |

Creating a bot does not enable it: a new bot is `disabled`, and the front-end enables it explicitly (which needs `admin`) once the configuration is confirmed.

- **Enable** re-validates the account references and the scheduled-runs quota.
- **Disable** cancels the bot's `pending` runs.
  A run that is already `running` is not interrupted and finishes normally; the bot's `activeRunId` shows it.
  Use the kill switch or cancel the run to interrupt it.
- **Archive** (`DELETE`) requires the bot to be `disabled` with no active run; otherwise it fails with `409 state_conflict`.

### Bot and run endpoints

| Method   | Path                       | Scope    | Description                                  |
|----------|----------------------------|----------|----------------------------------------------|
| `POST`   | `/v1/bots`                 | `manage` | Create a bot                                 |
| `GET`    | `/v1/bots`                 | `read`   | List bots, filterable by `state`             |
| `GET`    | `/v1/bots/{botId}`         | `read`   | Bot detail                                   |
| `PATCH`  | `/v1/bots/{botId}`         | `manage` | Update configuration (`If-Match` required)   |
| `POST`   | `/v1/bots/{botId}/enable`  | `admin`  | Enable the schedule (`If-Match` required)    |
| `POST`   | `/v1/bots/{botId}/disable` | `manage` | Pause the schedule (`If-Match` required)     |
| `DELETE` | `/v1/bots/{botId}`         | `manage` | Archive the bot                              |
| `POST`   | `/v1/bots/{botId}/runs`    | `manage` | Trigger an immediate ad-hoc run              |
| `POST`   | `/v1/kill-switch`          | `manage` | Emergency stop: disable bots and cancel runs |
| `GET`    | `/v1/runs`                 | `read`   | List runs across bots, filterable            |
| `GET`    | `/v1/runs/{runId}`         | `read`   | Run detail                                   |
| `POST`   | `/v1/runs/{runId}/cancel`  | `manage` | Cancel a run                                 |
| `GET`    | `/v1/runs/{runId}/log`     | `read`   | Run log                                      |

`POST /v1/bots` example:

```json
{
  "name": "momentum-main",
  "algorithmId": "algorithm_5t1",
  "build": { "kind": "track_promoted" },
  "strategyName": "momentum_v2",
  "accountIds": ["account_3kq"],
  "schedule": { "kind": "interval", "everySeconds": 60 }
}
```

`build` is a tagged selector: `{ "kind": "pinned", "buildId": "build_882" }` runs exactly that build, and `{ "kind": "track_promoted" }` runs whatever build the algorithm currently has promoted.

The response is `201` with:

```json
{
  "botId": "bot_4fd",
  "name": "momentum-main",
  "algorithmId": "algorithm_5t1",
  "build": { "kind": "track_promoted" },
  "resolvedBuildId": "build_870",
  "strategyName": "momentum_v2",
  "accountIds": ["account_3kq"],
  "schedule": { "kind": "interval", "everySeconds": 60, "misfireGraceSeconds": 60 },
  "state": "disabled",
  "activeRunId": null,
  "version": 1,
  "createdAt": "2026-07-14T09:00:00.000Z",
  "updatedAt": "2026-07-14T09:00:00.000Z",
  "streamSequence": 0
}
```

`resolvedBuildId` is the build the next run would execute.
`activeRunId` is the bot's pending or running run, if any.

Validation rules:

- `algorithmId` must reference an `active` algorithm when the bot is created.
- A `pinned` build must belong to `algorithmId` and be `ready`.
  `track_promoted` requires the algorithm to have a promoted build.
- `strategyName` must exist in the pinned or currently promoted build.
- Every referenced account must be a linked, active account of the owner.
- `schedule.kind` is `interval` (`everySeconds` of at least 10) or `cron` (`expression` in five-field cron syntax plus `timezone`, an IANA time-zone name such as `Europe/Zurich`).
  Both accept an optional `misfireGraceSeconds` (default: the interval or 60 seconds, whichever is smaller; 60 for cron).
- The bot count and scheduled-runs quotas in [Limits](#limits) apply.

`PATCH /v1/bots/{botId}` accepts the same fields minus `algorithmId`.
Bot mutations (`PATCH`, `enable`, `disable`) require `If-Match` and return the updated bot representation.
Enabling fails with `409 state_conflict` when a referenced account is no longer linked or active, and with `403 quota_exceeded` when it would exceed the scheduled-runs quota.

A tracking bot never silently runs an older build.
If the strategy is missing from the build a run resolves, the run fails with `strategy_not_found` (a client error), and promotion is blocked beforehand where it can be detected.

### Scheduling, overlap, and missed ticks

- A bot has at most one non-terminal run (`pending` or `running`).
  A tick that falls due while a run is active is recorded as a `skipped` run with `skipReason: "overlap"`.
  It is never queued behind the active run, so a slow run cannot cause a burst of double orders afterwards.
- A tick that could not start within `misfireGraceSeconds` of its due time (for example after a platform outage) is recorded as `skipped` with `skipReason: "misfire"`.
  Missed ticks are never backfilled; the schedule resumes with the next tick.
- Ad-hoc runs follow the same exclusion: `POST /v1/bots/{botId}/runs` fails with `409 state_conflict` while a run is active.
- Cron expressions are evaluated in the bot's `timezone`.
  A local time skipped by a daylight-saving change fires once at the first instant after the gap.
  A local time that occurs twice fires only the first time.

### Runs

| State       | Meaning                                                   |
|-------------|-----------------------------------------------------------|
| `pending`   | Materialized, waiting to start                            |
| `running`   | Claimed and executing                                     |
| `completed` | Finished successfully                                     |
| `failed`    | Finished with an error; `error` carries details           |
| `canceled`  | Canceled before or during execution                       |
| `skipped`   | Tick not executed; `skipReason` is `overlap` or `misfire` |

`from` and `to` bound `scheduledAt`.
Run lists accept `botId`, `state`, `from`, and `to`, and include the `streamSequence` of the `runs.{botId}` channel when filtered by `botId`.

`GET /v1/runs?botId=bot_4fd&state=failed&from=2026-07-13T00:00:00.000Z&to=2026-07-14T00:00:00.000Z` returns:

```json
{
  "data": [
    {
      "runId": "run_b7c",
      "botId": "bot_4fd",
      "algorithmId": "algorithm_5t1",
      "buildId": "build_870",
      "buildResolution": "promoted",
      "strategyName": "momentum_v2",
      "trigger": "schedule",
      "scheduledAt": "2026-07-14T09:31:00.000Z",
      "state": "failed",
      "completedAt": "2026-07-14T09:31:44.000Z",
      "logAvailable": true,
      "error": { "kind": "client", "code": "strategy_not_found", "message": "strategy not found" }
    }
  ],
  "nextCursor": null,
  "streamSequence": 412
}
```

`trigger` is `schedule` or `ad_hoc`.
`buildResolution` is `pinned` or `promoted`, and `buildId` is the build the run actually executed.
Skipped runs have no `buildId` and carry `skipReason`.

`error.kind` tells the front-end "fix your bot" from "platform problem", and `error.code` says exactly what happened:

| `error.kind` | Codes                                                                           |
|--------------|---------------------------------------------------------------------------------|
| `client`     | `strategy_not_found`, `invalid_context`, `invalid_portfolio`, `invalid_request` |
| `platform`   | `timeout`, `panic`, `resource_limit`, `internal`                                |

Clients tolerate new codes and fall back on `kind`.

`GET /v1/runs/{runId}/log` returns `200` with `{ "url": "https://...", "expiresAt": "..." }`, a short-lived pre-signed URL the browser fetches directly, and `404 not_found` when no log exists yet.
`logAvailable` on the run says whether it does.

`POST /v1/runs/{runId}/cancel` cancels a run:

- A `pending` run becomes `canceled` immediately.
- A `running` run gets a cancel request: the response is `202` with `cancelRequested: true`, and the run ends as `canceled` at its next safe point, or finishes normally if it was already completing.
- Orders a run has already submitted are not affected.
- Canceling a terminal run is idempotent and returns it unchanged with `200`.

`POST /v1/bots/{botId}/runs` materializes a run scheduled now, subject to the ad-hoc run rate limit, and returns `202` with the run representation.
Disabled and archived bots answer `409 state_conflict`; ad-hoc runs never bypass a paused schedule.

Run state changes are pushed on the `runs.{botId}` channel.

### Kill switch

`POST /v1/kill-switch` is the emergency stop.
It needs only `manage` (no step-up and no `If-Match`, so it works instantly) and is idempotent by nature:

```json
{ "accountIds": ["account_3kq"] }
```

`accountIds` is optional.
Without it the kill switch applies to every bot of the owner; with it, to bots referencing any of those accounts.
It disables the affected bots, cancels their pending runs, and sends a cancel request to their running runs.
Orders that were already submitted are left as they are.
The response is `202`:

```json
{ "disabledBotIds": ["bot_4fd"], "canceledRunIds": ["run_c01"] }
```

### Retention

Runs and their logs are retained for 90 days after a run reaches a terminal state and are deleted afterwards; `skipped` runs are retained for 7 days.
A deleted run disappears from lists and answers `404 not_found`.
Retention, together with the scheduled-runs quota, bounds the run store, because scheduling materializes one record per tick.

## Backtesting

### Backtest model

A **backtest** is an ad-hoc, resource-bounded experiment: it runs one strategy from one build many times over archived market data with simulated execution friction and a seeded random generator, streaming per-simulation results as they finish and finishing with a collated result set.
Backtests never touch live exchanges or real balances.

### Backtest lifecycle

| State       | Meaning                                                                        |
|-------------|--------------------------------------------------------------------------------|
| `queued`    | Accepted, waiting for a worker                                                 |
| `running`   | Simulations executing; partial results may be streaming                        |
| `completed` | All simulations finished; final results available                              |
| `failed`    | Rejected or aborted; `error` carries details. Results finished so far are kept |
| `canceled`  | Canceled by the owner. Results finished so far are kept                        |

### Backtest endpoints

| Method   | Path                                        | Scope    | Description                                             |
|----------|---------------------------------------------|----------|---------------------------------------------------------|
| `POST`   | `/v1/backtests`                             | `manage` | Create and start a backtest                             |
| `GET`    | `/v1/backtests`                             | `read`   | List the owner's backtests, filterable by `state`       |
| `GET`    | `/v1/backtests/{backtestId}`                | `read`   | Backtest status                                         |
| `POST`   | `/v1/backtests/{backtestId}/cancel`         | `manage` | Cancel a queued or running backtest                     |
| `DELETE` | `/v1/backtests/{backtestId}`                | `manage` | Delete a finished backtest and its results              |
| `GET`    | `/v1/backtests/{backtestId}/results`        | `read`   | Results finished so far, paginated                      |
| `GET`    | `/v1/backtests/{backtestId}/results/export` | `read`   | Pre-signed download of all results (finished backtests) |

Live results arrive on the `backtests.{backtestId}` stream channel; see [Streaming](#streaming).

### Creating a backtest

`POST /v1/backtests` example:

```json
{
  "algorithmId": "algorithm_5t1",
  "buildId": "build_882",
  "strategyName": "momentum_v2",
  "seed": 42
}
```

The response is `202` with `{ "backtestId": "backtest_31a", "state": "queued" }`.

| Field                                    | Required | Meaning                                                                                               |
|------------------------------------------|----------|-------------------------------------------------------------------------------------------------------|
| `algorithmId`, `buildId`, `strategyName` | yes      | The strategy to simulate. `buildId` is a concrete build, never a selector.                            |
| `seed`                                   | no       | Integer seed. If omitted, the server picks one and returns it in the status, so runs are reproducible |

Validation fails fast with `validation_failed`:

- `buildId` must be `ready`, and `strategyName` must exist in it.

No more than the per-owner concurrent backtest limit may be `queued` or `running` at once; a further creation returns `429 rate_limited` with `Retry-After`.

### Status

`GET /v1/backtests/{backtestId}` returns:

```json
{
  "backtestId": "backtest_31a",
  "algorithmId": "algorithm_5t1",
  "buildId": "build_882",
  "strategyName": "momentum_v2",
  "state": "running",
  "error": null,
  "seed": 42,
  "progress": { "completed": 37, "total": 100 },
  "createdAt": "2026-07-14T09:00:00.000Z",
  "startedAt": "2026-07-14T09:00:04.000Z",
  "completedAt": null,
  "expiresAt": null,
  "streamSequence": 37
}
```

`expiresAt` is set once the backtest is terminal and says when it will be deleted.

### Results and streaming

Per-simulation results are JSON objects produced by the strategy's simulation runtime.
Each finished simulation is assigned the next `sequence` of the backtest, and it is delivered twice: incrementally on the stream, and through the paginated results endpoint.

`GET /v1/backtests/{backtestId}/results` is readable in every state and returns the results finished so far.
This is the one list that is oldest first, in `sequence` order, so `cursor` pages forward through a growing set:

```json
{
  "data": [
    { "sequence": 1, "simulation": 13, "result": { "finalEquity": "10412.500000000", "...": "..." } }
  ],
  "nextCursor": null,
  "state": "completed",
  "complete": true
}
```

`complete` is `true` only when the backtest is `completed`; for `failed` and `canceled` backtests the results finished so far stay readable and `complete` stays `false`.
The full set can be large, so clients page through it or use the export instead.

`GET .../results/export` returns `200` with `{ "url": "https://...", "expiresAt": "...", "format": "ndjson" }`, a short-lived pre-signed URL of all results as newline-delimited JSON.
It fails with `409 state_conflict` until the backtest is terminal.

On the `backtests.{backtestId}` channel, frames are `{ "channel": "backtests.backtest_31a", "sequence": 7, "simulation": 13, "result": { ... } }`, and the frame's `sequence` equals the item's `sequence` in the results endpoint.
Unlike other channels, this one replays: a subscribe without `lastSequence` receives the stored frames from the start, and one with `lastSequence` receives the stored frames after it.
When the backtest reaches a terminal state the server sends one last frame, `{ "channel": "...", "sequence": 101, "complete": true, "state": "completed" }`, which carries no result, and no frames follow it.
A client that subscribes after that point still receives the stored frames and the final frame.
On `completed` the client can fetch the collated set over REST.

### Canceling and deleting

`POST /v1/backtests/{backtestId}/cancel` cancels a queued or running backtest and returns `202` with the status representation (state `canceled` once workers have stopped).
On a terminal backtest it is idempotent and returns the status unchanged with `200`.

`DELETE /v1/backtests/{backtestId}` deletes a terminal backtest and its results and returns `204`; on a `queued` or `running` backtest it fails with `409 state_conflict`.

A simulation result larger than the per-simulation limit fails the backtest with `error.code: "result_too_large"`; results finished before that remain readable.
Backtests and their results are retained for 30 days after a backtest reaches a terminal state and are deleted afterwards; a deleted backtest disappears from lists and answers `404 not_found`.
