# Front-end API

This document defines the API that the browser front-end speaks to. It covers the conventions
shared by every resource group and links to the per-area documents:

- [Accounts, portfolios, and orders](front-end-api-accounts.md)
- [Algorithms and builds](front-end-api-algorithms.md)
- [Bots and runs](front-end-api-bots.md)
- [Backtesting](front-end-api-backtests.md)
- [Market data](front-end-api-market-data.md)

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
- Identifiers are opaque strings prefixed by type: `usr_`, `bot_`, `run_`, `bld_`, `bt_`.
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

## Related references

- [Accounts, portfolios, and orders](front-end-api-accounts.md)
- [Algorithms and builds](front-end-api-algorithms.md)
- [Bots and runs](front-end-api-bots.md)
- [Backtesting](front-end-api-backtests.md)
- [Market data](front-end-api-market-data.md)
- [Infrastructure runbook](infra.md) for the service and dependency map
