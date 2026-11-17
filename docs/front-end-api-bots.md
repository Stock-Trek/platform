# Front-end API: bots and runs

Conventions, authentication, errors, pagination, and streaming are defined in
[front-end-api.md](front-end-api.md).

## Model

A **bot** is a user's persistent trading configuration: which compiled strategy runs, with
which parameters, against which exchange accounts, and on what schedule. A **run** is one
execution of a bot. Scheduling works by materializing runs: every due schedule tick exists as
a run record with its own status, error, and log, so history is inspectable and retries are
explicit.

Bot configuration is stored by the front-end API. Scheduling writes run records to the run
store (DynamoDB); the run invoker claims due runs and invokes the runner, which downloads the
pinned cwasm artifact, executes the strategy, submits signed order requests, and uploads run
logs.

## Bot lifecycle

| State | Meaning |
| --- | --- |
| `enabled` | Schedule produces runs; the bot may place orders |
| `disabled` | Config kept, schedule paused, no new runs |
| `archived` | Read-only; hidden from default lists |

Creating a bot does not enable it implicitly: the first creation returns `disabled` and the
front-end enables explicitly once parameters are confirmed.

## Endpoints

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

## Runs

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

## Related references

- [Front-end API](front-end-api.md)
- [Algorithms and builds](front-end-api-algorithms.md)
- [Backtesting](front-end-api-backtests.md)
