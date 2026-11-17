# Front-end API: backtesting

Conventions, authentication, errors, pagination, and streaming are defined in
[front-end-api.md](front-end-api.md).

## Model

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

## Lifecycle

| State | Meaning |
| --- | --- |
| `queued` | Accepted, waiting for a worker |
| `running` | Simulations executing; partial results may be streaming |
| `completed` | All simulations finished; final results available |
| `failed` | Rejected or aborted; `error` carries details |

## Endpoints

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

## Results and streaming

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

## Initial balances

`initialBalances` seeds the simulated portfolio. When omitted, the backtest starts from a
configurable default cash balance. Using a real linked account's current balances is a
front-end convenience: it copies the read-model snapshot into the request, never grants the
backtest access to the live account.

## Related references

- [Front-end API](front-end-api.md)
- [Bots and runs](front-end-api-bots.md)
- [Market data](front-end-api-market-data.md) for candle retention and corrections
- [Market data services](market-data-services.md) for the correction contract
