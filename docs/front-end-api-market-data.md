# Front-end API: market data

Conventions, authentication, errors, pagination, and streaming are defined in
[front-end-api.md](front-end-api.md).

## Model

The market data plane ingests exchange trades, aggregates them into candles at a fixed set of
aligned intervals, corrects candles when late or backfilled trades arrive, and archives
everything to Iceberg. The front-end reads two views:

- **Closed candles** from the archive, queryable history with revision tracking.
- **Live candles** over the stream: open candle updates as they tick and closed candle events.

Supported exchanges and assets are static platform enumerations (`CexId`, `AssetId`), so the
reference endpoints are cheap and highly cacheable. Candle intervals are the platform's
aligned windows (for example `1s`, `1m`, `5m`, `1h`, `1d`).

## Endpoints

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

## Corrections

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

## Streaming

The `candles.{cexId}.{base}.{quote}.{interval}` stream channel carries two frame types:

```json
{ "type": "open", "windowOpen": "2026-07-14T09:29:00.000Z", "progressMillis": 41250, "open": "...", "last": "..." }
{ "type": "closed", "windowClose": "2026-07-14T09:30:00.000Z", "revision": 1, "complete": true, "candle": { "...": "..." } }
```

Open frames update the in-progress candle; the closed frame is the authoritative candle and
may later be followed by higher-`revision` closed frames for the same window. Replay on
resubscribe is not provided for candles: clients recover by refetching `GET /v1/candles` for
the missed range and resuming the stream from the current window.

## Data quality

- `tradeCount` and `revision` let the front-end display confidence and update badges.
- Exchange connectivity is the platform's concern; the API surfaces it only through candle
  freshness (time since the last closed candle) and the `complete` flag, not through
  connector state.
- Candle values are decimal strings in the exchange's quote asset; `volume` is in the base
  asset.

## Related references

- [Front-end API](front-end-api.md)
- [Backtesting](front-end-api-backtests.md)
- [Market data services](market-data-services.md) for the aggregation and correction contract
- [Market data](market-data.md) for the ingest pipeline
