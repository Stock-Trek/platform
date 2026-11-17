# Front-end API: accounts, portfolios, and orders

Conventions, authentication, errors, pagination, and streaming are defined in
[front-end-api.md](front-end-api.md).

## Ownership model

An owner (the JWT `owner` claim) controls zero or more exchange accounts. Ownership of
`(cexId, accountId)` is recorded by the portfolio registry and drives order authorization
everywhere: order requests for accounts an owner does not own are rejected with `not_owned`
and treated as security incidents. Ownership changes are audited and propagate through the
ownership stream to the portfolio service connectors.

Every linked account additionally needs exchange API credentials. Credentials are write-only:
they are stored in Vault, never returned by any endpoint, and rotated by replacing them.

## Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/v1/accounts` | List the owner's linked exchange accounts |
| `POST` | `/v1/accounts` | Link an exchange account and submit its credentials |
| `GET` | `/v1/accounts/{cexId}/{accountId}` | Account detail and connector status |
| `DELETE` | `/v1/accounts/{cexId}/{accountId}` | Unlink the account |
| `PUT` | `/v1/accounts/{cexId}/{accountId}/credentials` | Replace exchange credentials |
| `GET` | `/v1/accounts/{cexId}/{accountId}/portfolio` | Portfolio snapshot for the account |
| `POST` | `/v1/orders` | Submit an order for a linked account |

## Linking and listing accounts

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

## Portfolio snapshots

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

## Orders

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

## Related references

- [Front-end API](front-end-api.md)
- [Bots and runs](front-end-api-bots.md)
- [Environment variables: portfolio service](environment-variables.md#portfolio-service)
