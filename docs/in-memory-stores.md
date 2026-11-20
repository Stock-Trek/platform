# In-memory store inventory

Every store that currently lives only in process memory, with replacement options and their trade-offs.
Use this to decide which stores move to an external system, in which order, and which should stay in memory.

## External stores the platform already runs

Any replacement should prefer these; introducing a new system is an option only where none of these fits.

| System | Currently used for |
| --- | --- |
| NATS JetStream | market events, portfolio updates/ownership, build requests |
| NATS KV | ingester sequences, aggregator checkpoints, portfolio read model, leases |
| S3 + Iceberg catalog | raw and compacted archives, build/run artifacts, logs |
| DynamoDB | portfolio ownership registry, scheduled run records |
| Valkey | order replay protection, token-usage queue |
| Vault | auth tokens, exchange credentials |
| SQS | build fan-out, auth audit delivery |
| SSM Parameter Store | webhook and backfiller secrets |

## How each store is judged

- **Durability:** does data survive process restart, or is it reconstructible from an external source?
- **Replica visibility:** do two replicas of the service need to agree on the content?
- **Retention and size:** is the data bounded and short-lived, or does it grow with users and history?
- **Access pattern:** point reads/writes, ranges, scans, or pub/sub?

The verdicts used below: **externalize** (system of record in memory is a real defect), **checkpoint** (in-memory working set is fine because an external durable copy exists), **cache** (in front of an external authority; replacement optional), **process-local** (in-process by design; moving it is usually wrong), **best-effort** (deliberately lossy).

## Front-end API stores

The front-end API (`crates/control-plane/front-end-api`) is a single long-running process that keeps its entire domain in `Mutex`-protected maps. It is the highest-priority cluster: it is a system of record, it loses everything on restart, and two replicas would diverge silently.

### AccountStore — `accounts/account_store.rs`

Linked exchange accounts, exchange API credentials in plaintext, and portfolio snapshots.

- **Replace with DynamoDB.** Same table family as the ownership registry; conditional writes for label uniqueness; credentials encrypted with a KMS key or re-homed in Vault.
  Pros: single-digit-ms latency, conditional expressions match the label-taken check, matches existing infra. Cons: credentials still stored by the API unless moved to Vault; scan-based listing needs careful key design.
- **Replace with Vault.** Credentials are secrets; the registry already trusts Vault.
  Pros: strongest secret handling, lease/rotation support. Cons: poor fit for non-secret account metadata, no secondary indexes, higher read latency for every listing call.
- **Replace with Postgres.** Relational fit for owner→account listings and uniqueness constraints.
  Pros: transactions, real indexes, mature tooling. Cons: new managed database in the dependency set, another failure domain for every request.

Likelihood: high. Plaintext credentials in process memory are the single largest defect in the codebase.

### SessionStore — `auth/session_store.rs`

Login credentials, access-session rows, and revocation flags, checked on every authenticated request.

- **Replace with Valkey.** Sessions are TTL-shaped data; revocation is a key delete; the auth plane already runs Valkey.
  Pros: TTL expiry for free, sub-ms checks, no new dependency. Cons: persistence tuning needed so a Valkey restart does not log everyone out; revocation checks become availability-critical.
- **Replace with DynamoDB.** Durable session rows with TTL deletion.
  Pros: survives restarts, consistent with run/ownership stores. Cons: per-request read cost and latency; access-token verification is on the hot path.
- **Stateless JWTs.** Drop the server-side session map and rely on short-lived signed tokens plus a revocation list.
  Pros: removes the store for the common case. Cons: revocation and scope changes need a deny-list, which is itself a store; step-up scopes still need server state.

Likelihood: high.

### RefreshStore — `auth/refresh_store.rs`

Refresh-token rotation families with replay grace windows.

- **Replace with Valkey.** One key per hashed token with TTL, one per family; atomic Lua for rotate-and-revoke-family.
  Pros: matches the replay-protection pattern already proven in auth-service; TTL fits token lifetimes. Cons: a family revoke over thousands of tokens needs scripting or an index set.
- **Replace with DynamoDB.** Token rows keyed by hash, family rows for revocation, conditional rotations.
  Pros: durable audit of rotation history; strong consistency. Cons: more expensive per rotation; TTL cleanup is near-immediate but eventual.

Likelihood: high. Rotation replay detection is a security control; it must not evaporate on restart.

### TicketStore — `streaming/ticket_store.rs`

Outstanding single-use streaming-ticket JTIs (JWT `jti` set).

- **Replace with Valkey.** `SET jti 1 NX EX ttl` gives exactly-once redeem semantics.
  Pros: one command, TTL matches ticket expiry, already deployed. Cons: adds a synchronous external check to every stream connect.
- **Stateless with short TTL.** Keep tickets as short-lived JWTs and accept that a redeemed ticket can be replayed until it expires.
  Pros: removes the store entirely. Cons: weakens single-use guarantees; replay would let a second listener attach to a stream.

Likelihood: high, bundled with the streaming work below.

### BotStore — `bots/bot_store.rs`

Bot configurations with state transitions.

- **Replace with DynamoDB.** Owner-partitioned rows, conditional state transitions.
  Pros: consistent with run records and ownership; cheap at this scale. Cons: listing by owner plus state needs an index.
- **Replace with Postgres.** Natural fit for config-shaped rows with filters.
  Pros: rich queries, transactions. Cons: new dependency for a low-volume table.

Likelihood: high. It is documented API state; losing bots on restart is a correctness bug, not an inconvenience.

### BuildStore — `algorithms/build_store.rs`

Algorithms, build summaries, strategies, and build logs.

- **Split by nature.** Algorithm/build metadata in DynamoDB; logs in S3 with a TTL link.
  Pros: metadata stays queryable; log bytes do not bloat a database row; matches the run-log pattern. Cons: two systems to query for a single build view.
- **All in Postgres.** Logs as large text columns with retention jobs.
  Pros: single consistent store. Cons: unbounded log rows drive vacuum pressure and cost.

Likelihood: high.

### RunStore — `bots/run_store.rs`

Run rows with retention sweeps and live streaming into channels.

- **Reuse the control-plane run store.** The platform already has a DynamoDB-backed `RunStore` (`crates/control-plane/run-store`) for scheduled runs; the front-end run records belong beside it, with the front-end owning a view over it.
  Pros: one run model across planes, existing table design, retention via TTL. Cons: couples the front-end to the control-plane schema; streaming still needs a pub/sub layer.
- **Separate DynamoDB table.** Front-end-owned rows with TTLs, logs in S3.
  Pros: independent lifecycle and schema. Cons: two run stores to operate and reconcile.

Likelihood: high.

### BacktestStore — `backtests/backtest_store.rs`

Backtest status plus result items, gated by a result-size limit.

- **Status in DynamoDB, results in S3.** Row holds pointers and status; large payloads go to the backtest-result S3 location that already exists in the runbook.
  Pros: respects the size limit structurally instead of by exception; cheap retention. Cons: result reads need an S3 round trip; presigning adds a step.
- **All in DynamoDB.** Items under the 400 KiB limit with a TTL.
  Pros: simplest; one store. Cons: the result-size guard becomes a hard failure more often; large results force client-side pagination workarounds.

Likelihood: high.

### ChannelRegistry and StreamService — `streaming/`

Per-channel frame sequences, bounded replay ring buffers, broadcast senders, and the live connection table.

- **Replace with NATS JetStream.** Each channel becomes a stream; replay is a consumer with a sequence; live delivery is a subscription; the connection table stays in process (it is per-replica by nature).
  Pros: durable replay windows for free, cross-replica fan-out, scales beyond one process; the platform already runs JetStream. Cons: per-channel streams need lifecycle management; ordering guarantees must be re-validated; higher publish latency.
- **Replace with Valkey streams.** `XADD`/`XRANGE` plus consumer tracking.
  Pros: bounded memory with `MAXLEN`, trivial replay reads. Cons: Valkey becomes availability-critical for every page; cross-replica scaling still limited by one Valkey.
- **Keep in process.** Accept single-replica streaming with reconnect-and-refetch semantics.
  Pros: lowest latency, no new failure modes. Cons: clients lose stream continuity across reconnects and replica restarts; no horizontal scaling.

Likelihood: medium. Streaming is the least mature part of the API; JetStream is the natural end state.

### IdempotencyStore — `api/idempotency.rs`

Twenty-four-hour replay of mutating requests keyed by `owner:endpoint:key` with a body hash.

- **Replace with DynamoDB.** Key plus TTL; conditional put wins the right to execute.
  Pros: replay protection survives restarts and works across replicas; TTL handles retention; conditional writes match the semantics exactly. Cons: adds a store hit to every mutating request; stored responses cap at 400 KiB.
- **Replace with Valkey.** `SET NX EX` for the claim, with the response stored alongside.
  Pros: fastest; TTL is native. Cons: response payloads in Valkey compete with latency-critical keys; durability across a Valkey restart is weaker.

Likelihood: high. Idempotency that resets on restart fails its contract precisely when clients retry after an API restart.

### Throttle — `limits/throttle.rs`

Fixed-window rate-limit counters per key.

- **Replace with Valkey.** Window counters via `INCR` plus `EXPIRE`, or a sliding-window script.
  Pros: shared across replicas, native TTL, sub-ms. Cons: every request gains a round trip; clock handling moves to the store.
- **Replace with DynamoDB.** Counter rows with TTL and atomic adds.
  Pros: durable, cheap at low rates. Cons: too slow and eventually consistent for hot-path limiting.
- **Keep in process.** Per-replica limits multiply by replica count.
  Pros: zero latency. Cons: limits are per replica and reset on restart; abusive clients get a fresh budget per replica.

Likelihood: medium. Needed once the API runs more than one replica or cares about restart-proof budgets.

## Data-plane working sets

These hold large hot state in memory by design, with durability handled externally. Replacing the in-memory part with an external store would be harmful; the invariants below are what to preserve.

- **AggregatorState** (`data-plane/aggregator/src/state`): open and closed candles, gaps, and stream progress for a shard. Keyed state is checkpointed to NATS KV before acknowledgement and rebuilt from the archive when a checkpoint is missing or incompatible. The worst-case memory is checked at startup against a static pair ceiling. Externalizing live candle state (for example into Valkey) would add a network round trip to every event and break the deterministic replay contract, while checkpoints and archive replay already provide recovery. Likelihood of needing an external store: none; keep checkpoint-based.
- **PortfolioState** (`data-plane/portfolio-service/src/state`): tracked accounts and latest portfolio update metadata per account. The durable read model is already external (`read_model/store.rs` persists records to a NATS KV bucket) and state is rebuilt on startup. Likelihood: none; already externalized where it matters.
- **SequenceTracker** (`data-plane/ingester/src/sequence_tracker.rs`): in-memory per-stream sequence progress persisted through `NatsSequenceStore`. Already follows the checkpoint pattern. Likelihood: none.
- **RawInventory** (`data-plane/compactor/src/source_inventory.rs`): incremental S3 listing cache with a periodic full reconcile. It is a rebuildable view over S3, bounded by poll counts, and never authoritative. An external store would only slow reconciliation. Likelihood: none.

## Caches in front of external systems

Each of these already has an external authority; the in-memory copy is a latency or rate-limit optimization. Replacing them is optional and usually wrong. For each, the external authority is noted so the cache can be flushed and rebuilt at will.

| Cache | Location | Authority | Notes |
| --- | --- | --- | --- |
| `TokenCache` (bounded LRU, positive/negative/generation) | auth-service | Vault tokens | Bounded, invalidation-aware. A shared Valkey cache would add a hop to the hot path for little gain; keep in process. |
| `JwksCache` | auth-service | auth JWKS endpoint | Single-entry with single-flight refresh and backoff. Nothing to externalize. |
| `JwtCache` | sandbox runner | auth service | Single-entry token cache with single-flight refresh. Per-invocation by design. |
| `GitHubAuth` app clients + installation LRU | platform-github | GitHub API | Installations are bounded and re-fetched on miss. A shared cache would trade latency for staleness on credentials. |
| `HashRouter` decoder cache | aggregator | static schema registry | Memoized per-exchange decoders. Trivially rebuildable. |
| Claim-index digest cache | archive catalog | Iceberg table metadata | Bounded point-lookup cache with atomic head updates; correctness never depends on it. |
| `PortfolioAccounts` credentials map | exchange-gateway | portfolio registry (ownership events) | Rebuilt from ownership events; the registry is the authority. Note the same plaintext-credentials concern as `AccountStore`: the long-term fix is Vault-backed credentials, not a different cache. |
| Webhook converter/secret cache | git-webhook | SSM | Single-flight reload with backoff. Process-local by design in a Lambda environment. |

## Process-local coordination

These are not stores in the durable sense; they exist for the lifetime of one connection, invocation, or lease and have no meaning outside the process. Moving them to an external store would add latency without adding correctness.

- **`CircuitBreaker` and `FlightGates`** (auth-service): per-process failure accounting and single-flight gates around Vault calls. A shared breaker would couple replicas; a shared gate is meaningless across processes.
- **Rate limiters and `Clock`** (exchange-gateway): per-connector exchange budget accounting and server-clock offset. Exchange budgets are per connection; a shared store would misrepresent them.
- **Websocket waiters, subscribers, and senders** (exchange-gateway): per-connection futures and channel registries. Pure connection state.
- **`HealthState`** (runtime), **`Watchdog`** (runner), **`Artifacts.compile_output`** (build-agent), **publisher stream memoization** (platform-nats): readiness flag, thread watchdog, captured build output for the current invocation, and a set of already-provisioned streams. All single-invocation or single-process.

## Best-effort dedup caches

`DeliveryDedup` (git-webhook) and `SqsDedup` (build-orchestrator) are bounded LRU caches that drop duplicate work inside one warm Lambda environment. Both are documented as best-effort: correctness never relies on them, and duplicates from other environments are handled downstream (SQS visibility, conditional commits).

- **Replace with Valkey.** Shared dedup across all environments.
  Pros: removes duplicate work globally; simpler downstream load. Cons: adds a mandatory external dependency to the webhook path — a Valkey outage would either fail deliveries or require a keep-working fallback, which reintroduces best-effort behavior.
- **Keep in process.**
  Pros: zero dependencies, already correct without it. Cons: duplicate work across environments; bounded by cache capacity.

Likelihood: low. Externalize only if SQS-side duplicate load is demonstrably costly.

## Test doubles

The in-memory implementations behind trait boundaries are intentionally memory-backed and live in test files; production wiring uses external stores.

- auth-service: `MemoryReplayStore`, `MemoryTokenStore`, `MemoryTokenUsageQueue`, `MemoryTokenAuditLog` stand in for Valkey, Vault, and SQS implementations (`valkey_replay_store.rs`, `vault_token_store.rs`, `valkey_token_usage_queue.rs`, `sqs_token_audit_log.rs`).
- portfolio-registry: `MemoryOwnershipStore` stands in for `DynamoDbOwnershipStore`.

No action needed; this is the seam that makes the production stores replaceable.

## Recommended order

1. Front-end API durable stores (accounts, sessions, refresh, tickets, bots, builds, runs, backtests, idempotency): DynamoDB for records, S3 for logs and large results, Valkey for session/TTL-shaped and hot-path state.
2. Streaming channel state: NATS JetStream channels, keeping the connection table in process.
3. Throttle: Valkey counters once multi-replica is real.
4. Leave the data-plane working sets, caches, and process-local coordination as they are; their external durability paths already exist.
