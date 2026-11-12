# Vault on the order-signing critical path: options

Status: option A implemented.

## Question

Order signing reads Vault synchronously on every request. With no cache or
circuit breaker, Vault latency, errors, or an outage directly add latency to
every order and can fail the order path. How can Vault be removed from the
per-order critical path without unsafe caching or revocation lag?

## Baseline before option A

Before this change, order signing read Vault synchronously on every request,
putting Vault latency, errors, and outages on the order path. The sandbox runner
caches the caller's JWT and posts an unsigned order request to the portfolio
service. `OrderRequestConnection::dispatch` verifies the JWT locally through
`JwksCache`. `OrderRequestService::sign_for_caller` checks ownership and calls
`AuthService::sign_order_request_for_token`, which calls `AuthService::token`,
then `TokenStore::get` on `VaultTokenStore`, which performs a Vault KV v2 HTTP
`GET` through `VaultClient`. `VaultClient` is constructed per store, uses the
10-second portfolio timeout, follows no redirects, and has no retry, cache, or failure tracking.

Token usage and audit events already go to Valkey. The remaining synchronous
dependency is the token read that supplies the secret and validity metadata.
The same read is used by signature verification, JWT issuance, and owner
indexes. JWT issuance is not per-order because the runner caches the JWT, but
verification is also hot if it is exposed on the order path.

## Constraints

- Sign only from the token secret with `OrdersWrite` scope, and only when the
  token is not revoked, expired, or past its rotation overlap.
- Revocation and expiry must take effect within an agreed bound; a cache must
  not extend access indefinitely.
- Fail closed when token validity cannot be established. Serving stale data
  during an outage must be explicit and bounded.
- Bound cache size, coalesce misses, and prevent caller-supplied ids from
  growing memory without limit.
- Never log secrets or export them as metrics, and minimise copies in memory.
- Keep replicas consistent and avoid a Vault stampede on cold start.

## Options

### A. Bounded in-process read-through cache with a circuit breaker

Wrap `TokenStore` with a caching decorator used by `AuthService`. Serve a cached
`ApiToken` within a short freshness window, coalesce concurrent misses for the
same token, and keep a bounded LRU plus a bounded negative cache. Track token
version or write time so local mutations invalidate entries. Add a circuit
breaker with closed, open, and half-open states around Vault reads. While open,
serve cached entries only until a configured maximum stale age; beyond that,
fail as retryable. A half-open probe performs one read, then closes or reopens
with backoff. The maximum stale age is the revocation lag.
Advantages: small local change; removes nearly all steady-state Vault reads;
protects against latency spikes and retry storms; no new infrastructure; easy to
test by wrapping the existing trait. Disadvantages: each replica has a separate
cache; revocation is delayed by up to the stale bound; an outage longer than the
stale bound still fails orders; secrets remain in process memory.

### B. Shared read-through cache in Valkey

Store token records, or a minimal signing projection containing secret, owner,
scope, expiry, and revocation fields, in Valkey with a TTL. `AuthService` reads
Valkey on a miss and populates it from Vault. Use a single-flight lock such as
`SET NX PX` to prevent a stampede. Invalidate or overwrite entries when tokens
are mutated; a token version or stream event prevents an older writer from
restoring stale data.
Advantages: hits are shared by all replicas; survives restarts; Valkey is
already used for replay protection and the usage queue; lower Vault load than
option A. Disadvantages: Valkey becomes a new critical signing dependency;
secrets are stored outside the process; invalidation must be correct across
writers; eviction can still fall back to Vault; the TTL must be reconciled with
revocation.

### C. Local materialised token read model fed by events

Make the auth service the writer of token state and publish immutable token
snapshots or change events. Each signing service keeps a local in-memory read
model, bootstrapped from a snapshot and then tailing an ordered event stream.
Include a monotonic token version or epoch so duplicate, reordered, or late
events can be rejected. Revocation is a new record version, not an in-place
mutation that a consumer can miss.
Advantages: no per-order network call; deterministic local checks; explicit
restart and replica recovery; event-driven revocation can be faster than a TTL;
aligns with the ownership and portfolio-update streams already in use.
Disadvantages: largest operational surface; needs a snapshot store, stream
retention, consumer checkpoints, readiness gating, and compaction; secrets are
replicated to every signing node and may be persisted; auth-service availability
is still needed to publish new tokens and revocations.

### D. Move signing behind an auth-service signing API

Keep token material inside the auth service. The portfolio service sends the
canonical order bytes and token id to an internal signing endpoint; the auth
service loads or caches the token and returns the signature. The portfolio
service then has no Vault dependency on the order path.
Advantages: one place owns secrets and revocation; one cache to operate; the
portfolio service no longer needs a broad Vault reader credential; the auth
service can sit behind a local sidecar or service mesh. Disadvantages: adds a
network hop and makes the signing endpoint a new critical path; capacity and
blast radius must cover all order traffic; the response must be authenticated
and replay-safe; it does not help unless the auth service itself caches or
materialises tokens.

### E. Replace Vault as the token read store

Keep Vault as the root of trust for encryption keys or initial secret material,
but move authoritative token records to a low-latency durable store with clear
read SLOs, such as a Valkey primary with persistence, a relational database, or
a purpose-built key-value service. The auth service is the only writer; signing
services read from the new store or a read replica.
Advantages: removes Vault from the per-order path by design rather than by
caching; predictable latency, capacity, and failover; cross-replica consistency
and restart recovery; Vault remains for key management and audit. Disadvantages:
the largest data and operational change; needs a schema, access control,
backups, disaster recovery, encryption at rest, and a migration story; the new
store must be at least as available as the order path.

### F. Vault read-path optimisation only

Increase Vault capacity or use performance replicas, a local Vault proxy or
agent, HTTP connection reuse, and tighter timeouts. Coalesce duplicate reads
where possible.
Advantages: no application redesign; can reduce latency and absorb bursts.
Disadvantages: still a synchronous network hop per order; does not protect
against a Vault outage or a thundering herd; performance replicas are a Vault
Enterprise feature; capacity work hides rather than removes the coupling.

### G. Degraded or fail-open signing

Sign from a cached token after its validity can no longer be confirmed, queue
orders until Vault returns, or skip revocation checks during an outage.
Advantages: orders continue during a control-plane outage. Disadvantages:
revoked or expired tokens can still sign; an outage becomes a security and
correctness incident; replay and audit semantics become unclear. This is only
acceptable for a product-approved, narrowly scoped, time-bounded mode with
explicit alerts, and it should not be the default.

## Comparison

| Option | Per-order Vault read | Cross-replica consistency | Revocation lag | Works during Vault outage | Main cost |
| --- | --- | --- | --- | --- | --- |
| A. Local cache + breaker | No on hit | Per replica | TTL plus stale bound | Within stale bound | Cache correctness |
| B. Shared Valkey cache | No on hit | Shared | TTL or invalidation | Within TTL | Secret in Valkey |
| C. Event-fed local model | No | Eventual, versioned | Event propagation | Yes, with complete model | Event pipeline |
| D. Auth-service signing API | No in portfolio | Shared auth service | Auth-service policy | No, unless auth service caches | Extra hop |
| E. New token store | No | Shared store | Store policy | Yes, if store is available | Store migration |
| F. Vault optimisation | Yes | N/A | Immediate | No | Infrastructure |
| G. Fail-open | No | N/A | Unbounded or undefined | Yes | Security risk |

## Recommendation

Use a layered approach rather than choosing one option in isolation.

1. Ship option A now: bounded cache, single-flight reads, circuit breaker,
   bounded stale-while-error, local invalidation on mutation, and metrics for
   hits, misses, stale serves, breaker state, and Vault latency. This removes
   Vault from the steady-state critical path and stops an outage from becoming a
   retry storm.
2. Choose option B or C next. Prefer B if the immediate need is multi-replica
   consistency and the security review permits token material in Valkey. Prefer
   C if revocation must propagate as an event and the platform can operate a
   snapshot plus stream consumer.
3. Decide between D and E as the long-term boundary. D is the smaller
   security-boundary change; E is the cleaner datastore change.
4. Do not use G as the default. If a degraded mode is required, define its
   maximum duration, which checks are suspended, how operators enable it, and
   how every use is audited and alerted.

The cache must be configurable: maximum entries, freshness TTL, maximum stale
age, breaker thresholds, open duration, and probe policy. These determine
availability and revocation lag and need an explicit security review.

## Acceptance criteria

- A cache hit performs no Vault request; concurrent misses for the same token
  produce one Vault read.
- Cache size and the negative cache are bounded; revocation and expiry take
  effect within the documented bound.
- During a Vault outage, cached tokens continue only within the stale bound;
  after that, signing returns a retryable failure and never succeeds from
  unvalidated state.
- The breaker opens after the configured pattern, allows one half-open probe,
  recovers without a thundering herd, and exposes hit, stale, transition, error,
  and latency metrics.
- Secrets are absent from logs, traces, metrics, and errors; multi-replica and
  restart tests show the chosen consistency and recovery.

## Open questions

1. What revocation-to-enforcement bound is required for orders: seconds,
   minutes, or immediate?
2. Is serving a cached token during a Vault outage acceptable, and for how long?
3. Is Valkey approved to hold token secrets, or must secrets remain only in
   process memory and Vault?
4. Is moving signing into the auth service (option D) acceptable, or must the
   portfolio service sign locally?
5. Can the platform commit to a durable event stream and snapshot store for
   option C, or is a store replacement (option E) preferable?
