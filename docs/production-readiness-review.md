# Production-readiness review

**Question:** is this repository production ready?

**Short answer: no, not as a whole.** The data plane is the most mature
component; the auth plane is cryptographically sound but its HTTP front door
and lifecycle handling need hardening; the portfolio/trading path is a working
skeleton whose production wiring is absent; and the release pipeline does not
publish or deploy. The codebase itself is clean: formatting, clippy and the
full test suite pass. The gaps are mostly integration, operations and
hardening rather than broken core logic.

This review is static plus build/test verification. It does not include
deployment, load, chaos, penetration or restore-drill testing.

## Verification baseline

| Check | Command | Result |
| --- | --- | --- |
| Formatting | `cargo fmt --all --check` | clean |
| Lints | `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings` | clean, 0 warnings |
| Tests | `cargo test --workspace --all-features --offline` | 1051 passed, 0 failed, 3 ignored |
| Rebase | `git fetch origin master && git rebase origin/master` | no-op (`HEAD == origin/master == d5b257a`) |

The 3 ignored tests are the intentional `rust,ignore` doctests in
`crates/foundation/traitreg-macros/src/lib.rs` (lines 16, 27, 140); there are no
`#[ignore]`-annotated tests in the workspace.

## Repository shape

- 36 workspace crates, 813 Rust files, ~79k lines, edition 2024, Rust 1.99.0.
- `mod.rs`: 116 files. None define functions, types, traits or constants; they
  only declare modules and re-export. The rule is satisfied.
- `lib.rs`: only `traitreg-macros` contains items (`register`/`registry` and the
  generated `impl`). That is expected for a proc-macro crate.
- `main.rs`: all are thin; `main` plus `mod`/`use`, with a few `const SERVICE`
  values. No business logic.
- No free-floating functions in library code. Top-level `fn`s are `main`,
  the proc-macro entry points, and serde helper functions in
  `foundation/type-bridge/src/util/serde_rounding_strategy.rs`. The rule is
  satisfied.
- Tests live in `tests.rs`, `*_tests.rs` or `src/tests/`; there are no inline
  `mod tests { ... }` blocks. Test code is `#[cfg(test)]`-gated.
- `test-utils` code in `platform-metrics` and `platform-market-data` is behind
  a feature, not compiled into production by default.

These are good structural results; the rest of this document is about gaps.

## Blockers

### 1. The portfolio/order-request service cannot serve real traffic

`PortfolioRunner` assembles a fully functional-looking HTTP server:

- `PortfolioService::in_memory()` (`crates/data-plane/portfolio-service/src/runner.rs:17`,
  `service.rs:25`) uses `NoopPortfolioFeed` (`feed.rs:28`), whose
  `set_accounts` returns `Ok(())` without contacting an exchange.
- The only non-test `PortfolioConnector` implementation is the trait itself;
  `GroupingPortfolioFeed` (`feed.rs:43`) is never constructed in production.
  There is no exchange connector wiring, no inbound portfolio update loop and
  no symbol/asset bootstrap.
- Portfolio state is a `RwLock` in memory (`service.rs:20`). It is not
  persisted, and there is no snapshot/restore path. A restart silently loses
  every tracked account.
- There is no admin or read HTTP surface for accounts, portfolios or tracked
  owners. `add_account`, `remove_account`, `add_user_account`,
  `remove_user_account`, `apply_portfolio`, `account_portfolio`, `portfolios`,
  `tracked_accounts` and `account_count` have callers only in
  `src/tests/*`; `add_user_account` and `remove_user_account` have no callers
  anywhere.

Consequence: after startup the tracked-account set is empty, so every order
request reaches the tracking check in `order_request.rs:142` and is rejected
with `403`/account-not-owned. The service is a well-tested signing and
authorization skeleton, not a deployable portfolio service.

To make this production-ready it needs, at minimum: a real
`PortfolioConnector`/feed, durable or rebuildable account state, an
operator/admin API (or configuration source) for tracking accounts, and an
end-to-end test that starts the runner and submits an order.

### 2. The JWT server is not hardened for direct exposure

`JwtServer::serve` (`crates/auth-plane/auth-service/src/jwt_server.rs:52-70`):

- accepts and `tokio::spawn`s an unbounded number of connections; there is no
  `Semaphore`, `JoinSet` or connection cap;
- sets no `TokioTimer`, no header-read timeout and no body-read timeout; the
  hyper default header timeout only takes effect when a timer is installed;
- collects the entire request body before checking `MAX_REQUEST_BYTES`
  (`jwt_server.rs:106-116`), so the 64 KiB limit does not bound memory use;
- returns from `serve` on the first `accept` error (`:54`), so a transient
  `EMFILE`/`ECONNABORTED` takes the endpoint down;
- has no shutdown/drain hook; `AuthServiceRunner` cancels the future
  (`auth_service_runner.rs:33-36`), leaving spawned connections detached.

The portfolio server does this correctly: it sets `TokioTimer`,
`HEADER_READ_TIMEOUT`, a `BODY_READ_TIMEOUT`, `MAX_CONNECTIONS`, a semaphore
and a `JoinSet` (`order_request_connection.rs:32-34`,
`order_request_server.rs`). The auth server should reuse the same pattern.

Severity depends on exposure. Behind an ALB/API Gateway with connection and
body limits this is lower; bound directly via `AUTH_SERVICE_BIND_ADDR` it is a
denial-of-service risk.

### 3. Auth-service lifecycle and replay configuration are inconsistent

- `AuthServiceRunner` waits on `tokio::signal::ctrl_c()` only
  (`auth_service_runner.rs:35`). Every other long-running service uses
  `Runtime::shutdown_signal()` (`foundation/runtime/src/runtime.rs:16`), which
  also handles SIGTERM. On Kubernetes/ECS, SIGTERM is the normal shutdown
  signal; this process ignores it until the grace period expires.
- The auth-service binary runs on a current-thread runtime
  (`auth-plane/auth-service/src/entry.rs:19`), unlike the multi-threaded data
  plane and portfolio service. This is defensible for a small HTTP server but
  should be a deliberate decision, not an accident.
- The binary constructs `MemoryReplayStore` (`auth_service_runner.rs:26`).
  `docs/auth-service-replay-protection.md` says Valkey is the production shared
  replay backend. Today the `/jwt` endpoint only issues tokens, so replay is
  not exercised there, but the wiring is a latent footgun: any future
  verification endpoint added to this binary would get per-process replay
  protection.
- Conversely, `PortfolioAuthService::connect`
  (`portfolio-service/src/auth_service.rs:12-26`) builds a `ValkeyReplayStore`
  and a random JWT signing key via `AuthService::new`, although the portfolio
  service only signs order requests with the API token secret and never
  verifies a JWT through that `AuthService`. That is an unnecessary startup
  dependency and a misleading configuration.

### 4. There are no end-to-end or cross-crate integration tests

All 1051 tests are unit-level (crate-local `src/tests`), plus a handful of
integration-style tests in `foundation/types/tests` and
`foundation/exchange-types/tests`. No test starts a runner, binds a server,
publishes to NATS, reads from S3/Iceberg or exercises a binary's real wiring.

This is why the portfolio `in_memory` wiring and the auth-server hardening
gaps are invisible: the units are correct, the composition is untested.
Production readiness needs at least:

- a smoke test per binary (config load, dependency connect, bind, shutdown);
- an order-request test through `OrderRequestServer` with a real `AuthService`
  fixture and delivery stub;
- a JWT issue/verify round trip through the HTTP server;
- a NATS → ingester → aggregator → archiver/compactor path test (can use
  testcontainers or local services).

### 5. The release pipeline does not publish or deploy

`.github/workflows/master.yml` runs fmt/clippy/test only. The tag creation,
ECR login, `docker/build-push-action` and `aws lambda update-function-code`
steps are all commented out (`master.yml:75-143`). Nothing in CI builds the
root `Dockerfile`, pushes an image or updates a deployment.

Additional release gaps:

- Root `Dockerfile` uses `rustserverless/lambda-rust:latest` and downloads
  wasmtime, cargo-lambda and zig by version with `curl` but no checksum
  verification.
- There is no `.dockerignore`; a local `target/` directory (tens of GB) is part
  of the Docker build context.
- The build expects private registry access
  (`ssh://git@ssh.shipyard.rs/stock-trek/crate-index.git` in
  `.cargo/config.toml`). CI sets up a read-only SSH key; external or mirrored
  builds cannot resolve `iris` and `query-params`.
- No SBOM, image scan, `cargo deny`, `cargo audit`, coverage or benchmark step.
  The commented Docker step explicitly sets `provenance: false` and
  `sbom: false`.

Until this is restored, "production ready" is a manual, undocumented process.

## High-priority issues

### JWKS/JWT caches serialize refresh and can amplify outages

`JwksCache::verify_remote` (`auth-plane/auth-service/src/jwks_cache.rs:99-130`)
holds the `Mutex` across `fetch().await`. While the auth service is slow or
down, every concurrent portfolio instance waits behind a 10-second fetch; when
the lock is released the next waiter starts another fetch because
`fetched_at` was never updated. The stale verifier is only used if the fetch
fails and the key is already cached, so a key rotation during an outage still
fails.

`JwtCache::token` (`sandbox-plane/runner/src/cwasm/jwt_cache.rs:93-107`) has
the same lock-across-await pattern. It also has no stale-token fallback: if
the refresh request fails, `token()` returns the error even though the
previously cached JWT may still be valid for up to the 30-second refresh
margin.

Recommended fix for both: a single-flight `OnceCell`/`tokio::sync::OnceCell`
(do not hold the mutex over I/O), exponential backoff after a failed refresh,
and a stale-while-error fallback bounded by the token's real `expires_at`
(store it in `CachedJwt`).

### JWT issuance returns 400 for backend outages

`JwtServer::dispatch` maps every `issue_jwt_for_owner` error to
`StatusCode::BAD_REQUEST` (`jwt_server.rs:129-132`). A Vault or Valkey outage
is not a client error; returning 400 tells clients not to retry. It should map
retryable `BpError`/`AuthError` variants to 503 and only invalid input to 400.

`OrderRequestError::from_signing_error` has a similar issue: any
`BpError::System(SystemError::External(_))` becomes `InvalidRequest` (400)
(`order_request_error.rs:96`), so a downstream HTTP failure is reported as a
bad request rather than a retryable 503.

### Metrics are recorded into a no-op recorder in long-running services

`platform-metrics` uses the `metrics` facade, which discards events unless a
global recorder is installed. Only the three Lambda binaries call
`SERVICE.init_lambda_metrics` (`build-agent`, `build-orchestrator`,
`git-webhook`). The long-running binaries (`ingester`, `aggregator`,
`archiver`, `compactor`, `rebuilder`, `backfiller`, `portfolio-service`,
`auth-service`) call only `init_logging`, which installs a tracing subscriber
and no metrics recorder.

As a result, the counters and gauges in `market_data.rs`, `auth.rs`,
`build_metrics.rs`, `route.rs`, `secret.rs` and `sqs.rs` are silently dropped
in exactly the services that need them. Either install a Prometheus/CloudWatch
recorder in `Service::init_logging` (or a new `init_metrics`) or remove the
dead metric calls.

### Order signing reads Vault on every request

`sign_with_loaded_token` calls `self.token(token_id)` for every order request
(`order_signing_service.rs`), and `JwtServer` does the same for every `/jwt`
call. There is no local token cache, no TTL, no negative cache and no circuit
breaker. Vault latency is therefore on the critical path of every order, and
a Vault outage stops all signing and JWT issuance.

This may be an intentional security choice (always read the authoritative
token), but it needs a documented availability target and a caching/breaker
strategy. At minimum, cache token metadata for a short TTL, invalidate on
rotation/revocation through the token audit stream, and fail closed with a
clear 503 when Vault is unavailable.

### The build-agent trusts an external verifier and inherits its environment

`Artifacts::build_wasm` runs `cargo build` with the agent's full environment
(`build-agent/src/artifacts/compile.rs:77`); it does not call `env_clear` or
`env_remove`. User code is validated by the external `stock_trek_check` crate
(`compile.rs:55-66`), which rejects at least `use std::fs` (tested in
`artifacts/tests.rs:251`).

Two things should be verified explicitly:

- that the checker rejects compile-time file reads such as
  `include_str!("/proc/self/environ")`, which would expose the agent's Vault
  token and pre-signed S3 URLs;
- that cargo/rustc run with a scrubbed environment (only `PATH`, `HOME`,
  `CARGO_HOME`, `RUSTUP_HOME`, target and offline flags), not the agent's
  secrets.

The zip extraction itself is well done: it rejects absolute paths, `..`,
backslashes and anything outside `user_generated`, and enforces declared and
actual extracted-size limits before/while writing
(`build-agent/src/source/source_archive.rs:222-238`).

### Dead and test-only API surface is shipped in production

Concrete zero-caller items:

| Item | Location |
| --- | --- |
| `record_job_success`, `record_job_error`, `record_lock_contention`, `record_lock_skip`, `record_lock_lost`, `record_ingest_ingested`, `record_ingest_gap`, `record_ingest_error`, `record_ingest_skipped`, `record_compaction_success`, `record_compaction_failure` | `foundation/metrics/src/market_data.rs` (definition only in all cases) |
| `NonZeroDurationSeconds`, `NonZeroDurationMilliseconds`, `NonZeroDurationMicroseconds`, `NonZeroDurationNanoseconds`, `NonZeroUsageCount`, `NonZeroTimestampMilliseconds` | `foundation/types/src/new_types.rs:418-423` |
| `download_bytes` | `data-plane/archive/src/s3/s3_object.rs:170` (`#[allow(unused)]`) |
| `previous_top` | `data-plane/archive/src/quantized_time/quantization.rs:102` (`#[allow(unused)]`) |
| `into_auto_resync`, `try_new_websocket_iris` | `integrations/exchange-gateway/src/connector/core.rs:119,175` |
| `NatsSubscriber`/`NatsMessage` | `foundation/nats/src/subscriber.rs` (only re-exported at `lib.rs:22`) |
| `rebuild_from_archive_with_schema` | `data-plane/aggregator/src/restore/rebuild.rs:58` |
| `EmptyArchivedEventSource` | `data-plane/aggregator/src/restore/replay.rs:32` (exported, never used) |
| `issue_jwt` | `auth-plane/auth-service/src/order_jwt_issuance.rs` (no callers) |
| `JwtCache::new` | `sandbox-plane/runner/src/cwasm/jwt_cache.rs:36` (`#[allow(dead_code)]`) |
| `WasmRunner::new` | `sandbox-plane/runner/src/cwasm/runtime.rs:58` (`#[allow(dead_code)]`) |
| `OrderRequestDispatcher::new` | `sandbox-plane/runner/src/cwasm/order_request_dispatcher.rs:24` (`#[allow(dead_code)]`) |

Test-only code that is always compiled into the production auth library:

- `MemoryTokenStore` (`memory_token_store.rs`, re-exported at `lib.rs:89`);
- `MemoryTokenUsageQueue` (`memory_token_usage_queue.rs`, re-exported at
  `lib.rs:90`);
- `MemoryReplayStore` (`memory_replay_store.rs`, re-exported at `lib.rs:88`),
  which is additionally used by the production auth-service runner.

These should be behind a `test-utils` feature (as `platform-metrics` and
`platform-market-data` already do) or moved to a test-support crate.

The portfolio mutation API is the same story: the whole in-memory
`add_*`/`track_*`/`apply_*` surface exists for tests, while no production API
calls it.

### Duplicated spill implementations in the rebuilder

`data-plane/rebuilder/src/rebuild/candle_spill.rs` (348 lines) and
`replay_spill.rs` (340 lines) implement nearly the same merge/sort/spill
algorithm (`SpillMerge`, key ordering, `BinaryHeap`, run fan-in) for two record
types. This is the strongest refactor candidate in the repo: extract a generic
`SpillMerge<K, V>` or a trait over the record/key, and keep only the
record-specific conversion and key construction.

### Large files and large test modules

59 of 612 non-test files exceed 200 lines; 12 exceed 300; 4 exceed 400; none
exceed 500. The largest non-test files:

| Lines | File |
| --- | --- |
| 439 | `foundation/type-bridge/src/converters/binance.rs` |
| 423 | `foundation/types/src/new_types.rs` |
| 409 | `data-plane/backfiller/src/backfiller/service.rs` |
| 406 | `foundation/metrics/src/descriptions.rs` |
| 386 | `data-plane/aggregator/src/runner/output/writer.rs` |
| 348 | `data-plane/rebuilder/src/rebuild/candle_spill.rs` |
| 340 | `data-plane/rebuilder/src/rebuild/replay_spill.rs` |
| 337 | `data-plane/aggregator/src/runner/dispatch.rs` |
| 325 | `foundation/market-data/src/decoders/binance/sbe/codec.rs` |
| 316 | `foundation/nats/src/consumer/jetstream_consumer.rs` |
| 306 | `foundation/exchange-types/src/binance/klines.rs` |
| 305 | `foundation/metrics/src/market_data.rs` |

The test bulk is larger: `aggregator/src/processor/tests.rs` is 990 lines,
`aggregator/src/runner/tests.rs` 955, `aggregator/src/restore/tests.rs` 883,
`sandbox-plane/runner/src/convert/tests.rs` 699,
`aggregator/src/state/tests.rs` 441. Splitting these by scenario would make
review and failure triage much easier.

These are not correctness bugs, but they work against the 100-200 line target
and make the "one obvious place" rule harder to follow.

### Documentation is stale, broken or missing

- `crates/data-plane/README.md:98` says the portfolio's "request-serving
  transport" is unwired, but `PortfolioRunner` constructs `OrderRequestServer`
  and serves HTTP. The configuration table at `:102-104` lists only `RUST_LOG`
  and omits `PORTFOLIO_ORDER_REQUEST_*` and the auth/Vault/Valkey variables.
- `crates/data-plane/README.md:118` links to
  `../../docs/rebuilder-scalability-options.md`; that file does not exist.
- There is no root `README.md`, `LICENSE`, `CONTRIBUTING.md`, `SECURITY.md`,
  `CHANGELOG.md` or `.github/CODEOWNERS`. For a 36-crate repo this is a
  significant onboarding and ownership gap.
- `traitreg-macros` is a fork of an MIT/Apache-2.0 crate but ships no license
  file and its README does not mention the fork or the modifications.
- `docs/auth-service-replay-protection.md` claims Valkey is the production
  replay backend, while the auth-service binary uses `MemoryReplayStore`.
- `docs/environment-variables.md` is otherwise comprehensive: every non-test
  environment string I found in the source is documented there.

### CI and supply chain

- `master.yml` loads two Infisical projects (AWS and publish) into the job
  environment before running `cargo test`/clippy. Any compromised build script
  or test can read those secrets. Prefer per-step secret scoping, and do not
  expose publish credentials to the check job.
- `test.yml` triggers on `push` to non-master branches, not `pull_request`;
  fork PRs get no CI signal in the base repository.
- No `cargo deny`/`cargo audit`/`cargo vet`, no license check, no dependency
  update automation (Dependabot/Renovate), no SBOM or image scan.
- The only Dockerfile uses `:latest` for the Rust Lambda base image and
  unverified `curl` downloads for wasmtime/cargo-lambda/zig.
- No benchmarks, fuzzing or property tests. Protocol decoders (SBE, JSON,
  zip), JWT/SigV4 parsing and spill/merge logic would benefit from fuzzing.
- `agent.yml` is 1144 lines; splitting it by concern would make it reviewable.

## Medium and low-priority findings

### Error and HTTP ergonomics

- `OrderRequestConnection::bearer` requires the literal `Bearer ` prefix
  (case-sensitive). RFC 6750 defines the scheme as case-insensitive; clients
  sending `bearer` get 401.
- `JwtServer` returns 404 for a wrong method; `OrderRequestServer` returns 405.
  Pick one contract and document it.
- Auth-server errors are plain text without `Content-Type`; portfolio errors
  are JSON. Clients need two parsers.
- No `/healthz` or `/readyz` on `JwtServer` or `OrderRequestServer`; readiness
  must be inferred from TCP connect or logs.
- No per-client rate limiting on either HTTP server. The portfolio has a
  global connection cap; the auth server has none.

### Configuration and defaults

- `AuthServiceConfig` is hardcoded to 30-day tokens, 7-day overlap and 5-minute
  signatures in two places: `auth_service_runner.rs:21-25` and
  `portfolio-service/src/auth_service.rs:18-22`. These should be constants or
  environment-driven, not duplicated literals.
- `Timeout::from_duration(Duration::ZERO)` silently means "disabled"
  (`foundation/time/src/timeouts.rs`). This is documented but easy to misuse;
  a `Timeout::disabled()` constructor or a `NonZero` argument would be safer.
- `JwtCache` and `JwksCache` accept `http` as well as `https`; production
  should require TLS or make the exception explicit.
- `NatsAddress` supports only `nats://`, not `tls://`; confirm that credentials
  never cross an untrusted network.

### API design and naming

- Six `#[allow(clippy::too_many_arguments)]` sites are candidates for parameter
  structs: `aggregator/src/processor/candle.rs:226`,
  `runner/dispatch.rs:37`, `runner/commit.rs:87`, `restore/rebuild.rs:57`,
  `config/validation/config_validation.rs:105`,
  `sandbox-plane/runner/src/cwasm/backtest/order_events.rs:7`.
- `BinanceKlineResponse` is a 12-field tuple struct with an
  `#[allow(unused)] String` positional field
  (`foundation/exchange-types/src/binance/klines.rs:63`). It mirrors the wire
  format, but named fields or a comment would make it maintainable.
- The directory is `host_allow_list/` while the type is `HostAllowlist`
  (`integrations/github/src/host_allow_list/host_allowlist.rs:51`).
- `nats/src/address/nats_address.rs` repeats the module name in the file name;
  `address.rs` would be simpler.
- `Timeouts` has no `Default` implementation (`foundation/time/src/timeouts.rs`);
  callers must spell out both connect and operation timeouts.

### Comments and tests

- No trivial `assert!(true)`, `1 == 1` or `assert_eq!(true, true)` tests were
  found. The test suite is substantive.
- A few tests only assert `.is_ok()` where a stronger assertion is available
  (for example `order_request_auth_tests.rs`,
  `jwt_server_config_tests.rs`, `token_rotation_tests.rs`). These are not
  silly, but they can hide regressions.
- Comments are generally high quality and explain invariants. The only
  redundant ones are the two-pass comments in
  `foundation/exchange-types/src/encode.rs` ("Compute capacity", "Second pass:
  encode"). No cleanup is needed.

### Workspace and tooling

- No `[workspace.lints]` section; clippy policy is enforced only in CI and via
  scattered `#[allow]` attributes. A workspace lint table would make the
  policy explicit.
- No `rust-version` in `Cargo.toml`; the toolchain file pins 1.99.0, but
  published crates do not declare an MSRV.
- `.gitignore` does not ignore `.env`, `*.pem`, `*.key` or `*.p12`. Add them
  as a safety net even though none are currently committed.
- `.gitattributes` marks `*.zip` as LFS but there are no zip files; harmless
  but dead configuration.
- `platform-proto-gen` is active, not dead: root `proto/*.proto` (11 files)
  generate `sandbox-plane/runner/src/generated/{dto.rs,mod.rs}`, and
  `generator/tests.rs::checked_in_sources_match_proto_files` enforces
  freshness. Regeneration is manual (`cargo run -p platform-proto-gen`); add
  it to CI or a `just`/`make` target so drift is caught before the test.

## What is already good

- **Clean build:** `cargo fmt`, clippy with `-D warnings` across all targets and
  features, and 1051 tests all pass.
- **No dangerous escapes in ordinary code:** `unsafe` is confined to the wasm
  guest entry/FFI (`sandbox-plane/runner/src/entry.rs`, `external.rs`) and the
  link-section attributes in `traitreg-macros`. No `todo!()`,
  `unimplemented!()`, `dbg!()` or `println!` in production code. The only
  `panic!` is the proc-macro's invalid-input path.
- **Secret handling:** constant-time comparisons (`subtle::ct_eq`) for API
  tokens, order signatures and GitHub webhook secrets; `SecretString` for
  tokens; redacted `Debug` implementations for `JwtServerConfig` and
  `ApiToken`.
- **Replay protection:** Valkey `SET NX PX` with a Lua compare-and-delete
  release, TTL taken from the signed `expires_at`, and fail-closed behavior on
  store errors. Signature verification happens before a nonce is reserved,
  so an attacker cannot burn nonces with invalid signatures.
- **JWT verification:** EdDSA-only, `kid` resolution, unknown-key rejection,
  explicit algorithm check, and a `JwksCache` that falls back to a stale key
  only when that key is present.
- **Bounded portfolio server:** connection cap, semaphore admission, header and
  body timeouts, `Limited` body reads, graceful drain and a clean
  `OrderRequestError` taxonomy that does not leak internal errors.
- **Build-agent hardening:** zip-slip rejection, declared/actual size limits,
  offline locked builds, a pristine per-request target directory, compile
  timeouts and a failure-publish budget.
- **Data-plane correctness details:** the sequence tracker represents
  `u64::MAX + 1` without overflow, distinguishes first/in-order/gap/duplicate/
  out-of-order observations, and the aggregator/rebuilder share the same
  closed-candle and correction rules.
- **Documentation of environment variables** is complete and centralized in
  `docs/environment-variables.md`.
- **Test organization:** tests are consistently separated from production
  code, and `test-utils` features are used where they exist.

## Suggested order of work

1. **Portfolio production wiring** (blocker): real connector/feed, durable
   account state, account-management API, end-to-end order test.
2. **Auth-server hardening** (blocker if exposed): connection cap, hyper timer,
   header/body timeouts, `Limited` body, accept backoff, graceful shutdown,
   SIGTERM via `Runtime::shutdown_signal()`, 503 mapping for store outages.
3. **Metrics recorder** for non-Lambda services, or delete the dead metric
   calls; add `/healthz` and `/readyz`.
4. **Release pipeline:** restore Docker build/push and Lambda update, add
   `.dockerignore`, pin base/tool versions with checksums, add SBOM/scan,
   `cargo audit`/`deny`, and a proto-gen freshness step.
5. **Dead-code and ownership cleanup:** remove the zero-caller APIs, gate
   memory stores behind `test-utils`, split the spill merge, split the largest
   files/test modules, resolve the `too_many_arguments` sites with parameter
   structs.
6. **Docs and tests:** root README/LICENSE/CONTRIBUTING, fix the stale
   portfolio section and broken link, add binary smoke tests, e2e tests,
   fuzzing for decoders and benchmarks for hot paths.

## Method and limitations

Reviewed the workspace source, Cargo manifests, CI workflows, Dockerfile,
documentation and test tree; ran the formatter, clippy and the full test suite
locally. No code was changed as part of the review. The private
`stock_trek_check` implementation, cloud infrastructure, runtime configuration
and production traffic were not available, so items that depend on them
(especially the build-agent verifier and the exposure of the auth server) are
called out as verification items rather than confirmed defects.
