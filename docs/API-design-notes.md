# Design notes (non-contract)

This section explains how the API is built.
Clients must not depend on anything here, and it may change without a version bump.

## Components

The front-end API is a new service that composes existing platform components and hides their internals from the browser:

```txt
Browser ⇄ Front-end API ⇄ Auth plane         (sessions, JWT verification)
                        ⇄ Portfolio registry (account ownership)
                        ⇄ Portfolio service  (read model)
                        ⇄ Build plane        (algorithms, build artifacts)
                        ⇄ Run store/invoker  (scheduled runs)
                        ⇄ Runner             (live runs, backtests)
```

The API owns two new durable stores, bot configuration and backtests, plus the session and idempotency stores.
Everything else is a projection of existing state: the ownership registry, the portfolio read model, build artifacts, and the run store.

## Authentication

- The API verifies JWTs locally against the auth service's JWKS (`/.well-known/jwks.json`) using a bounded key cache, and checks revocation on every request.
- Token minting goes through the auth service, which only mints for an owner it can authenticate: every minting request is signed with the login credential bound to that owner, and the API signs with the credential the user presented.
  The API therefore cannot mint a token for an owner it has not authenticated.
- Refresh tokens are stored hashed, grouped in families so that reuse of a rotated token can revoke the whole session.

## Accounts

- Ownership of an account is recorded by the portfolio registry and drives authorization everywhere.
  Ownership changes are audited and propagate through the ownership stream to the portfolio service connectors, which is why linking is asynchronous.
- Exchange credentials are stored in Vault and are write-only.
  Credential rotation is triggered on the connector when they are replaced.
- The registry keys ownership by server-assigned `accountId`.
  The exchange identifier and label are attributes, so the global key space contains no owner-chosen strings.
- Account identifiers appear in stream channel names; since they are server-assigned, no character restrictions are needed.

## Builds and bots

- The build plane watches repositories through the GitHub App; pushes to watched refs trigger builds.
  Revoked GitHub access moves the algorithm to `disconnected`.
- Scheduling writes run records to the run store.
  The run invoker claims due runs and invokes the runner, which executes the strategy, submits signed order requests, and uploads run logs.
  The at-most-one-active-run rule and misfire handling are enforced where runs are claimed, not in the browser.
- Cron schedules are expanded to compute the scheduled-runs quota.

## Backtests

- Execution uses the sandbox runner's backtest subsystem, which already enforces worker limits, watchdog timeouts, and memory limits.
- Market input is read from the candle archive, whose correction contract is why history beyond the archive's retention reach is refused.

## Streams

- Frames are sequenced per channel by the publisher.
  Each non-backtest channel keeps a bounded replay window (256 frames) in memory; backtest frames are stored durably alongside the results.
- Ownership of a channel is checked at subscribe time, and sessions are re-checked for revocation so that connections can be closed with `4401`.

## Limits

- Every limit is resolved from a backend matrix of tier to limit values, using the owner's tier.
  The API does not define limit values itself.
- A tier change takes effect on the next request and never deletes or disables existing resources.
- The login throttles apply before an owner is identified, so they are not tier-based.

## Related references

- Infrastructure runbook (`infra.md`) for the service and dependency map
- Market data services (`market-data-services.md`) for the aggregation and correction contract
- Market data (`market-data.md`) for the ingest pipeline
- Environment variables (`environment-variables.md`) for the portfolio service referenced above
- Health checks (`health-checks.md`) for the platform probe contract the API follows
