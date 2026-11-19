# Long-running services as pure libraries

Investigation of which services in this workspace could become pure libraries without losing
functionality or guarantees. A pure library exposes its functionality as callable operations and has
no resident process of its own; the caller owns the process model (Lambda invocation, cron job,
scheduler, or daemon).

## Method

A service can become a pure library without loss when all four hold:

1. **Trigger model.** Every unit of work is already started by an external, durable trigger (an SQS
   message, a Lambda invocation, a due run in DynamoDB, a queue to drain). The process adds nothing
   between triggers.
2. **Cross-event state.** No in-process state carries a correctness guarantee from one trigger to the
   next. Sequence positions, dedup, watermarks, and fencing must live in durable storage or be
   protected downstream.
3. **Resident timers.** No background timer provides a guarantee the caller cannot reproduce. Lease
   renewal at one third of the TTL, idle-watermark flushing, and flush tickers all require a clock
   that runs between events.
4. **Interfaces.** The service does not expose a listening endpoint (HTTP, websocket) or a live
   exchange connection as its product.

Every service here already separates wiring from logic through a lib-plus-bin split or handler
types, so the work below is about the process model, not code organization. Build-time and
non-service binaries are out of scope: `platform-proto-gen` is a code generator, and
`platform-runner` is already a pure library (`cdylib`/`rlib`) whose `cwasm-invoker` binary is a host
tool for the build agent Lambda. The foundation and integrations crates are already libraries.

## Verdicts

| Crate | Process model | Convertible | Reason |
| --- | --- | --- | --- |
| build-agent | Lambda | Yes | Per-event handler, no cross-event guarantees |
| build-orchestrator | Lambda | Yes | Per-event handler, no cross-event guarantees |
| git-webhook | Lambda | Yes | In-process dedup is best-effort and protected downstream |
| run-invoker | Batch cycle | Yes | One cycle per process run, claim is conditional |
| rebuilder | One-shot job | Yes | Already lib-first, runs to completion and exits |
| auth-relay | Cron batch | Yes | One drain per run, queue keeps pending records |
| compactor | Interval daemon | Yes | Correctness is lease and conditional-commit based |
| maintainer | Interval daemon | Yes | Ordered stages are lease-gated and idempotent |
| backfiller | Interval daemon | Yes | Per-gap work is stateless, retries live in NATS KV |
| ingester | Resident | No | The live websocket connection is the product |
| aggregator | Resident | No | Lease renewal, idle-watermark timers, in-memory keyed state |
| archiver | Resident | No | In-memory dedup buffer between flushes, startup recovery gating |
| portfolio-service | Resident | No | Live connectors, in-memory read model, order-request listener |
| portfolio-registry | Resident | No | Two always-on HTTP APIs plus the outbox relay |
| front-end-api | Resident | No | Always-on HTTP API, in-memory sessions |
| auth-service | Resident | No | Always-on JWT server, token cache, usage worker loop |

## Convertible services

### Lambda-triggered functions

`build-agent`, `build-orchestrator`, and `git-webhook` are Lambda functions. The "long-running"
aspect is the Lambda runtime loop, which belongs to the platform, not the code. Each handler
(`BuildAgent::handle`, `SqsHandler`, `WebhookHandler`) is per-event and holds no correctness state:
the webhook's warm-environment delivery dedup explicitly treats redeliveries handled by another
environment as deduped downstream when the orchestrator consumes them from SQS. Converting means
extracting the handler, config, and context construction into the library target and leaving the
binary as the Lambda bootstrap adapter. No functionality or guarantee is lost because every event is
self-contained and idempotency is enforced by the stores behind each handler.

### Batch and cron processes

`run-invoker` performs exactly one cycle per process run: read due runs from DynamoDB, claim each
conditionally, invoke the runner Lambda, and exit. It has no health listener and no resident loop,
so the scheduling is already external. `rebuilder` runs a rebuild to completion and exits; its
library already carries the entire rebuild, and the health listener serves only the orchestrator.
`auth-relay` drains the Valkey token-usage and audit queues in one pass and is documented to run
from a cron job every few minutes; a failed drain leaves records pending for the next run. All three
become pure libraries whose host must provide only the schedule. Durability and at-least-once
semantics live in DynamoDB claims, Iceberg-free deterministic outputs, and the queues themselves.

### Scheduler-driven interval daemons

`compactor`, `maintainer`, and `backfiller` are interval loops whose per-cycle work is a callable
operation and whose correctness never depends on the process:

- The compactor serializes colliding work with fenced leases per raw hourly prefix, and every commit
  is a conditional Iceberg update, so a cycle is safe wherever it runs.
- The maintainer runs its four ordered stages through a maintenance runner that is lease-gated and
  coordinated with the compactor at partition level; the S3 lifecycle backstop is an idempotent,
  one-time installation that becomes an explicit library call.
- The backfiller consumes gap markers, fetches missing events from the exchange REST API, and
  republishes into the same stream; retry state is NATS KV, so a cycle is stateless.

A pure library plus an external scheduler preserves all correctness guarantees. What moves is the
cadence contract: the compactor's poll interval, the maintainer's fixed interval with its
consecutive-window budgets, and the backfiller's gap-closure latency become the scheduler's
responsibility, and the library must document the intervals the caller must maintain. The compactor
also holds an incremental listing position in memory across cycles; a per-invocation host would do a
full listing each cycle, which is the same functionality at higher listing cost, or keep the
inventory object alive across cycles in a warm environment.

## Services that must stay resident

`ingester` subscribes to exchange websockets with reconnect supervision and publishes sequence
numbers and gap markers; the continuous connection is the functionality, and the gap-marker
guarantee depends on witnessing every event. `aggregator` renews shard leases at one third of the
TTL, flushes expired windows on an idle-watermark timer that must fire when no input arrives, keeps
keyed state in memory between checkpoints, and supervises shard writers; the single-writer and
correction contracts require that resident clock. `archiver` deduplicates records in an in-memory
buffer across flushes, so its duplicate-suppression guarantee is resident, and it gates readiness on
startup recovery. `portfolio-service` holds live exchange websocket connections with freshness
policies, restores an in-memory portfolio view from the read model, and accepts order requests on a
listener that the sandbox runner calls. `portfolio-registry` and `front-end-api` are always-on HTTP
APIs; `auth-service` is an always-on JWT server with a bounded token cache, circuit breaker, and
usage worker loop. A library cannot listen, hold a connection, or run a timer between calls, so
converting any of these shifts a functional guarantee onto a caller that cannot provide it.

## Cross-cutting guarantees

- **Fail-closed startup.** Configuration validation already lives in `from_env`/`from_vars`
  constructors inside the libraries, so converted services keep refusing to start on missing,
  malformed, or cross-field-invalid configuration.
- **Readiness.** Readiness is a deployment guarantee of the resident model. Converted services
  replace it with explicit connection and validation calls that the host must complete before
  serving; the libraries should expose those as distinct operations so a host cannot skip them.
- **Observability.** Logging and metrics initialization stays with the hosting process; the
  converted libraries keep emitting through the shared `platform-metrics` and tracing facilities, so
  dashboards and alerts carry over unchanged.
- **Concurrency.** Every converted service already protects concurrent invocations where needed:
  conditional run claims, fenced leases with conditional commits, and NATS KV retry state. No
  additional coordination is required when a scheduler replaces the resident loop.
