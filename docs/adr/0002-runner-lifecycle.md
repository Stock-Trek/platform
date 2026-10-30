# ADR 0002: Runner lifecycle and restart ownership

- Status: accepted
- Scope: every deployable binary under `crates/`, plus the sandbox `build-runner`

## Context

The workspace has several kinds of process entry point:

- long-running data-plane services (`ingester`, `backfiller`, `archiver`, `compactor`);
- a multi-role data-plane service (`aggregator`, which runs a router and one or more partition writers);
- a one-shot data job (`rebuilder`);
- Lambda handlers (`git-webhook`, `build-orchestrator`, `build-agent`);
- local tooling and child processes (`proto-gen`, `build-runner`).

Most service mains call a single `Runner::run().await` and return its result. The aggregator instead has an in-process supervisor that restarts the router and every partition independently with exponential backoff. Without a written contract it is unclear which behaviour is intended, who owns restarts, and what exit status and shutdown guarantees callers can rely on.

The repository does not contain the deployment manifests that start these processes. This ADR therefore defines the contract the binaries expose to any external supervisor (ECS, Kubernetes, systemd, a batch job scheduler, or Lambda) and keeps process-local restart logic to the one case where it is justified.

## Decision

### Lifecycle classes

Each binary belongs to exactly one lifecycle class:

| Class | Binaries | Restart owner | Shutdown owner |
| --- | --- | --- | --- |
| Long-running, single role | `ingester`, `backfiller`, `archiver`, `compactor` | External supervisor | Process (`Runtime::shutdown_signal`) |
| Long-running, multi-role | `aggregator` | In-process supervisor for router and partitions; external supervisor for the process | Process (`Runtime::shutdown_signal`) |
| One-shot job | `rebuilder` | External job runner, by retrying the whole job | Process termination, treated as an interrupted attempt |
| Lambda handler | `git-webhook`, `build-orchestrator`, `build-agent` | Lambda service retry policy, driven by the handler result | Lambda runtime |
| Local tooling | `proto-gen`, `build-runner` | Caller or build agent | Caller |

The class determines the restart policy. A new binary must choose one of these classes and document any deviation in an ADR. In particular, a new single-role service must not add its own restart loop.

### Long-running single-role services

`ingester`, `backfiller`, `archiver`, and `compactor` run one logical role per process.

- **Startup is fatal on failure.** Configuration and dependency setup happen before the work loop. A startup error returns an error from `main`, so the process exits non-zero and the external supervisor decides whether to retry.
- **No process-local restart loop.** The process does not count restarts, sleep between full attempts, or recreate its dependencies. That belongs to the external supervisor, which can apply its own backoff, health checks, and crash-loop limits.
- **Runtime errors use the narrowest safe recovery.** An operation is retried in process only when a retry cannot lose or reorder durable work. Examples are NATS reconnects, S3 upload retries, and backfiller gap retries. The durable contract remains: publish or commit before acknowledging a source message.
- **Expected transient failures may be logged and skipped.** The compactor, for example, logs a failed compaction cycle and continues with the next tick because each cycle is independently retryable. Fatal errors that make the work loop unable to continue are returned.
- **Unexpected completion is a failure.** A service whose input is expected to be infinite must not return `Ok(())` merely because its source ended before a shutdown request. It should exit non-zero so the external supervisor restarts it. A clean return is reserved for a requested shutdown.
- **Shutdown is graceful.** `SIGTERM` and `SIGINT` are handled only through `Runtime::shutdown_signal`/`Runtime::run_until_shutdown`. The service stops accepting new work, finishes or safely abandons the current unit, flushes and commits durable output, releases leases, and then exits zero. Finalisation failures exit non-zero.

### Aggregator: in-process supervision

The aggregator is the deliberate exception.

The deployment unit may contain one router plus every partition, or one partition when `AGGREGATOR_SHARD_INDEX` is set. Partitions are independent failure domains:

- the router must exist before partition consumers connect;
- a failed partition can be restarted without disturbing healthy partitions;
- leases and checkpoints make partition restart safe, and a restarted writer fences any previous writer;
- restarting the whole process for one failed partition would cause unnecessary lease churn and interrupt unrelated partitions.

The router and each partition are therefore supervised in process. The supervisor:

- restarts a task with exponential backoff, starting at 1 second and capped at 30 seconds;
- stops restarting when shutdown is requested;
- returns the first task error after all supervised tasks have stopped;
- leaves process-level failures (startup, panic outside a supervised task, OOM, exhausted restart budget) to the external supervisor.

The long-term behaviour should be:

- reset the backoff after a task has remained healthy for a sustained period, rather than keeping the previous failure's delay forever;
- retry only errors classified as retryable; a permanent configuration or data-format error must stop the task and eventually the process instead of looping forever;
- treat a task completing without a shutdown request as an unexpected exit and restart it, just like a retryable error;
- include jitter in the delay so replicas do not restart in lockstep after a shared dependency recovers.

`AGGREGATOR_SHARD_INDEX` remains the supported way to run one partition per process when independent scaling or failure isolation is more important than process overhead. The in-process supervisor still applies there; the external supervisor remains responsible for the process.

The aggregator must not grow into a general-purpose process host. Any further multi-role service requires a new ADR explaining why the roles cannot be separate externally supervised processes.

### One-shot jobs

The `rebuilder` is a job, not a service.

- It runs to completion once and exits zero on success.
- A failure exits non-zero and is retried by the external job runner as a whole.
- It does not sleep, retry, or restart itself.
- It must be safe to rerun. Output object names are content-addressed and the job reads a consistent archive handoff, so an interrupted attempt can be repeated without creating a second logical result.
- No graceful-drain contract is required. An interrupted attempt is treated as failed and is not reported as a successful rebuild.

### Lambda handlers

`git-webhook`, `build-orchestrator`, and `build-agent` do not own their process lifecycle. `lambda_runtime` and the Lambda service handle scaling, timeouts, and invocation retries.

The handler result is the retry signal:

- `git-webhook` translates outcomes into HTTP responses. Permanent client and server errors are returned as final responses; transient errors are returned as invocation errors so the caller or Lambda retries.
- `build-orchestrator` returns a partial SQS batch response. Only retryable failures are reported for redelivery; permanent failures are logged and discarded.
- `build-agent` returns an error only for retryable build failures. Permanent failures, such as a user compile error, are returned as a structured failure outcome so Lambda does not retry them.

Lambda handlers must respect the invocation deadline and must not add process shutdown hooks or process-wide restart loops.

### Local tooling and sandbox child processes

These are not deployment units:

- `proto-gen` is a build-time tool and reports failure through its exit status.
- `build-runner` is a short-lived child of `build-agent`. Its lifecycle is bounded by the agent invocation and its stable exit codes are part of the algorithm ABI. It must not be started as a long-running service.

## Consequences

- Every long-running process needs an external supervisor with restart-on-failure, backoff, and a termination grace period longer than the service's worst-case drain.
- Single-role services stay small and testable: their `Runner::run()` result is the complete lifecycle decision.
- The aggregator accepts more process-local complexity in exchange for partition-level failure isolation and safe lease handoff.
- Graceful shutdown must be implemented in the work loop, not in a signal handler that performs blocking work. A signal begins shutdown; the runner performs the drain and finalisation.
- Exit status zero means "requested shutdown completed" or "job completed". Any unexpected end, failed finalisation, or startup error is non-zero.
- Adding a binary requires adding it to the lifecycle table and choosing a restart owner. Adding another in-process supervisor requires an ADR.

## Follow-up required to make the implementation match this contract

The current code does not yet implement every point above:

- the aggregator supervisor retries all errors, treats a clean task exit as final, and does not reset its backoff after sustained health;
- some single-role work loops return success when their source ends instead of treating that as an unexpected exit;
- the aggregator does not distinguish retryable from permanent errors inside its supervisor;
- `rebuilder` does not handle shutdown explicitly, so an interrupted attempt exits through normal process termination.

These are implementation changes, not permission to introduce another lifecycle pattern. Until these are addressed, the external supervisor should be configured to restart both non-zero exits and unexpectedly successful exits for the affected long-running services.
