# Health checks

Long-running platform services expose an optional HTTP listener for orchestrator probes. Enable it
with `HEALTH_BIND_ADDR` (or its `HEALTH_BIND_ADDRESS` alias), or with a service-specific `<SERVICE>_HEALTH_BIND_ADDR` that takes
precedence over the shared value. When neither variable is set, the listener is not started.

The listener serves two endpoints:

| Endpoint | Meaning |
| --- | --- |
| `GET /healthz` | Liveness. The process can serve requests. This endpoint does not depend on downstream systems. |
| `GET /readyz` | Readiness. The process has completed startup and is ready to receive production traffic. |

Both endpoints return `204 No Content` when healthy. `GET /readyz` returns
`503 Service Unavailable` while startup or recovery is still in progress. `HEAD` is accepted on
both paths. Other methods return `405 Method Not Allowed`; unknown paths return `404 Not Found`.

Liveness is tied to process exit. A process that cannot start, loses its listener, or terminates
must be restarted by the orchestrator. A failed health listener is treated as a fatal service
error. Readiness is set only after the service has connected its required dependencies, completed
recovery work, and bound its production listener. It is cleared when the service begins shutting
down.

## Services

| Service | Service-specific variable prefix |
| --- | --- |
| Auth service | `AUTH_SERVICE` |
| Portfolio service | `PORTFOLIO_SERVICE` |
| Ingester | `INGESTER` |
| Backfiller | `BACKFILLER` |
| Aggregator | `AGGREGATOR` |
| Archiver | `ARCHIVER` |
| Compactor | `COMPACTOR` |
| Rebuilder | `REBUILDER` |

The portfolio registry exposes `/healthz` and `/readyz` on both its admin and read listeners. Its
readiness is true once those listeners are accepting requests.

## Probe guidance

Use `GET /healthz` for liveness probes with a restart policy. Use `GET /readyz` for readiness
probes that remove the instance from traffic without restarting it. During graceful shutdown,
orchestrators should rely on connection draining rather than a readiness probe alone.
