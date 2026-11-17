# Front-end API: algorithms and builds

Conventions, authentication, errors, pagination, and streaming are defined in
[front-end-api.md](front-end-api.md).

## Model

- An **algorithm** is a Git repository containing trading strategy code. The build plane
  watches the repository through the GitHub App; pushes to watched refs trigger builds.
- A **build** is one compile of a commit. It produces a compiled cwasm artifact in the
  artifact bucket together with its SHA-256, size, and the Wasmtime version required to run
  it. Artifact bytes are never proxied through the API; the runner fetches them directly.
- A **strategy** is a named algorithm inside a build. One build may expose several strategies.
  Strategy names are unique within a build; duplicate names fail the build.
- **Preferences** are the strategy's tunable parameters, described by a JSON schema the build
  exposes. Bots reference strategies by name and carry parameter values validated against
  this schema.

## Lifecycle

Algorithms and builds have independent lifecycles:

| Algorithm state | Meaning |
| --- | --- |
| `active` | Builds can be triggered and strategies can be used by bots |
| `archived` | Read-only; existing bots keep running, no new builds |

| Build state | Meaning |
| --- | --- |
| `queued` | Accepted, waiting for the build agent |
| `building` | Source fetched and compiling |
| `ready` | Artifact stored with SHA-256 and Wasmtime version |
| `failed` | Compilation or packaging failed; `error` carries the reason |

A build only becomes usable by bots when it is `ready`. Archiving an algorithm is a soft
delete: history stays queryable.

## Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/v1/algorithms` | List algorithms connected to the owner |
| `GET` | `/v1/algorithms/{algorithmId}` | Algorithm detail |
| `DELETE` | `/v1/algorithms/{algorithmId}` | Archive the algorithm |
| `GET` | `/v1/algorithms/{algorithmId}/builds` | List builds, newest first, filterable by `ref` and `status` |
| `POST` | `/v1/algorithms/{algorithmId}/builds` | Trigger a build for a ref or commit |
| `GET` | `/v1/algorithms/{algorithmId}/builds/{buildId}` | Build detail and status |
| `GET` | `/v1/algorithms/{algorithmId}/builds/{buildId}/strategies` | Strategies exposed by the build |
| `GET` | `/v1/algorithms/{algorithmId}/builds/{buildId}/strategies/{strategyName}/preferences` | Parameter schema |

Algorithm list items identify the connected repository:

```json
{
  "data": [
    {
      "algorithmId": "alg_5t1",
      "name": "momentum",
      "repository": { "provider": "github", "host": "github.com", "repo": "acme/momentum" },
      "defaultRef": "main",
      "state": "active",
      "latestReadyBuildId": "bld_882"
    }
  ],
  "nextCursor": null
}
```

## Triggering builds

`POST /v1/algorithms/{algorithmId}/builds` accepts:

```json
{ "ref": "heads/main", "commit": "e1f3a9b" }
```

`ref` uses the `heads/` or `tags/` form used by the build plane's source keys; `commit` pins an
exact revision and defaults to the ref head. The response is `202` with the build
representation. Builds are idempotent per `(ref, commit)`: re-triggering an existing build
returns that build.

`GET .../builds/{buildId}` returns:

```json
{
  "buildId": "bld_882",
  "algorithmId": "alg_5t1",
  "ref": "heads/main",
  "commit": "e1f3a9b",
  "state": "ready",
  "artifact": {
    "sha256": "9f2b…",
    "sizeBytes": 4211843,
    "wasmtimeVersion": "50.0.1"
  },
  "error": null,
  "createdAt": "2026-07-14T08:12:00.000Z",
  "completedAt": "2026-07-14T08:14:31.000Z"
}
```

Build state changes are pushed on the `builds.{algorithmId}` stream channel.

## Strategies and preferences

`GET .../strategies` enumerates the strategies compiled into a build by running the artifact's
metadata command in the sandbox:

```json
{ "data": [ { "name": "momentum_v2", "description": "Window-cross momentum" } ], "nextCursor": null }
```

`GET .../strategies/{strategyName}/preferences` returns the JSON schema bots must satisfy in
their `parameters` object. The schema is what the front-end renders as a parameter form. A
build that fails to report metadata is treated as `failed`.

```json
{
  "strategyName": "momentum_v2",
  "jsonSchema": {
    "type": "object",
    "properties": { "fast": { "type": "integer", "minimum": 2 }, "slow": { "type": "integer" } },
    "required": ["fast", "slow"]
  }
}
```

Bots pick the build and strategy they run; they may pin an exact `buildId` or track an
algorithm's `latestReadyBuildId`. Tracking upgrades happen only after the front-end has had a
chance to validate parameters against the new build's preference schema, so bots re-validate
on upgrade and fall back to the pinned build when validation fails.

## Related references

- [Front-end API](front-end-api.md)
- [Bots and runs](front-end-api-bots.md)
- [Infrastructure runbook](infra.md) for the build plane's provisioning steps
