# Compilation performance report

This report covers changes that can be made inside the repository. Measurements
come from `cargo metadata`, `cargo tree` on x86_64 Linux, and local package
checks. Workflow files under `.github/` are out of scope.

## Summary

Compilation is dominated by third-party dependencies and by linking test
binaries, not by the workspace sources.

- 37 workspace crates, 1,006 Rust files, about 94.5k lines of Rust.
- `Cargo.lock` contains 670 packages: 633 external crates and 37 workspace
  members. The all-features Linux graph contains 583 package IDs.
- 37 crate names resolve to multiple versions, adding 44 extra package copies.
- The largest workspace closures are `platform-archive` (400 packages),
  `platform-portfolio-service` (288), `platform-build-orchestrator` (282),
  `platform-git-webhook` and `platform-portfolio-registry` (267 each),
  `platform-runner` (260), and `platform-build-agent` (248).

There is no blanket dependency `opt-level` override. Development and test
builds compile dependencies at the default `opt-level = 0`; release uses
`opt-level = 3` with thin LTO. The remaining cost is the amount of dependency
code that must be built for a whole-workspace run, plus the number of binaries
and test targets that must be linked.

## Current build profiles

| Profile | Settings |
| --- | --- |
| `dev` | `debug = "line-tables-only"`; dependencies have `debug = false` |
| `test` | `debug = false` |
| `release` | default `opt-level = 3`, `lto = "thin"`, `codegen-units = 16` |
| `agent` | inherits `release`, `lto = false` |

The `agent` profile is used both by the Docker prebuild and by
`Artifacts::build_wasm`. User code therefore no longer relinks the runner with
ThinLTO on every build. The artifact is
`target/wasm32-wasip1/agent/platform_runner.wasm`; both paths must continue to
use the same profile name and file name.

## Where the dependency graph is largest

The following counts are unique package names reachable from a workspace crate
with its default features. They show which crates are expensive to check and
test individually.

| Workspace crate | Packages | Main drivers |
| --- | --- | --- |
| `platform-archive` | 400 | Iceberg, OpenDAL, Arrow/Parquet, AWS S3 |
| `platform-portfolio-service` | 288 | auth, Redis, Iris, reqwest, AWS SQS |
| `platform-build-orchestrator` | 282 | AWS SDK, GitHub/octocrab, NATS, Lambda |
| `platform-git-webhook` | 267 | GitHub/octocrab, AWS, NATS, Lambda |
| `platform-portfolio-registry` | 267 | auth, DynamoDB, Redis, AWS config |
| `platform-runner` | 260 | wasmtime, wasmtime-wasi, reqwest, websockets |
| `platform-build-agent` | 248 | AWS Lambda/S3, reqwest, NATS, ZIP |

`platform-archive` is the only crate that needs Iceberg and the full
Arrow/Parquet stack. `platform-runner` is the only crate that needs wasmtime on
the host. These two closures dominate any workspace-wide compilation.

## Feature boundaries

### `platform-errors`

The error crate keeps heavy dependencies behind narrow features:

| Feature | Adds |
| --- | --- |
| `archive` | `arrow-schema`, `parquet` |
| `zip` | `zip` |
| `http` | `http`, JSON error conversion |
| `http-client` | `http` plus `reqwest` error conversion |
| `storage` | AWS credential types, `url` |
| `lambda` | AWS Lambda error types |
| `github` | `octocrab`, base64 |

Only `platform-archive` enables `archive`. `platform-build-agent` and
`platform-build-orchestrator` enable `zip` instead, so their graphs no longer
include Arrow or Parquet.

Only crates that actually construct or convert `reqwest` errors enable
`http-client` (`platform-build-agent`, `platform-runner`, and
`platform-portfolio-service`). `platform-git-webhook`,
`platform-build-orchestrator`, `platform-portfolio-registry`, and
`platform-archive` use the lightweight `http` feature. The HTTP error type
still exposes `HttpError::Http`, `Rejected`, header errors, and JSON errors
without pulling in a second HTTP client.

### TLS providers

`async-nats` uses `aws-lc-rs` instead of `ring`. This avoids compiling both
crypto providers for binaries whose TLS stack is otherwise selected by the AWS
SDK and `async-nats`. `platform-build-agent`, `platform-git-webhook`,
`platform-build-orchestrator`, `platform-portfolio-registry`, and the host
build of `platform-runner` no longer compile `ring`.

`ring` remains in `platform-archive` and `platform-portfolio-service`. The
remaining paths are Parquet encryption, OpenDAL/`reqwest`, Redis, and Iris.
Removing them requires changes in those dependencies or a different feature
set; it is not a manifest change that can be made safely in this repository
alone.

### Workspace dependency defaults

The workspace already turns off unnecessary defaults for `aws-config`, the AWS
service crates, `octocrab`, `iceberg-storage-opendal`, `reqwest`, `wasmtime`,
`wasmtime-wasi`, `async-nats`, and `zip`. The remaining feature choices are
constrained by what those crates enable internally; for example, Iceberg enables
the default Parquet codecs and encryption even though the archive code only
needs a subset.

## Duplicate versions

37 crate names have more than one version. The largest duplicate is `reqwest`:
0.12.28 from Iceberg and 0.13.5 everywhere else. Other repeated families include
`syn` 2 and 3, `thiserror` 1 and 2, `base64` 0.22 and 0.23, `hashbrown` in five
versions, `rand` in three versions, and `sha2` 0.10 and 0.11. Each copy is
compiled separately.

Most duplicates come from transitive dependencies. Changing them requires
upstream releases or replacing a dependency; pinning a single version from this
workspace would not be safe without checking every parent's compatibility.

## Remaining opportunities

### Further reduce the build-agent wasm build

The dedicated `agent` profile removes ThinLTO from runtime user builds. The next
step is to measure the remaining time with `cargo build --timings` inside the
build agent. If linking the runner still dominates, extract user code into a
small leaf crate that depends on a precompiled runner, or lower the agent
profile to `opt-level = "s"` or `opt-level = 1` after checking algorithm runtime
requirements.

### Keep dependency features narrow

New workspace crates should request only the `platform-errors` features they
use. In particular, avoid enabling `archive`, `http-client`, or `github` when a
crate only needs `zip`, `http`, or another lightweight module.

### Investigate the remaining `ring` users

Parquet encryption is enabled by Iceberg's dependency on Parquet defaults, while
OpenDAL's `reqwest`, Redis, and Iris select `ring` themselves. If a future
Iceberg or Parquet release allows disabling encryption and codecs independently,
the archive closure can drop `ring` and several compression crates.

### Measure before making broader structural changes

Use `cargo test --timings` and `cargo build --timings` to separate dependency
codegen from linking. Isolating `platform-archive` or `platform-runner` into a
separate workspace or build group would remove them from the fast feedback
path, but it also requires coordinated workflow changes outside this report's
scope.
