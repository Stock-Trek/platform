# Compilation performance report

Static analysis only: no build or test command was run and no build
configuration was changed. Sources: `Cargo.lock`, the crate manifests,
`cargo metadata --offline`, the Dockerfile and the GitHub workflows.

## Summary

Compilation is dominated by dependencies, not by this repository's code.

- 37 workspace crates, 1,006 Rust files and about 94.5k lines of Rust.
- 689 packages in `Cargo.lock`: 652 external crates plus the 37 workspace
  members. The workspace graph reaches 676 of them.
- CI performs four whole-workspace Cargo invocations with different
  target/feature sets: clippy, test, check and doc.

The most likely cause of the 13-14 minute time is
`[profile.dev.package."*"] opt-level = 3` in `Cargo.toml`: every external crate
is code-generated at `-C opt-level=3` for every test, clippy and check run, on
top of a graph that includes wasmtime, Iceberg, Arrow/Parquet and the AWS SDK.

## Where the time goes

| Item | Count |
| --- | --- |
| Workspace crates / Rust files / lines | 37 / 1,006 / ~94,500 |
| Locked packages / external crates / reachable | 689 / 652 / 676 |
| Crate names with multiple versions | 45 (52 extra copies) |
| Test source files (`*_tests.rs` and `tests/`) | 248 |
| Binaries / libraries | 15 `main.rs` / 29 `lib.rs` |

Most code compiled is third-party, so profile and dependency choices matter far
more than changes to workspace sources.

### Heavy dependency closures

Approximate package counts reachable from one dependency (lockfile graph;
optional dependencies make these upper bounds):

| Dependency | Packages | Needed by |
| --- | --- | --- |
| `iceberg-storage-opendal` | 410 | `platform-archive` only |
| `iceberg-catalog-rest` | 328 | `platform-archive` only |
| `octocrab` | 232 | `platform-errors` (`github` feature) only |
| `aws-config` | 227 | `platform-aws` |
| `aws-sdk-s3` | 226 | data, build and control planes |
| `aws-sdk-dynamodb` | 212 | `platform-aws` |
| `wasmtime-wasi` | 190 | `platform-runner` only |
| `wasmtime` | 138 | `platform-runner` only |
| `async-nats` | 182 | messaging |
| `reqwest` 0.13 | 178 | HTTP clients |

The two largest heavy dependencies are confined to one workspace crate each:
`wasmtime`/`wasmtime-wasi` in `platform-runner` and Iceberg/OpenDAL in
`platform-archive`. A single `cargo test --workspace` compiles both for every
change.

### Duplicate versions and crypto backends

Both `ring` and `aws-lc-rs`/`aws-lc-sys` are compiled. `rustls` has both
providers enabled (`aws_lc_rs` from its default features and `ring` from
`tokio-websockets`/`async-nats`), so the `aws-lc-sys` C/assembly build cannot
be skipped. `parquet` also enables `ring` for encryption.

45 crate names resolve to more than one version; examples: `reqwest` 0.12.28
(Iceberg) and 0.13.5 (everything else), `syn` 2 and 3, `thiserror` 1 and 2,
`base64` 0.22 and 0.23, `hashbrown` in four versions, `rand` in three, and
`sha2` 0.10 and 0.11. Every duplicate is compiled separately.

### Features that turn on more of the graph than the code uses

- `iceberg-storage-opendal` defaults enable the OpenDAL memory, filesystem and
  S3 services; the archive code only constructs `OpenDalStorageFactory::S3`.
- `aws-sdk-s3` enables `sigv4a`, but no source uses SigV4a or a signing region.
- `aws-config` defaults pull the SSO and credentials process providers
  (`aws-sdk-sso`, `aws-sdk-ssooidc`, `tokio/process`).
- `parquet` is built with `zstd`, `lz4`, `brotli`, `snap`, `flate2` and
  `encryption` although only ZSTD is used. `iceberg` enables those `parquet`
  defaults itself, so this cannot be trimmed from this repository.
- `platform-errors`' `storage` feature includes `archive`, so consumers such as
  `platform-runner` also pull Arrow, Parquet and ZIP.

### CI structure

`.github/workflows/test.yml` runs, in order:

1. `cargo test --all-targets --all-features` (codegen and linking)
2. `cargo clippy --all-targets --all-features -- -D warnings` (independent
   check pass)
3. `cargo check --workspace` (third pass, default features)
4. `cargo doc --no-deps --all-features` (rustdoc for all 37 crates)

Clippy and rustc use separate fingerprints, so neither reuses the other's
artifacts. `cargo check --workspace` is subsumed by the `--all-features` clippy
pass here because every `default` feature set is empty. The doc pass is a
fourth full front-end pass.

`Swatinem/rust-cache` runs before the toolchain is installed, so its key does
not include the rustc version and it cannot cache `~/.rustup`. It also saves a
cache for every branch by default, competing for the repository cache quota.

## Recommendations, in order of expected impact

### 1. Stop building every dependency at `opt-level = 3`

Remove or lower the blanket override:

```toml
[profile.dev.package."*"]
opt-level = 1   # or delete this line; 0 is the fastest to compile
debug = false
```

`test` inherits `dev`, so this affects every unit test and clippy run.
`opt-level = 1` usually keeps most of the test-runtime benefit of O3 at a
fraction of the codegen cost. If a few crates need O3 at runtime (crypto or
decimal-heavy code), override those packages individually rather than `"*"`.
Consider `CARGO_PROFILE_DEV_DEBUG=0` in CI as well.

Alternatively, keep local developer settings and add a dedicated CI profile:
`[profile.ci] inherits = "dev"` with `debug = 0`, `incremental = false` and
its own dependency `opt-level`, then pass `--profile ci` to every CI command.

Verification: compare `cargo test --timings` before and after.

### 2. Run fewer full-workspace passes in CI

Keep two gates instead of four:

```text
cargo clippy --all-targets --all-features -- -D warnings   # check pass
cargo test --all-targets --all-features                     # codegen pass
```

Drop `cargo check --workspace` (covered by clippy). Move `cargo doc` to the
master/nightly workflow or a parallel job instead of running it on every push.
Set `CARGO_INCREMENTAL=0` so ephemeral runners do not write incremental data
that will never be reused.

### 3. Make the cache work for this build

- Put `Swatinem/rust-cache` after `setup-rust-toolchain` so it hashes the
  installed rustc and can cache the toolchain.
- Use a shared key and only save from master, for example
  `shared-key: linux-rust` and
  `save-if: ${{ github.ref == 'refs/heads/master' }}`.
- Enable `cache-all-crates: true`, and keep `CARGO_INCREMENTAL=0` so the target
  cache stays within the cache quota.
- If hits remain poor, add `sccache` (GitHub Actions or S3 backend) so clippy,
  test, doc and split jobs share compiled dependency objects. `cargo nextest`
  speeds up test execution but not compilation.

### 4. Isolate the heavy crates from the fast feedback path

`wasmtime`/`wasmtime-wasi` (up to 190 packages) and Iceberg/OpenDAL (up to 410)
are each needed by only one crate. Options, in increasing effort:

- Add `default-members` excluding `platform-runner` and the data-plane crates,
  and run a separate CI job for them. `--workspace` overrides
  `default-members`, so the default CI commands must stop using it.
- Split CI into a fast job and a heavy job on separate runners. They can share
  the cache/sccache, and the fast job gives feedback while the heavy job runs.
- Make `platform-runner`'s host-only `wasmtime` dependency optional behind the
  `cwasm` feature, enabling it only in the runner job.
- Consider a separate workspace for `platform-runner`: it is the only wasmtime
  consumer and the crate compiled for wasm32 by the build agent.

### 5. Trim dependency features

- Use `iceberg-storage-opendal = { version = "0.11", default-features = false,
  features = ["opendal-s3"] }`; only the S3 factory is used.
- Remove `sigv4a` from `aws-sdk-s3`; no SigV4a usage exists in the source.
- If SSO and process credentials are not used in deployment, build `aws-config`
  with `default-features = false` plus `behavior-version-latest`,
  `default-https-client` and `rt-tokio`.
- Standardise on one rustls provider; if every consumer can use `ring`, the
  `aws-lc-sys` C/assembly build disappears. This needs coordinating `reqwest`,
  `aws-smithy-http-client` and `jsonwebtoken`.
- Align duplicate major versions where the parent dependency allows it; the
  largest duplicate is `reqwest` 0.12 versus 0.13 from Iceberg.
- Split `platform-errors`' `storage` feature so it does not imply `archive`;
  S3 error mapping should not require Arrow, Parquet and ZIP.

### 6. Reduce link time and add CPU

- There are more than 50 test and binary targets. The default GNU linker is
  slow; installing `lld` or `mold` and setting the linker in
  `.cargo/config.toml` usually cuts link time substantially. Do it in one
  change, because changing `RUSTFLAGS` invalidates the build cache.
- `ubuntu-latest` provides 4 vCPUs; an 8- or 16-core larger runner roughly
  halves wall time for a build this parallel, at higher cost.
- Keep `codegen-units = 16` in the release profile; lowering it increases
  release build time.

### 7. Measure before and after

- Run `cargo test --timings` (and `cargo build --timings` for the Docker/lambda
  build) and upload the HTML as a CI artifact to see whether dependency codegen
  or linking dominates.
- Compare GitHub step durations and rust-cache logs for hits and evictions.
- Apply the changes in the order above and measure after each one; the profile
  change alone may account for most of the improvement.

## If the 13-14 minutes is the build agent, not CI

The build agent compiles user algorithms at runtime.
`Artifacts::prepare_target_dir` copies the prebuilt `target/` directory into a
fresh temporary directory per request, then `build_wasm` runs
`cargo build --release --target wasm32-wasip1`. Because release uses
`lto = "thin"`, each build relinks the runner with ThinLTO over all prebuilt
dependencies.

If that is the slow path:

- Add a dedicated profile for user builds, for example
  `[profile.agent] inherits = "release"` with `lto = false` and
  `opt-level = 1` or `"s"`, prebuild the target directory with the same profile,
  and pass `--profile agent` in `build_wasm`.
- Measure `prepare_target_dir`, `build_wasm` and `compile_wasm` separately, and
  use `cargo build --timings`.
- Longer term, make user code a separate leaf crate depending on a precompiled
  runner so user builds do not relink the whole dependency graph. Keep
  `-C cache=no` on `wasmtime compile` for isolation.
