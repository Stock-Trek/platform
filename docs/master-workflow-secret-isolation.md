# Master workflow secret isolation

Status: proposal only. No workflow is changed by this document. Copy-pasteable
replacement workflows are in `docs/examples/`.

## What `master.yml` does today

`.github/workflows/master.yml` has one job, `build-and-deploy` (line 20):

| Lines | Step | Effect |
| --- | --- | --- |
| 25-40 | two `Get secrets` steps | injects every secret from `aws`/`prod` and `publish`/`prod` into the job environment |
| 41-51 | Set up shipyard ssh auth | writes the prod-loaded read-only key to `~/.ssh/id_ed25519` |
| 52-61 | Generate GitHub App token | mints an installation token from the prod-loaded App private key |
| 62-67 | Checkout | checks out with that App token and `fetch-tags: true` |
| 80-104 | cargo-deny, cargo-audit, test, clippy, doc | runs third-party build scripts and test code |
| 107-175 | tag, ECR login, image build/push, Lambda update | commented out |

The workflow never deploys, yet every push to master gives the check job
production AWS and publish secrets plus a write-scoped App token, and then runs
`cargo test --all-targets --all-features` and dependency build scripts in that
same environment. A compromised crate, build script or test can read and
exfiltrate them. The name "Build and Deploy" is also wrong while deployment is
disabled.

`test.yml` has the same shape with the `agents`/`dev` identity. That key is
read-only and non-production, so the blast radius is smaller, but the pinning
and prefetch-then-offline pattern below applies there too.

## Target design

| | Checks job | Deploy job |
| --- | --- | --- |
| Runs | every master push | only after checks pass |
| Credentials | read-only Shipyard deploy key from a non-production identity | named AWS and App secrets from `prod` |
| Permissions | `contents: read`, `id-token: write` | `contents: write`, `id-token: write` |
| Environment | none | `production` with protection rules |
| Runs untrusted code | yes, after credentials are removed | no |

The Shipyard key is the only credential checks genuinely need: `Cargo.toml:89`
and `Cargo.toml:104` pull `iris` and `query-params` from the private `shipyard`
registry, and `exchange-types`, `market-data`, `ingester` and `exchange-gateway`
depend on them. The key must be read-only, non-production, and gone before
tests run.

## Task list

1. **Split the job.** Rename `build-and-deploy` to `checks` and drop "Deploy"
   from the workflow name while deployment stays disabled. When deployment is
   re-enabled, add the separate `deploy` job from
   `docs/examples/master-checks-and-deploy.yml`.
2. **Delete both `Get secrets` steps** (`master.yml:25-40`).
3. **Delete `Generate GitHub App token`** (`master.yml:52-61`) and the
   `Set up shipyard ssh auth` step (`master.yml:41-51`).
4. **Check out with the default token**: `persist-credentials: false`, no
   `fetch-tags`, no App token.
5. **Remove workflow-level `id-token: write`**; keep workflow-level
   `permissions: contents: read` and grant `id-token: write` only to the job
   that calls Infisical.
6. **Fetch only named Shipyard secrets** from a read-only CI identity using
   `secret-name`, not whole-project injection.
7. **Run cargo-deny with that read-only key** (it needs the private index),
   then prefetch dependencies with `./.github/actions/prefetch-cargo-deps`,
   which clears the key from `GITHUB_ENV` and sets `CARGO_NET_OFFLINE=true`.
8. **Run fmt, test, clippy and doc offline** so no later step needs the key.
9. **If deployment stays disabled, stop here**: no deploy job, no production
   secret loading anywhere in the workflow.
10. **If deployment is re-enabled**, add `deploy` with `needs: checks`,
    `environment: production`, `contents: write`, and named secret fetches.
11. **Pin every third-party action to a 40-character commit SHA** with a
    version comment. `master.yml` is mostly pinned already; the commented-out
    deploy steps still reference `@v6`, `@v2`, `@v4` and `@v7`.
12. **Add `.github/dependabot.yml`** with a `github-actions` ecosystem entry so
    the pins stay current, and run `actionlint` in CI.
13. **Apply the same treatment to `test.yml`**: named fetches, prefetch then
    offline, pinned actions, no whole-project injection.

## Private registry handling

`cargo-deny` runs in a container with its own Cargo home, so a prefetch on the
runner does not populate it and `--offline` cannot work there. Order the steps
so the key exists only as long as needed:

1. Fetch `SHIPYARD_KNOWN_HOST` and `SHIPYARD_SSH_PRIVATE_KEY_READONLY` by name.
2. Run `EmbarkStudios/cargo-deny-action` with those two values as `ssh-key`
   and `ssh-known-hosts`.
3. Run `./.github/actions/prefetch-cargo-deps` with the same values. It keeps
   the key in a short-lived `ssh-agent`, runs `cargo fetch --locked`, blanks
   `SHIPYARD_SSH_PRIVATE_KEY_READONLY` and `SHIPYARD_KNOWN_HOST` in
   `GITHUB_ENV`, and exports `CARGO_NET_OFFLINE=true`.
4. Run audit, fmt, test, clippy and doc with no Shipyard credential in the
   environment.

Do not export AWS, publish or App credentials into the checks job at all. A
dedicated CI/read-only identity is better than reusing the agents identity; the
examples use the existing `agents`/`dev` read-only values so they work without
new secrets.

## Pinned actions

| Action | Version | Commit |
| --- | --- | --- |
| `actions/checkout` | v5.1.0 | `fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09` |
| `actions-rust-lang/setup-rust-toolchain` | v2.0.1 | `a188e68d0fd2bd36ad51dccb2b07ba6e75105f30` |
| `Swatinem/rust-cache` | v2.9.3 | `eedcf2fb1c22f010a0eb66b92f378c110977f266` |
| `Infisical/secrets-action` | v1.0.18 | `d2e351f16c6ca20d17c85e6c992e04bdeb64e87d` |
| `EmbarkStudios/cargo-deny-action` | v2.1.1 | `3c6349835b2b7b196a839186cb8b78e02f7b5f25` |
| `rustsec/audit-check` | v2.0.0 | `69366f33c96575abad1ee0dba8212993eecbe998` |
| `actions/create-github-app-token` | v3.3.0 | `64ab729a6294000f5aa4a4bc0b73d1f32bc45510` |
| `aws-actions/configure-aws-credentials` | v6.3.0 | `e1253824e5c10ff9df46874f81ed3ec929e19cfd` |
| `aws-actions/amazon-ecr-login` | v2.1.7 | `aded0d722166a37980e030aa969dda0bfe6b6947` |
| `docker/setup-buildx-action` | v4.4.1 | `f87e5991a6d7451dcb8d9637bfbc97413f497069` |
| `docker/build-push-action` | v7.4.0 | `c3c9e263c25d99ce0380d002d59b67737d91b0dc` |

## Example workflows

- `docs/examples/master-checks-only.yml` — use this now, while deployment is
  disabled. Replace `.github/workflows/master.yml` with it.
- `docs/examples/master-checks-and-deploy.yml` — same checks job plus a
  `deploy` job that runs only on master, only after checks, behind the
  `production` environment. Use it only when deployment is re-enabled and
  validated.

Both are complete files. The deploy example assumes the App credentials and
`AWS_*` values live in `aws`/`prod`; adjust the `project-slug` if they do not.

Before enabling the deploy example, fix:

- `cargo pkgid --package build-agent` in the old commented block does not
  match the package, which is `platform-build-agent`
  (`crates/build-plane/build-agent/Cargo.toml:2`). The example uses the real
  name.
- The old block pushes `stock-trek-build-agent` but updates the Lambda from
  `algorithm-compiler:master`. The example derives both from the
  `IMAGE_REPOSITORY` job variable (`stock-trek-build-agent`); confirm the real
  ECR repository name.
- Nothing in the deployment block used the `publish` project, so the deploy job
  does not load it. Add it back only when publishing returns.

## Validate

```sh
actionlint .github/workflows/master.yml
grep -nE 'env\.(AWS|SHIPYARD|GITHUB_APP)|secrets\.' .github/workflows/master.yml
grep -n 'project-slug' .github/workflows/master.yml
```

Expected result: the checks job has no reference to production secret outputs
or `env.AWS_*`/`env.GITHUB_APP_*` values, and its only Infisical fetches are
the two non-production Shipyard secrets. `aws`/`prod` and `publish`/`prod`
appear only in the deploy job, which has `needs: checks` and
`environment: production`.

`rustsec/audit-check` documents `checks: write` and `issues: write` to publish
its result. The current workflow grants neither, and the examples keep that
behaviour, so audit failures surface in the step log rather than as a check
run. Add `checks: write` only if that reporting is required.
