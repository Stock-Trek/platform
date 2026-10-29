# Moving secrets from Infisical to AWS

This document records the migration away from Infisical for the `platform-git-webhook`
runtime and the remaining operational work needed to remove Infisical from the
repository's CI workflows.

## Current state

Before this change the webhook Lambda loaded `GITHUB_WEBHOOK_SECRET` from Infisical:

1. The Lambda used its AWS credentials to sign an STS `GetCallerIdentity` request.
2. It exchanged that signed request for an Infisical access token (`aws-auth`).
3. It called Infisical's `/api/v4/secrets` endpoint to read the whole project/
   environment and copied the result into an in-memory map.
4. `GitHubEventConverter` read `GITHUB_WEBHOOK_SECRET` from that map.

The GitHub Actions workflows (`agent.yml`, `master.yml`, `test.yml`) also fetch
secrets with `Infisical/secrets-action`. Those are CI credentials rather than
runtime configuration and are covered separately below.

## Runtime design

The webhook now reads a single AWS Secrets Manager secret through the normal AWS
SDK credential chain (the Lambda execution role). The secret must contain a JSON
object, which keeps the existing key-based lookup and makes it possible to add
more webhook values later:

```json
{
  "GITHUB_WEBHOOK_SECRET": "<github-webhook-signing-secret>"
}
```

Configuration changes:

| Previous | Replacement |
| --- | --- |
| `INFISICAL_IDENTITY_ID` | Removed |
| `INFISICAL_PROJECT_ID` | Removed |
| `INFISICAL_ENVIRONMENT` | Removed |
| `INFISICAL_BASE_URL` | Removed |
| — | `WEBHOOK_SECRET_ARN` (secret ARN or name) |

The webhook keeps its existing cache, refresh-on-signature-rejection, throttling,
and failure-backoff behaviour. Missing secrets, malformed JSON, and permanent
Secrets Manager errors are now surfaced as `SecretsError` values so they are not
retried indefinitely.

### IAM

The Lambda execution role needs read access to only that secret:

```json
{
  "Effect": "Allow",
  "Action": "secretsmanager:GetSecretValue",
  "Resource": "arn:aws:secretsmanager:<region>:<account-id>:secret:platform/git-webhook-*"
}
```

If the secret uses a customer-managed KMS key, the role also needs
`kms:Decrypt` on that key. Prefer an IAM policy scoped to the secret ARN rather
than `secretsmanager:*`.

### Deploying the secret

Create one secret per environment (for example with the AWS CLI):

```bash
aws secretsmanager create-secret \
  --name platform/git-webhook \
  --description "GitHub webhook signing secret" \
  --secret-string '{"GITHUB_WEBHOOK_SECRET":"<value>"}'
```

Then set the Lambda environment variable `WEBHOOK_SECRET_ARN` to the returned
ARN. Secrets Manager rotation can be used for the signing secret; the webhook
already refreshes its cached value when GitHub rejects a signature.

### Rollout

1. Create the Secrets Manager secret and grant the Lambda role access.
2. Deploy the new Lambda version with `WEBHOOK_SECRET_ARN` set.
3. Verify a GitHub ping/webhook is accepted and that signature rejection still
   triggers a refresh.
4. Remove the four `INFISICAL_*` environment variables.
5. Remove the Infisical identity's AWS access once no workloads use it.

## CI workflows

The workflows still use Infisical because their AWS role/account configuration is
not represented in this repository. Completing those migrations requires the
following per workflow:

1. Grant the GitHub Actions OIDC provider an AWS role that can read the required
   secrets.
2. Replace each `Infisical/secrets-action` step with
   `aws-actions/configure-aws-credentials` followed by a scoped
   `aws secretsmanager get-secret-value` call.
3. Map each Infisical secret name to its Secrets Manager ARN/name. The names
   currently referenced by the workflows are:
   - `DEEPSEEK_API_KEY`, `EDENAI_API_KEY`
   - `GITHUB_APP_AGENT_HELPER_CLIENT_ID`, `GITHUB_APP_AGENT_HELPER_PRIVATE_KEY`
   - `GITHUB_APP_CREATE_TAGS_CLIENT_ID`, `GITHUB_APP_CREATE_TAGS_PRIVATE_KEY`
   - `CARGO_REGISTRIES_SHIPYARD_TOKEN`
   - `SHIPYARD_SSH_PRIVATE_KEY_READONLY`, `SHIPYARD_KNOWN_HOST`
   - the deployment values read from the `aws` and `publish` projects in
     `master.yml`
4. Keep the existing least-privilege practice of fetching only the secret a job
   needs, and avoid injecting whole secret bundles into a job environment.

No role ARNs or account IDs are hard-coded here because they are deployment
configuration and should come from repository/environment variables.
