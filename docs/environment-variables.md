# Environment variables

This document lists the environment variables read directly by platform processes.

Every process reads and validates its configuration at startup. A required variable that is
missing, empty, or invalid causes the process to fail before it starts work. For optional
variables, an unset or empty value is treated as if the variable was not provided.

Variables used by more than one process are listed once in the shared section. The remaining
sections list the additional variables read by each process.

> The AWS SDK also reads its standard environment variables (for example `AWS_REGION`,
> `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and `AWS_PROFILE`) through
> its default region and credential chain. Those are not listed below because they are not read
> directly by platform code.

## Shared variables

### Required

| Variable | Used by | Description |
| --- | --- | --- |
| `AWS_CONNECT_TIMEOUT_SECS` | Build orchestrator, Git webhook | AWS SDK connection timeout, in seconds. |
| `AWS_LAMBDA_FUNCTION_NAME` | Build orchestrator, Git webhook, Build agent | Name of the current Lambda function, used as a metric dimension. The Lambda runtime sets this automatically. |
| `AWS_OPERATION_TIMEOUT_SECS` | Build orchestrator, Git webhook | AWS SDK operation timeout, in seconds. |
| `BUILD_AGENT_SOURCE_DOWNLOAD_TIMEOUT_SECS` | Build orchestrator, Build agent | Maximum time the build agent may spend downloading source, in seconds. |
| `BUILD_AGENT_TIMEOUT_SECS` | Build orchestrator, Build agent | Maximum time the build agent may spend on a build, in seconds. |
| `DEADLINE_BUFFER_SECS` | Build orchestrator, Git webhook | Time reserved for cleanup before the Lambda invocation deadline, in seconds. |
| `GITHUB_ENTERPRISE_ALLOWED_HOSTS` | Build orchestrator, Git webhook | Comma-separated list of GitHub hosts allowed for outbound requests. `github.com` is always allowed. |
| `ICEBERG_CATALOG_URI` | Archiver, Compactor, Rebuilder; optional for Aggregator | Iceberg REST catalog URI using the `http` or `https` scheme. Required by the archiver, compactor, and rebuilder. For the aggregator it is optional and enables the compacted/raw archive handoff when set. |
| `ICEBERG_CATALOG_WAREHOUSE` | Archiver, Compactor, Rebuilder; Aggregator when a catalog is configured | Iceberg warehouse URI. Must use the `s3://` scheme. |
| `ICEBERG_NAMESPACE` | Archiver, Compactor, Rebuilder; Aggregator when a catalog is configured | Iceberg namespace. |
| `RUST_LOG` | All services | Tracing filter, for example `info` or `platform_ingester=debug`. |
| `S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES` | Build orchestrator, Build agent | Maximum accepted source archive size, in bytes. |
| `SECRETS_SSM_DECRYPT` | Backfiller, Git webhook | Whether SSM parameters are decrypted, parsed as a boolean. |
| `SECRETS_SSM_PATH` | Backfiller, Git webhook | SSM parameter path to load secrets from. A leading `/` is added when missing, and a trailing `/` is rejected. |
| `SQS_QUEUE_URL` | Build orchestrator, Git webhook | URL of the SQS queue that carries build requests. |

### Optional

| Variable | Used by | Description |
| --- | --- | --- |
| `ICEBERG_CATALOG_TOKEN` | Archiver, Compactor, Rebuilder, Aggregator | Optional bearer token used to authenticate with the Iceberg REST catalog. When unset or empty, no token is sent. |

## Data plane

### Ingester

#### Required

| Variable | Description |
| --- | --- |
| `INGESTER_EXCHANGE` | Exchange identifier to connect to, for example `binance`. |
| `INGESTER_NATS_SUBJECT_PREFIX` | NATS subject prefix for published market events. |
| `INGESTER_NATS_URL` | NATS URL used to publish market events. |
| `INGESTER_STREAM_FORMAT` | Exchange stream format. Supported values are `json`, `binary`, `binance_sbe`, and `binance-sbe`. |
| `INGESTER_WEBSOCKET_URL` | Exchange WebSocket URL using the `ws` or `wss` scheme, including the trade streams to subscribe to. The Binance ingester rejects subscriptions that contain both `@trade` and `@aggTrade` for the same symbol. |

#### Optional

| Variable | Description |
| --- | --- |
| `INGESTER_WEBSOCKET_API_KEY` | Optional API key sent to the exchange WebSocket endpoint. |

### Backfiller

#### Required

| Variable | Description |
| --- | --- |
| `BACKFILLER_EVENT_SUBJECT_PREFIX` | NATS subject prefix for replayed market events. |
| `BACKFILLER_EXCHANGE` | Exchange identifier to backfill, for example `binance`. |
| `BACKFILLER_EXCHANGE_HTTP_URL` | Exchange REST base URL using the `http` or `https` scheme, used to fetch historical trades. |
| `BACKFILLER_GAP_SUBJECT_PREFIX` | NATS subject prefix carrying gap markers from the ingester. |
| `BACKFILLER_NATS_URL` | NATS URL used to receive gap markers and publish replayed events. |

#### Optional

No optional variables.

### Aggregator

#### Required

| Variable | Description |
| --- | --- |
| `AGGREGATOR_ALLOWED_LATENESS_MS` | How late a trade may arrive and still update a candle, in milliseconds. |
| `AGGREGATOR_CHECKPOINT_BUCKET` | NATS KV bucket used for checkpoints and writer leases. |
| `AGGREGATOR_CHECKPOINT_INTERVAL_SECS` | How often keyed state is checkpointed, in seconds. Must be lower than `AGGREGATOR_LEASE_TTL_SECS`. |
| `AGGREGATOR_CHECKPOINT_PREFIX` | Key prefix used for checkpoints and leases inside the checkpoint bucket. |
| `AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX` | NATS subject prefix for closed candles. |
| `AGGREGATOR_DURABLE_PREFIX` | Prefix for durable consumer names. The router uses `{prefix}-router`, and each partition uses `{prefix}-shard-{n}`. |
| `AGGREGATOR_EVENT_SUBJECT_PREFIX` | NATS subject prefix for source market events. |
| `AGGREGATOR_GAP_SUBJECT_PREFIX` | NATS subject prefix carrying gap markers from the ingester. |
| `AGGREGATOR_INTERVALS` | Comma-separated candle intervals to generate, for example `1m,5m,1h`. |
| `AGGREGATOR_LEASE_TTL_SECS` | Lifetime of the single-writer partition lease, in seconds. |
| `AGGREGATOR_NATS_URL` | NATS URL used for source, gap, shard, and candle subjects. |
| `AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX` | NATS subject prefix for open candle updates. |
| `AGGREGATOR_REPLAY_START_POLICY` | State to start a partition from: `checkpoint`, `archive`, `earliest`, or `latest`. |
| `AGGREGATOR_SHARD_COUNT` | Number of keyed partitions. Must not exceed the archive routing bucket count. |
| `AGGREGATOR_SHARD_SUBJECT_PREFIX` | NATS subject prefix owned by the aggregator for routed shard inputs. |
| `AGGREGATOR_STATE_SCHEMA_VERSION` | State schema version written to new state and required for checkpoint restore. |
| `AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS` | Idle time before the wall clock advances the event-time watermark and closes expired windows, in seconds. |

#### Optional

| Variable | Description |
| --- | --- |
| `AGGREGATOR_ARCHIVE_S3_BUCKET` | S3 bucket containing archived market events. Required when `AGGREGATOR_REPLAY_START_POLICY` is `checkpoint` or `archive`; optional for `earliest` and `latest`. Archive recovery is enabled when set. |
| `AGGREGATOR_ARCHIVE_S3_PREFIX` | Raw archive prefix used when no Iceberg catalog is configured. Required when `AGGREGATOR_ARCHIVE_S3_BUCKET` is set. |
| `AGGREGATOR_SHARD_INDEX` | Zero-based partition index to run as a single partition. When unset or empty, the process runs every partition. Must be lower than `AGGREGATOR_SHARD_COUNT`. |

### Archiver

#### Required

| Variable | Description |
| --- | --- |
| `ARCHIVER_BUCKET_SECS` | Archive time bucket size, in seconds. |
| `ARCHIVER_CLOSED_CANDLE_SUBJECT_PREFIX` | NATS subject prefix for closed candles. |
| `ARCHIVER_DEDUPE_CAPACITY` | Capacity of the in-memory archive deduplication buffer. |
| `ARCHIVER_EVENT_SUBJECT_PREFIX` | NATS subject prefix for source market events. |
| `ARCHIVER_FLUSH_INTERVAL_SECS` | Maximum time between archive flushes, in seconds. |
| `ARCHIVER_MAX_RECORDS` | Maximum number of records buffered before an archive flush. |
| `ARCHIVER_NATS_URL` | NATS URL used to subscribe to market events and closed candles. |
| `ARCHIVER_S3_BUCKET` | S3 bucket for archived raw objects. |
| `ARCHIVER_S3_PREFIX` | S3 prefix for archived raw objects. |

#### Optional

No optional variables.

### Compactor

#### Required

| Variable | Description |
| --- | --- |
| `COMPACTOR_MIN_SOURCE_FILES` | Minimum number of source files required before a compaction group is processed. |
| `COMPACTOR_OUTPUT_PREFIX` | S3 prefix for compacted output objects. |
| `COMPACTOR_POLL_INTERVAL_SECS` | How often the compactor polls for new source files, in seconds. |
| `COMPACTOR_S3_BUCKET` | S3 bucket containing raw archive objects. |
| `COMPACTOR_S3_PREFIX` | S3 prefix containing raw archive objects to compact. |
| `COMPACTOR_SETTLE_SECS` | How long source files must remain unchanged before they are compacted, in seconds. |
| `COMPACTOR_SPILL_RECORDS` | Number of records buffered before spilling to disk during compaction. |

#### Optional

No optional variables.

### Rebuilder

#### Required

| Variable | Description |
| --- | --- |
| `REBUILDER_ALLOWED_LATENESS_MS` | How late a trade may arrive and still update a candle during the rebuild, in milliseconds. |
| `REBUILDER_BUCKET_MILLIS` | Output bucket size in milliseconds. |
| `REBUILDER_DRY_RUN` | Whether to rebuild without writing output, parsed as a boolean. |
| `REBUILDER_FINISH_OPEN_WINDOWS` | Whether to close candles that are still open when the archived input ends, parsed as a boolean. |
| `REBUILDER_INTERVALS` | Comma-separated candle intervals to regenerate, for example `1m,5m,1h`. |
| `REBUILDER_OUTPUT_LAYER` | Candle output layer: `raw` or `compacted`. |
| `REBUILDER_OUTPUT_PREFIX` | S3 prefix for rebuilt candle objects. |
| `REBUILDER_S3_BUCKET` | S3 bucket for rebuilt candle output. |
| `REBUILDER_SOURCE_LAYER` | Event source layer: `auto`, `raw`, or `compacted`. `auto` reads compacted files plus the un-compacted raw tail. |
| `REBUILDER_STATE_EPOCH` | State epoch assigned to regenerated candle revisions. |

#### Optional

No optional variables.

## Build plane

### Build orchestrator

#### Required

| Variable | Description |
| --- | --- |
| `AWS_ENDPOINT_URL_S3` | S3-compatible endpoint URL using the `http` or `https` scheme, used for source archives and artifact access. |
| `BUILD_AGENT_FUNCTION_NAME` | Name of the build agent Lambda function that the orchestrator invokes. |
| `GITHUB_APP_ID` | GitHub App ID used to authenticate with the GitHub API. |
| `GITHUB_APP_PRIVATE_KEY` | GitHub App private key in PEM format. Literal `\n` sequences are converted to newlines. |
| `GITHUB_CONNECT_TIMEOUT_SECS` | GitHub API connection timeout, in seconds. |
| `GITHUB_MAX_CACHED_INSTALLATIONS` | Maximum number of cached GitHub App installation tokens. |
| `GITHUB_OPERATION_TIMEOUT_SECS` | GitHub API operation timeout, in seconds. |
| `GIT_MAX_DIRECTORY_BYTES` | Maximum total size of a fetched repository directory, in bytes. |
| `GIT_MAX_DIRECTORY_DEPTH` | Maximum directory depth allowed when walking a fetched repository. |
| `GIT_MAX_DIRECTORY_FILES` | Maximum number of files allowed in a fetched directory. |
| `S3_BUCKET_USER_ARTIFACTS` | S3 bucket for user source archives and build artifacts. |
| `S3_MAX_ARTIFACT_SIZE_BYTES` | Maximum accepted build artifact size, in bytes. |
| `S3_SIGNED_URL_EXPIRY_SECS` | Lifetime of S3 signed URLs, in seconds. Must be at least the build agent timeout plus the source download timeout plus a five-minute upload margin, and at most seven days. |
| `SQS_DEDUP_MAX_MESSAGES` | Maximum number of SQS message IDs kept in the in-memory deduplication cache. |
| `SQS_DEDUP_TTL_SECS` | How long SQS message IDs remain in the deduplication cache, in seconds. |
| `SQS_PROCESS_CONCURRENCY` | Maximum number of SQS messages processed concurrently. |

#### Optional

No optional variables.

### Git webhook

#### Required

| Variable | Description |
| --- | --- |
| `WEBHOOK_COLD_START_REFRESH_TIMEOUT_SECS` | Maximum time allowed to refresh the webhook secret during a cold start, in seconds. |
| `WEBHOOK_DEDUP_MAX_DELIVERIES` | Maximum number of GitHub delivery IDs kept in the in-memory deduplication cache. |
| `WEBHOOK_DEDUP_TTL_SECS` | How long GitHub delivery IDs remain in the deduplication cache, in seconds. |
| `WEBHOOK_REFRESH_COOLDOWN_SECS` | Minimum interval between webhook secret refreshes, in seconds. |
| `WEBHOOK_SECRET_FAILURE_BACKOFF_SECS` | Backoff applied after a failed webhook secret refresh, in seconds. |

#### Optional

No optional variables.

### Build agent

#### Required

| Variable | Description |
| --- | --- |
| `BUILD_AGENT_BUILD_CACHE_DIR` | Directory used to cache build dependencies and outputs between builds. |
| `BUILD_AGENT_MAX_EXTRACTED_SOURCE_SIZE_BYTES` | Maximum total size of extracted source, in bytes. |
| `BUILD_AGENT_SOURCE_DIR` | Working directory where the build agent unpacks and builds source. |
| `BUILD_AGENT_FAILURE_PUBLISH_BUDGET_SECS` | Time budget reserved for publishing a failure result before the invocation deadline, in seconds. |

#### Optional

No optional variables.

## Secrets

Some values are read from AWS Systems Manager Parameter Store instead of the process
environment. The backfiller and Git webhook load every parameter under `SECRETS_SSM_PATH` and
look up secrets by name:

- The Git webhook expects `GITHUB_WEBHOOK_SECRET`.
- The backfiller expects `BACKFILLER_EXCHANGE_API_KEY`.

These names are SSM parameter names, not environment variables.
