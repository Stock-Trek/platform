# Environment variables

This document lists the environment variables read directly by platform processes. Each process
reads and validates its configuration at startup, before doing any work.

- Required variables must be present with a non-empty, valid value. A missing or invalid value
  fails startup.
- Optional variables are treated as unset when they are absent or empty.
- Values are read only from the process environment. No variable falls back to a built-in default
  value.

Variables read by more than one process are listed once under [Shared variables](#shared-variables).
The remaining sections list the additional variables read by each process. When a variable is only
required in some configurations, the condition is stated in its description.

> The AWS SDK also reads its standard environment variables (for example `AWS_REGION`,
> `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and `AWS_PROFILE`) through
> its default region and credential chains. Those are not listed below because platform code does
> not read them directly.

## Shared variables

### Required

| Variable | Used by | Description |
| --- | --- | --- |
| `AWS_CONNECT_TIMEOUT_SECS` | Build orchestrator, Git webhook, Run invoker | Timeout for establishing AWS SDK connections, in seconds. |
| `AWS_LAMBDA_FUNCTION_NAME` | Build orchestrator, Git webhook, Build agent | Name of the current Lambda function, used as a metric dimension. The Lambda runtime sets this automatically. |
| `AWS_OPERATION_TIMEOUT_SECS` | Build orchestrator, Git webhook, Run invoker | Timeout for AWS SDK operations, in seconds. |
| `BUILD_AGENT_SOURCE_DOWNLOAD_TIMEOUT_SECS` | Build orchestrator, Build agent | Maximum time the build agent may spend downloading source, in seconds. |
| `BUILD_AGENT_TIMEOUT_SECS` | Build orchestrator, Build agent | Maximum time the build agent may spend on a build, in seconds. |
| `DEADLINE_BUFFER_SECS` | Build orchestrator, Git webhook | Time reserved for cleanup before the Lambda invocation deadline, in seconds. |
| `GITHUB_ENTERPRISE_ALLOWED_HOSTS` | Build orchestrator, Git webhook | Comma-separated allowlist of GitHub hosts for outbound HTTPS requests, with optional ports (for example `ghe.example.com` or `ghe.example.com:8443`). `github.com` is always allowed. |
| `ICEBERG_CATALOG_URI` | Archiver, Compactor, Rebuilder; Aggregator for `checkpoint`/`archive` | Iceberg REST catalog URI using the `http` or `https` scheme. Required by the archiver, compactor, and rebuilder, and by the aggregator when `AGGREGATOR_REPLAY_START_POLICY` is `checkpoint` or `archive`, where archive recovery reads the compacted/raw Iceberg handoff. |
| `ICEBERG_CATALOG_WAREHOUSE` | Archiver, Compactor, Rebuilder, Aggregator | Iceberg warehouse URI. Must use the `s3://` scheme. Required when `ICEBERG_CATALOG_URI` is configured. |
| `ICEBERG_NAMESPACE` | Archiver, Compactor, Rebuilder, Aggregator | Iceberg namespace. Required when `ICEBERG_CATALOG_URI` is configured. |
| `RUST_LOG` | All services | Tracing filter, for example `info` or `platform_ingester=debug`. |
| `S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES` | Build orchestrator, Build agent | Maximum accepted source archive size, in bytes. |
| `SECRETS_SSM_DECRYPT` | Backfiller, Git webhook | Whether SSM parameters are decrypted, parsed as `true` or `false`. |
| `SECRETS_SSM_PATH` | Backfiller, Git webhook | SSM parameter path to load secrets from. A leading `/` is added when missing, and a trailing `/` is rejected. |
| `SQS_QUEUE_URL` | Build orchestrator, Git webhook | URL of the SQS queue that carries build requests. |

### Optional

| Variable | Used by | Description |
| --- | --- | --- |
| `ICEBERG_CATALOG_TOKEN` | Archiver, Compactor, Rebuilder, Aggregator | Bearer token used to authenticate with the Iceberg REST catalog. When unset or empty, no token is sent. |

## Data plane

### Ingester

#### Required

| Variable | Description |
| --- | --- |
| `INGESTER_EXCHANGE` | Exchange identifier to connect to, for example `binance`. |
| `INGESTER_NATS_SUBJECT_PREFIX` | NATS subject prefix for published market events. |
| `INGESTER_NATS_URL` | NATS URL used to publish market events. |
| `INGESTER_STREAM_FORMAT` | Exchange stream format. Supported values are `json` and `binary`; `binance_sbe` and `binance-sbe` are aliases for `binary`. Binary formats are currently supported only for Binance. |
| `INGESTER_WEBSOCKET_URL` | Exchange WebSocket URL using the `ws` or `wss` scheme, including the trade streams to subscribe to. The Binance ingester rejects subscriptions that contain both `@trade` and `@aggTrade` for the same symbol. |

#### Optional

| Variable | Description |
| --- | --- |
| `INGESTER_WEBSOCKET_API_KEY` | API key sent as the `X-MBX-APIKEY` header. Currently supported only for Binance. |

### Backfiller

#### Required

| Variable | Description |
| --- | --- |
| `BACKFILLER_EVENT_SUBJECT_PREFIX` | NATS subject prefix for replayed market events. |
| `BACKFILLER_EXCHANGE` | Exchange identifier to backfill, for example `binance`. |
| `BACKFILLER_EXCHANGE_HTTP_URL` | Exchange REST base URL using the `http` or `https` scheme, used to fetch historical trades. |
| `BACKFILLER_EXCHANGE_SIGNING_ALGORITHM` | Exchange request signing algorithm. Must be `ed25519`. |
| `BACKFILLER_EXCHANGE_TRADING_MODE` | Exchange trading mode: `real` for production or `paper` for testnet. |
| `BACKFILLER_GAP_SUBJECT_PREFIX` | NATS subject prefix carrying gap markers from the ingester. |
| `BACKFILLER_NATS_URL` | NATS URL used to receive gap markers and publish replayed events. |

#### Optional

No process-specific optional variables.

### Aggregator

#### Required

| Variable | Description |
| --- | --- |
| `AGGREGATOR_ALLOWED_LATENESS_MS` | How late a trade may arrive and still update a candle, in milliseconds. |
| `AGGREGATOR_CHECKPOINT_BUCKET` | NATS KV bucket used for checkpoints and writer leases. Must contain only alphanumeric characters, dashes, and underscores. |
| `AGGREGATOR_CHECKPOINT_INTERVAL_SECS` | How often keyed state is checkpointed, in seconds. Must be lower than `AGGREGATOR_LEASE_TTL_SECS`. |
| `AGGREGATOR_CHECKPOINT_PREFIX` | Key prefix used for checkpoints and leases inside the checkpoint bucket. Must be a valid NATS subject prefix. |
| `AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES` | Startup ceiling, in bytes, for the estimated worst-case retained closed-candle state per shard. The static pair ceiling, per-shard share, and per-interval caps must fit within this budget. |
| `AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX` | NATS subject prefix for closed candles. |
| `AGGREGATOR_CORRECTION_CUTOFF_MS` | Event-time correction cutoff in milliseconds. Must be greater than zero and at least `AGGREGATOR_ALLOWED_LATENESS_MS`; inputs whose candle window ended before `watermark - cutoff` are rejected. |
| `AGGREGATOR_DURABLE_PREFIX` | Prefix for durable consumer names. The router uses `{prefix}-router`, and each shard uses `{prefix}-shard-{n}`. Must be a valid NATS subject prefix. |
| `AGGREGATOR_EVENT_SUBJECT_PREFIX` | NATS subject prefix for source market events. |
| `AGGREGATOR_GAP_SUBJECT_PREFIX` | NATS subject prefix carrying gap markers from the ingester. |
| `AGGREGATOR_LEASE_TTL_SECS` | Lifetime of the single-writer shard lease, in seconds. |
| `AGGREGATOR_MAX_CLOSED_CANDLES_PER_INTERVAL` | Per-interval closed-candle caps as comma-separated `<interval>=<count>` entries, for example `1m=1440,5m=288,1h=48`. Requires exactly one positive entry for every supported interval; missing, duplicate, unknown, malformed, and non-positive entries are rejected. |
| `AGGREGATOR_NATS_URL` | NATS URL used for source events, gap markers, routed shards, and candle publishing. |
| `AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX` | NATS subject prefix for open candle updates. |
| `AGGREGATOR_REPLAY_START_POLICY` | State to start a shard from: `checkpoint`, `archive`, `earliest`, or `latest`. |
| `AGGREGATOR_SHARD_COUNT` | Number of keyed shards. Must be positive and must not exceed the archive routing bucket count. |
| `AGGREGATOR_SHARD_SUBJECT_PREFIX` | NATS subject prefix owned by the aggregator for routed shard inputs. |
| `AGGREGATOR_STATE_SCHEMA_VERSION` | State schema version written to new state and required for checkpoint restore. |
| `AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS` | How long input may be idle before the aggregator flushes already-expired candle windows, in seconds. Idle flushes do not advance event time. |

#### Optional

| Variable | Description |
| --- | --- |
| `AGGREGATOR_SHARD_INDEX` | Zero-based shard index to run as a single shard. When unset or empty, the process runs every shard. Must be lower than `AGGREGATOR_SHARD_COUNT`. |

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

No process-specific optional variables.

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

No process-specific optional variables.

### Rebuilder

#### Required

| Variable | Description |
| --- | --- |
| `REBUILDER_ALLOWED_LATENESS_MS` | How late a trade may arrive and still update a candle during the rebuild, in milliseconds. |
| `REBUILDER_BUCKET_MILLIS` | Output archive bucket size in milliseconds. |
| `REBUILDER_CORRECTION_CUTOFF_MS` | Event-time correction cutoff in milliseconds. Must be greater than zero and at least `REBUILDER_ALLOWED_LATENESS_MS`; inputs whose candle window ended before `watermark - cutoff` are rejected. |
| `REBUILDER_DRY_RUN` | Whether to rebuild without writing output, parsed as a boolean. |
| `REBUILDER_FINISH_OPEN_WINDOWS` | Whether to close candles that are still open when the archived input ends, parsed as a boolean. |
| `REBUILDER_MAX_CLOSED_CANDLES_PER_INTERVAL` | Per-interval closed-candle caps as comma-separated `<interval>=<count>` entries. Requires exactly one positive entry for every supported interval; missing, duplicate, unknown, malformed, and non-positive entries are rejected. |
| `REBUILDER_OUTPUT_LAYER` | Output candle layer: `raw` or `compacted`. |
| `REBUILDER_OUTPUT_PREFIX` | S3 prefix for rebuilt candle objects. |
| `REBUILDER_S3_BUCKET` | S3 bucket for rebuilt candle output. |
| `REBUILDER_SOURCE_LAYER` | Event source layer: `auto`, `raw`, or `compacted`. `auto` reads compacted files plus the un-compacted raw tail. |
| `REBUILDER_SPILL_DIR` | Directory for replay and candle spill runs and for temporary candle output files. |
| `REBUILDER_SPILL_RECORDS` | Records staged in memory before spill runs are written during a rebuild; also bounds the merge read buffers. Must be greater than zero. |
| `REBUILDER_STATE_EPOCH` | State epoch assigned to regenerated candle revisions. |

## Build plane

### Build orchestrator

#### Required

| Variable | Description |
| --- | --- |
| `AWS_ENDPOINT_URL_S3` | S3-compatible endpoint URL using the `http` or `https` scheme, used for source archives and build artifact access. |
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

No process-specific optional variables.

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

No process-specific optional variables.

### Build agent

#### Required

| Variable | Description |
| --- | --- |
| `BUILD_AGENT_BUILD_CACHE_DIR` | Directory used to cache build dependencies and outputs between builds. |
| `BUILD_AGENT_FAILURE_PUBLISH_BUDGET_SECS` | Time budget reserved for publishing a failure result before the invocation deadline, in seconds. |
| `BUILD_AGENT_MAX_EXTRACTED_SOURCE_SIZE_BYTES` | Maximum total size of extracted source, in bytes. |
| `BUILD_AGENT_SOURCE_DIR` | Working directory where the build agent unpacks and builds source. |

#### Optional

No process-specific optional variables.

## Control plane

### Run invoker

The run invoker checks the DynamoDB run store for due algorithm runs and invokes the runner with
their details.

#### Required

| Variable | Description |
| --- | --- |
| `RUN_INVOKER_ARTIFACT_BUCKET` | S3 bucket containing compiled runner cwasm artifacts. |
| `RUN_INVOKER_LOGS_BUCKET` | S3 bucket that receives the algorithm run logs the runner uploads with its pre-signed POST. |
| `RUN_INVOKER_MAX_BATCH` | Maximum number of due runs claimed per invocation. |
| `RUN_INVOKER_RUNNER_FUNCTION_NAME` | Name of the runner Lambda function invoked for every due run. |
| `RUN_INVOKER_TABLE` | DynamoDB table holding the scheduled algorithm runs. |
| `S3_SIGNED_URL_EXPIRY_SECS` | Lifetime of the pre-signed S3 URLs handed to the runner, in seconds. |

#### Optional

No process-specific optional variables.

## Sandbox plane

### CWASM invoker

The native cwasm host reads these required variables when it starts. Missing, empty, and invalid
values fail startup.

#### Required

| Variable | Description |
| --- | --- |
| `CWASM_INVOKER_CWASM_DOWNLOAD_TIMEOUT_SECS` | Maximum time spent downloading a cwasm artifact, in seconds. |
| `CWASM_INVOKER_EXECUTION_TIMEOUT_SECS` | Maximum guest execution time before the watchdog interrupts it, in seconds. |
| `CWASM_INVOKER_MAX_CWASM_BYTES` | Maximum accepted cwasm artifact size, in bytes. |
| `CWASM_INVOKER_MAX_GUEST_MEMORY_BYTES` | Maximum linear memory available to the guest, in bytes. |
| `CWASM_INVOKER_MAX_TABLE_ELEMENTS` | Maximum number of elements in each guest table. |

## Secrets

Some values are read from AWS Systems Manager Parameter Store instead of the process environment.
The backfiller and Git webhook load every parameter under `SECRETS_SSM_PATH` and look secrets up by
their final path segment.

| Consumer | Parameter | Description |
| --- | --- | --- |
| Git webhook | `GITHUB_WEBHOOK_SECRET` | Secret used to verify GitHub webhook signatures. |
| Backfiller | `BACKFILLER_EXCHANGE_<EXCHANGE>_<MODE>_API_KEY` | API key for the configured exchange and trading mode. |
| Backfiller | `BACKFILLER_EXCHANGE_<EXCHANGE>_<MODE>_API_SECRET` | API secret or private signing key for the configured exchange and trading mode. |

For the backfiller, `<EXCHANGE>` is the upper-case exchange name from `BACKFILLER_EXCHANGE` and
`<MODE>` is `REAL` or `PAPER`, derived from `BACKFILLER_EXCHANGE_TRADING_MODE`. For Binance in real
mode it reads `BACKFILLER_EXCHANGE_BINANCE_REAL_API_KEY` and
`BACKFILLER_EXCHANGE_BINANCE_REAL_API_SECRET`; in paper mode it reads
`BACKFILLER_EXCHANGE_BINANCE_PAPER_API_KEY` and `BACKFILLER_EXCHANGE_BINANCE_PAPER_API_SECRET`.
Every exchange uses the same pattern, so multiple exchanges can share one SSM path.

These names are SSM parameter names, not environment variables.
