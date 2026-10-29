# Market data

## Ingester

Data will be ingested via websocket by Ingester and sent to a NATS Jetstream instance. It stores sequence numbers with the events. It also detects missing events and adds gap markers for the Backfiller.

## Backfiller

It listens for gap markers, fetches the missing data and pushes data into the same stream as Ingester.

## Aggregator

Aggregator consumes both market events and gap markers from NATS. A router hashes each exchange and trading pair onto a shard subject, and one partition writer owns every candle key on that shard. Gap markers close candle windows that ended before the gap was observed.

Each partition writer keeps the open candles up to date, sends updated open candles to NATS KV, and publishes closed candles to a Jetstream stream with a revision number. Source positions and keyed state are checkpointed before a message is acknowledged, so a restarted partition resumes from the first unprocessed message.

### Scaling contract

The aggregator is horizontally scalable by partition, not by sharing a partition between writers. Candle ownership is determined by `(exchange, trading pair)`, so every candle key has exactly one writer for a given shard count. Changing `AGGREGATOR_SHARD_COUNT` changes that mapping and requires a rebuild rather than an in-place rolling change.

A partition writer uses a fenced lease in the checkpoint bucket. Only the lease holder may write checkpoints and publish candle revisions. Checkpoints contain the next source sequence, the event-time watermark, the writer generation, and the serialized keyed state. A writer acknowledges source messages only after its checkpoint is durable.

On startup a writer applies `AGGREGATOR_REPLAY_START_POLICY`:

| Policy | Behaviour |
| --- | --- |
| `checkpoint` | Resume a compatible checkpoint, otherwise rebuild from the raw-event archive. |
| `archive` | Ignore checkpoints and rebuild keyed state from the raw-event archive. |
| `earliest` | Ignore persisted state and replay the live source from the beginning. |
| `latest` | Ignore persisted state and process only messages published after startup. |

Archive rebuilds replay raw events through the same deterministic candle processor as live traffic. Events at or before the rebuilt watermark are then used only to advance the source position, so applying the archive and the live log cannot double-apply trades. See [ADR 0001](../../docs/adr/0001-scalable-aggregator.md) for the event-log, keyed-state, checkpoint, and rebuild contract.

### Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `AGGREGATOR_SHARD_COUNT` / `AGGREGATOR_PARTITION_COUNT` | `16` | Number of keyed partitions. |
| `AGGREGATOR_SHARD_INDEX` / `AGGREGATOR_PARTITION_INDEX` | all partitions | Run only one partition in this process. |
| `AGGREGATOR_CHECKPOINT_INTERVAL_SECS` | `5` | Checkpoint cadence. Must be lower than the lease TTL. |
| `AGGREGATOR_CHECKPOINT_BUCKET` | derived from the shard prefix | NATS KV bucket for checkpoints and leases. |
| `AGGREGATOR_CHECKPOINT_KEY_PREFIX` / `AGGREGATOR_CHECKPOINT_PREFIX` | `partition` | Key prefix inside the checkpoint bucket. |
| `AGGREGATOR_ALLOWED_LATENESS_MS` | `5000` | How late a trade may arrive and still update a candle. |
| `AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS` | `60` | Idle time before the wall clock advances the event-time watermark and closes expired windows. |
| `AGGREGATOR_REPLAY_START_POLICY` | `checkpoint` | `checkpoint`, `archive`, `earliest`, or `latest`. |
| `AGGREGATOR_STATE_SCHEMA_VERSION` | current code version | State schema version written to new state and required for checkpoint restore. |
| `AGGREGATOR_CONSUMER_PREFIX` / `AGGREGATOR_DURABLE_PREFIX` | `aggregator` | Durable consumer prefix. The router uses `{prefix}-router`; partitions use `{prefix}-shard-{n}`. |
| `AGGREGATOR_LEASE_TTL_SECS` | `30` | Single-writer lease lifetime. |
| `AGGREGATOR_ARCHIVE_S3_BUCKET` | unset | Enables raw-event archive recovery. |
| `AGGREGATOR_ARCHIVE_S3_PREFIX` | `market-data/raw` | Archive prefix used for rebuilds. |

### Observability

Aggregator metrics are named `market_data_aggregator_*` and cover:

- checkpoint age and checkpoint failures;
- watermark lag behind wall-clock time;
- replayed events and replay progress;
- state restore duration by recovery path;
- writer rebalances/lease acquisition;
- duplicate events and incomplete candle windows.

## Archiver

Archiver will be a durable Jetstream consumer, and periodically flush de-duplicated data to parquet files bucketed by exchange event timestamp and send it using Apache Iceberg to S3. It will also listen to closed candles and archive those as well.

## Compactor

Compactor will run regularly converting the raw archived S3 files into clean and tidy files, partitioning by symbol and sorting by timestamp.

## Rebuilder

Rebuilder replays archived market events from Iceberg to deterministically regenerate candles after an aggregator configuration or code change. It prefers compacted event partitions, which are already de-duplicated and sorted by exchange stream, timestamp, and sequence, and falls back to raw partitions when no compacted table exists. Events are grouped by their decoded exchange and trading pair and replayed through the aggregator's pure candle processor using a new state epoch, so all streams for a pair (such as `@trade` and `@aggTrade`) update the same candles. Regenerated candles can be written either as raw hourly objects in the raw candle table or as candle-key partitions in the compacted candle table.

### Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `REBUILDER_S3_BUCKET` | required | Archive bucket for rebuilt candle output. |
| `REBUILDER_OUTPUT_PREFIX` | `market-data/rebuilt` | Prefix for rebuilt candle objects. |
| `REBUILDER_SOURCE_LAYER` | `auto` | Event source layer: `auto`, `raw`, or `compacted`. |
| `REBUILDER_OUTPUT_LAYER` | `raw` | Candle output layer: `raw` or `compacted`. |
| `REBUILDER_INTERVALS` | `1s,1m,5m` | Comma-separated candle intervals to regenerate. |
| `REBUILDER_ALLOWED_LATENESS_MS` | `5000` | How late a trade may arrive and still update a candle. |
| `REBUILDER_STATE_EPOCH` | `1` | State epoch assigned to regenerated candle revisions. |
| `REBUILDER_FINISH_OPEN_WINDOWS` | `true` | Close candles that are still open when archived input ends. |
| `REBUILDER_BUCKET_MILLIS` | `3600000` | Output bucket size in milliseconds. |
| `REBUILDER_DRY_RUN` | `false` | Rebuild without writing output. |
