# Market data

## Ingester

Data will be ingested via websocket by Ingester and sent to a NATS Jetstream instance. It stores sequence numbers with the events. It also detects missing events and adds gap markers for the Backfiller.

Sequence progress is persisted per stream in a NATS KV bucket derived from the configured subject prefix (for example, `market_ingester-sequences` for `INGESTER_NATS_SUBJECT_PREFIX=market`). On restart the ingester resumes from the last durable boundary, so events missed while it was down, or while its final publish was failing, still produce gap markers for the Backfiller.

The websocket URL must not subscribe to both `@trade` and `@aggTrade` for the same symbol. They carry the same economic trades with independent sequence numbers, so the ingester rejects overlapping subscriptions at startup to avoid double-counting in candles and trade counts.

## Backfiller

It listens for gap markers, fetches the missing data and pushes data into the same stream as Ingester.

## Aggregator

Aggregator consumes both market events and gap markers from NATS. A router hashes each exchange and trading pair onto a shard subject, and one shard writer owns every candle key on that shard. Gap markers close candle windows that ended before the gap was observed.

Each shard writer keeps the open candles up to date, sends updated open candles to NATS KV, and publishes closed candles to a Jetstream stream with a revision number. Source positions and keyed state are checkpointed before a message is acknowledged, so a restarted shard resumes from the first unprocessed message.

### Scaling contract

The aggregator is horizontally scalable by shard, not by sharing a shard between writers. Candle ownership is determined by `(exchange, trading pair)`, so every candle key has exactly one writer for a given shard count. Changing `AGGREGATOR_SHARD_COUNT` changes that mapping and requires a rebuild rather than an in-place rolling change.

A shard writer uses a fenced lease in the checkpoint bucket. Only the lease holder may write checkpoints and publish candle revisions. Checkpoints contain the next source sequence, the event-time watermark, the writer generation, and the serialized keyed state. A writer acknowledges source messages only after its checkpoint is durable.

On startup a writer applies `AGGREGATOR_REPLAY_START_POLICY`:

| Policy | Behaviour |
| --- | --- |
| `checkpoint` | Resume a compatible checkpoint, otherwise rebuild from the compacted archive plus the un-compacted raw tail. |
| `archive` | Ignore checkpoints and rebuild keyed state from the compacted archive plus the un-compacted raw tail. |
| `earliest` | Ignore persisted state and replay the live source from the beginning. |
| `latest` | Ignore persisted state and process only messages published after startup. |

Archive rebuilds replay raw events through the same deterministic candle processor as live traffic. When an Iceberg catalog is configured, recovery reads the compacted event files and only the raw objects that are not represented by the compacted snapshot. Without a catalog it falls back to listing `AGGREGATOR_ARCHIVE_S3_PREFIX` and replaying the raw history. Both raw and compacted objects are grouped by a fixed virtual routing bucket, so each shard only reads the objects it owns and every archived event is replayed by exactly one recovery worker. Events at or before the rebuilt watermark are then used only to advance the source position, so applying the archive and the live log cannot double-apply trades. Recovery continues the highest state epoch recorded on the replayed events, so rebuilding the same inputs reproduces the same deterministic output ids. Regenerating candles under a changed configuration uses the rebuilder with a new configured epoch instead. See [ADR 0001](../../docs/adr/0001-scalable-aggregator.md) for the event-log, keyed-state, checkpoint, and rebuild contract.

### Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `AGGREGATOR_INTERVALS` | required | Comma-separated candle intervals to generate. |
| `AGGREGATOR_GAP_SUBJECT_PREFIX` | required | Subject prefix carrying gap markers from the ingester. |
| `AGGREGATOR_SHARD_SUBJECT_PREFIX` | required | Subject prefix owned by the aggregator for routed shard inputs. |
| `AGGREGATOR_SHARD_COUNT` | required | Number of keyed shards. |
| `AGGREGATOR_SHARD_INDEX` | all shards | Run only one shard in this process. |
| `AGGREGATOR_CHECKPOINT_INTERVAL_SECS` | required | Checkpoint cadence. Must be lower than the lease TTL. |
| `AGGREGATOR_CHECKPOINT_BUCKET` | required | NATS KV bucket for checkpoints and leases. |
| `AGGREGATOR_CHECKPOINT_PREFIX` | required | Key prefix inside the checkpoint bucket. |
| `AGGREGATOR_ALLOWED_LATENESS_MS` | required | How late a trade may arrive and still update a candle. |
| `AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS` | required | Idle time before the wall clock advances the event-time watermark and closes expired windows. |
| `AGGREGATOR_REPLAY_START_POLICY` | required | `checkpoint`, `archive`, `earliest`, or `latest`. |
| `AGGREGATOR_STATE_SCHEMA_VERSION` | required | State schema version written to new state and required for checkpoint restore. |
| `AGGREGATOR_DURABLE_PREFIX` | required | Durable consumer prefix. The router uses `{prefix}-router`; shards use `{prefix}-shard-{n}`. |
| `AGGREGATOR_LEASE_TTL_SECS` | required | Single-writer lease lifetime. |
| `AGGREGATOR_ARCHIVE_S3_BUCKET` | unset | Enables archive recovery. |
| `AGGREGATOR_ARCHIVE_S3_PREFIX` | required when archive recovery is enabled | Raw archive prefix used when no Iceberg catalog is configured. |
| `ICEBERG_CATALOG_URI` | unset | Enables the compacted/raw handoff for archive recovery. |
| `ICEBERG_CATALOG_WAREHOUSE` | required when the catalog is configured | Iceberg warehouse URI. |
| `ICEBERG_NAMESPACE` | required when the catalog is configured | Iceberg namespace. |

### Observability

Aggregator metrics are named `market_data_aggregator_*` and cover:

- checkpoint age and checkpoint failures;
- watermark lag behind wall-clock time;
- replayed events and replay progress;
- state restore duration by recovery path;
- writer rebalances/lease acquisition;
- duplicate events and incomplete candle windows.

## Archiver

Archiver will be a durable Jetstream consumer, and periodically flush de-duplicated data to parquet files bucketed by exchange event timestamp and send it using Apache Iceberg to S3. Raw objects are also grouped by a fixed virtual routing bucket derived from the trading pair, so archive layout does not depend on the current aggregator shard count. It will also listen to closed candles and archive those as well.

## Compactor

Compactor will run regularly converting the raw archived S3 files into clean and tidy files, partitioning by symbol and routing bucket and sorting by timestamp.

Each compacted Iceberg snapshot records the raw source object URIs it represents in the same commit as the compacted data files. That snapshot is the explicit compacted/raw handoff: recovery readers replay all live compacted files plus only the raw objects that are absent from the source-object set. A raw object is therefore either fully represented by compacted files or still in the raw tail, never both. Because the compacted files and source-object set are read from one snapshot, a concurrent compaction commit cannot create a gap or a duplicate.

## Rebuilder

Rebuilder replays archived market events from Iceberg to deterministically regenerate candles after an aggregator configuration or code change. With `REBUILDER_SOURCE_LAYER=auto` it reads compacted event partitions plus the raw tail that has not been compacted yet, using the same handoff contract as aggregator recovery. Explicit `raw` and `compacted` modes read only that layer. Events are grouped by their decoded exchange and trading pair and replayed through the aggregator's pure candle processor using a new state epoch, so all streams for a pair (such as `@trade` and `@aggTrade`) update the same candles. Regenerated candles can be written either as raw hourly objects in the raw candle table or as candle-key partitions in the compacted candle table.

### Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `REBUILDER_S3_BUCKET` | required | Archive bucket for rebuilt candle output. |
| `REBUILDER_OUTPUT_PREFIX` | required | Prefix for rebuilt candle objects. |
| `REBUILDER_SOURCE_LAYER` | required | Event source layer: `auto`, `raw`, or `compacted`. |
| `REBUILDER_OUTPUT_LAYER` | required | Candle output layer: `raw` or `compacted`. |
| `REBUILDER_INTERVALS` | required | Comma-separated candle intervals to regenerate. |
| `REBUILDER_ALLOWED_LATENESS_MS` | required | How late a trade may arrive and still update a candle. |
| `REBUILDER_STATE_EPOCH` | required | State epoch assigned to regenerated candle revisions. |
| `REBUILDER_FINISH_OPEN_WINDOWS` | required | Close candles that are still open when archived input ends. |
| `REBUILDER_BUCKET_MILLIS` | required | Output bucket size in milliseconds. |
| `REBUILDER_DRY_RUN` | required | Rebuild without writing output. |
| `ICEBERG_CATALOG_URI` | required | Iceberg REST catalog URI. |
| `ICEBERG_CATALOG_WAREHOUSE` | required | Iceberg warehouse URI. |
| `ICEBERG_NAMESPACE` | required | Iceberg namespace. |
