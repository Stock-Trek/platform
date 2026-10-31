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

Idle flushing only closes windows that are already expired at the current event-time watermark. It never advances event time to the wall clock, so backfilled trades for older windows still find and revise the retained full closed candle instead of creating a partial correction candle.

### Closed-candle retention

A closed candle is retained in shard state after it is published so that later inputs can still use the original full candle. A late or backfilled trade revises the retained candle and publishes a new deterministic revision, while a gap marker can mark it incomplete. Inputs for windows that have fallen outside the correction contract are rejected rather than folded into a partial correction candle, and gap markers outside the contract only record and report the gap. Each rejection and eviction is counted so out-of-contract inputs are observable instead of silently degrading candle correctness.

The retention policy is an event-time correction cutoff combined with a hard per-key cap on hot closed candles. Shard state keeps only the newest `N` closed candles for each `CandleKey`. An input whose window end is older than `watermark - correction_cutoff` is rejected, quarantined, or annotated according to the correction contract, even if it falls within the per-key cap. Conversely, when more than `N` candles for a key are inside the cutoff, the oldest are evicted in deterministic event-time order and treated as outside the contract. The effective correction reach for a key is therefore the shorter of the cutoff and its newest `N` windows. Eviction is driven by event time and input order, never wall-clock time or cache access order. The same rule is applied during live processing, checkpoint restore, archive rebuilds, and rebuilder replay. A restored checkpoint that retains more than the configured cap is pruned to the newest `N` windows before it is used, and the rebuilder emits the same accepted corrections and rejections as live processing.

- **Memory bound:** At most `N` closed candles per candle key, so the hot heap, checkpoint payload, and gap scan are bounded by the key count times `N`. This is an absolute shard bound because the key count is fixed before startup by the finite `CexId` and `AssetId` enums and the configured window intervals; new or churning trading pairs can populate more of those keys over time but cannot raise the ceiling. A `CandleKey` is an exchange, an ordered base/quote asset pair, and a configured interval, so the ceiling is the number of `CexId` values times the number of `AssetId` values squared times the number of configured intervals. The cutoff does not bound memory by itself but limits the event-time range over which the per-key cap can be consumed.
- **Correction correctness and reach:** Full correction and gap propagation are preserved inside the intersection of the cutoff and the newest `N` windows. Reach is `min(correction_cutoff, N × interval)` for each key, so it is predictable in event time when `N` windows cover more than the cutoff, and predictable in window count when the cutoff is generous. Because the same `N` applies to every interval, event-time reach still differs between a 1-second and a daily candle. Outside the reach, inputs are explicitly reported as rejected, quarantined, or incomplete rather than silently folded into a partial correction.
- **Configuration validation:** At configuration load, the total shard count and total key count must be validated together so that the per-shard key count implied by them (total key count divided by total shard count) multiplied by `N` does not exceed the configured memory budget for closed candles. The total key count is the full static ceiling, not just the pairs that have traded so far. If that worst case would run out of memory, startup must fail with a clear error rather than accepting a configuration that can OOM later. Because later trading pairs only populate keys already included in that ceiling, no revalidation is needed when new keys first appear; the check must instead be repeated whenever a deployment changes the enum sets, configured intervals, shard count, or `N`, because those can change the ceiling or its per-shard share.
- **Pros:** Provides a hard per-key memory bound together with an explicit, testable correction contract; requires no cold tier or extra durability boundary; is far simpler to operate than hot state plus a cold store; protects against a single busy key and still evicts when the event-time watermark stalls because the `N`-window cap remains effective; and makes out-of-contract inputs observable instead of silently degrading correctness.
- **Cons:** Cannot reduce the total shard bound below the fixed key ceiling times `N`, so `N` must be chosen against every possible key and may need to be very small when the enum sets or interval list are large; cannot correct deeper than `N` windows even when the cutoff is large, so deep backfills beyond that history are lost; `N` and the cutoff must be validated against every configured interval so that operators understand the per-interval reach differences; rejected or quarantined inputs need a repair path and may discard data that could have been corrected; and eviction must remain driven by event time and input order so live processing, checkpoint restore, and archive rebuilds stay reproducible.

Implementation tasks for this policy are listed in [closed-candle-retention-tasks.md](closed-candle-retention-tasks.md).

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

`checkpoint` and `archive` require archive recovery to be configured because either policy may need to rebuild keyed state. The process fails at startup when `AGGREGATOR_ARCHIVE_S3_BUCKET` is missing for those policies. `earliest` and `latest` explicitly discard persisted state and can run without an archive.

A retryable checkpoint read failure is surfaced to the supervisor instead of silently falling back to the archive. If archive replay finds no events, the shard starts with explicit empty state and is not reported as a successful archive recovery.

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
| `AGGREGATOR_MAX_CLOSED_CANDLES_PER_KEY` | required | Maximum closed candles retained per candle key. The oldest are evicted as newer windows close. |
| `AGGREGATOR_CORRECTION_CUTOFF_MS` | required | Event-time correction cutoff. Must not be shorter than `AGGREGATOR_ALLOWED_LATENESS_MS`. |
| `AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES` | required | Worst-case hot closed-candle memory budget across all shards. Startup fails if the static key ceiling and cap exceed it. |
| `AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS` | required | Idle time before expired windows are flushed and state is checkpointed. Idle flushing never advances event time with the wall clock. |
| `AGGREGATOR_REPLAY_START_POLICY` | required | `checkpoint`, `archive`, `earliest`, or `latest`. |
| `AGGREGATOR_STATE_SCHEMA_VERSION` | required | State schema version written to new state and required for checkpoint restore. |
| `AGGREGATOR_DURABLE_PREFIX` | required | Durable consumer prefix. The router uses `{prefix}-router`; shards use `{prefix}-shard-{n}`. |
| `AGGREGATOR_LEASE_TTL_SECS` | required | Single-writer lease lifetime. |
| `AGGREGATOR_ARCHIVE_S3_BUCKET` | required for `checkpoint`/`archive` | S3 bucket containing archived market events. Enables archive recovery. Optional for `earliest`/`latest`. |
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
- duplicate events and incomplete candle windows;
- retained closed candles and closed-candle evictions;
- trades and gap windows rejected outside the correction contract.

## Archiver

Archiver will be a durable Jetstream consumer, and periodically flush de-duplicated data to parquet files bucketed by exchange event timestamp and send it using Apache Iceberg to S3. Raw objects are also grouped by a fixed virtual routing bucket derived from the trading pair, so archive layout does not depend on the current aggregator shard count. It will also listen to closed candles and archive those as well.

## Compactor

Compactor will run regularly converting the raw archived S3 files into clean and tidy files, partitioning by symbol and routing bucket and sorting by timestamp.

Each compacted Iceberg snapshot records the raw source object URIs it represents in the same commit as the compacted data files. That snapshot is the explicit compacted/raw handoff: recovery readers replay all live compacted files plus only the raw objects that are absent from the source-object set. A raw object is therefore either fully represented by compacted files or still in the raw tail, never both. Because the compacted files and source-object set are read from one snapshot, a concurrent compaction commit cannot create a gap or a duplicate.

## Rebuilder

Rebuilder replays archived market events from Iceberg to deterministically regenerate candles after an aggregator configuration or code change. With `REBUILDER_SOURCE_LAYER=auto` it reads compacted event partitions plus the raw tail that has not been compacted yet, using the same handoff contract as aggregator recovery. Explicit `raw` and `compacted` modes read only that layer. Events are grouped by their decoded exchange and trading pair and replayed through the aggregator's pure candle processor using a new state epoch, so all streams for a pair (such as `@trade` and `@aggTrade`) update the same candles. Regenerated candles can be written either as raw hourly objects in the raw candle table or as candle-key partitions in the compacted candle table. The closed-candle cap bounds the rebuilder's active processor state; the final output collection still materialises every regenerated candle revision.

### Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `REBUILDER_S3_BUCKET` | required | Archive bucket for rebuilt candle output. |
| `REBUILDER_OUTPUT_PREFIX` | required | Prefix for rebuilt candle objects. |
| `REBUILDER_SOURCE_LAYER` | required | Event source layer: `auto`, `raw`, or `compacted`. |
| `REBUILDER_OUTPUT_LAYER` | required | Candle output layer: `raw` or `compacted`. |
| `REBUILDER_INTERVALS` | required | Comma-separated candle intervals to regenerate. |
| `REBUILDER_ALLOWED_LATENESS_MS` | required | How late a trade may arrive and still update a candle. |
| `REBUILDER_MAX_CLOSED_CANDLES_PER_KEY` | required | Maximum closed candles retained per candle key during replay. |
| `REBUILDER_CORRECTION_CUTOFF_MS` | required | Event-time correction cutoff. Must not be shorter than `REBUILDER_ALLOWED_LATENESS_MS`. |
| `REBUILDER_STATE_EPOCH` | required | State epoch assigned to regenerated candle revisions. |
| `REBUILDER_FINISH_OPEN_WINDOWS` | required | Close candles that are still open when archived input ends. |
| `REBUILDER_BUCKET_MILLIS` | required | Output bucket size in milliseconds. |
| `REBUILDER_DRY_RUN` | required | Rebuild without writing output. |
| `ICEBERG_CATALOG_URI` | required | Iceberg REST catalog URI. |
| `ICEBERG_CATALOG_WAREHOUSE` | required | Iceberg warehouse URI. |
| `ICEBERG_NAMESPACE` | required | Iceberg namespace. |
