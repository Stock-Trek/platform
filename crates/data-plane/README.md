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

A closed candle is retained in shard state after it is published so that later inputs can still use the original full candle. A late or backfilled trade revises the retained candle and publishes a new deterministic revision, while a gap marker can mark it incomplete. If the candle had already been discarded, the same input would either create a partial correction candle with only the newly arrived trades or fail to propagate the gap's incompleteness to the published closed candle.

There is currently no retention horizon or eviction path for `AggregatorState::closed_candles`. `retain_closed_candle` adds or replaces a candle, but `prune_closed_windows` intentionally prunes only `known_gaps` and `incomplete_windows`; it does not remove closed candles. Consequently every processed window remains in memory, is serialized into every checkpoint, participates in state equality, and is scanned when a gap looks for affected windows. Archive rebuilds replay the same processor and therefore accumulate every historical candle in the rebuilt state too.

This is not because an existing prune call is missing by mistake. `AGGREGATOR_ALLOWED_LATENESS_MS` controls when a window closes and how a gap affects it; it does not bound how late a later correction may be. The processor accepts corrections for any older window, and event time is never advanced to the wall clock, so the current semantics provide no safe age-based cutoff at which a closed candle can be removed. Retaining the full candle is intentional for correction correctness.

The unbounded growth itself is an unresolved production limitation, not a deliberate steady-state policy. Any bounded strategy must define the oldest window that can still be corrected, evict older candles deterministically, and apply the same rule during replay and archive rebuilds so state and output revisions remain reproducible. Until such a strategy is configured, long-running shards will grow in memory, checkpoint payload size, restore time, and gap-scan cost with every candle ever produced.

Key cardinality is not the source of that unbounded growth. A `CandleKey` is an exchange, an ordered base/quote asset pair, and a configured interval (window size), all drawn from finite enum and configuration sets. The maximum number of keys is therefore fixed before startup as the number of `CexId` values times the number of `AssetId` values squared times the number of configured intervals. Pairs that start trading later only populate keys inside that fixed ceiling; the unbounded dimension is time, because every new closed window adds another candle under an existing key.

### Bounded-memory retention options

No bounded strategy can keep accepting corrections of arbitrary depth without retaining enough information somewhere to reconstruct the original full candle. Each option below therefore chooses what remains in hot shard state, what is kept elsewhere, and what happens when an input falls outside the retained range. Eviction must be driven by event time and input order, not wall-clock time or cache access order; otherwise live processing, checkpoint restore, and archive rebuilds will produce different revisions.

#### Event-time retention horizon

Retain a closed candle while its window is inside a configured event-time horizon of the shard watermark, for example until its end time is at or before `watermark - horizon`. Evict older candles in the same deterministic step that closes and prunes windows. The horizon must be much larger than `AGGREGATOR_ALLOWED_LATENESS_MS`, because allowed lateness controls when a window closes rather than how late a correction may arrive.

- **Memory bound:** The candle-key universe is finite and fixed before startup, so state is bounded by the number of keys times the number of windows inside the horizon. New trading pairs can populate more of those keys but cannot raise the fixed key ceiling. The bound can still be large, so a global item or byte limit may be needed to keep it inside the memory budget. Evicted candles also leave the checkpoint and gap scan.
- **Correction correctness and reach:** Full correction and gap propagation are preserved for every candle inside the horizon. Beyond it, a late trade becomes a partial correction and a gap cannot mark the published candle incomplete. Reach is exactly the configured event-time distance behind the watermark, independent of wall-clock time.
- **Pros:** Predictable in time, simple to reason about, tunable to the longest expected backfill or gap delay, and directly reduces checkpoint size, restore time, and gap-scan cost.
- **Cons:** A long horizon chosen to maximize correctness can still exhaust memory at high cardinality or throughput; it needs a companion policy for inputs beyond the horizon; and a stalled event-time watermark must not prevent old corrections from being evicted immediately.

#### Per-key window count cap

Retain only the newest N closed windows for each candle key and evict the lowest-start window when the cap is exceeded.

- **Memory bound:** Bounded by the number of candle keys times N. Because the candle-key universe is fixed before startup, this is also a hard shard bound. It controls a single busy key without allowing that key to consume the whole shard.
- **Correction correctness and reach:** Full corrections and gap propagation are preserved for the newest N windows of each key. Coverage is N times the interval for that key, so the same N gives very different event-time reach for a 1-second and a daily candle. Older candles fall back to partial-correction or no-gap-propagation behaviour.
- **Pros:** Simple, deterministic, robust against a single hot key, and requires no time-horizon decision.
- **Cons:** Does not prevent OOM when the fixed key ceiling times N exceeds the memory budget, so N must be sized against every possible key rather than only the pairs currently trading; coverage differs by interval; and cold keys retain more absolute history than necessary while hot keys can still be cut off inside a useful time range.

#### Global count or byte cap

Cap the total number of retained candles, or their serialized size, per shard. Evict the oldest candles by event-time end or start with a stable key tie-break when the cap is exceeded; optionally reserve a small per-key floor.

- **Memory bound:** A hard shard-level bound independent of candle cardinality and traffic, provided the cap is enforced before the next checkpoint and the overhead per candle remains approximately constant.
- **Correction correctness and reach:** Full correction and gap propagation apply only to candles still inside the cap. The event-time reach is variable: busy periods or many new keys cause older candles to be evicted sooner. Beyond the cap, corrections are partial or gaps cannot be propagated.
- **Pros:** The most direct protection against OOM and oversized checkpoints; deterministic if eviction is ordered from the event log; works without relying on the candle-key ceiling being small.
- **Cons:** The retained time range is not predictable and shrinks under load; a naive global order can starve quiet keys unless a per-key floor is added; and a per-key floor multiplies the fixed key ceiling, so it must be included when sizing the hard bound.

#### Hot state plus a durable cold store

Keep only a bounded recent set of closed candles in the shard's hot state. Move older candles to a durable keyed store, such as an indexed database or object store, and fetch, update, and write back a candle when a late trade or gap refers to it. The checkpoint can contain the hot state plus a cold-store version or watermark rather than every historical candle.

- **Memory bound:** The process heap and checkpoint are bounded by the hot tier and cache, not by total history. The cold store grows with its own retention policy; if it is local disk, this avoids OOM but can still exhaust disk, whereas an object store with a longer retention horizon avoids both heap and checkpoint growth.
- **Correction correctness and reach:** Full correction and gap propagation are preserved for every candle still in the cold store. Reach is the cold store's retention, which can be much longer than the hot horizon, up to the archive or storage lifetime.
- **Pros:** The best balance of bounded memory and long correction reach; keeps checkpoint and gap scans small; can reuse an existing durable candle index or archive.
- **Cons:** Adds another consistency boundary: cold-store writes, checkpoint fencing, and publication must be ordered so a restored writer cannot publish from stale state. It also adds I/O latency, operational complexity, and storage cost, and replay or rebuild must reproduce the same tiering decisions.

#### Reconstruct from the archive on demand

Evict old candles completely. When a late trade or gap refers to a missing window, read the archived raw events for that window, replay them through the same processor to reconstruct the full candle, then apply the new input and publish the revision. This can be synchronous or done by a background repair worker.

- **Memory bound:** The hot state remains bounded by the chosen horizon or cap; each reconstruction needs only one window's working set. The archive may be effectively unbounded without affecting process memory or checkpoint size.
- **Correction correctness and reach:** Correction and gap propagation can be as complete as the archive, so reach is limited by raw history and the archive's retention rather than by shard memory. The archive must also contain the gap or incompleteness evidence needed to reconstruct the candle's completeness, not just its trade data.
- **Pros:** No new long-lived candle store is required, reuses the existing archive, and can handle corrections arbitrarily far back while keeping hot-path memory bounded.
- **Cons:** Random archive lookups and replay are expensive, so synchronous correction on the hot path may be too slow; an index is needed to find a window efficiently. Correctness under concurrent compaction, the compacted/raw handoff, repeated corrections, and deterministic revision ordering is complex.

#### Hard maximum correction age

Retain candles to a configured horizon or cap, and explicitly reject, quarantine, or annotate inputs older than the maximum correction age instead of publishing a partial correction. The rejected input is routed to a dead-letter stream or metric rather than being silently folded into a new candle containing only its own trades.

- **Memory bound:** Same as the retention horizon or cap it accompanies; the difference is the defined behaviour beyond the limit rather than the bound itself.
- **Correction correctness and reach:** Full within the configured maximum age. Beyond it, the system intentionally does not correct the older candle and reports the loss rather than claiming a complete revision. Reach is exactly the configured maximum age.
- **Pros:** Makes the guarantee explicit, prevents misleading partial corrections, and gives operators a signal that backfill or source lag exceeded the contract.
- **Cons:** Discards data that may still be valid, requires a quarantine or alerting path, and does not provide deep historical corrections on its own.

#### Event-time correction cutoff with a per-key hot candle cap

Combine a configured event-time cutoff for corrections with a hard per-key cap on retained closed candles. The hot store keeps only the newest N closed candles for each key. An input whose window end is older than `watermark - correction_cutoff` is rejected, quarantined, or annotated according to the correction contract, even if it falls within the per-key cap. Conversely, when more than N candles for a key are inside the cutoff, the oldest are evicted in deterministic event-time order and treated as outside the contract. The effective correction reach for a key is therefore the shorter of the cutoff and its newest N windows.

- **Memory bound:** At most N closed candles per candle key, so the hot heap, checkpoint payload, and gap scan are bounded by the key count times N. This is an absolute shard bound because the key count is fixed before startup by the finite `CexId` and `AssetId` enums and the configured window intervals; new or churning trading pairs can populate more of those keys over time but cannot raise the ceiling. The cutoff does not bound memory by itself but limits the event-time range over which the per-key cap can be consumed.
- **Correction correctness and reach:** Full correction and gap propagation are preserved inside the intersection of the cutoff and the newest N windows. Reach is `min(correction_cutoff, N × interval)` for each key, so it is predictable in event time when N windows cover more than the cutoff, and predictable in window count when the cutoff is generous. Because the same N applies to every interval, event-time reach still differs between a 1-second and a daily candle. Outside the reach, inputs are explicitly reported as rejected, quarantined, or incomplete rather than silently folded into a partial correction.
- **Pros:** Provides a hard per-key memory bound together with an explicit, testable correction contract; requires no cold tier or extra durability boundary; is far simpler to operate than hot state plus a cold store; protects against a single busy key and still evicts when the event-time watermark stalls because the N-window cap remains effective; and makes out-of-contract inputs observable instead of silently degrading correctness.
- **Cons:** Cannot reduce the total shard bound below the fixed key ceiling times N, so N must be chosen against every possible key and may need to be very small when the enum sets or interval list are large; cannot correct deeper than N windows even when the cutoff is large, so deep backfills beyond that history are lost; N and the cutoff must be validated against every configured interval so that operators understand the per-interval reach differences; rejected or quarantined inputs need a repair path and may discard data that could have been corrected; and eviction must remain driven by event time and input order so live processing, checkpoint restore, and archive rebuilds stay reproducible.
- **Config-load memory caveat:** At configuration load, the total shard count and total key count must be validated together so that the per-shard key count implied by them (total key count divided by total shard count) multiplied by N does not exceed the configured memory budget for closed candles. The total key count is the full static ceiling, the number of `CexId` values times the number of `AssetId` values squared times the number of configured window intervals, not just the pairs that have traded so far. If that worst case would run out of memory, startup must fail with a clear error rather than accepting a configuration that can OOM later. Because later trading pairs only populate keys already included in that ceiling, no revalidation is needed when new keys first appear; the check must instead be repeated whenever a deployment changes the enum sets, configured intervals, shard count, or N, because those can change the ceiling or its per-shard share.

#### Techniques that do not bound retention by themselves

Compressing the serialized candle list or omitting `closed_candles` from checkpoints reduces payload size but does not stop in-memory growth; omitting it also means a restored shard must replay history to recover corrections. Increasing shard count divides the history but does not stop the aggregate or per-shard long-run growth. Those measures can complement a bound, but they are not substitutes for an eviction policy.

A robust production combination is a deterministic event-time horizon or global byte cap for hot state, a durable cold tier or archive reconstruction for older corrections, and an explicit maximum correction age for anything beyond the deepest tier. That provides a hard OOM bound, full corrections within the hot horizon, and a defined, observable degradation outside it. If no cold tier is available, a horizon plus global cap plus explicit quarantine is safer than allowing unbounded state or silently publishing partial corrections.

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
