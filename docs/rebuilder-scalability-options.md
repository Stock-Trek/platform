# Rebuilder scalability options (TODO #159)

## Problem

The rebuilder currently has two unbounded, whole-run collections:

1. `IcebergEventReader::read_all` decodes every selected archive object in full and appends every event to one `Vec<MarketEvent>`. Although `ArchiveParquet::decode_reader_batches` can stream bounded record batches, `decode_reader` is used here, so even a single large Parquet object is materialized as a complete `ArchiveBatch` before its records are copied into the run-wide vector.
2. `RebuildOptions::rebuild` groups every decoded event into `BTreeMap<PairKey, Vec<ReplayEvent>>`, collects every emitted `ClosedCandle` into `regenerated`, then builds another sorted `Vec<ClosedCandle>`. `S3CandleSink::write` then clones all candles into `ArchiveRecord`s, sorts the whole set, encodes each output partition to an in-memory `Vec<u8>`, and uploads those bytes.

Peak memory is therefore proportional to the number of archived events plus the number of unique regenerated candles plus the largest encoded output object. The aggregator's per-interval closed-candle caps bound only the processor's active state; they do not bound the input, output, or sink staging.

The rebuild must preserve these current invariants no matter which option is chosen:

- events are replayed per `(exchange, base, quote)` with all streams for the pair sharing one processor;
- event order is deterministic (`timestamp`, `sequence`, `stream`, `payload`) and duplicate sequences within a stream are dropped;
- every regenerated candle keeps its deterministic revision id and the configured state epoch;
- event, candle, and revision semantics remain identical to live processing, including lateness, correction cutoff, per-interval eviction, and `finish_open_windows`;
- raw/compacted handoff and Iceberg registration stay exactly-once from the reader's point of view;
- output object paths and partition contents remain deterministic so a failed run can be retried safely.

## Options

### Option A: External sort plus streaming replay and sink (recommended)

> **Status: implemented.** The rebuilder streams `ArchiveParquetStream` batches, stages replay
> records and closed candles in shared archive spill runs (`platform-archive::spill`), and uploads
> candle objects from staging files. `REBUILDER_SPILL_RECORDS` bounds the in-memory stage and, via
> the derived merge fan-in and per-run batch size, the k-way merge buffers; `REBUILDER_SPILL_DIR` places the spill and
> staging files; spilled record counts, temporary bytes written, and the largest spill run are
> reported on completion. The deterministic invariants below are covered by
> `crates/data-plane/rebuilder/src/rebuild/tests.rs`.

Keep a single rebuilder process, but replace each whole-run collection with a bounded spill/merge pipeline:

1. Stream Parquet objects in bounded batches with `ArchiveParquet::decode_reader_batches` / `ArchiveParquetStream` instead of `decode_reader`.
2. Decode each event, split it into its per-pair trades, and push the replay records into an external sorter keyed by `(pair, timestamp, sequence, stream, payload)`. Spill sorted Parquet runs once a configurable record limit is reached, then k-way merge the runs. The compactor already has the required primitives (`DedupeSpill`, `PartitionSpill`, spill-run writer/readers, and merge heaps); they should be moved into a shared archive utility rather than duplicated.
3. Consume one pair group at a time from the merge, run `CandleProcessor` over that pair, and push emitted `ClosedCandle`s into a second external sorter/spill keyed by the sink's existing order (`ArchiveBucket`, `routing_bucket`, compacted partition, `sort_key`, `key`). Suppress duplicate revisions by `message_id` during the merge.
4. Write candle Parquet directly to temporary files in partition order with `ArchiveParquetWriter`, upload each completed file with a file-backed S3 body (the compactor's `write_object_from_path` pattern), and register the resulting `IcebergDataFile`s only after the run has uploaded them. Keep the existing content-derived file names so retries are idempotent.

This reduces peak memory to the spill record limit, the processor state, one Parquet batch, and one output object's write buffers. Disk usage becomes the new bounded resource and must be configured and monitored. It requires no new service and reuses code paths that are already tested by the compactor.

Important details:

- The event spill key must reproduce the current pair-level sort order, and duplicate-sequence suppression must remain `(stream, sequence)`-scoped. A generic archive-record de-duplication key is not equivalent because it includes payload and epoch.
- The candle merge must retain all revisions for a candle key; only identical `message_id`s are dropped. The current final sort by revision id can be produced from the merge order without another in-memory sort.
- If the sink writes while events are still being replayed, a later candle may revise an earlier window. Append-only revision output handles that, but the sink must not assume that every revision for a candle has arrived before its bucket is closed. Either emit all revisions before finalizing a partition (requires a merge/spill step) or rely on the archive's append-only revision model and deterministic ids.
- A single catalog registration after all uploads preserves the current commit behavior; a registration failure leaves unregistered objects that a retry can safely overwrite.

### Option B: Pre-shuffle events into pair-partitioned files

Add a preparation job (or a mode of the compactor) that reads the same compacted/raw-tail handoff, normalizes events into per-pair partitions, and writes them sorted by the replay order. The rebuilder then streams one pair partition at a time and writes candles with the streaming sink from Option A.

Pros:

- The rebuilder becomes a simple sequential scan with no external sort at rebuild time.
- Preparation can be reused by repeated rebuilds, can run in parallel by pair, and can be implemented with DataFusion/Arrow or the existing compactor spill code.
- Pair partitions are naturally aligned with the processor's unit of work.

Cons:

- Requires a full extra copy of the event history in temporary or persistent storage, plus lifecycle/cleanup and handoff rules for that copy.
- New pair partitions become stale after live archive/compaction advances, so the job needs an incremental or repeated full-refresh contract.
- A very popular pair still forms one huge partition, although streaming reads mean it need not fit in memory.
- The output path can still exceed memory unless the streaming sink from Option A is also implemented.

### Option C: Shard by routing bucket and bounded event-time windows

Use the archive's fixed `RoutingBucket::COUNT` and hourly layout to split the rebuild into independent work units, for example `(routing_bucket, time_window)`. Each worker reads only the files for its bucket/window, replays them, and writes only that output slice. A worker can additionally use Option A's spill sort for the events in its shard.

To preserve correction semantics, each window must start with enough overlap (`max_interval + allowed_lateness + correction_cutoff`, or the retention reach if shorter) to reconstruct open candles and accept late trades. Windows can run in parallel because each output is append-only and revision ids are deterministic.

Pros:

- Horizontally scalable by bucket/window without changing candle ownership.
- Bounded working set and a natural fit for multiple pods or jobs.
- Uses the existing archive layout and `RoutingBucket` ownership rules.

Cons:

- Overlapping windows replay events more than once and can emit the same revision from adjacent windows; the sink must de-duplicate by `message_id` or tolerate idempotent overwrites.
- The correction cutoff can make the overlap large, reducing the benefit when operators configure a long correction horizon.
- A hot routing bucket still bounds the work for one worker; bucket skew must be measured.
- More complex orchestration, progress tracking, retry, and catalog-commit coordination than Option A.

### Option D: Operational stopgap only

Before a full streaming implementation, reduce the immediate OOM risk by:

- switching at least to `decode_reader_batches` so one object is not decoded twice;
- adding a preflight estimate for event records, output candles, and encoded object sizes, and failing fast when a configured memory/disk budget would be exceeded;
- documenting that production rebuilds are limited to a bounded source layer and time range until the streaming path exists;
- optionally processing compacted stream partitions with a per-file merge and writing candles per pair.

This is small and low risk, but it does not make all-history rebuilds an operational production path. It is a mitigation, not the final fix.

### Option E: Distributed compute engine

Use DataFusion, Ballista, Spark, or a similar engine to scan Iceberg, externally sort/shuffle by pair and time, and write partition files, with the candle fold either expressed as a custom operator/UDF or as a separate deterministic worker stage.

Pros:

- Scales beyond one node and provides mature external sort, spill, and shuffle.
- Can handle multi-terabyte histories and parallel output writes.

Cons:

- Large dependency and operational footprint.
- The candle processor is a custom Rust fold with keyed state and correction semantics; expressing it as a SQL/UDF operator or serializing trades into a worker protocol is significant work and a new determinism risk.
- Overkill unless archive volume is expected to outgrow the single-node Option A/C path.

## Recommendation

Implement Option A first:

1. Move the compactor's spill/merge helpers into a shared archive module and add a bounded event sorter that preserves the current pair-level ordering and duplicate-sequence rules.
2. Change `IcebergEventReader` to stream Parquet batches and feed the sorter instead of returning `Vec<MarketEvent>`.
3. Process merged pair groups one at a time and stream emitted candles through the existing sink order, using a candle spill and file-backed uploads.
4. Add spill/temp-directory configuration, observability (`events_spilled`, `candles_spilled`, temporary bytes, largest write), and integration tests that run with a tiny spill limit and compare `message_id` multisets and stats against the current in-memory implementation.

Option C can then be layered on top for throughput and for histories that are too large for one worker, without changing the candle/sink contract. Option B is worth considering only if rebuilds are frequent enough that pre-shuffling the archive amortizes the extra copy. Option D should be treated as an interim guardrail, and Option E only if single-node external sorting is no longer sufficient.

## Validation checklist

- Same fixture rebuilt with spill limit 1 and with no spilling produces identical candle `message_id`s, revisions, and stats.
- Duplicate rows across files, duplicate sequences within one stream, and equal sequence numbers across `@trade`/`@aggTrade` behave exactly as today.
- Late trades, correction-cutoff rejection, per-interval eviction, and `finish_open_windows` match live processing fixtures.
- `auto`, `raw`, and `compacted` source modes still use the compacted/raw handoff correctly, and both raw and compacted output layers produce the expected paths/tables.
- A sink or catalog failure leaves no partially registered files; rerunning produces the same object keys and revisions.
- Memory stays below the configured limit for a generated multi-file history, and temporary disk usage is bounded and reported.
