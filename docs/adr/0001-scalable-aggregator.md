# ADR 0001: Scalable market-data aggregation

- Status: accepted
- Scope: `crates/data-plane/aggregator`

## Context

The aggregator originally kept open candles in process memory and used one durable consumer name for every replica. Two replicas could therefore receive different events for the same candle, and a restart could lose state that had already been published. Horizontal scaling needs a stable ownership boundary, a durable position, and a deterministic way to rebuild state.

## Decision

### Event log

Market events and gap markers are the source of truth. The ingester and backfiller append them to a NATS JetStream stream with sequence numbers. The router repartitions those inputs onto shard subjects. A partition writer consumes only its shard, in stream order, and acknowledges a message only after the effects of that message are durable.

The event log is also archived as raw Parquet objects. The archive is ordered by event time and sequence and is sufficient to rebuild a partition without relying on another replica's memory.

### Keyed state

Candle state is keyed by exchange, trading pair, and interval. The partition function maps every key to exactly one shard, so all intervals for a pair live together and no two writers own the same candle. State includes open candles, closed candles retained for late corrections, deduplication sequence ranges, known gaps, incomplete windows, and event-time watermarks. Gap bookkeeping marks affected candles incomplete, and that flag is carried through to published and archived closed candles. Bookkeeping for windows that have already closed is pruned from checkpoints.

### Checkpoint contract

A checkpoint is one atomic NATS KV value containing:

- the shard/partition and source stream identity;
- the next source sequence to deliver;
- the event-time watermark;
- the writer identity and fencing token;
- the state schema version;
- the serialized keyed state.

The checkpoint is written before source acknowledgements. A writer must hold the partition lease; a newer fencing token invalidates an older writer. A checkpoint is restorable only when its partition, stream, source position, and schema version match the reader's configuration.

Checkpoint keys use a configurable prefix inside a configurable KV bucket. This lets several environments share a NATS deployment without sharing state.

### Rebuild contract

Recovery follows the configured replay policy:

- `checkpoint` restores compatible state and resumes at `next_sequence`.
- `archive` ignores checkpoints and replays archived raw events through the deterministic candle processor.
- `earliest` starts with empty state at the beginning of the live log.
- `latest` starts with empty state at the end of the live log.

An archive rebuild records its event-time watermark. The live source is then consumed from the beginning. Inputs at or before that watermark are skipped only when they are already represented in the rebuilt deduplication state (or carry no sequence); new backfills with unseen sequences still pass through the candle fold and revise retained closed candles. This makes the archive-plus-log handoff idempotent without dropping late data. Closed-candle revisions are derived from source offsets and state epoch, not from process-local counters.

When the shard count or key mapping changes, operators select `archive` (or `earliest` with an empty live stream) or run the Rebuilder. There is no mixed old/new partition mapping rollout.

## Consequences

- Partitions can be spread across replicas because the lease prevents concurrent writers.
- Any replica can take over by reading the checkpoint and fencing the previous writer.
- A missing or incompatible checkpoint is recoverable from archived events.
- Operators must choose a new consumer prefix or shard mapping deliberately when changing topology; durable names intentionally preserve ownership history.
- Wall-clock idle time closes quiet windows only after the configured idle timeout but does not advance per-stream event-time watermarks; inputs for already-closed windows are folded into retained closed candles instead of being dropped.

## Observability

The aggregator exports `market_data_aggregator_*` metrics for checkpoint age and failures, watermark lag, replay progress, state restore duration, rebalances, duplicate events, and incomplete windows.
