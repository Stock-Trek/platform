# Closed-candle retention: decisions required before implementation

This document is the decision register for the
[hot state plus durable cold store](README.md#hot-state-plus-a-durable-cold-store)
implementation task list. It records the choices that must be made before the
task list is turned into implementation work. The choices are intentionally
open; each decision lists the available options and the consequences that must
be accepted.

The work is greenfield. There is no existing retention data, no deployed
aggregator, and no compatibility requirement. Every decision can therefore
choose the final design directly rather than a migration path.

All decisions are subject to the same non-negotiable constraints:

- event-time and input-order determinism for live processing, checkpoint
  restore, archive rebuild, and the rebuilder;
- a single fenced writer per shard;
- a bounded hot heap and checkpoint payload;
- no silent loss of correction correctness outside an explicitly documented
  retention boundary.

## D1. What bounds the hot tier?

This decides how many closed candles remain in
`AggregatorState::closed_candles`, what causes an eviction, and what the
checkpoint size limit actually is.

**Options**

- **A. Event-time retention horizon.** Retain every candle whose window end is
  newer than `shard watermark - horizon`. Evict older candles during the same
  deterministic step that closes and prunes windows.
  - Memory is bounded by key cardinality and window density inside the
    horizon, not by total history.
  - Reach is predictable in event time, but a long horizon can still exhaust
    memory at high cardinality or throughput.
  - The horizon must be comfortably larger than the allowed lateness; allowed
    lateness controls window closure, not correction age.
- **B. Per-key window-count cap.** Retain only the newest N closed windows for
  each candle key.
  - Gives a hard per-key bound and protects against one busy key.
  - Does not bound the shard when new keys keep appearing.
  - The same N gives very different event-time reach for one-second and daily
    candles.
- **C. Global candle-count cap.** Retain at most N closed candles per shard,
  evicting the oldest by event-time end or start with a stable key tie-break.
  - Hard shard bound independent of key cardinality.
  - Reach varies with traffic and key cardinality; quiet keys can be evicted
    before busy keys unless a per-key floor is reserved.
- **D. Global serialized-byte cap.** Retain closed candles until their
  estimated or actual serialized size exceeds a byte budget.
  - Directly targets OOM and checkpoint-size limits.
  - Requires size accounting, a stable eviction order, and a decision about
    whether the estimate or the true serialized form is authoritative.
- **E. Hybrid horizon plus a hard cap.** Use an event-time horizon for the
  normal predictable reach and a global count or byte cap as the final OOM
  defence.
  - Preserves a useful correction window in normal operation while providing a
    hard bound under exceptional cardinality or load.
  - Needs a deterministic rule for which bound wins and how the two orders are
    composed.

**Also decide**

- Whether open candles, known gaps, and incomplete-window records count toward
  the same limit.
- Whether a per-key floor is reserved and, if so, what happens when the floor
  multiplied by the key count exceeds the global cap.
- The stable tie-break when multiple candles share a window end or start.
- Whether the cap is enforced before every checkpoint, only on watermark
  advance, or on a schedule.
- Whether eviction can be temporarily blocked by a pending cold write.

## D2. How long does the cold tier retain candles, and what is the deepest correction reach?

The cold tier's retention policy is the effective maximum age for a full
correction and for propagating a gap to an already-published candle.

**Options**

- **A. Archive lifetime.** Keep cold candles for the same period as raw or
  compacted archive data.
  - Reach is as deep as the archive and there is one retention story for the
    whole system.
  - The cold store inherits the archive's storage cost and deletion schedule.
- **B. Fixed event-time retention.** Delete a cold candle once its window is
  older than a configured event-time distance from the current watermark.
  - Predictable in event time and easy to explain.
  - The cold store can grow without a hard byte limit if the watermark stalls
    or history is replayed.
- **C. Fixed wall-clock retention.** Delete cold records after a configured
  age in wall-clock time.
  - Simple for storage operations and legal retention requirements.
  - A replay or rebuild later sees a different cold set than the original
    processing did, so reach is not reproducible and historical corrections
    may become impossible.
- **D. Size-based retention.** Delete the oldest cold records when the store
  exceeds a count or byte budget.
  - Bounds storage cost, but makes correction reach depend on history volume
    and can silently shorten reach.
- **E. No automatic expiry.** Keep cold records until an operator deletes them.
  - Maximum correction reach and simplest correctness story.
  - Storage grows forever and deletion becomes an explicit operational task.

**Also decide**

- Whether a cold record can be deleted while the archive still contains the raw
  events needed to reconstruct it, and whether reconstruction is an accepted
  substitute for retention.
- Whether deletion is a hard delete, a tombstone, or a move to a colder archive
  tier.
- Whether cold retention may be shorter than archive retention, and how that
  is surfaced to operators and clients.
- Whether retention is global, per exchange or pair, or per candle interval.

## D3. What happens when an input is older than the retained correction range?

This defines the visible contract at and beyond the retention boundary. It
must cover a late trade and a gap marker separately if they have different
correctness consequences.

**Options**

- **A. Publish a partial correction.** Create a new candle containing only the
  newly arriving trades, as the current fallback would do when the original
  candle is absent.
  - Processing continues and the input is not lost.
  - The result is not a full correction and can be misleading unless the output
    is explicitly marked as partial.
- **B. Reject or quarantine the input.** Route it to a dead-letter stream,
  metric, or repair queue and do not publish a misleading candle.
  - Makes the boundary explicit and observable.
  - Requires a decision about whether the source message is acknowledged, left
    unacknowledged, or acknowledged after durable quarantine.
- **C. Reconstruct from the archive on demand.** Replay the archived events for
  the missing window before applying the late input.
  - Preserves full correctness without a large hot state.
  - Adds archive lookup and replay cost, needs an index, and must handle
    concurrent compaction and repeated corrections.
- **D. Stop the shard or fail closed.** Refuse to advance the checkpoint until
  an operator changes the retention policy or restores the missing data.
  - Strongest correctness guarantee.
  - Turns a retention event into an availability incident.
- **E. Record incompleteness or a correction exception.** Publish an explicit
  marker, metric, or annotated result rather than silently treating the result
  as a complete revision.
  - Preserves visibility without pretending the original candle was recovered.
  - Requires an output contract for annotations or side-channel records.

**Also decide**

- Whether a late trade and a gap marker use the same fallback, or whether a gap
  is allowed to mark a candle incomplete while a trade is rejected.
- Whether a window that never existed is treated differently from a window that
  was evicted.
- Whether an out-of-range input is acknowledged, retried, or quarantined, and
  how that choice interacts with checkpoint ordering.
- Whether the fallback is configurable per environment or fixed for the final
  implementation.

## D4. Which technology owns the cold tier?

The cold store must support point reads and writes for late trades and a
bounded range query for gaps. Its retention, backup, and failure behaviour
become part of the aggregator's correctness contract.

**Options**

- **A. Existing archive plus an index.** Store candle state beside or derived
  from the archive and add a point/range index.
  - Reuses existing durability, retention, and operational tooling.
  - Random reads and writes may be expensive, and archive compaction can
    conflict with mutable candle corrections.
- **B. Indexed relational or key-value database.** Use a database with a
  primary key for candle identity and a secondary index for gap queries.
  - Fast point reads, atomic conditional writes, and straightforward range
    queries.
  - Adds a new stateful dependency, backup procedure, and operational cost.
- **C. Object store with a separate index.** Keep candle values in object
  storage and maintain an index elsewhere.
  - Cheap durable storage and good capacity growth.
  - Index consistency, small-write latency, and multi-step updates are harder.
- **D. NATS KV or another existing platform store.** Reuse the messaging
  platform's key-value facility.
  - Minimal new infrastructure and familiar operations.
  - May have value-size, key-count, latency, and query limitations that are
    unsuitable for long-lived candle history.
- **E. Embedded local store per shard.** Use a local embedded database on the
  shard's host.
  - Low latency and simple point access.
  - Durability across host replacement, backup, and fencing during lease
    takeover must be solved explicitly.
- **F. Reconstruct from the archive instead of a cold store.** Treat the
  archive as the only cold tier and rebuild a window on demand.
  - No new long-lived candle store.
  - Requires an efficient archive index and makes synchronous corrections
    expensive.

**Also decide**

- Whether the cold tier is one shared service for all shards or isolated per
  shard or environment.
- Whether it is operated by the aggregator process or by a separate service.
- How backup, restore, disaster recovery, and point-in-time consistency are
  provided.
- Whether the store supports conditional writes and, if not, how stale writers
  are prevented from overwriting newer corrections.

## D5. What is the cold record identity, layout, encoding, and version?

This determines whether a candle can be found, whether the layout survives a
shard-count change, and whether old records can be read after a code change.

**Key-layout options**

- **A. Global key.** Key by `(CandleKey, window_start)`, independent of shard
  index.
  - Survives resharding and lets rebuild or repair read the same record.
  - Multiple shards must never write the same key; ownership still comes from
    the shard router and fencing token.
- **B. Shard-scoped key.** Include the current shard index in the key.
  - Simple isolation and routing.
  - Requires rekeying or migration whenever `AGGREGATOR_SHARD_COUNT` changes.
- **C. Virtual routing bucket plus candle key.** Keep the fixed virtual routing
  bucket used by the archive in the key, but not the live shard index.
  - Preserves the archive's resharding properties.
  - Adds one more dimension to every lookup and query.
- **D. Fully decomposed key.** Store exchange, trading pair, interval, and
  window start as separate indexed columns or path components.
  - Natural range queries and readable storage layout.
  - Encoding and normalization of each component must be fixed and versioned.

**Value-layout options**

- **A. Full `Candle` plus completeness.** Store the complete candle needed to
  apply a later trade or gap.
  - Direct and correct; requires the value schema to evolve with the candle
    type.
- **B. Full candle plus correction provenance.** Also store the revision
  metadata needed to reproduce deterministic output identifiers and ordering.
  - Avoids reconstructing provenance from a different state epoch.
  - Larger values and a stricter schema contract.
- **C. Minimal market fields plus completeness.** Store only OHLCV, counts, and
  the incomplete flag.
  - Smaller records, but any future correction logic needs additional fields
    and may not be able to reproduce the original output.
- **D. Reference to archived events.** Store only a pointer and reconstruct the
  candle on every read.
  - Keeps records small and makes the archive authoritative.
  - Moves replay cost to every correction and depends on archive availability.

**Encoding and versioning options**

- **A. JSON.** Matches the existing checkpoint readability and tooling.
- **B. Protobuf.** Compact, explicitly versioned, and already used elsewhere in
  the repository.
- **C. Custom binary or existing columnar format.** Best size and scan
  performance, at the cost of bespoke tooling.
- **D. Version every record.** Include a record schema version and reject or
  upgrade unknown versions.
- **E. Version the store contract.** Keep records opaque and version the store
  generation as a whole, rebuilding rather than reading an incompatible
  generation.

**Also decide**

- Whether a cold record stores a monotonic version for conditional updates.
- Whether a state epoch or store generation is part of the value and how it
  interacts with deterministic revision identifiers.
- Which sort order makes the gap range query efficient.

## D6. How are gap-affected cold candles found?

`mark_affected_windows` currently scans open and hot closed candles. Once older
candles are cold, gap propagation needs an index or query that can find them
without scanning the whole history.

**Options**

- **A. Primary-key range scan.** Order the cold store so it can scan by
  exchange, optional trading pair, interval, and window-start range directly.
  - No secondary index to keep consistent.
  - Only viable if the primary key supports the required prefix.
- **B. Secondary gap index.** Maintain a separate index keyed by exchange,
  pair or stream, interval, and window start.
  - Efficient queries regardless of primary value layout.
  - Index writes and deletes must be atomic with candle writes or repaired on
    recovery.
- **C. Materialized window index.** Store one immutable row per candle window
  with completeness and location metadata, separate from the full candle value.
  - Fast gap scans and compact query results.
  - Adds another mutable record and another consistency boundary.
- **D. Archive/event-log query.** Find the affected windows by querying archived
  events or candle partitions rather than the cold store.
  - Reuses the archive as the source of truth.
  - Query latency and compaction handoff complexity move onto gap handling.
- **E. No cold gap query.** Limit gap propagation to hot candles and use the D3
  fallback for older windows.
  - Simplest implementation, but explicitly weakens correctness beyond the hot
    horizon.

**Also decide**

- How a gap's `stream` and optional trading pair map to candle keys.
- The exact time range to query: gap detection time, previous stream
  watermark, or the gap's missing sequence range projected onto event time.
- The maximum number of windows or bytes a gap query may return, and what
  happens when the bound is reached.
- How duplicate results are removed when a window is both hot and cold.
- How query results are ordered so emitted revisions and
  `IncompleteWindow` records remain deterministic.

## D7. What does the checkpoint persist, and what is authoritative?

The checkpoint must stay small while still allowing a restored writer to know
which cold records its hot state and source position depend on.

**Hot-state options**

- **A. Hot candles plus cold metadata.** Persist the bounded hot candle set and
  cold-store identity, generation, schema version, and a durable watermark or
  version.
  - Meets the bounded-checkpoint requirement.
  - Requires the restore path to validate and reconcile the metadata.
- **B. Hot candles only.** Persist the hot set and derive cold metadata from
  configuration.
  - Smaller schema change.
  - A configuration or restore mistake can point a restored shard at the wrong
    cold generation.
- **C. Hot candles plus pending cold mutations.** Add an outbox of cold writes
  that were not yet confirmed when the checkpoint was written.
  - Allows the checkpoint and cold write to be decoupled safely.
  - Grows the checkpoint by the size of the pending work and needs a drain
    protocol.
- **D. No hot candles.** Rebuild all hot state from cold records or the archive
  on restore.
  - Small checkpoint.
  - Slower restore and more cold-store dependency at startup.

**Metadata options**

- **A. Store generation and schema version only.**
- **B. Store generation plus a durable high-water mark for eviction.**
- **C. Store generation plus a digest or manifest of the hot-to-cold handoff.**
- **D. Per-record versions or fencing tokens in the checkpoint.** Stronger
  conflict detection, but the checkpoint must list enough records to be
  useful, which risks growing without a bound.

**Also decide**

- That state equality includes only hot candles and the chosen metadata, never
  the full cold contents or a purely performance cache.
- Whether `STATE_SCHEMA_VERSION` starts at the new final schema version (the
  greenfield choice) and what a version mismatch does at startup.
- Whether a restored writer trusts the checkpoint's hot set as complete or
  validates it against the cold watermark.
- Which component is authoritative for an evicted candle: the cold store, the
  archive, or a defined precedence between them.

## D8. When and how does deterministic eviction run?

Eviction must happen at the same logical point in live processing, replay, and
rebuild, and must not remove the only copy of a candle before the cold write is
durable.

**Timing options**

- **A. Inline with window close and prune.** Evict immediately after
  `close_expired_at` closes expired windows and calls
  `prune_closed_windows`.
  - One deterministic step and no background race.
  - Adds cold-write latency to the processing path.
- **B. Before checkpoint, after processing a batch.** Accumulate eviction
  candidates and drain them before the checkpoint barrier.
  - Amortizes cold writes and keeps a clear durability boundary.
  - Needs a pending-eviction set in state or in the runner.
- **C. Asynchronously after checkpoint.** Let a background worker evict and
  write cold records after the checkpoint advances.
  - Keeps the hot path fast.
  - A crash can leave a checkpoint that claims state the cold store has not
    received, unless the pending work is recorded.
- **D. Background compaction of the hot tier.** Periodically trim the hot set
  independently of closing windows.
  - Simple conceptually, but risks racing with the deterministic processor and
    making replay decisions depend on scheduling.

**Ordering and failure options**

- **A. Write cold, then remove hot.** Never remove the last durable copy.
  - Adds an I/O failure point before eviction completes.
- **B. Remove hot, then write cold, with a pending retry record.** Fast, but
  requires durable retry state.
- **C. Retain the hot candle and fail or defer the checkpoint when a cold write
  fails.** Strict and simple to reason about; couples checkpoint availability
  to cold-store availability.
- **D. Drop the hot candle and fall back to partial corrections.** Preserves
  availability, but silently weakens correctness unless D3 chooses and exposes
  that fallback.

**Also decide**

- The stable eviction order, including the tie-break for equal timestamps.
- The maximum number of evictions or bytes written per batch.
- Whether the cold write is a put, a conditional put, or a delete-and-put.
- Whether eviction deletes old cold records at the same time or leaves deletion
  to a separate retention job.
- How replay and rebuild apply the same eviction decisions when their event
  timing or batch boundaries differ.

## D9. What is the durability order for cold write, publication, checkpoint, and acknowledgement?

A correction can touch four durable boundaries. The chosen order determines
which crash points can lose a correction or publish a stale revision.

**Options**

- **A. Cold write, publish, checkpoint, acknowledge.** The cold update is
  durable before the published revision and before the source position
  advances.
  - A crash after the cold write but before publication is repaired by replay.
  - A cold-store outage blocks publication and checkpointing for corrections.
- **B. Publish, cold write, checkpoint, acknowledge.** Reuse the existing
  publish-then-checkpoint emphasis.
  - A crash after publication but before the cold write republishes the same
    deterministic revision on replay, assuming the input is replayed and the
    message id is idempotent.
  - A stale cold value must never be read by a new writer before the retry.
- **C. Durable outbox in the checkpoint.** Put the intended cold mutation in
  the checkpoint, advance the checkpoint, then drain it idempotently before or
  after publication.
  - Decouples checkpoint progress from cold availability.
  - Adds an outbox lifecycle, retry policy, and a decision about whether reads
    must observe pending writes.
- **D. Single transaction spanning cold store and checkpoint.** Use a store
  that can commit both atomically.
  - Strongest and simplest recovery model.
  - Constrains the cold-tier technology and may not be available with NATS KV
    checkpoints.

**Fencing and conflict options**

- **A. Checkpoint fencing token only.** The current lease token protects
  checkpoint writes; cold writes also need a check against the current owner.
- **B. Per-record conditional writes.** Each cold write supplies the expected
  version or generation so a stale writer cannot overwrite a newer value.
- **C. Store generation swap.** A rebuild or new writer uses a new generation
  and old generations are read-only.
- **D. Last-writer-wins with replay repair.** Simplest, but only safe if the
  writer lease and replay ordering make stale writes impossible.

**Also decide**

- The recovery action for every crash point between the four boundaries.
- Whether a cold write failure blocks source acknowledgement or is quarantined.
- How published message identifiers remain deterministic across a cold-read
  retry.
- Whether a correction is published once per cold update or coalesced, and how
  coalescing preserves deterministic ordering.

## D10. Is a corrected cold candle cached or promoted back into hot state?

This decides whether repeated corrections cost a cold read every time and
whether the hot state can regrow after eviction.

**Options**

- **A. No cache and no promotion.** Read the cold record, apply the correction,
  write it back, and leave it cold.
  - Simplest state model and the strongest bound.
  - Every correction pays cold latency, and a burst of corrections is
    expensive.
- **B. Ephemeral bounded read cache.** Cache cold values for reads only; cache
  entries are not checkpointed and cannot affect output.
  - Improves latency without changing recovery state.
  - Requires a size bound, invalidation on write, and a policy for cache
    misses.
- **C. Bounded promotion into hot state.** Move a corrected candle back into
  `closed_candles` until the next normal eviction.
  - Subsequent corrections are fast and the candle remains visible to hot gap
    scans.
  - Increases checkpoint size and must be reproduced deterministically during
    replay; a correction burst cannot be allowed to bypass the hot bound.
- **D. Time-based promotion.** Keep corrected candles hot for a configured
  event-time or wall-clock period.
  - Balances latency and simplicity.
  - Wall-clock promotion is not reproducible across replay and rebuild;
    event-time promotion must be tied to the watermark.
- **E. Write-back cache with deferred cold writes.** Treat the cache as the
  latest value until a later flush.
  - Fastest path.
  - Introduces a second durability tier and makes crash recovery significantly
    harder.

**Also decide**

- Whether misses are negatively cached and how a later cold write invalidates
  the negative entry.
- Whether a cached or promoted value participates in state equality.
- How cache or promotion eviction interacts with the D1 hot bound.
- What happens when the cache is full during a correction burst.

## D11. What happens when the cold tier is slow, unavailable, or lost?

The cold tier is now part of the correction correctness path, so degradation
must be an explicit mode rather than an unhandled error.

**Options**

- **A. Fail closed.** Stop acknowledging and checkpointing affected work until
  the cold tier recovers.
  - No silent correctness loss.
  - A cold-tier outage becomes an aggregator outage.
- **B. Continue new processing and reject or quarantine corrections.** Keep
  advancing source positions for new candles while routing cold-dependent work
  to a repair queue.
  - Preserves availability and bounds the impact.
  - Corrections are delayed and the repair path becomes part of the contract.
- **C. Degrade to hot-only corrections.** Treat out-of-range inputs as missing
  and apply the D3 fallback.
  - Keeps the shard running.
  - Silently changes correctness unless the fallback is explicit and
    observable.
- **D. Rebuild or restore the cold tier automatically.** Reconstruct from the
  archive or a backup before resuming corrections.
  - Restores the intended contract without operator intervention.
  - Recovery time and archive dependency may be large.
- **E. Continue with bounded retries and then alert.** Retry the cold operation
  for a configured period before selecting one of the above modes.
  - Avoids treating a transient error as an outage.
  - Needs a clear retry budget and a terminal action.

**Also decide**

- Whether startup with an unavailable or uninitialized cold tier fails, waits,
  or starts in degraded mode.
- What happens if the cold store is restored from a backup that is older than
  the latest checkpoint.
- Which metrics and alerts distinguish transient errors, data loss, and
  deliberate degradation.
- How operators repair candles that were quarantined while the cold tier was
  unavailable.

## D12. How do replay, archive rebuild, resharding, and the rebuilder keep parity with live processing?

Rebuilds must make the same eviction and cold-write decisions as live
processing, or a later correction can produce a different result from the
original run.

**Options**

- **A. Rebuild into the same cold store and generation.** Replay repopulates
  hot and cold state in place.
  - One authoritative store and simple lookups.
  - A failed or partial rebuild can leave stale records mixed with new ones,
    and concurrent live writers must be excluded.
- **B. Rebuild into a new generation, then atomically switch.** Live processing
  continues on the old generation while the new one is built; readers and
  writers switch together.
  - Clean rollback and no mixed-generation reads.
  - Requires generation metadata in the checkpoint and a coordinated cutover.
- **C. Rebuild outputs only and derive cold state separately.** Treat the cold
  store as a cache that can be recreated from the archive.
  - Keeps rebuild focused on candle output.
  - The cold store may not match the state used to produce those outputs until
    a separate repopulation succeeds.
- **D. Treat the archive as the only source of truth.** Do not write cold state
  during rebuild; reconstruct it lazily on the first correction.
  - Avoids rebuilding a large cold store.
  - Makes every deep correction pay archive replay cost and depends on archive
    completeness.
- **E. Keep the cold store independent of shard count.** Derive record identity
  from the virtual routing bucket and candle key so any shard can rebuild or
  read it.
  - Resharding does not require cold-store migration.
  - Requires a documented ownership and fencing rule for shared keys.

**Also decide**

- Whether a change to the hot horizon, cold retention, or eviction order
  requires a full rebuild or is safe to apply to existing state.
- How a state epoch change interacts with existing cold records and published
  revisions.
- Whether a rebuild must delete cold records that no longer exist in the
  rebuilt history.
- How the rebuilder's different batching and event source remain
  deterministic under the same eviction rules.

## D13. What is the configuration, validation, and change contract?

The final implementation needs a configuration surface that makes the chosen
limits understandable and prevents unsafe combinations.

**Options**

- **A. One event-time horizon plus a maximum correction age.** Smallest
  configuration surface; cold retention follows the horizon or archive.
  - Easy to explain, but cannot separately tune hot memory and cold storage.
- **B. Separate hot bound, cold retention, and maximum correction age.** Full
  control over each tier.
  - Flexible, but more combinations to validate and more ways to misconfigure.
- **C. Per-interval overrides.** Different horizons or caps for one-second and
  daily candles.
  - Matches very different window densities.
  - Multiplies configuration and validation complexity and can make global
    memory accounting harder.
- **D. Per-key overrides or floors.** Protect important keys from global
  eviction.
  - Useful for high-value instruments.
  - Weakens the hard bound when key cardinality is unbounded.
- **E. Static configuration only.** Changes require a restart or rebuild.
  - Simple and reproducible.
- **F. Dynamic reload of retention settings.** Change limits without a restart.
  - Operationally convenient, but live and replayed decisions can diverge
    unless the change point is captured in state.

**Also decide**

- Required versus optional settings, and whether the final implementation has
  any unbounded legacy mode (the greenfield answer is no).
- Validation rules, including horizon versus allowed lateness, cold retention
  versus hot reach and maximum correction age, and consistency across live
  writers, restore, and rebuild.
- Whether retention units are milliseconds, event-time distance, window counts,
  or bytes, and how the units are named to avoid ambiguity.
- Which settings trigger a rebuild and which can take effect at the next
  checkpoint.
- The metrics and alerts that expose hot count, cold count, checkpoint size,
  evictions, misses, fallback corrections, and retention deletions.

## Decision-to-task-list mapping

| Decision | Blocks these implementation steps from the README task list |
| --- | --- |
| D1 Hot-tier bound | 1, 3, 6, 10, 13 |
| D2 Cold retention and reach | 1, 2, 3, 6, 11 |
| D3 Behaviour outside reach | 1, 7, 8, 11 |
| D4 Cold-tier technology | 2, 3, 4, 9, 11 |
| D5 Record layout and encoding | 2, 4, 8 |
| D6 Gap query index | 2, 4, 8 |
| D7 Checkpoint contents and authority | 5, 9, 11 |
| D8 Eviction timing and failure handling | 6, 9, 11, 13 |
| D9 Durability order and fencing | 5, 6, 7, 9, 11, 13 |
| D10 Caching and promotion | 10, 13 |
| D11 Cold-tier degradation | 11, 12, 14 |
| D12 Replay, rebuild, and resharding parity | 5, 6, 11, 13, 14 |
| D13 Configuration and change contract | 1, 3, 12, 15 |

## Resolution record

Record the selected option and any sub-decisions before implementation starts.
A decision is not resolved until its validation, failure, and replay behaviour
are also chosen.

| Decision | Selected option | Sub-decisions and notes | Owner | Date |
| --- | --- | --- | --- | --- |
| D1 | | | | |
| D2 | | | | |
| D3 | | | | |
| D4 | | | | |
| D5 | | | | |
| D6 | | | | |
| D7 | | | | |
| D8 | | | | |
| D9 | | | | |
| D10 | | | | |
| D11 | | | | |
| D12 | | | | |
| D13 | | | | |
