# Implementation tasks: event-time correction cutoff with a per-key hot candle cap

This document lists the work needed in this repository to implement the
"Event-time correction cutoff with a per-key hot candle cap" retention policy
described in [README.md](README.md#closed-candle-retention).

The intended behaviour is:

- Keep at most `N` closed candles per `CandleKey` in hot state.
- Reject, rather than partially correct, an input whose candle window ended
  before `watermark - correction_cutoff`.
- Treat a candle evicted by the per-key cap as outside the correction contract,
  even when it is still inside the event-time cutoff.
- Apply exactly the same rules during live processing, checkpoint restore,
  archive rebuild, and rebuilder replay.

This is greenfield work. Do not add checkpoint migration, dual-read, dual-write,
or legacy compatibility paths. If the serialized state shape changes, bump
`STATE_SCHEMA_VERSION`; an old checkpoint then takes the existing incompatible
checkpoint path and is rebuilt from the archive.

## 1. Add the retention configuration

- [ ] Add required environment variable constants in
      `crates/data-plane/aggregator/src/config/env.rs`:
      - `AGGREGATOR_MAX_CLOSED_CANDLES_PER_KEY` for `N`.
      - `AGGREGATOR_CORRECTION_CUTOFF_MS` for the event-time correction cutoff.
      - `AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES` for the config-load
        memory check.
- [ ] Re-export the new constants from
      `crates/data-plane/aggregator/src/config/mod.rs`.
- [ ] Add the three values to `AggregatorConfig` in
      `crates/data-plane/aggregator/src/config/aggregator.rs`.
- [ ] Parse them as required positive values in
      `crates/data-plane/aggregator/src/config/loader.rs`. Convert the cutoff to
      `DurationMilliseconds` and use checked conversion for the byte budget.
- [ ] Extend `ProcessorConfig` in
      `crates/data-plane/aggregator/src/processor/config.rs` with
      `max_closed_candles_per_key` and `correction_cutoff`.
- [ ] Make `AggregatorConfig::processor_config()` pass both values through.
- [ ] Add `REBUILDER_MAX_CLOSED_CANDLES_PER_KEY` and
      `REBUILDER_CORRECTION_CUTOFF_MS` to
      `crates/data-plane/rebuilder/src/config.rs`, and pass them into the
      rebuilder's `ProcessorConfig` in `crates/data-plane/rebuilder/src/lib.rs`.
- [ ] Keep test-only constructors explicit about the retention policy. Tests
      that do not exercise retention should use a clearly named helper with
      retention disabled or a large cap, rather than relying on an implicit
      default that could mask a regression.

## 2. Validate the configuration at startup

Add a validation helper in
`crates/data-plane/aggregator/src/config/validation.rs` and call it from
`AggregatorConfig::from_vars`.

- [ ] Reject `max_closed_candles_per_key == 0`.
- [ ] Reject `correction_cutoff == 0`.
- [ ] Reject `correction_cutoff < allowed_lateness`. A cutoff shorter than the
      allowed-lateness window could reject inputs that are still legitimately
      updating an open candle.
- [ ] Add a single source of truth for the static cardinalities of `CexId` and
      `AssetId`. Prefer a dependency version of `stock-trek-types` that exposes
      `EnumCount`/`EnumIter`; otherwise add explicit constants beside the
      validation code and a test/checklist entry that updates them whenever the
      dependency's enums change.
- [ ] Compute the static candle-key ceiling as:

      CexId count × AssetId count² × configured interval count

      This is a conservative ceiling because it includes invalid same-asset
      pairs. It must use the full enum cardinality and configured intervals, not
      the keys observed so far.
- [ ] Compute the worst-case key count per shard as
      `ceil(total_keys / shard_count)`.
- [ ] Estimate closed-candle memory with checked arithmetic:
      `keys_per_shard × N × estimated_bytes_per_candle`. Include the candle,
      candle key, `Vec`/`HashMap` entry overhead, and a documented conservative
      allowance. Keep the estimate in one named constant or function so tests
      and documentation use the same value.
- [ ] Reject startup when that estimate exceeds
      `AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES`, with a `config_invalid`
      error naming the variables and the calculated worst case.
- [ ] Handle multiplication and division overflow explicitly rather than
      wrapping.
- [ ] Do not revalidate when a previously unused key starts trading. The static
      ceiling already includes it. Revalidation only happens on a new startup
      when enum sets, intervals, shard count, `N`, cutoff, or the budget change.
- [ ] Add unit tests for:
      - zero/negative `N`, zero cutoff, and cutoff below allowed lateness;
      - the exact static key-ceiling calculation;
      - a budget that is just large enough and one that is one byte too small;
      - checked-arithmetic overflow;
      - a configuration whose worst case passes because it uses the full static
        ceiling rather than the currently populated keys.

## 3. Enforce the per-key cap in state

- [ ] Change closed-candle retention in
      `crates/data-plane/aggregator/src/state/candles.rs` so every insert or
      replacement enforces the configured per-key cap.
- [ ] Keep the existing start-time ordering. When a key exceeds `N`, evict the
      lowest-start candle from the front of that key's `Vec`.
- [ ] Return the number of evicted candles (and the evicted candles if needed
      for metrics) so the processor and runner can report them.
- [ ] Do not evict on a replacement of an existing window: replacing a retained
      candle must not change the number of candles for that key.
- [ ] Add a helper that tells the processor whether retaining a candidate
      window would immediately evict that same candidate. When a key already has
      `N` candles and the candidate starts before the oldest retained window, it
      is outside the per-key contract and must not be inserted, published, or
      counted as a successful correction.
- [ ] Ensure the cap also applies when `CandleProcessor::from_state` loads a
      checkpoint. A checkpoint restored under a smaller `N` must be pruned to the
      current cap before processing continues.
- [ ] Keep `closed_candles_len()` and state equality/serialization working. If a
      new persisted field is needed, update `AggregatorState`, `PartialEq`,
      serialization tests, and `STATE_SCHEMA_VERSION`.

## 4. Enforce the correction cutoff

- [ ] Add one shared predicate or helper for "this closed window is inside the
      correction contract". It must use the effective event-time watermark for
      the key or stream, never wall-clock time.
- [ ] Reject a closed-window correction when
      `window_end < watermark - correction_cutoff`.
- [ ] Apply the check before every closed-candle mutation:
      - `apply_late_trade` in
        `crates/data-plane/aggregator/src/processor/candle.rs`;
      - `open_closed_correction` in the same file;
      - any path that would retain a newly reconstructed closed candle.
- [ ] Do not publish a partial correction for an out-of-contract window. Skip
      the candle mutation and record the rejection/count instead. The source
      event is still deduplicated and may still advance event time according to
      the existing event-processing rules.
- [ ] Emit an operator-visible structured warning or rate-limited aggregate for
      each rejected correction, including the exchange, pair, interval, window
      bounds, watermark, and cutoff. This is the repair signal for data that
      arrived outside the correction contract; do not drop it silently.
- [ ] Apply the cutoff to gap propagation in
      `crates/data-plane/aggregator/src/processor/window.rs`. Filter affected
      windows before `record_incomplete_window` and before publishing an open
      or closed revision. Keep the gap marker's normal stream/watermark and
      `known_gaps` bookkeeping, but do not mutate candle windows outside the
      correction contract.
- [ ] Ensure a correction for a candle evicted by the per-key cap is rejected
      by the same out-of-contract path when the cap would evict it immediately.
      It must not create a fresh partial candle containing only the late trade.
- [ ] Extend `ProcessorUpdate` in
      `crates/data-plane/aggregator/src/processor/update.rs` with counts for
      out-of-contract trades and affected gap windows, plus the eviction count.
      Keep `is_empty()` consistent with the intended checkpoint/output
      behaviour for count-only updates.
- [ ] Keep all decisions independent of map iteration order. Any list of
      affected or evicted candles must be sorted by `CandleKey::subject_suffix`
      and window start before mutation or publication, matching the existing
      deterministic ordering.

## 5. Preserve live, checkpoint, archive, and rebuild parity

- [ ] Extend `RestoreOptions` and `RestoreOptions::from_config` in
      `crates/data-plane/aggregator/src/restore/rebuild.rs` with the retention
      cap and cutoff.
- [ ] Thread those values through `rebuild_state_from_archive`,
      `rebuild_state_from_archive_with_schema`, and the internal rebuild path
      so archive replay constructs the same `ProcessorConfig` as live traffic.
- [ ] Update every `ProcessorConfig::new` call site, including the rebuilder,
      so there is no path that processes with a different retention policy.
- [ ] Confirm that applying the same input sequence with the same cap and
      cutoff produces the same state, revisions, evictions, and out-of-contract
      decisions in live processing and archive replay.
- [ ] Confirm that a restored checkpoint with too many candles is pruned
      deterministically before it is used.
- [ ] Add tests that replay the same events with a cap and cutoff and assert
      identical state and closed-candle message ids.
- [ ] Add a rebuilder test that regenerated candles match the live processor for
      events that exercise both cap eviction and an out-of-contract correction.
- [ ] Document that the rebuilder still materialises all regenerated output
      candles; the cap bounds its active processor state, not the final output
      collection.

## 6. Add observability

- [ ] Add metric names and descriptions in
      `crates/foundation/metrics/src/descriptions.rs` and recording helpers in
      `crates/foundation/metrics/src/market_data.rs` for:
      - retained closed candles per shard;
      - closed candles evicted by the per-key cap;
      - out-of-contract trade corrections;
      - out-of-contract gap-affected windows.
- [ ] Record the new update counts from
      `crates/data-plane/aggregator/src/runner/dispatch.rs`.
- [ ] Record the retained-closed-candle gauge after processing and after restore
      so operators can see the effect of `N` before a checkpoint grows.
- [ ] Include the new metrics in the aggregator observability list in
      `crates/data-plane/README.md`.
- [ ] Add tests for the metric-recording helpers where the metrics crate already
      has a testable pattern.

## 7. Cover the behaviour with tests

Add focused tests in the existing modules:

- [ ] `state/tests.rs`:
      - cap keeps exactly the newest `N` candles for one key;
      - replacing an existing window does not evict anything;
      - inserting an older window at capacity is rejected by the processor and
        does not silently remove a newer candle;
      - multiple keys each get an independent cap;
      - checkpoint JSON round-trip preserves the retained set.
- [ ] `processor/tests.rs`:
      - a late trade inside the cutoff still revises the retained full candle;
      - a late trade outside the cutoff publishes nothing and does not create a
        partial correction;
      - a correction for a cap-evicted window publishes nothing;
      - a gap inside the cutoff marks and republishes the affected retained
        candle;
      - a gap outside the cutoff does not mark an old retained candle while it
        still records the source gap;
      - deterministic ordering when several keys and windows are affected;
      - replaying the same inputs produces identical state and revisions.
- [ ] `restore/tests.rs`:
      - an oversized restored checkpoint is pruned to the current cap;
      - archive rebuild uses the same cap and cutoff as live processing.
- [ ] `rebuilder` tests:
      - cap/cutoff configuration is parsed and rejected correctly;
      - rebuilt output matches live processing when an old correction is
        rejected.
- [ ] Runner integration tests:
      - eviction and out-of-contract counts reach the metrics path;
      - a count-only out-of-contract update does not publish a candle.

## 8. Update documentation

- [ ] Replace the "unbounded growth is an unresolved limitation" description in
      the closed-candle retention section of
      `crates/data-plane/README.md` with the implemented behaviour: per-key cap,
      event-time cutoff, rejection instead of partial correction, and restore
      pruning.
- [ ] Add the new aggregator variables to the README configuration table.
- [ ] Add the new rebuilder variables to the rebuilder configuration table.
- [ ] Add the aggregator variables to `docs/environment-variables.md`, including
      the cutoff/lateness relationship and the static memory validation.
- [ ] Add the rebuilder variables to `docs/environment-variables.md`.
- [ ] Update the retention options text if the final implementation changes any
      of the option's stated guarantees.

## 9. Verification

- [ ] Run `cargo fmt`.
- [ ] Run `cargo clippy` for the affected crates and fix all warnings.
- [ ] Run the aggregator, rebuilder, and metrics tests.
- [ ] Verify startup fails clearly, before processing, for a configuration whose
      worst-case per-shard memory exceeds the budget.
- [ ] Verify a shard with a checkpoint produced under the new configuration
      restores with no more than `N` closed candles per key.
- [ ] Verify archive replay and the rebuilder produce the same candle revisions
      as live processing for the same cap and cutoff.
