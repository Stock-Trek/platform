# Implementation tasks: event-time correction cutoff with configurable per-key hot candle caps

This document lists the work needed in this repository to implement the
"Event-time correction cutoff with configurable per-key hot candle caps"
retention policy described in [README.md](README.md#closed-candle-retention).

The intended behaviour is:

- Keep at most `N(key)` closed candles per `CandleKey`, where `N(key)` is
  configured for that key individually. A required default applies to keys
  without an override, and an optional override list sets specific keys.
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

## Retention configuration format

`AGGREGATOR_DEFAULT_MAX_CLOSED_CANDLES_PER_KEY` is the required fallback cap.
`AGGREGATOR_MAX_CLOSED_CANDLES_PER_KEY` is an optional comma-separated list of
`<candle-key>=<count>` overrides. `<candle-key>` uses the
`CandleKey::subject_suffix()` form `{exchange}.{base}.{quote}.{interval}`, for
example:

```text
binance.BTC.USDT.1m=2880,binance.ETH.USDT.1m=720
```

Whitespace around entries is ignored. The same `CandleKey` may not appear twice,
every override count must be positive, and every override key must parse to a
known exchange, asset pair, and configured interval. The effective cap is
`N(key) = override(key)` when present, otherwise the default. The rebuilder uses
the equivalent `REBUILDER_DEFAULT_MAX_CLOSED_CANDLES_PER_KEY` and
`REBUILDER_MAX_CLOSED_CANDLES_PER_KEY` variables with the same format.

## 1. Add the retention configuration

- [ ] Add required environment variable constants in
      `crates/data-plane/aggregator/src/config/env.rs`:
      - `AGGREGATOR_DEFAULT_MAX_CLOSED_CANDLES_PER_KEY` for the default
        `N(key)` used by keys without an override.
      - `AGGREGATOR_MAX_CLOSED_CANDLES_PER_KEY` for the optional per-key
        overrides.
      - `AGGREGATOR_CORRECTION_CUTOFF_MS` for the event-time correction cutoff.
      - `AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES` for the config-load
        memory check.
- [ ] Re-export the new constants from
      `crates/data-plane/aggregator/src/config/mod.rs`.
- [ ] Introduce one retention-policy type (for example
      `ClosedCandleRetention`) that owns the default cap and the deterministic
      per-key override map, exposes `cap_for(&CandleKey)`, and can be cloned
      into `ProcessorConfig` without re-parsing strings.
- [ ] Parse the default and overrides as required positive values in
      `crates/data-plane/aggregator/src/config/loader.rs`. Parse each override
      to a `CandleKey`, reject duplicate keys and unknown or unconfigured
      keys, convert the cutoff to `DurationMilliseconds`, and use checked
      conversion for the byte budget.
- [ ] Add the default, override map, cutoff, and byte budget to
      `AggregatorConfig` in
      `crates/data-plane/aggregator/src/config/aggregator.rs`.
- [ ] Extend `ProcessorConfig` in
      `crates/data-plane/aggregator/src/processor/config.rs` with the
      retention policy and `correction_cutoff`.
- [ ] Make `AggregatorConfig::processor_config()` pass the retention policy and
      cutoff through.
- [ ] Add `REBUILDER_DEFAULT_MAX_CLOSED_CANDLES_PER_KEY`,
      `REBUILDER_MAX_CLOSED_CANDLES_PER_KEY`, and
      `REBUILDER_CORRECTION_CUTOFF_MS` to
      `crates/data-plane/rebuilder/src/config.rs`, parse the override list with
      the same rules, and pass them into the rebuilder's `ProcessorConfig` in
      `crates/data-plane/rebuilder/src/lib.rs`.
- [ ] Keep test-only constructors explicit about the retention policy. Tests
      that do not exercise retention should use a clearly named helper with
      retention disabled or a large default cap, rather than relying on an
      implicit default that could mask a regression. Tests that exercise
      per-key behaviour must set the default and overrides explicitly.

## 2. Validate the configuration at startup

Add a validation helper in
`crates/data-plane/aggregator/src/config/validation.rs` and call it from
`AggregatorConfig::from_vars`.

- [ ] Reject a default cap of `0`.
- [ ] Reject an override cap of `0`, a duplicate override key, a malformed
      override entry, and an override key that does not resolve to a known
      exchange, asset pair, and configured interval.
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
- [ ] Resolve the effective `N(key)` for every key in the static ceiling,
      applying the override map and falling back to the default.
- [ ] Compute the worst-case key count per shard as
      `ceil(total_keys / shard_count)`.
- [ ] Compute the worst-case closed-candle count per shard as the sum of the
      `keys_per_shard` largest effective per-key caps. This reduces to
      `keys_per_shard × N(default)` when no overrides are configured, and
      remains a conservative bound when overrides differ.
- [ ] Estimate closed-candle memory with checked arithmetic:
      `worst_case_closed_candles_per_shard × estimated_bytes_per_candle`.
      Include the candle, candle key, `Vec`/`HashMap` entry overhead, and a
      documented conservative allowance. Keep the estimate in one named
      constant or function so tests and documentation use the same value.
- [ ] Reject startup when that estimate exceeds
      `AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES`, with a `config_invalid`
      error naming the variables, the effective per-key caps that determine the
      worst case, and the calculated worst case.
- [ ] Handle multiplication and division overflow explicitly rather than
      wrapping.
- [ ] Do not revalidate when a previously unused key starts trading. The static
      ceiling already includes it. Revalidation only happens on a new startup
      when enum sets, intervals, shard count, the default cap, the overrides,
      cutoff, or the budget change.
- [ ] Add unit tests for:
      - zero, negative, or otherwise invalid default and override caps, zero
        cutoff, and cutoff below allowed lateness;
      - duplicate, malformed, and unknown override keys;
      - the exact static key-ceiling calculation;
      - the worst-case calculation with a mix of default and overridden keys;
      - a budget that is just large enough and one that is one byte too small;
      - checked-arithmetic overflow;
      - a configuration whose worst case passes because it uses the full static
        ceiling rather than the currently populated keys.

## 3. Enforce the per-key cap in state

- [ ] Change closed-candle retention in
      `crates/data-plane/aggregator/src/state/candles.rs` so every insert or
      replacement enforces the configured `N(key)` for that key.
- [ ] Keep the existing start-time ordering. When a key exceeds `N(key)`, evict
      the lowest-start candle from the front of that key's `Vec`.
- [ ] Return the number of evicted candles (and the evicted candles if needed
      for metrics) so the processor and runner can report them.
- [ ] Do not evict on a replacement of an existing window: replacing a retained
      candle must not change the number of candles for that key.
- [ ] Add a helper that tells the processor whether retaining a candidate
      window would immediately evict that same candidate. When a key already has
      `N(key)` candles and the candidate starts before the oldest retained
      window, it is outside the per-key contract and must not be inserted,
      published, or counted as a successful correction.
- [ ] Ensure the cap also applies when `CandleProcessor::from_state` loads a
      checkpoint. A checkpoint restored under a smaller default or a changed
      per-key override must be pruned to the current `N(key)` for every key
      before processing continues.
- [ ] Keep `closed_candles_len()` and state equality/serialization working. If a
      new persisted field is needed, update `AggregatorState`, `PartialEq`,
      serialization tests, and `STATE_SCHEMA_VERSION`. The retention policy
      itself is configuration, not persisted state, so it should not be stored
      in each checkpoint.

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
      bounds, watermark, cutoff, and the key's effective cap. This is the repair
      signal for data that arrived outside the correction contract; do not drop
      it silently.
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
      policy (default and per-key overrides) and cutoff.
- [ ] Thread those values through `rebuild_state_from_archive`,
      `rebuild_state_from_archive_with_schema`, and the internal rebuild path
      so archive replay constructs the same `ProcessorConfig` as live traffic.
- [ ] Update every `ProcessorConfig::new` call site, including the rebuilder,
      so there is no path that processes with a different retention policy.
- [ ] Confirm that applying the same input sequence with the same default,
      overrides, and cutoff produces the same state, revisions, evictions, and
      out-of-contract decisions in live processing and archive replay.
- [ ] Confirm that a restored checkpoint with too many candles is pruned
      deterministically to the current `N(key)` before it is used.
- [ ] Add tests that replay the same events with per-key caps and a cutoff and
      assert identical state and closed-candle message ids.
- [ ] Add a rebuilder test that regenerated candles match the live processor for
      events that exercise both per-key cap eviction and an out-of-contract
      correction.
- [ ] Document that the rebuilder still materialises all regenerated output
      candles; the per-key caps bound its active processor state, not the final
      output collection.

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
      so operators can see the effect of the per-key caps before a checkpoint
      grows. Record the configured default and override count at startup so the
      active policy is visible.
- [ ] Include the new metrics in the aggregator observability list in
      `crates/data-plane/README.md`.
- [ ] Add tests for the metric-recording helpers where the metrics crate already
      has a testable pattern.

## 7. Cover the behaviour with tests

Add focused tests in the existing modules:

- [ ] `state/tests.rs`:
      - each key keeps exactly its newest `N(key)` candles;
      - an override on one key does not change the cap of another key using the
        default;
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
      - replaying the same inputs produces identical state and revisions;
      - the same event sequence produces different, independently correct
        results for two keys with different configured caps.
- [ ] `restore/tests.rs`:
      - an oversized restored checkpoint is pruned to the current per-key caps,
        including when an override or the default has changed;
      - archive rebuild uses the same default, overrides, and cutoff as live
        processing.
- [ ] `rebuilder` tests:
      - default, override, and cutoff configuration is parsed and rejected
        correctly;
      - rebuilt output matches live processing when an old correction is
        rejected and when a per-key cap evicts a window.
- [ ] Runner integration tests:
      - eviction and out-of-contract counts reach the metrics path;
      - a count-only out-of-contract update does not publish a candle.

## 8. Update documentation

- [ ] Replace the "unbounded growth is an unresolved limitation" description in
      the closed-candle retention section of
      `crates/data-plane/README.md` with the implemented behaviour:
      configurable per-key caps with a default and overrides, event-time cutoff,
      rejection instead of partial correction, and restore pruning.
- [ ] Document the override syntax and the interaction between the default cap
      and per-key overrides.
- [ ] Add the new aggregator variables to the README configuration table.
- [ ] Add the new rebuilder variables to the rebuilder configuration table.
- [ ] Add the aggregator variables to `docs/environment-variables.md`, including
      the override syntax, the cutoff/lateness relationship, and the static
      memory validation.
- [ ] Add the rebuilder variables to `docs/environment-variables.md`.
- [ ] Update the retention options text if the final implementation changes any
      of the option's stated guarantees.

## 9. Verification

- [ ] Run `cargo fmt`.
- [ ] Run `cargo clippy` for the affected crates and fix all warnings.
- [ ] Run the aggregator, rebuilder, and metrics tests.
- [ ] Verify startup fails clearly, before processing, for a configuration whose
      worst-case per-shard memory exceeds the budget, including when the excess
      is caused by a per-key override.
- [ ] Verify a shard with a checkpoint produced under the new configuration
      restores with no more than `N(key)` closed candles per key.
- [ ] Verify archive replay and the rebuilder produce the same candle revisions
      as live processing for the same default, overrides, and cutoff.
