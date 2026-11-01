# AlignedWindow gaps

`stock-trek`'s `AlignedWindow` is the source of truth for candle intervals.
This crate adds only the behaviour the enum does not provide:

| Missing from `AlignedWindow` | Where this repo handles it |
| --- | --- |
| Window bounds containing an arbitrary timestamp | `AlignedWindowExt::bounds_for` returns the inclusive start and exclusive end. The enum only exposes `millis_upper_bound`, `checked_add`, and `checked_sub` relative to a known datetime. |
| Calendar-aligned month and year flooring | `bounds_for` floors month/year windows to calendar boundaries; `checked_add_months` only advances from an existing datetime. |
| Explicit rejection of future variants | `AlignedWindow` is `#[non_exhaustive]`, so external matches require a wildcard. `bounds_for` returns `AlignedWindowError::UnsupportedWindow` for any variant it does not explicitly align instead of silently applying a default duration. |
| Canonical label serialization | `candle::interval::serde_label` serializes candle keys with `AlignedWindow::label()` rather than stock-trek's derived snake_case variant names. |
| Case-insensitive label parsing | `parse_interval` trims and lowercases labels before `AlignedWindow::try_from`. Only canonical labels are accepted; legacy aliases and numeric durations are rejected. |
