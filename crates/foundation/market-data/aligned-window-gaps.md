# AlignedWindow gaps

`stock-trek`'s `AlignedWindow` is the source of truth for candle intervals and
window alignment. As of stock-trek 0.15.3, this repo only needs these additions:

| Missing from `AlignedWindow` | Where this repo handles it |
| --- | --- |
| Canonical label serialization | `candle::interval::serde_label` serializes candle keys with `AlignedWindow::label()` because stock-trek's derived serde form uses snake_case variant names such as `seconds60` instead of labels such as `60s`. |
| Case-insensitive label parsing | `parse_interval` trims and lowercases labels before `AlignedWindow::try_from`, which only accepts exact canonical labels. |
| Exact duration for calendar windows | `millis_upper_bound` gives an upper bound rather than the actual month or year length. The rebuilder uses it only to size its final flush window; candle bounds come from `AlignedWindow::bounds_for`. |
