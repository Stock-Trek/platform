# AlignedWindow gaps

`stock-trek`'s `AlignedWindow` is the source of truth for candle intervals,
window alignment, and label-based serialization. As of stock-trek 0.16.0, this
repo only needs these additions:

| Missing from `AlignedWindow` | Where this repo handles it |
| --- | --- |
| Case-insensitive label parsing | `parse_interval` trims and lowercases labels before `AlignedWindow::try_from`, which only accepts exact canonical labels. |
| Exact duration for calendar windows | `WindowMillis::upper_bound` gives an upper bound rather than the actual month or year length. The rebuilder uses it only to size its final flush window; candle bounds come from `AlignedWindow::bounds_for`. |
