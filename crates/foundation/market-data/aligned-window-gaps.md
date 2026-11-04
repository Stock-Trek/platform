# AlignedWindow gaps

`stock-trek`'s `AlignedWindow` is the source of truth for candle intervals,
window alignment, and label parsing and serialization. As of stock-trek 0.16.0,
this repo only needs this addition:

| Missing from `AlignedWindow` | Where this repo handles it |
| --- | --- |
| Exact duration for calendar windows | `WindowMillis::upper_bound` gives an upper bound rather than the actual month or year length. The rebuilder uses it only to size its final flush window; candle bounds come from `AlignedWindow::bounds_for`. |
