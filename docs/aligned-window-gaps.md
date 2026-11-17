# AlignedWindow gaps

`stock-trek`'s `AlignedWindow` is the source of truth for candle intervals,
window alignment, and label parsing and serialization. `WindowMillis` does not
provide a timestamp-independent exact duration for calendar windows. Exact
bounds for a timestamp are available from `AlignedWindow::bounds_for`, and
`AlignedWindow::months_exact` identifies month-based windows.

`WindowMillis::upper_bound` is still used where a conservative duration is
sufficient:

| Use | Location |
| --- | --- |
| Sizing the final rebuilder flush | `crates/data-plane/rebuilder/src/rebuild/rebuild_options.rs` |
| Scaling the volume of an open backtest candle | `crates/sandbox-plane/runner/src/cwasm/backtest/market.rs` |
| Validating backtest open-candle progress against the driving window | `crates/sandbox-plane/runner/src/cwasm/backtest/spec.rs` |

Candle starts and ends come from `AlignedWindow::bounds_for`, so calendar
candles use their actual month or year length.
