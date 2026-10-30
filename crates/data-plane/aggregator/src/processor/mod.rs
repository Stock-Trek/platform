//! Deterministic candle aggregation.
//!
//! [`CandleProcessor`] is a pure fold over keyed state: it never reads the wall
//! clock and performs no I/O. The worker persists the updated state and emits
//! the returned outputs, which makes replaying an event log reproducible.
//!
//! The processor is split into focused modules: `candle` folds trades into
//! open or closed candles, `dedup` rejects already-seen events, `window`
//! closes expired windows and marks windows affected by source gaps,
//! `config` holds processing settings, `update` carries processing outputs,
//! and `engine` coordinates the keyed processor.

mod candle;
mod config;
mod dedup;
mod engine;
mod update;
mod window;

#[cfg(test)]
mod tests;

pub use config::ProcessorConfig;
pub use engine::CandleProcessor;
pub use update::ProcessorUpdate;
