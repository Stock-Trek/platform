mod error;
mod fills;
mod market;
mod market_data;
mod order_events;
mod orders;
mod pipeline;
mod portfolio;
mod publish;
mod run;
mod simulation;
mod slots;
mod spec;
mod worker_limits;
mod worker_runtime;

#[cfg(test)]
mod pipeline_tests;
#[cfg(test)]
mod publish_tests;
#[cfg(test)]
mod run_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod worker_limits_tests;

pub use error::{BacktestError, Input};
pub use run::Backtest;
use run::{Balances, PriceMap};
pub use worker_limits::{
    ABSOLUTE_MAX_WORKERS, DEFAULT_MAX_WORKERS, MAX_SIMULATION_COUNT, WorkerLimits,
};
pub use worker_runtime::WorkerRuntime;
