mod error;
mod fills;
mod market;
mod market_data;
mod order_events;
mod orders;
mod portfolio;
mod publish;
mod run;
mod simulation;
mod slots;
mod spec;

#[cfg(test)]
mod publish_tests;
#[cfg(test)]
mod run_tests;
#[cfg(test)]
mod tests;

pub use error::{BacktestError, Input};
pub use platform_errors::PlatformResult;
pub use run::BackTest;
use run::{Balances, PriceMap};
