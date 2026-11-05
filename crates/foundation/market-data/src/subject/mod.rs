//! NATS subject construction for the market data event contract.

mod market_event_subject;

#[cfg(test)]
mod tests;

pub use market_event_subject::*;
