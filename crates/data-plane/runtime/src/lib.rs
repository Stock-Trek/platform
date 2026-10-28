//! Shared configuration and runtime scaffolding for the data-plane services.
//!
//! The ingester, aggregator, archiver, backfiller, and compactor all read the
//! same kinds of values from the environment and run the same shutdown loop.
//! Keeping those rules here means a malformed NATS address, subject prefix, or
//! non-positive duration is rejected consistently across every service.

pub mod config;
pub mod runtime;

pub use config::Config;
pub use runtime::Runtime;
