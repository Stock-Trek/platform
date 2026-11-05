//! Shard processing worker lifecycle.

mod processor_runner;

#[cfg(test)]
mod tests;

pub use processor_runner::*;
