//! Single-writer leases and fencing tokens for shard state.

mod lease;

#[cfg(test)]
pub(crate) mod tests;

pub use lease::*;
