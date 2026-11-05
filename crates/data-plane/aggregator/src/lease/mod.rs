//! Single-writer leases and fencing tokens for shard state.

mod writer_lease;

#[cfg(test)]
pub(crate) mod tests;

pub use writer_lease::*;
