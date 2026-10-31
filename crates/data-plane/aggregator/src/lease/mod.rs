//! Single-writer leases and fencing tokens for shard state.

mod implementation;

#[cfg(test)]
pub(crate) mod tests;

pub use implementation::*;
