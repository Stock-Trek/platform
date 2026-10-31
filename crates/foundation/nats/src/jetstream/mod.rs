//! JetStream naming and error helpers shared by the publisher, consumer, and KV store.

mod implementation;

#[cfg(test)]
mod tests;

pub use implementation::*;
