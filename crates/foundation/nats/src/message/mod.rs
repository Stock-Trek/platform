//! NATS message representation shared by core and JetStream flows.

mod implementation;

#[cfg(test)]
mod tests;

pub use implementation::*;
