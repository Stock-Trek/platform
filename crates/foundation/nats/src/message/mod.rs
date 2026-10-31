//! NATS message representation shared by core and JetStream flows.

mod message;

#[cfg(test)]
mod tests;

pub use message::*;
