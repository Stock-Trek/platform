//! NATS message representation shared by core and JetStream flows.

mod nats_message;

#[cfg(test)]
mod tests;

pub use nats_message::*;
