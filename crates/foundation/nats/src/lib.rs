//! NATS client shared by the market data services.
//!
//! This module wraps [`async_nats`] so services share one implementation of
//! JetStream publishing, durable pull consumers, and KV access.

mod jetstream;
mod jetstream_consumer;
mod kv;
mod kv_watch;
mod nats_address;
mod nats_message;
mod nats_subject;
mod publisher;

#[cfg(test)]
mod jetstream_consumer_tests;
#[cfg(test)]
mod nats_address_tests;
#[cfg(test)]
mod nats_message_tests;
#[cfg(test)]
mod nats_subject_tests;

pub use jetstream::{ACK_WAIT, JetStream, StreamSpec};
pub use jetstream_consumer::{ConsumerStart, JetStreamConsumer};
pub use kv::KeyValue;
pub use kv_watch::{KeyValueChange, KeyValueWatch};
pub use nats_address::NatsAddress;
pub use nats_message::Message;
pub use nats_message::*;
pub use nats_subject::NatsSubject;
pub use publisher::{EventPublisher, NatsPublisher, PublishAck};
