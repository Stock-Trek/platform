//! NATS client shared by the market data services.
//!
//! This module wraps [`async_nats`] so services share one implementation of
//! JetStream publishing, durable pull consumers, and KV access.

mod address;
mod consumer;
mod jetstream;
mod kv;
mod message;
mod publisher;
mod subject;
mod subscriber;

pub use address::NatsAddress;
pub use consumer::{ConsumerStart, JetStreamConsumer};
pub use jetstream::{ACK_WAIT, JetStream};
pub use kv::KeyValue;
pub use message::Message;
pub use publisher::{EventPublisher, NatsPublisher, PublishAck};
pub use subject::NatsSubject;
pub use subscriber::NatsSubscriber;
