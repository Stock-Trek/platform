//! Minimal NATS client shared by the market data services.
//!
//! Publishes and subscriptions run over JetStream so that events survive
//! reconnects and restarts, with a core-NATS fallback in [`NatsSubscriber`].

mod address;
mod connection;
mod consumer;
mod jetstream;
mod kv;
mod message;
mod publisher;
mod subject;
mod subscriber;

pub use address::NatsAddress;
pub use consumer::JetStreamConsumer;
pub use jetstream::JetStream;
pub use kv::KeyValue;
pub use message::Message;
pub use publisher::{EventPublisher, NatsPublisher, PublishAck};
pub use subject::NatsSubject;
pub use subscriber::NatsSubscriber;
