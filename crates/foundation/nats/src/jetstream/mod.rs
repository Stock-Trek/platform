//! JetStream naming and error helpers shared by the publisher, consumer, and KV store.

mod jetstream_helpers;
mod stream_spec;

#[cfg(test)]
mod tests;

pub use jetstream_helpers::{
    ACK_WAIT, JetStream, STREAM_MAX_AGE, STREAM_MAX_BYTES, STREAM_MAX_MESSAGES,
};
pub use stream_spec::{
    DEFAULT_DUPLICATE_WINDOW, DEFAULT_STREAM_REPLICAS, MAX_STREAM_REPLICAS, MIN_STREAM_REPLICAS,
    StreamSpec,
};
