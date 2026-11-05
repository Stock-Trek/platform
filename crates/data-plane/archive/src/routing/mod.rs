//! Stable routing keys shared by live aggregation and archived objects.
//!
//! A routing key is a pure function of `(exchange, trading pair)`. Archive
//! files are grouped into a fixed number of virtual buckets derived from that
//! key, so changing the active shard count only changes which buckets a
//! shard owns. It never changes the contents of an archive bucket.

mod routing_key;

#[cfg(test)]
mod tests;

pub use routing_key::*;
