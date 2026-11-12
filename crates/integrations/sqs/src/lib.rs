mod message_dedup;
mod sqs;

#[cfg(test)]
mod message_dedup_tests;

pub use crate::{
    message_dedup::{SqsDedup, SqsDedupGuard},
    sqs::{Sqs, SqsConfig},
};
