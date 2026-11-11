use crate::TokenUsageQueueConfig;
use std::time::Duration;

struct ConfigFixture;

impl ConfigFixture {
    fn config(capacity: usize, batch_size: usize) -> TokenUsageQueueConfig {
        TokenUsageQueueConfig::new(
            "redis://valkey.example.com:6379",
            "auth-usage",
            Duration::from_secs(5),
            capacity,
            batch_size,
        )
        .expect("valid usage queue config")
    }
}

#[test]
fn accepts_a_bounded_queue_with_a_smaller_batch() {
    let config = ConfigFixture::config(1000, 100);

    assert_eq!(config.capacity(), 1000);
    assert_eq!(config.batch_size(), 100);
    assert_eq!(config.key_prefix(), "auth-usage");
}

#[test]
fn rejects_a_zero_capacity_or_batch() {
    assert!(
        TokenUsageQueueConfig::new(
            "redis://valkey.example.com:6379",
            "auth-usage",
            Duration::from_secs(5),
            0,
            1,
        )
        .is_err()
    );
    assert!(
        TokenUsageQueueConfig::new(
            "redis://valkey.example.com:6379",
            "auth-usage",
            Duration::from_secs(5),
            1,
            0,
        )
        .is_err()
    );
}

#[test]
fn rejects_a_batch_larger_than_the_queue() {
    assert!(
        TokenUsageQueueConfig::new(
            "redis://valkey.example.com:6379",
            "auth-usage",
            Duration::from_secs(5),
            10,
            11,
        )
        .is_err()
    );
}
