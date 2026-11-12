use crate::{
    JetStream, StreamSpec,
    jetstream::{
        DEFAULT_DUPLICATE_WINDOW, DEFAULT_STREAM_REPLICAS, STREAM_MAX_AGE, STREAM_MAX_BYTES,
        STREAM_MAX_MESSAGES,
    },
};
use async_nats::jetstream::stream;
use std::time::Duration;

struct Fixture;

impl Fixture {
    fn spec() -> StreamSpec {
        StreamSpec::new(
            "portfolio.updates.v1",
            Duration::from_secs(7 * 24 * 60 * 60),
            1024 * 1024 * 1024,
            10_000_000,
        )
        .with_replicas(3)
        .with_duplicate_window(Duration::from_secs(120))
    }
}

#[test]
fn portfolio_streams_use_bounded_retention_and_replication() {
    let config = Fixture::spec().config().expect("config");

    assert_eq!(config.name, "portfolio-updates-v1");
    assert_eq!(config.subjects, vec!["portfolio.updates.v1.>".to_string()]);
    assert_eq!(config.storage, stream::StorageType::File);
    assert_eq!(config.retention, stream::RetentionPolicy::Limits);
    assert_eq!(config.num_replicas, 3);
    assert_eq!(config.max_age, Duration::from_secs(7 * 24 * 60 * 60));
    assert_eq!(config.max_bytes, 1024 * 1024 * 1024);
    assert_eq!(config.max_messages, 10_000_000);
    assert_eq!(config.duplicate_window, Duration::from_secs(120));
}

#[test]
fn stream_specs_reject_unbounded_or_unsafe_settings() {
    for spec in [
        Fixture::spec().with_replicas(0),
        Fixture::spec().with_replicas(6),
        Fixture::spec().with_duplicate_window(Duration::ZERO),
        Fixture::spec().with_duplicate_window(Duration::from_secs(8 * 24 * 60 * 60)),
        StreamSpec::new("portfolio", Duration::ZERO, 1, 1),
        StreamSpec::new("portfolio", Duration::from_secs(1), 0, 1),
        StreamSpec::new("portfolio", Duration::from_secs(1), 1, 0),
    ] {
        assert!(spec.validate().is_err());
    }
}

#[test]
fn stream_specs_use_three_replicas_by_default() {
    let spec = StreamSpec::new("portfolio", Duration::from_secs(1), 1, 1);

    assert_eq!(spec.replicas(), DEFAULT_STREAM_REPLICAS);
    assert_eq!(spec.duplicate_window(), DEFAULT_DUPLICATE_WINDOW);
}

#[test]
fn market_data_streams_have_bounded_retention_and_storage() {
    let config = JetStream::stream_config("market.events");

    assert_eq!(config.name, "market-events");
    assert_eq!(config.subjects, vec!["market.events.>".to_string()]);
    assert_eq!(config.storage, stream::StorageType::File);
    assert_eq!(config.retention, stream::RetentionPolicy::Limits);
    assert_eq!(config.max_age, STREAM_MAX_AGE);
    assert_eq!(config.max_bytes, STREAM_MAX_BYTES);
    assert_eq!(config.max_messages, STREAM_MAX_MESSAGES);
    assert!(!config.max_age.is_zero());
    assert!(config.max_bytes > 0);
    assert!(config.max_messages > 0);
}

#[test]
fn stream_names_are_sanitize_valid_tokens() {
    assert_eq!(JetStream::stream_name("market.events"), "market-events");
    assert_eq!(
        JetStream::stream_name("market.candles.closed"),
        "market-candles-closed"
    );
    assert_eq!(
        JetStream::kv_bucket_name("market.candles.open"),
        "market_candles_open"
    );
    assert_eq!(JetStream::kv_bucket_name("market-events"), "market-events");
}

#[test]
fn message_ids_are_deterministic_and_payload_sensitive() {
    let first = JetStream::message_id("subject.a", b"payload");
    let again = JetStream::message_id("subject.a", b"payload");
    let other_payload = JetStream::message_id("subject.a", b"different");
    let other_subject = JetStream::message_id("subject.b", b"payload");

    assert_eq!(first, again);
    assert_ne!(first, other_payload);
    assert_ne!(first, other_subject);
}
