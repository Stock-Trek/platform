use super::*;

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
