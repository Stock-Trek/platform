use super::*;
use crate::event::ParsedMessage;
use platform_errors::IngesterError;
use platform_types::new_types::TimestampMilliseconds;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use iris::Codec;
use serde_json::json;
use tokio_websockets::Message;

fn codec() -> BinanceJsonCodec {
    BinanceJsonCodec::new(BinanceJsonMapping)
}

fn decode(frame: Vec<u8>) -> Result<ParsedMessage, IngesterError> {
    codec().decode(Message::binary(frame)).unwrap().result
}

#[test]
fn parses_combined_json_streams() {
    let frame = serde_json::to_vec(&json!({
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1_700_000_000_123_i64,
            "s": "BTCUSDT",
            "t": 42,
            "p": "100.5",
            "q": "1"
        }
    }))
    .unwrap();

    let parsed = decode(frame.clone()).unwrap();

    assert_eq!(parsed.stream, "btcusdt@trade");
    assert_eq!(parsed.sequence, Some(42));
    assert_eq!(
        parsed.exchange_timestamp,
        Some(TimestampMilliseconds(1_700_000_000_123))
    );
    assert_eq!(BASE64.decode(&parsed.payload).unwrap(), frame);
}

#[test]
fn derives_a_stream_name_and_range_for_raw_depth_messages() {
    let frame = serde_json::to_vec(&json!({
        "e": "depthUpdate",
        "E": 1_700_000_000_123_i64,
        "s": "BTCUSDT",
        "U": 90,
        "u": 99,
        "pu": 89
    }))
    .unwrap();

    let parsed = decode(frame).unwrap();

    assert_eq!(parsed.stream, "btcusdt@depthUpdate");
    assert_eq!(parsed.sequence, Some(99));
    assert_eq!(parsed.first_sequence, Some(90));
    assert_eq!(parsed.previous_sequence, Some(89));
    assert_eq!(parsed.sequence_range(), Some((90, 99)));
}

#[test]
fn depth_messages_without_previous_update_id_are_accepted() {
    let frame = serde_json::to_vec(&json!({
        "e": "depthUpdate",
        "s": "BTCUSDT",
        "U": 90,
        "u": 99
    }))
    .unwrap();

    let parsed = decode(frame).unwrap();

    assert_eq!(parsed.sequence, Some(99));
    assert_eq!(parsed.first_sequence, Some(90));
    assert_eq!(parsed.previous_sequence, None);
}

#[test]
fn depth_messages_require_both_ends_of_the_update_range() {
    let frame = serde_json::to_vec(&json!({
        "e": "depthUpdate",
        "s": "BTCUSDT",
        "u": 99
    }))
    .unwrap();

    let error = decode(frame).unwrap_err();

    assert!(matches!(
        error,
        IngesterError::MissingSequence { field: "U" }
    ));
}

#[test]
fn depth_messages_reject_invalid_update_ranges() {
    let frame = serde_json::to_vec(&json!({
        "e": "depthUpdate",
        "s": "BTCUSDT",
        "U": 100,
        "u": 99
    }))
    .unwrap();

    let error = decode(frame).unwrap_err();

    assert!(matches!(
        error,
        IngesterError::InvalidSequence { field: "U", .. }
    ));
}

#[test]
fn depth_messages_reject_a_previous_update_id_inside_the_range() {
    let frame = serde_json::to_vec(&json!({
        "e": "depthUpdate",
        "s": "BTCUSDT",
        "U": 100,
        "u": 105,
        "pu": 100
    }))
    .unwrap();

    let error = decode(frame).unwrap_err();

    assert!(matches!(
        error,
        IngesterError::InvalidSequence { field: "pu", .. }
    ));
}

#[test]
fn json_events_without_a_sequence_are_accepted() {
    let frame = serde_json::to_vec(&json!({
        "stream": "btcusdt@kline_1m",
        "data": {
            "e": "kline",
            "E": 1_700_000_000_123_i64,
            "s": "BTCUSDT"
        }
    }))
    .unwrap();

    let parsed = decode(frame).unwrap();

    assert_eq!(parsed.sequence, None);
}

#[test]
fn sequenced_json_events_must_include_their_sequence() {
    let frame = serde_json::to_vec(&json!({
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "s": "BTCUSDT"
        }
    }))
    .unwrap();

    let error = decode(frame).unwrap_err();

    assert!(matches!(
        error,
        IngesterError::MissingSequence { field: "t" }
    ));
}
