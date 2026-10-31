use super::*;
use iris::Codec;
use platform_errors::IngesterError;
use tokio_websockets::Message;
use serde_json::json;

#[test]
fn envelope_mapping_extracts_the_stream() {
    let frame = serde_json::to_vec(&json!({
        "stream": "btcusdt@trade",
        "data": { "e": "trade" }
    }))
    .unwrap();
    let codec = MappedJsonCodec::new(EnvelopeJsonMapping);

    let parsed = codec
        .decode(Message::binary(frame))
        .unwrap()
        .result
        .unwrap();

    assert_eq!(parsed.stream, "btcusdt@trade");
    assert_eq!(parsed.sequence, None);
    assert_eq!(parsed.exchange_timestamp, None);
}

#[test]
fn envelope_mapping_requires_a_stream() {
    let frame = serde_json::to_vec(&json!({ "e": "trade" })).unwrap();
    let codec = MappedJsonCodec::new(EnvelopeJsonMapping);

    let error = codec.decode(Message::binary(frame)).unwrap().result;

    assert!(matches!(error, Err(IngesterError::MissingField("stream"))));
}

#[test]
fn malformed_json_is_reported_without_failing_the_codec() {
    let codec = MappedJsonCodec::new(EnvelopeJsonMapping);

    let result = codec.decode(Message::text("not json".to_string()));

    assert!(matches!(
        result.unwrap().result,
        Err(IngesterError::Json { .. })
    ));
}
