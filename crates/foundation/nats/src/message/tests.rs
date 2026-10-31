use super::*;

#[test]
fn converts_async_nats_messages() {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", "market-events.1");
    let message = async_nats::Message {
        subject: "market.events.binance.btcusdt.trade".to_string().into(),
        reply: Some("_INBOX.1".to_string().into()),
        payload: b"payload".to_vec().into(),
        headers: Some(headers),
        status: async_nats::StatusCode::from_u16(404).ok(),
        description: Some("No Messages".to_string()),
        length: 7,
    };

    let message = Message::from(message);

    assert_eq!(message.subject, "market.events.binance.btcusdt.trade");
    assert_eq!(message.reply_to.as_deref(), Some("_INBOX.1"));
    assert_eq!(message.status, Some(404));
    assert_eq!(message.header("nats-msg-id"), Some("market-events.1"));
    assert_eq!(message.payload, b"payload");
    assert_eq!(message.stream, None);
    assert_eq!(message.stream_sequence, None);
    assert_eq!(message.consumer_sequence, None);
}

#[test]
fn preserves_jetstream_metadata_fields() {
    let message = Message {
        subject: "market.events.binance.btcusdt.trade".to_string(),
        stream: Some("market".to_string()),
        stream_sequence: Some(42),
        consumer_sequence: Some(7),
        ..Message::default()
    };

    assert_eq!(message.stream.as_deref(), Some("market"));
    assert_eq!(message.stream_sequence, Some(42));
    assert_eq!(message.consumer_sequence, Some(7));
}
