use platform_market_data::envelope::EnvelopeCodec;
use platform_market_data::event::GapMarker;
use platform_nats::Message;

#[test]
fn decodes_valid_gap_markers() {
    let expected = GapMarker::gap(7, 7);
    let message = Message {
        payload: expected.encode().unwrap(),
        ..Message::default()
    };

    assert_eq!(GapMarker::decode(&message.payload).ok(), Some(expected));
}

#[test]
fn skips_malformed_gap_markers() {
    let message = Message {
        payload: b"not json".to_vec(),
        ..Message::default()
    };

    assert!(GapMarker::decode(&message.payload).is_err());
}
