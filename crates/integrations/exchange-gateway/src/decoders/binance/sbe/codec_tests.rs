use super::codec::BinanceSbeCodec;
use super::frame_tests::SbeFrames;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use iris::{Codec, ConnectionError};
use platform_errors::IngesterError;
use platform_market_data::ParsedMessage;
use platform_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use tokio_websockets::Message;

struct Decoder;

impl Decoder {
    fn decode(frame: Vec<u8>) -> Result<ParsedMessage, IngesterError> {
        BinanceSbeCodec
            .decode(Message::binary(frame))
            .unwrap()
            .result
    }
}

#[test]
fn decodes_binance_sbe_trades() {
    let frame = SbeFrames::sbe_trades_frame();

    let parsed = Decoder::decode(frame.clone()).unwrap();

    assert_eq!(parsed.stream, "btcusdt@trade");
    assert_eq!(parsed.sequence, Some(43));
    assert_eq!(
        parsed.exchange_timestamp,
        Some(TimestampMilliseconds(1_700_000_000_123))
    );
    assert_eq!(BASE64.decode(&parsed.payload).unwrap(), frame);
}

#[test]
fn decodes_sbe_trade_entries() {
    let trades = BinanceSbeCodec
        .decode_trades(&SbeFrames::sbe_trades_frame())
        .unwrap();

    assert_eq!(trades.len(), 2);
    assert_eq!(trades[0].symbol, "btcusdt");
    assert_eq!(trades[0].event_time_us, 1_700_000_000_123_000);
    assert_eq!(
        trades[0].price,
        Decimal::from_str_exact("0.000001").unwrap()
    );
    assert_eq!(
        trades[0].quantity,
        Decimal::from_str_exact("0.00000001").unwrap()
    );
    assert_eq!(trades[1].price, trades[0].price);
    assert_eq!(trades[1].quantity, trades[0].quantity);
}

#[test]
fn non_trade_sbe_frames_have_no_trade_entries() {
    assert!(
        BinanceSbeCodec
            .decode_trades(&SbeFrames::sbe_best_bid_ask_frame())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn decodes_binance_sbe_best_bid_ask() {
    let parsed = Decoder::decode(SbeFrames::sbe_best_bid_ask_frame()).unwrap();

    assert_eq!(parsed.stream, "btcusdt@bestBidAsk");
    assert_eq!(parsed.sequence, Some(99));
    assert_eq!(
        parsed.exchange_timestamp,
        Some(TimestampMilliseconds(1_700_000_000_123))
    );
}

#[test]
fn decodes_sbe_best_bid_ask_fields() {
    let best_bid_ask = BinanceSbeCodec
        .decode_best_bid_ask(&SbeFrames::sbe_best_bid_ask_frame())
        .unwrap();

    assert_eq!(best_bid_ask.symbol, "btcusdt");
    assert_eq!(best_bid_ask.event_time_us, 1_700_000_000_123_000);
    assert_eq!(
        best_bid_ask.bid_price,
        Decimal::from_str_exact("100.25").unwrap()
    );
    assert_eq!(
        best_bid_ask.bid_quantity,
        Decimal::from_str_exact("2.5").unwrap()
    );
    assert_eq!(
        best_bid_ask.ask_price,
        Decimal::from_str_exact("100.5").unwrap()
    );
    assert_eq!(
        best_bid_ask.ask_quantity,
        Decimal::from_str_exact("1").unwrap()
    );
}

#[test]
fn decodes_binance_sbe_depth_snapshot_last_update_id() {
    let parsed = Decoder::decode(SbeFrames::sbe_depth_snapshot_frame()).unwrap();

    assert_eq!(parsed.stream, "btcusdt@depth20");
    assert_eq!(parsed.sequence, Some(456));
}

#[test]
fn decodes_binance_sbe_depth_diff_and_skips_groups() {
    let parsed = Decoder::decode(SbeFrames::sbe_depth_diff_frame()).unwrap();

    assert_eq!(parsed.stream, "btcusdt@depth");
    assert_eq!(parsed.sequence, Some(123));
}

#[test]
fn rejects_unknown_sbe_templates() {
    let error = Decoder::decode(SbeFrames::sbe_header(1)).unwrap_err();

    assert!(matches!(
        error,
        IngesterError::UnknownSbeTemplate { template_id: 1 }
    ));
}

#[test]
fn rejects_non_binary_frames() {
    let result = BinanceSbeCodec.decode(Message::text("{}".to_string()));

    assert!(matches!(result, Err(ConnectionError::BadMessagePayload)));
}

#[test]
fn encoding_requests_is_unsupported() {
    let result = BinanceSbeCodec.encode(&());

    assert!(matches!(result, Err(ConnectionError::Codec(_))));
}
