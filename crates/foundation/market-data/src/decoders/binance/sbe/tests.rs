use super::codec::{
    BINANCE_BEST_BID_ASK_TEMPLATE_ID, BINANCE_DEPTH_DIFF_TEMPLATE_ID,
    BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID, BINANCE_SBE_SCHEMA_ID, BINANCE_TRADES_TEMPLATE_ID,
    BinanceSbeCodec,
};
use crate::event::ParsedMessage;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use iris::{Codec, ConnectionError};
use platform_errors::IngesterError;
use platform_types::new_types::TimestampMilliseconds;
use rust_decimal::Decimal;
use tokio_websockets::Message;

fn sbe_header(template_id: u16) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&18u16.to_le_bytes());
    frame.extend_from_slice(&template_id.to_le_bytes());
    frame.extend_from_slice(&BINANCE_SBE_SCHEMA_ID.to_le_bytes());
    frame.extend_from_slice(&0u16.to_le_bytes());
    frame
}

fn append_var_string(frame: &mut Vec<u8>, value: &str) {
    frame.push(value.len() as u8);
    frame.extend_from_slice(value.as_bytes());
}

fn sbe_trades_frame() -> Vec<u8> {
    let event_time_us = 1_700_000_000_123_000_i64;
    let mut frame = sbe_header(BINANCE_TRADES_TEMPLATE_ID);
    frame.extend_from_slice(&event_time_us.to_le_bytes());
    frame.extend_from_slice(&event_time_us.to_le_bytes());
    frame.push((-8_i8) as u8);
    frame.push((-8_i8) as u8);
    frame.extend_from_slice(&25u16.to_le_bytes());
    frame.extend_from_slice(&2u32.to_le_bytes());
    for trade_id in [42_i64, 43_i64] {
        frame.extend_from_slice(&trade_id.to_le_bytes());
        frame.extend_from_slice(&100_i64.to_le_bytes());
        frame.extend_from_slice(&1_i64.to_le_bytes());
        frame.push(0);
    }
    append_var_string(&mut frame, "btcusdt");
    frame
}

fn sbe_best_bid_ask_frame() -> Vec<u8> {
    let mut frame = sbe_header(BINANCE_BEST_BID_ASK_TEMPLATE_ID);
    frame.extend_from_slice(&1_700_000_000_123_000_i64.to_le_bytes());
    frame.extend_from_slice(&99_i64.to_le_bytes());
    frame.push((-2_i8) as u8);
    frame.push((-3_i8) as u8);
    frame.extend_from_slice(&10_025_i64.to_le_bytes());
    frame.extend_from_slice(&2_500_i64.to_le_bytes());
    frame.extend_from_slice(&10_050_i64.to_le_bytes());
    frame.extend_from_slice(&1_000_i64.to_le_bytes());
    append_var_string(&mut frame, "btcusdt");
    frame
}

fn sbe_depth_snapshot_frame() -> Vec<u8> {
    let mut frame = sbe_header(BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID);
    frame.extend_from_slice(&1_700_000_000_123_000_i64.to_le_bytes());
    frame.extend_from_slice(&456_i64.to_le_bytes());
    frame.push(0);
    frame.push(0);
    for _ in 0..2 {
        frame.extend_from_slice(&16u16.to_le_bytes());
        frame.extend_from_slice(&1u16.to_le_bytes());
        frame.extend_from_slice(&[0u8; 16]);
    }
    append_var_string(&mut frame, "btcusdt");
    frame
}

fn sbe_depth_diff_frame() -> Vec<u8> {
    let mut frame = sbe_header(BINANCE_DEPTH_DIFF_TEMPLATE_ID);
    frame.extend_from_slice(&1_700_000_000_123_000_i64.to_le_bytes());
    frame.extend_from_slice(&100_i64.to_le_bytes());
    frame.extend_from_slice(&123_i64.to_le_bytes());
    frame.push(0);
    frame.push(0);
    for _ in 0..2 {
        frame.extend_from_slice(&16u16.to_le_bytes());
        frame.extend_from_slice(&1u16.to_le_bytes());
        frame.extend_from_slice(&[0u8; 16]);
    }
    append_var_string(&mut frame, "btcusdt");
    frame
}

fn decode(frame: Vec<u8>) -> Result<ParsedMessage, IngesterError> {
    BinanceSbeCodec
        .decode(Message::binary(frame))
        .unwrap()
        .result
}

#[test]
fn decodes_binance_sbe_trades() {
    let frame = sbe_trades_frame();

    let parsed = decode(frame.clone()).unwrap();

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
    let trades = BinanceSbeCodec.decode_trades(&sbe_trades_frame()).unwrap();

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
            .decode_trades(&sbe_best_bid_ask_frame())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn decodes_binance_sbe_best_bid_ask() {
    let parsed = decode(sbe_best_bid_ask_frame()).unwrap();

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
        .decode_best_bid_ask(&sbe_best_bid_ask_frame())
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
    let parsed = decode(sbe_depth_snapshot_frame()).unwrap();

    assert_eq!(parsed.stream, "btcusdt@depth20");
    assert_eq!(parsed.sequence, Some(456));
}

#[test]
fn decodes_binance_sbe_depth_diff_and_skips_groups() {
    let parsed = decode(sbe_depth_diff_frame()).unwrap();

    assert_eq!(parsed.stream, "btcusdt@depth");
    assert_eq!(parsed.sequence, Some(123));
}

#[test]
fn rejects_unknown_sbe_templates() {
    let error = decode(sbe_header(1)).unwrap_err();

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
