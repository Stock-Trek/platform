use super::codec::BinanceSbeCodec;
use super::frame::{
    BINANCE_BEST_BID_ASK_TEMPLATE_ID, BINANCE_DEPTH_DIFF_TEMPLATE_ID,
    BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID, BINANCE_SBE_SCHEMA_ID, BINANCE_SBE_VERSION,
    BINANCE_TRADES_TEMPLATE_ID,
};
use iris::Codec as _;
use tokio_websockets::Message;

pub(super) struct SbeFrames;

impl SbeFrames {
    pub(super) fn sbe_header(template_id: u16) -> Vec<u8> {
        let block_length = match template_id {
            BINANCE_BEST_BID_ASK_TEMPLATE_ID => 50,
            BINANCE_DEPTH_DIFF_TEMPLATE_ID => 26,
            BINANCE_TRADES_TEMPLATE_ID | BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID => 18,
            _ => 18,
        };
        Self::sbe_header_with(
            template_id,
            block_length,
            BINANCE_SBE_SCHEMA_ID,
            BINANCE_SBE_VERSION,
        )
    }

    pub(super) fn sbe_header_with(
        template_id: u16,
        block_length: u16,
        schema_id: u16,
        version: u16,
    ) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&block_length.to_le_bytes());
        frame.extend_from_slice(&template_id.to_le_bytes());
        frame.extend_from_slice(&schema_id.to_le_bytes());
        frame.extend_from_slice(&version.to_le_bytes());
        frame
    }

    pub(super) fn append_var_string(frame: &mut Vec<u8>, value: &str) {
        frame.push(value.len() as u8);
        frame.extend_from_slice(value.as_bytes());
    }

    pub(super) fn sbe_trades_frame() -> Vec<u8> {
        let event_time_us = 1_700_000_000_123_000_i64;
        let mut frame = Self::sbe_header(BINANCE_TRADES_TEMPLATE_ID);
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
        Self::append_var_string(&mut frame, "btcusdt");
        frame
    }

    pub(super) fn sbe_best_bid_ask_frame() -> Vec<u8> {
        let mut frame = Self::sbe_header(BINANCE_BEST_BID_ASK_TEMPLATE_ID);
        frame.extend_from_slice(&1_700_000_000_123_000_i64.to_le_bytes());
        frame.extend_from_slice(&99_i64.to_le_bytes());
        frame.push((-2_i8) as u8);
        frame.push((-3_i8) as u8);
        frame.extend_from_slice(&10_025_i64.to_le_bytes());
        frame.extend_from_slice(&2_500_i64.to_le_bytes());
        frame.extend_from_slice(&10_050_i64.to_le_bytes());
        frame.extend_from_slice(&1_000_i64.to_le_bytes());
        Self::append_var_string(&mut frame, "btcusdt");
        frame
    }

    pub(super) fn sbe_depth_snapshot_frame() -> Vec<u8> {
        let mut frame = Self::sbe_header(BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID);
        frame.extend_from_slice(&1_700_000_000_123_000_i64.to_le_bytes());
        frame.extend_from_slice(&456_i64.to_le_bytes());
        frame.push(0);
        frame.push(0);
        for _ in 0..2 {
            frame.extend_from_slice(&16u16.to_le_bytes());
            frame.extend_from_slice(&1u16.to_le_bytes());
            frame.extend_from_slice(&[0u8; 16]);
        }
        Self::append_var_string(&mut frame, "btcusdt");
        frame
    }

    pub(super) fn sbe_depth_diff_frame() -> Vec<u8> {
        let mut frame = Self::sbe_header(BINANCE_DEPTH_DIFF_TEMPLATE_ID);
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
        Self::append_var_string(&mut frame, "btcusdt");
        frame
    }
}

#[test]
fn rejects_unknown_sbe_schema_ids() {
    let frame = SbeFrames::sbe_header_with(
        BINANCE_TRADES_TEMPLATE_ID,
        18,
        BINANCE_SBE_SCHEMA_ID + 1,
        BINANCE_SBE_VERSION,
    );

    let error = BinanceSbeCodec
        .decode(Message::binary(frame))
        .unwrap()
        .result;

    assert!(matches!(
        error,
        Err(platform_errors::IngesterError::MalformedSbeFrame { .. })
    ));
}

#[test]
fn rejects_unsupported_sbe_versions() {
    let frame = SbeFrames::sbe_header_with(
        BINANCE_TRADES_TEMPLATE_ID,
        18,
        BINANCE_SBE_SCHEMA_ID,
        BINANCE_SBE_VERSION + 1,
    );

    let error = BinanceSbeCodec
        .decode(Message::binary(frame))
        .unwrap()
        .result;

    assert!(matches!(
        error,
        Err(platform_errors::IngesterError::MalformedSbeFrame { .. })
    ));
}

#[test]
fn rejects_mismatched_sbe_block_lengths() {
    let frame = SbeFrames::sbe_header_with(
        BINANCE_BEST_BID_ASK_TEMPLATE_ID,
        18,
        BINANCE_SBE_SCHEMA_ID,
        BINANCE_SBE_VERSION,
    );

    let error = BinanceSbeCodec
        .decode(Message::binary(frame))
        .unwrap()
        .result;

    assert!(matches!(
        error,
        Err(platform_errors::IngesterError::MalformedSbeFrame { .. })
    ));
}

#[test]
fn rejects_truncated_sbe_headers() {
    let error = BinanceSbeCodec
        .decode(Message::binary(vec![0; 7]))
        .unwrap()
        .result;

    assert!(matches!(
        error,
        Err(platform_errors::IngesterError::MalformedSbeFrame { .. })
    ));
}

#[test]
fn identifies_only_matching_sbe_headers() {
    assert!(BinanceSbeCodec::is_sbe_frame(&SbeFrames::sbe_trades_frame()));
    assert!(BinanceSbeCodec::is_sbe_frame(
        &SbeFrames::sbe_best_bid_ask_frame()
    ));
    assert!(BinanceSbeCodec::is_sbe_frame(
        &SbeFrames::sbe_depth_snapshot_frame()
    ));
    assert!(BinanceSbeCodec::is_sbe_frame(
        &SbeFrames::sbe_depth_diff_frame()
    ));
    assert!(!BinanceSbeCodec::is_sbe_frame(b"{}"));
    assert!(!BinanceSbeCodec::is_sbe_frame(&SbeFrames::sbe_header(1)));

    let mut frame = SbeFrames::sbe_header(BINANCE_TRADES_TEMPLATE_ID);
    frame[4..6].copy_from_slice(&(BINANCE_SBE_SCHEMA_ID + 1).to_le_bytes());
    assert!(!BinanceSbeCodec::is_sbe_frame(&frame));
}
