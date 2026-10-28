use crate::quantized_time::{Quantization, TopOfMinute};

pub const PARQUET_SUFFIX: &str = ".parquet";

pub const UPDATE_QUANTIZATION: Quantization = Quantization::Minutes(TopOfMinute::Six);

/// Maximum number of one-minute candles an exchange will return in one request.
pub const MAX_KLINE_LIMIT: u32 = 1_000;

const _: () = assert!(
    UPDATE_QUANTIZATION.max_window_candles() <= MAX_KLINE_LIMIT,
    "UPDATE_QUANTIZATION produces windows larger than the exchange kline limit"
);

/// Number of one-minute candles needed to fill a single update window.
pub const KLINE_LIMIT: Option<u32> = Some(UPDATE_QUANTIZATION.max_window_candles());
