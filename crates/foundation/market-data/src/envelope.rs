//! JSON envelope codec for messages published on the market data subjects.

use crate::{
    candle::ClosedCandle,
    event::{GapMarker, MarketEvent},
};
use platform_errors::BpResult;
use serde::{Serialize, de::DeserializeOwned};

/// Shared encode/decode methods for market data envelope payloads.
pub trait EnvelopeCodec: Serialize + DeserializeOwned + Sized {
    /// Encodes a market data message into the JSON bytes carried by NATS.
    fn encode(&self) -> BpResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Decodes the JSON payload of a market data message.
    fn decode(payload: &[u8]) -> BpResult<Self> {
        Ok(serde_json::from_slice(payload)?)
    }
}

impl<T> EnvelopeCodec for T where T: Serialize + DeserializeOwned {}

/// A typed JSON envelope for a market data message.
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope<T> {
    value: T,
}

impl<T> Envelope<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T: Serialize> Envelope<T> {
    pub fn encode(&self) -> BpResult<Vec<u8>> {
        Ok(serde_json::to_vec(&self.value)?)
    }
}

impl<T: DeserializeOwned> Envelope<T> {
    pub fn decode(payload: &[u8]) -> BpResult<Self> {
        Ok(Self::new(serde_json::from_slice(payload)?))
    }
}

pub type MarketEventEnvelope = Envelope<MarketEvent>;
pub type GapMarkerEnvelope = Envelope<GapMarker>;
pub type ClosedCandleEnvelope = Envelope<ClosedCandle>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candle::{Candle, CandleInterval, CandleKey};
    use platform_types::new_types::TimestampMilliseconds;
    use stock_trek::markets::{MarketCandle, MarketOhlcv};
    use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

    fn event() -> MarketEvent {
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(7),
            exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_000)),
            received_at: TimestampMilliseconds(1_700_000_000_001),
            payload: "eyJlIjoidHJhZGUifQ==".to_string(),
        }
    }

    fn gap() -> GapMarker {
        GapMarker {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            previous_sequence: 6,
            observed_sequence: 8,
            missing_from: 7,
            missing_to: 7,
            detected_at: TimestampMilliseconds(1_700_000_000_002),
        }
    }

    fn closed_candle() -> ClosedCandle {
        ClosedCandle {
            revision: 3,
            candle: Candle {
                key: CandleKey::new(
                    CexId::Binance,
                    TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
                    CandleInterval::from_label("1m").unwrap(),
                ),
                market_candle: MarketCandle::new(
                    60_000,
                    120_000,
                    60_000,
                    true,
                    MarketOhlcv::new(1.0, 2.0, 0.5, 1.5, 10.0, 15.0, 1.5),
                    5,
                ),
            },
        }
    }

    #[test]
    fn market_event_round_trips_through_the_envelope() {
        let expected = event();
        let encoded = Envelope::new(expected.clone()).encode().unwrap();
        let decoded = MarketEvent::decode(&encoded).unwrap();

        assert_eq!(decoded, expected);
    }

    #[test]
    fn gap_marker_round_trips_through_the_envelope() {
        let expected = gap();
        let encoded = expected.encode().unwrap();
        let decoded = GapMarker::decode(&encoded).unwrap();

        assert_eq!(decoded, expected);
    }

    #[test]
    fn closed_candle_round_trips_through_the_envelope() {
        let expected = closed_candle();
        let encoded = ClosedCandleEnvelope::new(expected.clone())
            .encode()
            .unwrap();
        let decoded = ClosedCandleEnvelope::decode(&encoded).unwrap();

        assert_eq!(decoded.encode().unwrap(), encoded);
    }

    #[test]
    fn invalid_payloads_return_an_error() {
        assert!(MarketEvent::decode(b"not json").is_err());
    }
}
