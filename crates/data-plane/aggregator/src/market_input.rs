use platform_errors::PlatformResult;
use platform_market_data::{EVENT_CATEGORY, EnvelopeCodec, GAP_CATEGORY, GapMarker, MarketEvent};
use platform_nats::Message;
use platform_types::TimestampMilliseconds;
use serde::{Deserialize, Serialize};
use stock_trek_types::cex::cex_id::CexId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketInput {
    Event(MarketEvent),
    Gap(GapMarker),
}

impl MarketInput {
    pub fn exchange(&self) -> CexId {
        match self {
            Self::Event(event) => event.exchange,
            Self::Gap(gap) => gap.exchange,
        }
    }

    pub fn is_gap(&self) -> bool {
        matches!(self, Self::Gap(_))
    }

    pub fn encode(&self) -> PlatformResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode(payload: &[u8]) -> PlatformResult<Self> {
        Ok(serde_json::from_slice(payload)?)
    }

    pub fn from_market_message(message: &Message) -> PlatformResult<Self> {
        if Self::is_gap_subject(&message.subject) {
            Ok(Self::Gap(GapMarker::decode(&message.payload)?))
        } else {
            Ok(Self::Event(MarketEvent::decode(&message.payload)?))
        }
    }

    pub fn is_gap_subject(subject: &str) -> bool {
        Self::subject_category(subject) == Some(GAP_CATEGORY)
    }

    pub fn is_event_subject(subject: &str) -> bool {
        Self::subject_category(subject) == Some(EVENT_CATEGORY)
    }

    fn subject_category(subject: &str) -> Option<&str> {
        let segments: Vec<&str> = subject.split('.').collect();
        segments
            .windows(2)
            .rev()
            .find_map(|window| {
                let category = window[0];
                let exchange = window[1];
                (Self::is_category(category) && CexId::try_from(exchange).is_ok())
                    .then_some(category)
            })
            .or_else(|| {
                segments
                    .get(segments.len().checked_sub(3)?)
                    .copied()
                    .filter(|segment| Self::is_category(segment))
            })
    }

    fn is_category(segment: &str) -> bool {
        segment == EVENT_CATEGORY || segment == GAP_CATEGORY
    }

    pub fn timestamp(&self) -> TimestampMilliseconds {
        match self {
            Self::Event(event) => event.exchange_timestamp.unwrap_or(event.received_at),
            Self::Gap(gap) => gap.detected_at,
        }
    }
}
