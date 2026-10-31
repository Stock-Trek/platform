//! NATS subject construction for the market data event contract.

use platform_nats::NatsSubject;
use stock_trek_types::cex::cex_id::CexId;

/// Subject category used for market events.
pub const EVENT_CATEGORY: &str = "events";
/// Subject category used for gap markers.
pub const GAP_CATEGORY: &str = "gaps";

/// Builds the event and gap subjects shared by the ingester, backfiller, and
/// their consumers.
///
/// Every subject has the form `{prefix}.{category}.{exchange}.{stream}`, where
/// the stream is normalized to a single NATS token. Keeping the layout in one
/// place prevents producers and consumers from drifting apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketEventSubject {
    prefix: String,
    exchange: CexId,
}

impl MarketEventSubject {
    pub fn new(prefix: impl Into<String>, exchange: CexId) -> Self {
        Self {
            prefix: prefix.into(),
            exchange,
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn exchange(&self) -> CexId {
        self.exchange
    }

    /// The subject for a market event from `stream`.
    pub fn event(&self, stream: &str) -> String {
        self.subject(EVENT_CATEGORY, stream)
    }

    /// The subject for a gap marker on `stream`.
    pub fn gap(&self, stream: &str) -> String {
        self.subject(GAP_CATEGORY, stream)
    }

    /// A wildcard covering every market event and gap subject under this
    /// prefix, regardless of exchange or stream.
    pub fn wildcard(&self) -> String {
        Self::prefix_wildcard(&self.prefix)
    }

    /// A wildcard covering events for this exchange.
    pub fn event_wildcard(&self) -> String {
        self.category_wildcard(EVENT_CATEGORY)
    }

    /// A wildcard covering gap markers for this exchange.
    pub fn gap_wildcard(&self) -> String {
        self.category_wildcard(GAP_CATEGORY)
    }

    /// A wildcard covering every subject under `prefix`.
    pub fn prefix_wildcard(prefix: &str) -> String {
        format!("{prefix}.>")
    }

    fn category_wildcard(&self, category: &str) -> String {
        format!("{}.{}.{}.>", self.prefix, category, self.exchange)
    }

    fn subject(&self, category: &str, stream: &str) -> String {
        format!(
            "{}.{}.{}.{}",
            self.prefix,
            category,
            self.exchange,
            NatsSubject::normalize_segment(stream)
        )
    }
}

#[cfg(test)]
mod tests;
