use crate::routing_key::RoutingKey;
use platform_market_data::MarketEvent;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArchivedMarketEvent {
    pub event: MarketEvent,
    pub state_epoch: u64,
}

impl ArchivedMarketEvent {
    pub fn new(event: MarketEvent, state_epoch: u64) -> Self {
        Self { event, state_epoch }
    }

    pub fn event(&self) -> &MarketEvent {
        &self.event
    }

    pub fn state_epoch(&self) -> u64 {
        self.state_epoch
    }

    pub fn routing_key(&self) -> RoutingKey {
        RoutingKey::for_event(&self.event)
    }
}

impl From<(MarketEvent, u64)> for ArchivedMarketEvent {
    fn from((event, state_epoch): (MarketEvent, u64)) -> Self {
        Self::new(event, state_epoch)
    }
}

impl From<MarketEvent> for ArchivedMarketEvent {
    fn from(event: MarketEvent) -> Self {
        Self::new(event, 0)
    }
}

impl Deref for ArchivedMarketEvent {
    type Target = MarketEvent;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}
