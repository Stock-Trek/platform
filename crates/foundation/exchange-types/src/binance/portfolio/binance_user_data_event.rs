use super::{BinanceAccountBalanceEvent, wire_event::WireEvent};
use platform_errors::{BpError, BpResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceUserDataEvent {
    event_type: String,
    event_time: i64,
    version: Option<u64>,
    balances: Option<Vec<BinanceAccountBalanceEvent>>,
}

impl BinanceUserDataEvent {
    pub fn from_value(value: &serde_json::Value) -> BpResult<Self> {
        let wire: WireEvent = serde_json::from_value(value.clone()).map_err(BpError::external)?;
        Ok(Self {
            event_type: wire.event_type,
            event_time: wire.event_time,
            version: wire.version,
            balances: wire.balances.map(|balances| {
                balances
                    .into_iter()
                    .map(|balance| {
                        BinanceAccountBalanceEvent::new(balance.asset, balance.free, balance.locked)
                    })
                    .collect()
            }),
        })
    }

    pub fn event_time(&self) -> i64 {
        self.event_time
    }

    pub fn version(&self) -> Option<u64> {
        self.version
    }

    pub fn balances(&self) -> Option<&[BinanceAccountBalanceEvent]> {
        self.balances.as_deref()
    }

    pub fn is_listen_key_expired(&self) -> bool {
        self.event_type == "listenKeyExpired"
    }

    pub fn is_account_position(&self) -> bool {
        self.event_type == "outboundAccountPosition"
    }
}
