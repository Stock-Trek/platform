use platform_errors::{BpError, BpResult};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceAccountBalanceEvent {
    asset: String,
    free: String,
    locked: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceUserDataEvent {
    event_type: String,
    event_time: i64,
    version: Option<u64>,
    balances: Option<Vec<BinanceAccountBalanceEvent>>,
}

impl BinanceAccountBalanceEvent {
    pub fn asset(&self) -> &str {
        &self.asset
    }

    pub fn free(&self) -> &str {
        &self.free
    }

    pub fn locked(&self) -> &str {
        &self.locked
    }
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
                    .map(|balance| BinanceAccountBalanceEvent {
                        asset: balance.asset,
                        free: balance.free,
                        locked: balance.locked,
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

#[allow(non_snake_case)]
#[derive(Deserialize)]
struct WireEvent {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "u")]
    version: Option<u64>,
    #[serde(rename = "B")]
    balances: Option<Vec<WireBalance>>,
}

#[allow(non_snake_case)]
#[derive(Deserialize)]
struct WireBalance {
    #[serde(rename = "a")]
    asset: String,
    #[serde(rename = "f")]
    free: String,
    #[serde(rename = "l")]
    locked: String,
}
