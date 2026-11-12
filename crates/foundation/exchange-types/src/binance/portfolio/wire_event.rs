use super::wire_balance::WireBalance;
use serde::Deserialize;

#[allow(non_snake_case)]
#[derive(Deserialize)]
pub(super) struct WireEvent {
    #[serde(rename = "e")]
    pub(super) event_type: String,
    #[serde(rename = "E")]
    pub(super) event_time: i64,
    #[serde(rename = "u")]
    pub(super) version: Option<u64>,
    #[serde(rename = "B")]
    pub(super) balances: Option<Vec<WireBalance>>,
}
