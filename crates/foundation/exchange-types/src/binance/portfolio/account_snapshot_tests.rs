use crate::{
    binance::{
        account_status::BinanceAccountStatusResponse,
        exchange::BinanceExchange,
        response::{BinanceMetadata, BinanceResponse, BinanceResponsePayload},
    },
    exchange_account_snapshot::{AccountSnapshotContext, ETExchangeAccountSnapshot},
    portfolio::AccountEvent,
};
use chrono::{TimeZone, Utc};
use serde_json::json;
use std::collections::HashMap;
use stock_trek_types::cex::asset_id::AssetId;

struct Fixture;

impl Fixture {
    fn mapping(asset: &str) -> Option<AssetId> {
        match asset {
            "BTC" => Some(AssetId::Bitcoin),
            "USDT" => Some(AssetId::TetherUSD),
            _ => None,
        }
    }

    fn response() -> BinanceResponse<BinanceAccountStatusResponse> {
        let account = serde_json::from_value::<BinanceAccountStatusResponse>(json!({
            "accountType": "SPOT",
            "balances": [
                {"asset": "BTC", "free": "1.5", "locked": "0.5"},
                {"asset": "USDT", "free": "250", "locked": "0"},
                {"asset": "UNKNOWN", "free": "10", "locked": "0"}
            ],
            "buyerCommission": 0,
            "canDeposit": true,
            "canTrade": true,
            "canWithdraw": true,
            "commissionRates": {
                "buyer": "0",
                "maker": "0",
                "seller": "0",
                "taker": "0"
            },
            "makerCommission": 0,
            "permissions": [],
            "sellerCommission": 0,
            "takerCommission": 0,
            "updateTime": 1_700_000_000_000_i64
        }))
        .expect("account");
        BinanceResponse {
            metadata: BinanceMetadata {
                retry_after: None,
                status: 200,
                usage: HashMap::new(),
                websocket_id: None,
            },
            payload: BinanceResponsePayload::Success(account),
        }
    }
}

#[test]
fn account_snapshot_maps_balances_and_timestamp() {
    let context = AccountSnapshotContext::new(&Fixture::mapping);
    let snapshot = <BinanceExchange as ETExchangeAccountSnapshot>::account_snapshot(
        Fixture::response(),
        &context,
    )
    .expect("snapshot");

    assert_eq!(snapshot.balances().get(&AssetId::Bitcoin), Some(&2.0));
    assert_eq!(snapshot.balances().get(&AssetId::TetherUSD), Some(&250.0));
    assert_eq!(snapshot.balances().len(), 2);
    assert_eq!(
        snapshot.occurred_at(),
        Utc.timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("timestamp")
    );
}

#[test]
fn account_event_maps_position_balances() {
    let context = AccountSnapshotContext::new(&Fixture::mapping);
    let event = <BinanceExchange as ETExchangeAccountSnapshot>::account_event(
        &json!({
            "e": "outboundAccountPosition",
            "E": 1_700_000_000_000_i64,
            "u": 42,
            "B": [
                {"a": "BTC", "f": "1.5", "l": "0.5"},
                {"a": "USDT", "f": "250", "l": "0"}
            ]
        }),
        &context,
    )
    .expect("event")
    .expect("position");

    let AccountEvent::Position(event) = event else {
        panic!("expected position");
    };
    assert_eq!(event.version(), 42);
    assert_eq!(event.balances().get(&AssetId::Bitcoin), Some(&2.0));
    assert_eq!(event.balances().get(&AssetId::TetherUSD), Some(&250.0));
}
