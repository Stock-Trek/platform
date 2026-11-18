use crate::accounts::account_types::{AccountSummary, LinkAccountRequest};
use crate::accounts::portfolio_types::{OrderType, PendingOrder, PortfolioSnapshot};
use chrono::{TimeZone, Utc};
use serde_json::json;
use stock_trek_types::cex::{
    asset_id::AssetId, cex_id::CexId, side::Side, tag::Tag, trading_pair::TradingPair,
};

struct Fixture;

impl Fixture {
    fn at() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 14, 9, 0, 0).unwrap()
    }

    fn link_request(cex_id: &str) -> Result<LinkAccountRequest, serde_json::Error> {
        serde_json::from_value(json!({
            "cexId": cex_id,
            "label": "main",
            "apiKey": "0123456789abcdef",
            "apiSecret": "fedcba9876543210"
        }))
    }

    fn pending_order() -> PendingOrder {
        PendingOrder {
            tag: Tag::new("order_9x2"),
            symbol: TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            side: Side::Sell,
            order_type: OrderType::Limit,
            quantity: "0.100000000".to_string(),
            price: "104250.000000000".to_string(),
            placed_at: Self::at(),
        }
    }

    fn account_summary() -> AccountSummary {
        AccountSummary {
            account_id: "account_3kq".to_string(),
            cex_id: CexId::Binance,
            label: "main".to_string(),
            active: true,
            connector: crate::accounts::Connector::connected(Self::at()),
            version: 1,
            updated_at: Self::at(),
        }
    }
}

#[test]
fn link_requests_accept_stock_trek_exchange_names() {
    let request = Fixture::link_request("Binance").expect("decode");
    assert_eq!(request.cex_id, CexId::Binance);
}

#[test]
fn link_requests_reject_unknown_exchanges() {
    assert!(Fixture::link_request("binance").is_err());
    assert!(Fixture::link_request("robinhood").is_err());
}

#[test]
fn a_pending_snapshot_serializes_with_the_exchange_name() {
    let snapshot = PortfolioSnapshot::pending("account_3kq", CexId::Binance);
    let encoded = serde_json::to_value(&snapshot).expect("encode");
    assert_eq!(encoded["accountId"], "account_3kq");
    assert_eq!(encoded["cexId"], "Binance");
    assert!(encoded["asOf"].is_null());
    assert!(encoded["assets"].as_array().expect("array").is_empty());
    assert!(
        encoded["pendingOrders"]
            .as_array()
            .expect("array")
            .is_empty()
    );
}

#[test]
fn pending_orders_serialize_stock_trek_types() {
    let encoded = serde_json::to_value(Fixture::pending_order()).expect("encode");
    assert_eq!(encoded["tag"], "order_9x2");
    assert_eq!(encoded["symbol"]["base"], "Bitcoin");
    assert_eq!(encoded["symbol"]["quote"], "TetherUSD");
    assert_eq!(encoded["side"], "Sell");
    assert_eq!(encoded["type"], "limit");
}

#[test]
fn account_summaries_carry_the_stock_trek_exchange() {
    let encoded = serde_json::to_value(Fixture::account_summary()).expect("encode");
    assert_eq!(encoded["cexId"], "Binance");
}
