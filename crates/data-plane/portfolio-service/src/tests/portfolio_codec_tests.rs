use crate::{AccountKey, AccountPortfolio, PORTFOLIO_UPDATE_SCHEMA_VERSION, PortfolioUpdate};
use chrono::{TimeZone, Utc};
use std::collections::BTreeMap;
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};

fn update(version: u64) -> PortfolioUpdate {
    let portfolio = AccountPortfolio::new(
        BTreeMap::from([(AssetId::Bitcoin, 1.25), (AssetId::TetherUSD, 100.0)]),
        Vec::new(),
    )
    .expect("portfolio");
    PortfolioUpdate::versioned(
        AccountKey::from((CexId::Binance, AccountId::new("primary"))),
        Some("alice".to_string()),
        portfolio,
        version,
        Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap(),
    )
    .with_exchange_time(Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 4).unwrap())
}

#[test]
fn round_trips_a_versioned_update() {
    let original = update(7);
    let decoded =
        PortfolioUpdate::from_bytes(&original.to_bytes().expect("encode")).expect("decode");
    assert_eq!(decoded.account(), original.account());
    assert_eq!(decoded.owner(), Some("alice"));
    assert_eq!(decoded.version(), 7);
    assert_eq!(decoded.occurred_at(), original.occurred_at());
    assert_eq!(decoded.exchange_time(), original.exchange_time());
    assert_eq!(decoded.portfolio().asset_total(AssetId::Bitcoin), 1.25);
    assert_eq!(decoded.portfolio().asset_total(AssetId::TetherUSD), 100.0);
}

#[test]
fn rejects_unknown_schema_versions() {
    let original = update(1);
    let mut value: serde_json::Value =
        serde_json::from_slice(&original.to_bytes().expect("encode")).expect("json");
    value["schema_version"] = serde_json::json!(PORTFOLIO_UPDATE_SCHEMA_VERSION + 1);
    let payload = serde_json::to_vec(&value).expect("json");
    assert!(PortfolioUpdate::from_bytes(&payload).is_err());
}

#[test]
fn rejects_malformed_payloads() {
    assert!(PortfolioUpdate::from_bytes(b"not json").is_err());
}
