use crate::binance::portfolio::event_parser::EventParser;
use crate::portfolio::AccountEvent;
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
}

#[test]
fn parses_position_events_into_mapped_balances() {
    let event = EventParser::parse(
        &serde_json::json!({
            "e": "outboundAccountPosition",
            "E": 1_700_000_000_000_i64,
            "u": 42,
            "B": [
                {"a": "BTC", "f": "1.5", "l": "0.5"},
                {"a": "USDT", "f": "250", "l": "0"}
            ]
        }),
        &Fixture::mapping,
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

#[test]
fn parses_listen_key_expiration_events() {
    let event = EventParser::parse(
        &serde_json::json!({"e": "listenKeyExpired", "E": 1_700_000_000_000_i64}),
        &Fixture::mapping,
    )
    .expect("event")
    .expect("event");
    assert_eq!(event, AccountEvent::SessionExpired);
}
