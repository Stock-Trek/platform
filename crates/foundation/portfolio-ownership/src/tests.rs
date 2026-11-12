use crate::{AccountKey, OwnershipAction, OwnershipEvent};
use chrono::{TimeZone, Utc};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[test]
fn account_key_round_trips_through_its_storage_form() {
    let key = AccountKey::parse(&CexId::Binance.to_string(), "primary").expect("parse");
    assert_eq!(key.cex_id(), CexId::Binance);
    assert_eq!(key.account_id(), &AccountId::new("primary"));
    assert_eq!(key.storage_key(), format!("{}:primary", CexId::Binance));
    assert_eq!(
        key.into_parts(),
        (CexId::Binance, AccountId::new("primary"))
    );
}

#[test]
fn account_key_rejects_unknown_exchanges_and_empty_accounts() {
    assert!(AccountKey::parse("kraken", "primary").is_err());
    assert!(AccountKey::parse(&CexId::Binance.to_string(), "").is_err());
    assert!(AccountKey::parse(&CexId::Binance.to_string(), " ").is_err());
}

#[test]
fn ownership_event_serializes_its_audit_fields() {
    let occurred_at = Utc.with_ymd_and_hms(2026, 11, 11, 12, 0, 0).unwrap();
    let event = OwnershipEvent::new(
        "audit-1",
        "alice",
        AccountKey::new(CexId::Binance, "primary"),
        OwnershipAction::Add,
        3,
        occurred_at,
    );
    let encoded = serde_json::to_string(&event).expect("encode");
    let decoded: OwnershipEvent = serde_json::from_str(&encoded).expect("decode");
    assert_eq!(decoded.audit_id(), "audit-1");
    assert_eq!(decoded.owner(), "alice");
    assert_eq!(decoded.account(), event.account());
    assert_eq!(decoded.action(), OwnershipAction::Add);
    assert!(decoded.action().is_add());
    assert!(!OwnershipAction::Remove.is_add());
    assert_eq!(decoded.version(), 3);
    assert_eq!(decoded.occurred_at(), occurred_at);
}
