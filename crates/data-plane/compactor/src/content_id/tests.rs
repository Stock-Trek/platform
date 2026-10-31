use super::*;
use platform_archive::ArchiveRecord;
use platform_market_data::event::MarketEvent;
use platform_types::new_types::TimestampMilliseconds;
use stock_trek_types::cex::cex_id::CexId;

fn event(payload: &str) -> ArchiveRecord {
    ArchiveRecord::MarketEvent(
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btc_usdt@trade".to_string(),
            sequence: Some(1),
            exchange_timestamp: Some(TimestampMilliseconds(1_000)),
            received_at: TimestampMilliseconds(1_000),
            payload: payload.to_string(),
        }
        .into(),
    )
}

fn content_id(record: &ArchiveRecord) -> u64 {
    let mut content_id = ContentId::new();
    content_id.update(record).unwrap();
    content_id.value()
}

#[test]
fn content_ids_are_stable_and_differ_for_different_records() {
    let first = content_id(&event("payload"));
    let second = content_id(&event("payload"));
    let different = content_id(&event("other payload"));

    assert_eq!(first, second);
    assert_ne!(first, different);
    assert_ne!(first, ContentId::new().value());
}
