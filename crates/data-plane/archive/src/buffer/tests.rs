use super::*;
use crate::ArchiveRecord;
use platform_market_data::event::MarketEvent;
use platform_types::new_types::TimestampMilliseconds;
use stock_trek_types::cex::cex_id::CexId;

fn event(sequence: u64) -> ArchiveRecord {
    ArchiveRecord::MarketEvent(
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(1_700_000_000_000)),
            received_at: TimestampMilliseconds(1_700_000_000_123),
            payload: format!("payload-{sequence}"),
        }
        .into(),
    )
}

#[test]
fn duplicate_records_are_suppressed() {
    let mut buffer = ArchiveBuffer::new(10, 10);

    assert!(buffer.push(event(1)));
    assert!(!buffer.push(event(1)));
    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer.duplicates(), 1);
}

#[test]
fn dedupe_keys_expire_but_buffered_records_remain() {
    let mut buffer = ArchiveBuffer::new(10, 1);

    assert!(buffer.push(event(1)));
    assert!(buffer.push(event(2)));
    assert_eq!(buffer.len(), 2);
    assert!(buffer.push(event(1)));
    assert_eq!(buffer.len(), 3);
}

#[test]
fn flush_is_requested_at_the_record_limit() {
    let mut buffer = ArchiveBuffer::new(2, 10);

    assert!(!buffer.should_flush());
    buffer.push(event(1));
    assert!(!buffer.should_flush());
    buffer.push(event(2));
    assert!(buffer.should_flush());
}
