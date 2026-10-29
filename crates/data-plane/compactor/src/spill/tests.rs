use super::*;
use platform_archive::{ArchiveKind, ArchiveRecord, ArchiveSortKey};
use platform_market_data::event::MarketEvent;
use platform_types::new_types::TimestampMilliseconds;
use stock_trek_types::cex::cex_id::CexId;

fn event(stream: &str, sequence: u64, timestamp: i64, received_at: i64) -> ArchiveRecord {
    ArchiveRecord::MarketEvent(
        MarketEvent {
            exchange: CexId::Binance,
            stream: stream.to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(timestamp)),
            received_at: TimestampMilliseconds(received_at),
            payload: format!("payload-{sequence}"),
        }
        .into(),
    )
}

#[test]
fn dedupe_spill_prefers_records_from_later_runs() {
    let directory = tempfile::tempdir().unwrap();
    let mut spill = DedupeSpill::new(ArchiveKind::Events, directory.path(), 2).unwrap();

    spill.push(event("trades", 1, 1_000, 1_000)).unwrap();
    spill.push(event("trades", 2, 2_000, 2_000)).unwrap();
    spill.push(event("trades", 1, 1_500, 1_500)).unwrap();

    let mut deduped = spill.finish().unwrap();
    let first = deduped.next_record().unwrap().unwrap();
    let second = deduped.next_record().unwrap().unwrap();
    assert!(deduped.next_record().unwrap().is_none());

    let ArchiveRecord::MarketEvent(first) = first else {
        panic!("expected a market event");
    };
    let ArchiveRecord::MarketEvent(second) = second else {
        panic!("expected a market event");
    };
    assert_eq!(first.sequence, Some(1));
    assert_eq!(first.received_at, TimestampMilliseconds(1_500));
    assert_eq!(second.sequence, Some(2));
}

#[test]
fn partition_spill_merges_runs_in_partition_and_sort_order() {
    let directory = tempfile::tempdir().unwrap();
    let mut spill = PartitionSpill::new(ArchiveKind::Events, directory.path(), 1).unwrap();

    spill.push(event("b", 1, 20_000, 20_000)).unwrap();
    spill.push(event("a", 2, 30_000, 30_000)).unwrap();
    spill.push(event("a", 3, 10_000, 10_000)).unwrap();

    let mut records = spill.finish().unwrap();
    let mut partitions = Vec::new();
    let mut timestamps = Vec::new();
    while let Some(item) = records.next_item().unwrap() {
        partitions.push(item.partition);
        let ArchiveSortKey::MarketEvent { timestamp, .. } = item.sort_key else {
            panic!("expected an event sort key");
        };
        timestamps.push(timestamp);
    }

    assert_eq!(
        partitions,
        vec!["kind=events/exchange=binance/stream=a"; 2]
            .into_iter()
            .chain(std::iter::once("kind=events/exchange=binance/stream=b"))
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(timestamps, vec![10_000, 30_000, 20_000]);
}
