use super::*;
use crate::{ArchiveBatch, ArchiveKind};
use crate::record::{ArchiveRecord, ArchivedMarketEvent};
use platform_market_data::event::MarketEvent;
use platform_types::new_types::TimestampMilliseconds;
use std::io::Cursor;
use stock_trek_types::cex::cex_id::CexId;

fn event(sequence: u64) -> ArchiveRecord {
    ArchiveRecord::MarketEvent(ArchivedMarketEvent::new(
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(sequence),
            exchange_timestamp: Some(TimestampMilliseconds(sequence as i64)),
            received_at: TimestampMilliseconds(sequence as i64),
            payload: "payload".to_string(),
        },
        0,
    ))
}

#[tokio::test]
async fn async_reader_streams_bounded_batches() {
    let batch = ArchiveBatch::new(ArchiveKind::Events, vec![event(1), event(2), event(3)]).unwrap();
    let encoded = ArchiveParquet::encode(&batch).unwrap();
    let reader = Cursor::new(encoded);

    let mut stream = ArchiveParquetStream::with_batch_size(ArchiveKind::Events, reader, 2)
        .await
        .unwrap();

    assert_eq!(stream.next_batch().await.unwrap().unwrap().len(), 2);
    assert_eq!(stream.next_batch().await.unwrap().unwrap().len(), 1);
    assert!(stream.next_batch().await.unwrap().is_none());
}
