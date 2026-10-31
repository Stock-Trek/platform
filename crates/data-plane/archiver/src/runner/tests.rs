use super::*;
use crate::config::ArchiverConfig;
use crate::sink::ArchiveSink;
use crate::source::ArchiveSource;
use async_trait::async_trait;
use platform_archive::ArchiveRecord;
use platform_errors::BpResult;
use platform_market_data::event::MarketEvent;
use platform_types::new_types::TimestampMilliseconds;
use std::sync::{Arc, Mutex};
use stock_trek_types::cex::cex_id::CexId;

#[derive(Default)]
struct FakeSource {
    records: Vec<ArchiveRecord>,
    fail: bool,
    acked: Arc<Mutex<usize>>,
}

#[async_trait]
impl ArchiveSource for FakeSource {
    async fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>> {
        if self.fail {
            return Err(platform_errors::BpError::external("source failed"));
        }
        if self.records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.records.remove(0)))
        }
    }

    async fn ack(&mut self, _record: &ArchiveRecord) -> BpResult<()> {
        *self.acked.lock().expect("acked lock") += 1;
        Ok(())
    }
}

#[derive(Default, Clone)]
struct FakeSink {
    writes: Arc<Mutex<usize>>,
    fail: bool,
}

#[async_trait]
impl ArchiveSink for FakeSink {
    async fn write(&self, records: &mut [ArchiveRecord]) -> BpResult<()> {
        if self.fail {
            return Err(platform_errors::BpError::bad_value(
                "sink",
                "test",
                "write failed",
            ));
        }
        *self.writes.lock().expect("writes lock") += records.len();
        Ok(())
    }
}

fn config() -> ArchiverConfig {
    ArchiverConfig {
        log_filter: "info".to_string(),
        nats_url: "nats://localhost:4222".to_string(),
        event_subject_prefix: "market.events".to_string(),
        closed_candle_subject_prefix: "market.candles.closed".to_string(),
        s3_bucket: "archive".to_string(),
        s3_prefix: "raw".to_string(),
        flush_interval: std::time::Duration::from_secs(1),
        bucket_millis: 3_600_000,
        max_records: 100,
        dedupe_capacity: 100,
        iceberg: platform_archive::IcebergConfig {
            catalog_uri: "http://localhost:8181".to_string(),
            warehouse: "s3://archive/iceberg".to_string(),
            token: None,
            namespace: "market_data".to_string(),
        },
    }
}

fn record() -> ArchiveRecord {
    ArchiveRecord::MarketEvent(
        MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(1),
            exchange_timestamp: Some(TimestampMilliseconds(1_000)),
            received_at: TimestampMilliseconds(1_000),
            payload: "payload".to_string(),
        }
        .into(),
    )
}

#[tokio::test]
async fn flushes_and_acknowledges_before_reporting_the_source_end() {
    let sink = FakeSink::default();
    let observed = sink.clone();
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![record()],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    assert!(
        ArchiverRunner::new(config(), source, sink)
            .run()
            .await
            .is_err()
    );

    assert_eq!(*observed.writes.lock().unwrap(), 1);
    assert_eq!(*acked.lock().unwrap(), 1);
}

#[tokio::test]
async fn does_not_acknowledge_when_the_flush_fails() {
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![record()],
        acked: acked.clone(),
        ..FakeSource::default()
    };
    let sink = FakeSink {
        fail: true,
        ..FakeSink::default()
    };

    assert!(
        ArchiverRunner::new(config(), source, sink)
            .run()
            .await
            .is_err()
    );
    assert_eq!(*acked.lock().unwrap(), 0);
}

#[tokio::test]
async fn acknowledges_duplicate_records_without_treating_the_end_as_success() {
    let sink = FakeSink::default();
    let acked = Arc::new(Mutex::new(0));
    let source = FakeSource {
        records: vec![record(), record()],
        acked: acked.clone(),
        ..FakeSource::default()
    };

    assert!(
        ArchiverRunner::new(config(), source, sink)
            .run()
            .await
            .is_err()
    );

    assert_eq!(*acked.lock().unwrap(), 2);
}

#[tokio::test]
async fn propagates_source_error() {
    let sink = FakeSink::default();
    let source = FakeSource {
        records: Vec::new(),
        fail: true,
        ..FakeSource::default()
    };

    let error = ArchiverRunner::new(config(), source, sink)
        .run()
        .await
        .unwrap_err();

    assert_eq!(error.to_string(), "source failed");
}
