use crate::{config::ArchiverConfig, sink::ArchiveSink, source::ArchiveSource};
use platform_archive::ArchiveBuffer;
use platform_errors::BpResult;
use platform_metrics::descriptions::{
    DROP_REASON_DUPLICATE, FLUSH_REASON_INTERVAL, FLUSH_REASON_SHUTDOWN, FLUSH_REASON_SIZE,
    STAGE_ARCHIVER,
};
use platform_metrics::market_data::MarketDataMetrics;
use platform_runtime::Runtime;
use std::time::Instant;
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

pub struct ArchiverRunner<S, K> {
    config: ArchiverConfig,
    source: S,
    sink: K,
}

impl<S, K> ArchiverRunner<S, K>
where
    S: ArchiveSource,
    K: ArchiveSink,
{
    pub fn new(config: ArchiverConfig, source: S, sink: K) -> Self {
        Self {
            config,
            source,
            sink,
        }
    }

    pub async fn run(self) -> BpResult<()> {
        let Self {
            config,
            mut source,
            sink,
        } = self;
        let mut buffer = ArchiveBuffer::new(config.max_records, config.dedupe_capacity);
        let mut flush_ticker = time::interval(config.flush_interval);
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!(
            event_subject_prefix = %config.event_subject_prefix,
            closed_candle_subject_prefix = %config.closed_candle_subject_prefix,
            s3_bucket = %config.s3_bucket,
            s3_prefix = %config.s3_prefix,
            "archiver started"
        );

        let result = Runtime::run_until_shutdown(Self::process_records(
            &mut source,
            &sink,
            &mut buffer,
            &mut flush_ticker,
        ))
        .await;
        let flush_result = Self::flush(&sink, &mut buffer, FLUSH_REASON_SHUTDOWN).await;
        result?;
        flush_result
    }

    async fn process_records(
        source: &mut S,
        sink: &K,
        buffer: &mut ArchiveBuffer,
        flush_ticker: &mut time::Interval,
    ) -> BpResult<()> {
        loop {
            tokio::select! {
                record = source.next_record() => {
                    match record {
                        Ok(Some(record)) => {
                            if !buffer.push(record) {
                                MarketDataMetrics::record_dropped_event(
                                    STAGE_ARCHIVER,
                                    DROP_REASON_DUPLICATE,
                                );
                            }
                            if buffer.should_flush() {
                                Self::flush(sink, buffer, FLUSH_REASON_SIZE).await?;
                            }
                        }
                        Ok(None) => {
                            info!("archive source ended");
                            break;
                        }
                        Err(error) => {
                            warn!(%error, "archive source stopped");
                            break;
                        }
                    }
                }
                _ = flush_ticker.tick() => {
                    Self::flush(sink, buffer, FLUSH_REASON_INTERVAL).await?;
                }
            }
        }
        Ok(())
    }

    async fn flush(sink: &K, buffer: &mut ArchiveBuffer, reason: &'static str) -> BpResult<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        let records = buffer.len();
        let started_at = Instant::now();
        let result = sink.write(buffer.records_mut()).await;
        MarketDataMetrics::record_archive_flush(
            reason,
            records,
            result.is_ok(),
            started_at.elapsed(),
        );
        result?;
        buffer.clear_records();
        info!(
            records,
            duplicates = buffer.duplicates(),
            "flushed archive batch"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use platform_archive::ArchiveRecord;
    use platform_market_data::event::MarketEvent;
    use platform_types::new_types::TimestampMilliseconds;
    use std::sync::{Arc, Mutex};
    use stock_trek_types::cex::cex_id::CexId;

    #[derive(Default)]
    struct FakeSource {
        records: Vec<ArchiveRecord>,
    }

    #[async_trait]
    impl ArchiveSource for FakeSource {
        async fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>> {
            if self.records.is_empty() {
                Ok(None)
            } else {
                Ok(Some(self.records.remove(0)))
            }
        }
    }

    #[derive(Default, Clone)]
    struct FakeSink {
        writes: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl ArchiveSink for FakeSink {
        async fn write(&self, records: &mut [ArchiveRecord]) -> BpResult<()> {
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
        ArchiveRecord::MarketEvent(MarketEvent {
            exchange: CexId::Binance,
            stream: "btcusdt@trade".to_string(),
            sequence: Some(1),
            exchange_timestamp: Some(TimestampMilliseconds(1_000)),
            received_at: TimestampMilliseconds(1_000),
            payload: "payload".to_string(),
        })
    }

    #[tokio::test]
    async fn runs_with_injected_source_and_sink() {
        let sink = FakeSink::default();
        let observed = sink.clone();
        let source = FakeSource {
            records: vec![record()],
        };

        ArchiverRunner::new(config(), source, sink)
            .run()
            .await
            .unwrap();

        assert_eq!(*observed.writes.lock().unwrap(), 1);
    }
}
