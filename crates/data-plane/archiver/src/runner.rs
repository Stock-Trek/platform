use crate::{
    config::ArchiverConfig,
    sink::{ArchiveSink, S3ParquetSink},
    source::NatsArchiveSource,
};
use platform_archive::ArchiveBuffer;
use platform_errors::BpResult;
use platform_metrics::descriptions::{
    DROP_REASON_DUPLICATE, FLUSH_REASON_INTERVAL, FLUSH_REASON_SHUTDOWN, FLUSH_REASON_SIZE,
    STAGE_ARCHIVER,
};
use platform_metrics::market_data::MarketDataMetrics;
use std::time::Instant;
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

pub struct ArchiverRunner {
    config: ArchiverConfig,
}

impl ArchiverRunner {
    pub fn new(config: ArchiverConfig) -> Self {
        Self { config }
    }

    pub async fn run(self) -> BpResult<()> {
        let mut source = NatsArchiveSource::connect(&self.config).await?;
        let sink = S3ParquetSink::connect(&self.config).await?;
        let mut buffer = ArchiveBuffer::new(self.config.max_records, self.config.dedupe_capacity);
        let mut flush_ticker = time::interval(self.config.flush_interval);
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!(
            event_subject_prefix = %self.config.event_subject_prefix,
            closed_candle_subject_prefix = %self.config.closed_candle_subject_prefix,
            s3_bucket = %self.config.s3_bucket,
            s3_prefix = %self.config.s3_prefix,
            "archiver started"
        );

        let result = self
            .run_until_shutdown(&mut source, &sink, &mut buffer, &mut flush_ticker)
            .await;
        let flush_result = self.flush(&sink, &mut buffer, FLUSH_REASON_SHUTDOWN).await;
        result?;
        flush_result
    }

    async fn run_until_shutdown(
        &self,
        source: &mut NatsArchiveSource,
        sink: &S3ParquetSink,
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
                                self.flush(sink, buffer, FLUSH_REASON_SIZE).await?;
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
                    self.flush(sink, buffer, FLUSH_REASON_INTERVAL).await?;
                }
                signal = platform_runtime::Runtime::shutdown_signal() => {
                    signal?;
                    break;
                }
            }
        }
        Ok(())
    }

    async fn flush(
        &self,
        sink: &S3ParquetSink,
        buffer: &mut ArchiveBuffer,
        reason: &'static str,
    ) -> BpResult<()> {
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
