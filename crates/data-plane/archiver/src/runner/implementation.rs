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
        let flush_result =
            Self::flush(&mut source, &sink, &mut buffer, FLUSH_REASON_SHUTDOWN).await;
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
                            if buffer.would_duplicate(&record) {
                                MarketDataMetrics::record_dropped_event(
                                    STAGE_ARCHIVER,
                                    DROP_REASON_DUPLICATE,
                                );
                                source.ack(&record).await?;
                            } else {
                                buffer.push(record);
                                if buffer.should_flush() {
                                    Self::flush(source, sink, buffer, FLUSH_REASON_SIZE).await?;
                                }
                            }
                        }
                        Ok(None) => {
                            return Err(platform_errors::BpError::external(
                                "archive source ended before shutdown was requested",
                            ));
                        }
                        Err(error) => {
                            warn!(%error, "archive source stopped");
                            return Err(error);
                        }
                    }
                }
                _ = flush_ticker.tick() => {
                    Self::flush(source, sink, buffer, FLUSH_REASON_INTERVAL).await?;
                }
            }
        }
    }

    async fn flush(
        source: &mut S,
        sink: &K,
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
        for record in buffer.records() {
            source.ack(record).await?;
        }
        buffer.clear_records();
        info!(
            records,
            duplicates = buffer.duplicates(),
            "flushed archive batch"
        );
        Ok(())
    }
}
