//! Deterministic candle rebuild from archived market events.
//!
//! The rebuilder reads compacted event partitions plus the raw tail that has not
//! been compacted yet. Events are grouped by exchange trading pair and replayed
//! through the aggregator's pure candle processor, so all streams for a pair
//! update the same candles and the same archived input always produces the same
//! candle revisions. Regenerated candles can be written to either the raw or
//! the compacted candle layer.

pub mod config;
pub mod events;
pub mod rebuild;
pub mod sink;

use crate::{
    config::RebuildConfig,
    events::{EventSource, IcebergEventReader},
    rebuild::RebuildOptions,
    sink::{CandleSink, S3CandleSink},
};
use platform_aggregator::processor::ProcessorConfig;
use platform_archive::IcebergCatalog;
use platform_errors::BpResult;
use std::sync::Arc;
use tracing::info;

pub use crate::config::RebuildSourceLayer;

impl RebuildConfig {
    /// Runs one rebuild from the configured Iceberg source to the output archive.
    pub async fn run(self) -> BpResult<()> {
        let catalog = Arc::new(IcebergCatalog::connect(&self.iceberg).await?);
        let mut source = IcebergEventReader::connect(catalog.as_ref(), self.source_layer).await?;
        let events = source.read_events().await?;
        let options = RebuildOptions {
            processor: ProcessorConfig::new(
                self.intervals.clone(),
                self.allowed_lateness,
                self.max_closed_candles_per_interval.clone(),
                self.correction_cutoff,
            ),
            state_epoch: self.state_epoch,
            finish_open_windows: self.finish_open_windows,
        };
        let result = options.rebuild(events)?;

        info!(
            source_layer = source.source_layer().label(),
            compacted_files = source.compacted_files().len(),
            raw_tail_files = source.raw_tail_files().len(),
            source_files = source.file_count(),
            events = result.stats.events_read,
            trades = result.stats.trades_applied,
            decode_failures = result.stats.decode_failures,
            duplicate_events = result.stats.duplicate_events,
            late_trades = result.stats.late_trades,
            closed_candles = result.stats.closed_candles,
            unique_candles = result.stats.unique_candles,
            "rebuilt candles from archived events"
        );

        if self.dry_run {
            info!("dry run enabled; rebuilt candles were not written");
            return Ok(());
        }

        let sink = S3CandleSink::connect(
            &self.s3_bucket,
            &self.output_prefix,
            self.bucket_millis,
            catalog,
            self.output_layer,
        )
        .await?;
        sink.write(&result.candles).await?;
        Ok(())
    }
}
