//! Deterministic candle rebuild from archived market events.
//!
//! The rebuilder reads compacted event partitions plus the raw tail that has
//! not been compacted yet, streaming one bounded Parquet batch at a time.
//! Events are staged per decoded trading pair in a bounded external sort and
//! replayed through the aggregator's pure candle processor, so all streams for
//! a pair update the same candles and the same archived input always produces
//! the same candle revisions. Regenerated candles are themselves spilled in
//! the sink's object order and uploaded one object file at a time, so neither
//! the event stream nor the candle output is ever fully materialized in
//! memory. Output can be written to either the raw or the compacted layer.

pub mod config;
pub mod events;
pub mod rebuild;
pub mod sink;

use crate::{
    config::RebuildConfig,
    events::IcebergEventReader,
    rebuild::{RebuildOptions, RebuildStats, candle_spill::CandleSpill, replay_spill::ReplaySpill},
    sink::{CandleRecordSource, CandleSink, S3CandleSink},
};
use platform_aggregator::processor::ProcessorConfig;
use platform_archive::IcebergCatalog;
use platform_errors::{BpError, BpResult};
use std::{collections::HashMap, sync::Arc};
use tracing::info;

pub use crate::config::RebuildSourceLayer;

/// Streams closed candles from the rebuild spill to the sink in object order,
/// counting how many unique revisions reach the sink.
struct RebuildCandles {
    records: crate::rebuild::candle_spill::CandleRecords,
    emitted: usize,
}

impl RebuildCandles {
    fn new(records: crate::rebuild::candle_spill::CandleRecords) -> Self {
        Self {
            records,
            emitted: 0,
        }
    }

    fn emitted(&self) -> usize {
        self.emitted
    }
}

impl CandleRecordSource for RebuildCandles {
    fn next_candle(&mut self) -> BpResult<Option<platform_market_data::candle::ClosedCandle>> {
        let candle = self.records.next_candle()?;
        if candle.is_some() {
            self.emitted += 1;
        }
        Ok(candle)
    }
}

impl RebuildConfig {
    /// Runs one rebuild from the configured Iceberg source to the output
    /// archive.
    ///
    /// Events are streamed, staged, and replayed through bounded spill runs;
    /// candles are streamed to the sink as one archive object per partition
    /// group. The final partial candle is closed deterministically when the
    /// input ends, because rebuilds normally run over a finite time range.
    pub async fn run(self) -> BpResult<()> {
        let catalog = Arc::new(IcebergCatalog::connect(&self.iceberg).await?);
        let mut reader = IcebergEventReader::connect(catalog.as_ref(), self.source_layer).await?;
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

        // Stage: decode archived events and spill one replay record per
        // decoded trading pair. Nothing accumulates beyond `spill_records`.
        let work_dir = tempfile::tempdir().map_err(|error| {
            BpError::io(
                "create rebuild temporary directory",
                std::env::temp_dir().display().to_string(),
                error,
            )
        })?;
        let mut stats = RebuildStats::default();
        let mut decoders = HashMap::new();
        let mut replay = ReplaySpill::new(work_dir.path().join("replay"), self.spill_records)?;
        while let Some(batch) = reader.next_batch().await? {
            for record in batch.into_records() {
                let event = match record {
                    platform_archive::ArchiveRecord::MarketEvent(archived) => archived.event,
                    platform_archive::ArchiveRecord::ClosedCandle(_) => continue,
                };
                options.push_event(event, &mut decoders, &mut replay, &mut stats)?;
            }
        }
        stats.events_spilled = replay.spilled_records();

        // Replay: consume the merged event stream one pair at a time and spill
        // every closed candle in the sink's object order.
        let mut candles = CandleSpill::new(
            work_dir.path().join("candles"),
            self.bucket_millis,
            self.output_layer,
            self.spill_records,
        )?;
        options.replay(replay.finish()?, &mut candles, &mut stats)?;
        stats.candles_spilled = candles.spilled_records();
        let mut source = RebuildCandles::new(candles.finish()?);

        if self.dry_run {
            while source.next_candle()?.is_some() {}
            stats.unique_candles = source.emitted();
            info!(
                source_layer = reader.source_layer().label(),
                compacted_files = reader.compacted_files().len(),
                raw_tail_files = reader.raw_tail_files().len(),
                source_files = reader.file_count(),
                events = stats.events_read,
                trades = stats.trades_applied,
                decode_failures = stats.decode_failures,
                duplicate_events = stats.duplicate_events,
                late_trades = stats.late_trades,
                closed_candles = stats.closed_candles,
                unique_candles = stats.unique_candles,
                events_spilled = stats.events_spilled,
                candles_spilled = stats.candles_spilled,
                "rebuilt candles from archived events"
            );
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
        sink.write(&mut source, work_dir.path()).await?;
        stats.unique_candles = source.emitted();
        info!(
            closed_candles = stats.closed_candles,
            unique_candles = stats.unique_candles,
            "registered rebuilt candles"
        );
        Ok(())
    }
}
