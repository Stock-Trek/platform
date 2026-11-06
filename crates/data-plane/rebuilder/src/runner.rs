use crate::{
    config::RebuildConfig,
    events::IcebergEventReader,
    rebuild::{
        RebuildOptions, RebuildStats, SpillUsage, candle_spill::CandleSpill,
        replay_spill::ReplaySpill,
    },
    sink::{CandleRecordSource, CandleSink, S3CandleSink},
};
use platform_aggregator::processor::ProcessorConfig;
use platform_archive::IcebergCatalog;
use platform_errors::{BpError, BpResult};
use std::{collections::HashMap, path::Path, sync::Arc};
use tracing::info;

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
    fn work_dir(spill_dir: &Path) -> BpResult<tempfile::TempDir> {
        std::fs::create_dir_all(spill_dir).map_err(|error| {
            BpError::io(
                "create rebuild spill directory",
                spill_dir.display().to_string(),
                error,
            )
        })?;
        tempfile::TempDir::new_in(spill_dir).map_err(|error| {
            BpError::io(
                "create rebuild temporary directory",
                spill_dir.display().to_string(),
                error,
            )
        })
    }

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
        let work_dir = Self::work_dir(&self.spill_dir)?;
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
        let mut spill_usage = SpillUsage::default();

        // Replay: consume the merged event stream one pair at a time and spill
        // every closed candle in the sink's object order.
        let events = replay.finish()?;
        stats.events_spilled = replay.spilled_records();
        spill_usage.add(replay.usage());
        let mut candles = CandleSpill::new(
            work_dir.path().join("candles"),
            self.bucket_millis,
            self.output_layer,
            self.spill_records,
        )?;
        options.replay(events, &mut candles, &mut stats)?;
        let records = candles.finish()?;
        stats.candles_spilled = candles.spilled_records();
        spill_usage.add(candles.usage());
        stats.spill_bytes_written = spill_usage.bytes_written;
        stats.largest_spill_run_bytes = spill_usage.largest_run_bytes;
        let mut source = RebuildCandles::new(records);
        info!(
            events_spilled = stats.events_spilled,
            candles_spilled = stats.candles_spilled,
            spill_bytes_written = stats.spill_bytes_written,
            largest_spill_run_bytes = stats.largest_spill_run_bytes,
            "staged rebuild spill runs"
        );

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
