use crate::maintenance_executor::ArchiveMaintenanceExecutor;
use crate::{ArchiveIcebergTable, MaintenanceBlock};
use platform_errors::PlatformResult;
use platform_metrics::MarketDataMetrics;

pub const METADATA_GROWTH_ALERT_CODE: &str = "metadata_growth_exceeded";
pub const BUDGET_ALERT_CODE: &str = "maintenance_budget_exhausted";

impl ArchiveMaintenanceExecutor {
    /// Compares the metadata object counts of every archive table against the
    /// previous maintenance cycle, updates the baselines, and meters the
    /// counts.
    ///
    /// Returns the tables whose growth percentage exceeds the configured
    /// budget. The first cycle only records the baseline.
    pub(crate) async fn metadata_growth(&self) -> PlatformResult<Vec<String>> {
        let threshold = u64::from(self.config.max_metadata_growth_percent);
        let mut grown = Vec::new();
        for table in ArchiveIcebergTable::all() {
            let count = self
                .catalog
                .metadata_file_count(table, self.objects.as_ref())
                .await?;
            MarketDataMetrics::record_maintenance_metadata_files(table.name(), count);
            let previous = self
                .metadata_counts
                .lock()
                .insert(table.name().to_string(), count);
            if let Some(previous) = previous
                && previous > 0
                && count.saturating_sub(previous) * 100 / previous > threshold
            {
                grown.push(table.name().to_string());
            }
        }
        Ok(grown)
    }

    /// Pauses raw-table cleanup when metadata growth exceeded the budget.
    ///
    /// Returns the blocking outcome for the raw-deletion stage, or `None` when
    /// growth is within budget and cleanup may proceed.
    pub(crate) async fn metadata_growth_block(&self) -> PlatformResult<Option<MaintenanceBlock>> {
        let grown = self.metadata_growth().await?;
        if grown.is_empty() {
            return Ok(None);
        }
        MarketDataMetrics::record_maintenance_alert("raw_deletion", METADATA_GROWTH_ALERT_CODE);
        tracing::warn!(
            tables = %grown.join(","),
            "archive maintenance paused raw cleanup because metadata growth exceeded the budget"
        );
        Ok(Some(MaintenanceBlock::new(
            METADATA_GROWTH_ALERT_CODE,
            "metadata growth exceeded the configured percentage budget; raw cleanup is paused",
            false,
        )))
    }

    /// Reserves one maintenance commit and its bytes from the hourly budget.
    pub(crate) fn reserve_budget(&self, bytes: u64) -> bool {
        self.budget.try_reserve(Self::now(), bytes)
    }

    pub(crate) fn budget_block(
        stage: crate::MaintenanceStage,
        planned: usize,
    ) -> crate::StageOutcome {
        crate::StageOutcome::blocked(
            stage,
            planned,
            MaintenanceBlock::new(
                BUDGET_ALERT_CODE,
                "maintenance exhausted its hourly commit or byte budget",
                false,
            ),
        )
    }

    pub(crate) fn record_budget_alert(stage: &'static str) {
        MarketDataMetrics::record_maintenance_alert(stage, BUDGET_ALERT_CODE);
    }
}
