use crate::{AccountKey, CexAccountPortfolio, PortfolioUpdateMetadata};
use chrono::{DateTime, Utc};
use platform_errors::{BuildError, BuildResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use stock_trek_types::cex::asset_id::AssetId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioRecord {
    account: AccountKey,
    balances: BTreeMap<AssetId, f64>,
    version: u64,
    occurred_at: DateTime<Utc>,
    exchange_time: Option<DateTime<Utc>>,
    applied_at: DateTime<Utc>,
}

impl PortfolioRecord {
    pub fn from_parts(
        account: AccountKey,
        portfolio: &CexAccountPortfolio,
        metadata: &PortfolioUpdateMetadata,
    ) -> Self {
        Self {
            account,
            balances: portfolio
                .asset_totals()
                .map(|(asset, total)| (*asset, *total))
                .collect(),
            version: metadata.version(),
            occurred_at: metadata.occurred_at(),
            exchange_time: metadata.exchange_time(),
            applied_at: metadata.applied_at(),
        }
    }

    pub fn account(&self) -> &AccountKey {
        &self.account
    }

    pub fn metadata(&self) -> PortfolioUpdateMetadata {
        PortfolioUpdateMetadata::new(
            self.version,
            self.occurred_at,
            self.exchange_time,
            self.applied_at,
        )
    }

    pub fn into_portfolio(self) -> BuildResult<CexAccountPortfolio> {
        CexAccountPortfolio::new(self.balances, Vec::new()).map_err(BuildError::external)
    }
}
