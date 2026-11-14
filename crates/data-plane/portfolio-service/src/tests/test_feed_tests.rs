use crate::{AccountKey, CexAccountPortfolio, PortfolioService, PortfolioUpdate};
use platform_exchange_gateway::PortfolioFeed;
use std::sync::Arc;
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedCall {
    pub cex_id: CexId,
    pub accounts: std::collections::BTreeSet<AccountId>,
}

#[derive(Debug, Default)]
pub struct RecordingFeed {
    calls: parking_lot::Mutex<Vec<FeedCall>>,
    fail_next: std::sync::atomic::AtomicBool,
}

impl RecordingFeed {
    pub fn calls(&self) -> Vec<FeedCall> {
        self.calls.lock().clone()
    }

    pub fn fail_next(&self) {
        self.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl PortfolioFeed for RecordingFeed {
    async fn set_accounts(
        &self,
        cex_id: CexId,
        accounts: std::collections::BTreeSet<AccountId>,
    ) -> platform_errors::PlatformResult<()> {
        if self
            .fail_next
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(platform_errors::PlatformError::external(
                std::io::Error::other("feed failed"),
            ));
        }
        self.calls.lock().push(FeedCall { cex_id, accounts });
        Ok(())
    }
}

pub struct Fixture;

impl Fixture {
    pub fn account(account_id: &str) -> AccountKey {
        AccountKey::new(CexId::Binance, account_id)
    }

    pub fn portfolio(asset_id: AssetId, total: f64) -> CexAccountPortfolio {
        CexAccountPortfolio::new([(asset_id, total)], Vec::new()).expect("valid portfolio")
    }

    pub fn update(account_id: &str, total: f64) -> PortfolioUpdate {
        PortfolioUpdate::new(
            Self::account(account_id),
            Self::portfolio(AssetId::Bitcoin, total),
        )
    }

    pub fn service() -> (Arc<RecordingFeed>, PortfolioService) {
        let feed = Arc::new(RecordingFeed::default());
        let service = PortfolioService::new(feed.clone());
        (feed, service)
    }
}
