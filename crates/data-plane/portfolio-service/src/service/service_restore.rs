use super::PortfolioService;
use crate::{
    AccountKey,
    read_model::{PortfolioReadModel, PortfolioRecord},
};
use platform_errors::BpResult;
use std::collections::BTreeSet;
use stock_trek_types::cex::cex_id::CexId;

impl PortfolioService {
    pub fn restore(&self, model: PortfolioReadModel) -> BpResult<()> {
        let (ownership, portfolios) = model.into_parts();
        {
            let mut state = self.state.write();
            let mut registry = self.ownership.write();
            for record in ownership {
                state.track_for_owner(record.owner(), record.account());
                registry.restore(record.account(), record.owner(), record.version());
            }
        }
        for record in portfolios {
            if !self.state.read().is_tracked(record.account()) {
                continue;
            }
            let account = record.account().clone();
            let metadata = record.metadata();
            let portfolio = record.into_portfolio()?;
            self.state
                .write()
                .restore_portfolio(account, portfolio, metadata);
        }
        Ok(())
    }

    pub async fn sync_feed(&self) -> BpResult<()> {
        let cex_ids = self
            .state
            .read()
            .tracked_accounts()
            .into_iter()
            .map(|account| account.cex_id())
            .collect::<BTreeSet<CexId>>();
        for cex_id in cex_ids {
            let accounts = self.state.read().accounts_for_cex(cex_id);
            self.feed.set_accounts(cex_id, accounts).await?;
        }
        Ok(())
    }

    pub fn portfolio_record(&self, account: &AccountKey) -> Option<PortfolioRecord> {
        Some(PortfolioRecord::from_parts(
            account.clone(),
            &self.account_portfolio(account)?,
            &self.update_metadata(account)?,
        ))
    }
}
