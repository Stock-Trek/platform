use crate::AccountKey;
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use platform_exchange_gateway::portfolio::PortfolioUpdate as GatewayPortfolioUpdate;
use std::collections::BTreeMap;
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

pub use stock_trek::PendingOrder;

pub type AccountPortfolio = stock_trek::CexAccountPortfolio;
pub type AccountPortfolios = BTreeMap<CexId, BTreeMap<AccountId, AccountPortfolio>>;

impl TryFrom<GatewayPortfolioUpdate> for PortfolioUpdate {
    type Error = BpError;

    fn try_from(update: GatewayPortfolioUpdate) -> BpResult<Self> {
        let portfolio = AccountPortfolio::new(
            update
                .balances()
                .iter()
                .map(|(asset, total)| (*asset, *total)),
            Vec::new(),
        )
        .map_err(BpError::external)?;
        Ok(Self::versioned(
            AccountKey::from((update.cex_id(), update.account_id().clone())),
            None,
            portfolio,
            update.version(),
            update.occurred_at(),
        )
        .with_optional_exchange_time(update.exchange_time()))
    }
}

pub const PORTFOLIO_UPDATE_SCHEMA_VERSION: u32 = 1;
pub const PORTFOLIO_UPDATE_STREAM_PREFIX: &str = "portfolio.updates.v1";
pub const PORTFOLIO_UPDATE_SUBJECT: &str = "portfolio.updates.v1.events";

#[derive(Debug, Clone)]
pub struct PortfolioUpdate {
    account: AccountKey,
    owner: Option<String>,
    portfolio: AccountPortfolio,
    version: u64,
    occurred_at: DateTime<Utc>,
    exchange_time: Option<DateTime<Utc>>,
}

impl PortfolioUpdate {
    pub fn new(account: AccountKey, portfolio: AccountPortfolio) -> Self {
        Self {
            account,
            owner: None,
            portfolio,
            version: 0,
            occurred_at: Utc::now(),
            exchange_time: None,
        }
    }

    pub fn versioned(
        account: AccountKey,
        owner: Option<String>,
        portfolio: AccountPortfolio,
        version: u64,
        occurred_at: DateTime<Utc>,
    ) -> Self {
        Self {
            account,
            owner,
            portfolio,
            version,
            occurred_at,
            exchange_time: None,
        }
    }

    pub fn with_exchange_time(mut self, exchange_time: DateTime<Utc>) -> Self {
        self.exchange_time = Some(exchange_time);
        self
    }

    pub fn account(&self) -> &AccountKey {
        &self.account
    }

    pub fn owner(&self) -> Option<&str> {
        self.owner.as_deref()
    }

    pub fn portfolio(&self) -> &AccountPortfolio {
        &self.portfolio
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn exchange_label(&self) -> String {
        self.account.cex_id().to_string().to_lowercase()
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    pub fn exchange_time(&self) -> Option<DateTime<Utc>> {
        self.exchange_time
    }

    pub fn into_parts(self) -> (AccountKey, AccountPortfolio) {
        (self.account, self.portfolio)
    }
}
