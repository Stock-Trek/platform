use crate::portfolio::CexAccountPortfolio;
use crate::{AccountKey, portfolio_codec::PortfolioUpdateWire};
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};
use platform_exchange_gateway::ConnectorPortfolioUpdate;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as DeError};

#[derive(Debug, Clone)]
pub struct PortfolioUpdate {
    account: AccountKey,
    owner: Option<String>,
    portfolio: CexAccountPortfolio,
    version: u64,
    occurred_at: DateTime<Utc>,
    exchange_time: Option<DateTime<Utc>>,
}

impl PortfolioUpdate {
    pub fn new(account: AccountKey, portfolio: CexAccountPortfolio) -> Self {
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
        portfolio: CexAccountPortfolio,
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

    pub fn portfolio(&self) -> &CexAccountPortfolio {
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

    pub fn into_parts(self) -> (AccountKey, CexAccountPortfolio) {
        (self.account, self.portfolio)
    }

    pub fn to_bytes(&self) -> PlatformResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(payload: &[u8]) -> PlatformResult<Self> {
        serde_json::from_slice(payload).map_err(PlatformError::external)
    }

    pub fn with_optional_exchange_time(mut self, exchange_time: Option<DateTime<Utc>>) -> Self {
        if let Some(exchange_time) = exchange_time {
            self = self.with_exchange_time(exchange_time);
        }
        self
    }
}

impl Serialize for PortfolioUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        PortfolioUpdateWire::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PortfolioUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PortfolioUpdateWire::deserialize(deserializer)?;
        wire.try_into().map_err(D::Error::custom)
    }
}

impl TryFrom<ConnectorPortfolioUpdate> for PortfolioUpdate {
    type Error = PlatformError;

    fn try_from(update: ConnectorPortfolioUpdate) -> PlatformResult<Self> {
        let portfolio = CexAccountPortfolio::new(
            update
                .balances()
                .iter()
                .map(|(asset, total)| (*asset, *total)),
            Vec::new(),
        )
        .map_err(PlatformError::external)?;
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
