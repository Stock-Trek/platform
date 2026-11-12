use crate::read_model::{OwnershipRecord, PortfolioReadModel, PortfolioRecord};
use platform_errors::{BuildError, BuildResult};
use platform_nats::KeyValue;

const OWNERSHIP_PREFIX: &str = "read_model.ownership";
const PORTFOLIO_PREFIX: &str = "read_model.portfolio";

pub struct PortfolioReadModelStore {
    values: KeyValue,
}

impl PortfolioReadModelStore {
    pub async fn connect(url: &str, name: &str, bucket: &str) -> BuildResult<Self> {
        Ok(Self {
            values: KeyValue::connect(url, name, bucket).await?,
        })
    }

    pub async fn load(&self) -> BuildResult<PortfolioReadModel> {
        let mut ownership = Vec::new();
        let mut portfolios = Vec::new();
        for key in self.values.keys().await? {
            let Some(payload) = self.values.get_last(&key).await? else {
                continue;
            };
            if key.starts_with(OWNERSHIP_PREFIX) {
                ownership.push(
                    serde_json::from_slice::<OwnershipRecord>(&payload)
                        .map_err(BuildError::external)?,
                );
            } else if key.starts_with(PORTFOLIO_PREFIX) {
                portfolios.push(
                    serde_json::from_slice::<PortfolioRecord>(&payload)
                        .map_err(BuildError::external)?,
                );
            }
        }
        Ok(PortfolioReadModel::new(ownership, portfolios))
    }

    pub async fn save_ownership(&self, record: &OwnershipRecord) -> BuildResult<()> {
        let payload = serde_json::to_vec(record).map_err(BuildError::external)?;
        self.values
            .put(&Self::key(OWNERSHIP_PREFIX, record.account()), &payload)
            .await?;
        Ok(())
    }

    pub async fn remove_ownership(&self, account: &crate::AccountKey) -> BuildResult<()> {
        Self::remove(&self.values, &Self::key(OWNERSHIP_PREFIX, account)).await
    }

    pub async fn save_portfolio(&self, record: &PortfolioRecord) -> BuildResult<()> {
        let payload = serde_json::to_vec(record).map_err(BuildError::external)?;
        self.values
            .put(&Self::key(PORTFOLIO_PREFIX, record.account()), &payload)
            .await?;
        Ok(())
    }

    pub async fn remove_portfolio(&self, account: &crate::AccountKey) -> BuildResult<()> {
        Self::remove(&self.values, &Self::key(PORTFOLIO_PREFIX, account)).await
    }

    async fn remove(values: &KeyValue, key: &str) -> BuildResult<()> {
        if values.get_last(key).await?.is_some() {
            values.delete(key).await?;
        }
        Ok(())
    }

    fn key(prefix: &str, account: &crate::AccountKey) -> String {
        format!("{prefix}.{}", account.storage_key())
    }
}
