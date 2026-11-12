use super::{OwnershipRecord, PortfolioReadModel, PortfolioRecord};
use platform_errors::{BpError, BpResult};
use platform_nats::KeyValue;

const OWNERSHIP_PREFIX: &str = "read_model.ownership";
const PORTFOLIO_PREFIX: &str = "read_model.portfolio";

pub struct PortfolioReadModelStore {
    values: KeyValue,
}

impl PortfolioReadModelStore {
    pub async fn connect(url: &str, name: &str, bucket: &str) -> BpResult<Self> {
        Ok(Self {
            values: KeyValue::connect(url, name, bucket).await?,
        })
    }

    pub async fn load(&self) -> BpResult<PortfolioReadModel> {
        let mut ownership = Vec::new();
        let mut portfolios = Vec::new();
        for key in self.values.keys().await? {
            let Some(payload) = self.values.get_last(&key).await? else {
                continue;
            };
            if key.starts_with(OWNERSHIP_PREFIX) {
                ownership.push(
                    serde_json::from_slice::<OwnershipRecord>(&payload)
                        .map_err(BpError::external)?,
                );
            } else if key.starts_with(PORTFOLIO_PREFIX) {
                portfolios.push(
                    serde_json::from_slice::<PortfolioRecord>(&payload)
                        .map_err(BpError::external)?,
                );
            }
        }
        Ok(PortfolioReadModel::new(ownership, portfolios))
    }

    pub async fn save_ownership(&self, record: &OwnershipRecord) -> BpResult<()> {
        let payload = serde_json::to_vec(record).map_err(BpError::external)?;
        self.values
            .put(&Self::key(OWNERSHIP_PREFIX, record.account()), &payload)
            .await?;
        Ok(())
    }

    pub async fn remove_ownership(&self, account: &crate::AccountKey) -> BpResult<()> {
        Self::remove(&self.values, &Self::key(OWNERSHIP_PREFIX, account)).await
    }

    pub async fn save_portfolio(&self, record: &PortfolioRecord) -> BpResult<()> {
        let payload = serde_json::to_vec(record).map_err(BpError::external)?;
        self.values
            .put(&Self::key(PORTFOLIO_PREFIX, record.account()), &payload)
            .await?;
        Ok(())
    }

    pub async fn remove_portfolio(&self, account: &crate::AccountKey) -> BpResult<()> {
        Self::remove(&self.values, &Self::key(PORTFOLIO_PREFIX, account)).await
    }

    async fn remove(values: &KeyValue, key: &str) -> BpResult<()> {
        if values.get_last(key).await?.is_some() {
            values.delete(key).await?;
        }
        Ok(())
    }

    fn key(prefix: &str, account: &crate::AccountKey) -> String {
        format!("{prefix}.{}", account.storage_key())
    }
}
