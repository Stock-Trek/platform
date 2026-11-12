use platform_errors::{BuildError, BuildResult};
use serde::{Deserialize, Serialize};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AccountKey {
    cex_id: CexId,
    account_id: AccountId,
}

impl AccountKey {
    pub fn new(cex_id: CexId, account_id: impl AsRef<str>) -> Self {
        Self {
            cex_id,
            account_id: AccountId::new(account_id.as_ref()),
        }
    }

    pub fn from_account_id(cex_id: CexId, account_id: AccountId) -> Self {
        Self { cex_id, account_id }
    }

    pub fn parse(cex_id: &str, account_id: &str) -> BuildResult<Self> {
        if account_id.trim().is_empty() {
            return Err(BuildError::bad_value(
                "account id",
                account_id,
                "must not be empty",
            ));
        }
        let cex_id = cex_id
            .parse::<CexId>()
            .map_err(|_| BuildError::bad_value("cex id", cex_id, "must be a supported exchange"))?;
        Ok(Self::new(cex_id, account_id))
    }

    pub fn cex_id(&self) -> CexId {
        self.cex_id
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn into_parts(self) -> (CexId, AccountId) {
        (self.cex_id, self.account_id)
    }

    pub fn storage_key(&self) -> String {
        format!("{}:{}", self.cex_id, self.account_id.0)
    }
}

impl From<(CexId, AccountId)> for AccountKey {
    fn from((cex_id, account_id): (CexId, AccountId)) -> Self {
        Self::from_account_id(cex_id, account_id)
    }
}
