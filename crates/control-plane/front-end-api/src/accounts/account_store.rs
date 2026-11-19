use crate::accounts::PortfolioSnapshot;
use crate::accounts::{AccountSummary, Connector, ConnectorReason};
use crate::api::Id;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use platform_exchange_types::encrypt::EncryptionAlgorithm;
use std::collections::HashMap;
use stock_trek_types::cex::cex_id::CexId;

const MIN_CREDENTIAL_LENGTH: usize = 16;
const MAX_CREDENTIAL_LENGTH: usize = 256;

pub struct Credentials {
    pub api_key: String,
    pub api_secret: String,
    #[allow(dead_code)]
    pub encryption_algorithm: EncryptionAlgorithm,
}

impl Credentials {
    pub fn new(api_key: &str, api_secret: &str, encryption_algorithm: EncryptionAlgorithm) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            encryption_algorithm,
        }
    }

    fn connector(&self, now: DateTime<Utc>) -> Connector {
        let key = self.api_key.chars().count();
        let secret = self.api_secret.chars().count();
        let key_ok = (MIN_CREDENTIAL_LENGTH..=MAX_CREDENTIAL_LENGTH).contains(&key);
        let secret_ok = (MIN_CREDENTIAL_LENGTH..=MAX_CREDENTIAL_LENGTH).contains(&secret);
        if key_ok && secret_ok {
            Connector::connected(now)
        } else {
            Connector::failed(ConnectorReason::InvalidCredentials, now)
        }
    }
}

#[derive(Debug)]
pub enum LinkConflict {
    LabelTaken,
}

struct AccountRow {
    owner: String,
    summary: AccountSummary,
    credentials: Option<Credentials>,
    linked_at: DateTime<Utc>,
}

pub struct AccountStore {
    rows: Mutex<Vec<AccountRow>>,
    snapshots: Mutex<HashMap<String, PortfolioSnapshot>>,
}

impl AccountStore {
    pub fn new() -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
            snapshots: Mutex::new(HashMap::new()),
        }
    }

    pub fn link(
        &self,
        owner: &str,
        cex_id: CexId,
        label: &str,
        credentials: Credentials,
        now: DateTime<Utc>,
    ) -> Result<AccountSummary, LinkConflict> {
        let mut rows = self.rows.lock();
        let taken = rows.iter().any(|row| {
            row.owner == owner && row.summary.cex_id == cex_id && row.summary.label == label
        });
        if taken {
            return Err(LinkConflict::LabelTaken);
        }
        let summary = AccountSummary {
            account_id: Id::mint("account"),
            cex_id,
            label: label.to_string(),
            encryption_algorithm: credentials.encryption_algorithm,
            active: true,
            connector: Connector::starting(now),
            version: 1,
            updated_at: now,
        };
        rows.push(AccountRow {
            owner: owner.to_string(),
            summary: summary.clone(),
            credentials: Some(credentials),
            linked_at: now,
        });
        drop(rows);
        self.snapshots.lock().insert(
            summary.account_id.clone(),
            PortfolioSnapshot::pending(&summary.account_id, cex_id),
        );
        Ok(summary)
    }

    pub fn list(&self, owner: &str) -> Vec<AccountSummary> {
        let rows = self.rows.lock();
        let mut listed: Vec<&AccountRow> = rows.iter().filter(|row| row.owner == owner).collect();
        listed.sort_by_key(|row| std::cmp::Reverse(row.linked_at));
        listed.into_iter().map(|row| row.summary.clone()).collect()
    }

    pub fn get(&self, owner: &str, account_id: &str) -> Option<AccountSummary> {
        self.rows
            .lock()
            .iter()
            .find(|row| row.owner == owner && row.summary.account_id == account_id)
            .map(|row| row.summary.clone())
    }

    pub fn replace_credentials(
        &self,
        owner: &str,
        account_id: &str,
        credentials: Credentials,
        now: DateTime<Utc>,
    ) -> Option<AccountSummary> {
        let mut rows = self.rows.lock();
        let row = rows
            .iter_mut()
            .find(|row| row.owner == owner && row.summary.account_id == account_id)?;
        row.summary.encryption_algorithm = credentials.encryption_algorithm;
        row.credentials = Some(credentials);
        row.summary.connector = Connector::starting(now);
        row.summary.version += 1;
        row.summary.updated_at = now;
        Some(row.summary.clone())
    }

    pub fn validate(
        &self,
        owner: &str,
        account_id: &str,
        now: DateTime<Utc>,
    ) -> Option<AccountSummary> {
        let mut rows = self.rows.lock();
        let row = rows
            .iter_mut()
            .find(|row| row.owner == owner && row.summary.account_id == account_id)?;
        let credentials = row.credentials.as_ref()?;
        row.summary.connector = credentials.connector(now);
        row.summary.version += 1;
        row.summary.updated_at = now;
        Some(row.summary.clone())
    }

    pub fn unlink(&self, owner: &str, account_id: &str) -> bool {
        let mut rows = self.rows.lock();
        let length = rows.len();
        rows.retain(|row| !(row.owner == owner && row.summary.account_id == account_id));
        self.snapshots.lock().remove(account_id);
        rows.len() != length
    }

    pub fn active(&self, owner: &str, account_id: &str) -> bool {
        self.get(owner, account_id)
            .is_some_and(|account| account.active)
    }

    pub fn count(&self, owner: &str) -> usize {
        self.rows
            .lock()
            .iter()
            .filter(|row| row.owner == owner)
            .count()
    }

    pub fn snapshot(&self, owner: &str, account_id: &str) -> Option<PortfolioSnapshot> {
        self.get(owner, account_id)?;
        self.snapshots.lock().get(account_id).cloned()
    }

    pub fn snapshots(&self, owner: &str) -> Vec<PortfolioSnapshot> {
        self.list(owner)
            .iter()
            .filter_map(|account| self.snapshot(owner, &account.account_id))
            .collect()
    }
}
