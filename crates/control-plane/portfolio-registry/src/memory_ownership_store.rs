use crate::{
    AccountMapping, OwnershipCommand, OwnershipCommit, OwnershipRecord, OwnershipStore,
    OwnershipTransition,
};
use async_trait::async_trait;
use platform_errors::BuildResult;
use platform_portfolio_ownership::{AccountKey, OwnershipEvent};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct MemoryOwnershipStore {
    records: Mutex<BTreeMap<AccountKey, OwnershipRecord>>,
}

impl MemoryOwnershipStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl OwnershipStore for MemoryOwnershipStore {
    async fn commit(&self, command: OwnershipCommand) -> BuildResult<OwnershipCommit> {
        let mut records = self.records.lock().await;
        let existing = records.get(&command.account).cloned();
        match OwnershipTransition::apply(existing, &command)? {
            OwnershipTransition::Unchanged(record) => Ok(OwnershipCommit::Unchanged(record)),
            OwnershipTransition::Applied(record) => {
                let event = record.pending_events.last().cloned().ok_or_else(|| {
                    platform_errors::BuildError::external(std::io::Error::other(
                        "missing outbox event",
                    ))
                })?;
                records.insert(command.account, record);
                Ok(OwnershipCommit::Applied(event))
            }
        }
    }

    async fn get(&self, account: &AccountKey) -> BuildResult<Option<OwnershipRecord>> {
        Ok(self.records.lock().await.get(account).cloned())
    }

    async fn owner_accounts(&self, owner: &str) -> BuildResult<Vec<AccountMapping>> {
        let records = self.records.lock().await;
        Ok(records
            .values()
            .filter(|record| record.active && record.owner == owner)
            .map(OwnershipRecord::mapping)
            .collect())
    }

    async fn pending_events(&self, limit: usize) -> BuildResult<Vec<OwnershipEvent>> {
        let records = self.records.lock().await;
        Ok(records
            .values()
            .flat_map(|record| record.pending_events.iter().cloned())
            .take(limit)
            .collect())
    }

    async fn acknowledge(&self, event: &OwnershipEvent) -> BuildResult<()> {
        if let Some(record) = self.records.lock().await.get_mut(event.account()) {
            record
                .pending_events
                .retain(|pending| pending.version() != event.version());
        }
        Ok(())
    }
}
