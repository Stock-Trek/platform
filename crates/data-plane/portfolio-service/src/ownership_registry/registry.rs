use super::{decision::Decision, ownership_apply::OwnershipApply, ownership_entry::OwnershipEntry};
use crate::AccountKey;
use platform_portfolio_ownership::OwnershipEvent;
use std::collections::BTreeMap;

#[derive(Default)]
pub struct OwnershipRegistry {
    entries: BTreeMap<AccountKey, OwnershipEntry>,
}

impl OwnershipRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn accepts(&self, event: &OwnershipEvent) -> bool {
        Self::decision(event, self.entries.get(event.account())).is_ok()
    }

    pub fn apply(&mut self, event: &OwnershipEvent) -> OwnershipApply {
        let existing = self.entries.get(event.account());
        let decision = match Self::decision(event, existing) {
            Ok(decision) => decision,
            Err(outcome) => return outcome,
        };
        self.entries.insert(
            event.account().clone(),
            OwnershipEntry {
                owner: event.owner().to_string(),
                version: event.version(),
                active: event.action().is_add(),
            },
        );
        OwnershipApply::Applied {
            previous_owner: decision.previous_owner,
            was_active: decision.was_active,
        }
    }

    pub fn version(&self, account: &AccountKey) -> Option<u64> {
        self.entries.get(account).map(|entry| entry.version)
    }

    fn decision(
        event: &OwnershipEvent,
        existing: Option<&OwnershipEntry>,
    ) -> Result<Decision, OwnershipApply> {
        let Some(existing) = existing else {
            return if event.action().is_add() {
                Ok(Decision::new(None, false))
            } else {
                Err(OwnershipApply::IgnoredStale)
            };
        };
        if event.version() <= existing.version {
            return Err(OwnershipApply::IgnoredStale);
        }
        if existing.active && existing.owner != event.owner() {
            return Err(OwnershipApply::IgnoredConflict);
        }
        if !event.action().is_add() && existing.owner != event.owner() {
            return Err(OwnershipApply::IgnoredConflict);
        }
        Ok(Decision::new(Some(existing.owner.clone()), existing.active))
    }
}
