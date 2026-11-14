use crate::{OwnershipCommand, OwnershipRecord};
use platform_errors::{PlatformError, PlatformResult};
use platform_portfolio_ownership::OwnershipEvent;

pub enum OwnershipTransition {
    Applied(OwnershipRecord),
    Unchanged(OwnershipRecord),
}

impl OwnershipTransition {
    pub fn apply(
        existing: Option<OwnershipRecord>,
        command: &OwnershipCommand,
    ) -> PlatformResult<Self> {
        match existing {
            None => Self::initial(command),
            Some(record) => Self::next(record, command),
        }
    }

    fn initial(command: &OwnershipCommand) -> PlatformResult<Self> {
        if !command.action.is_add() {
            return Err(PlatformError::auth_invalid_field(
                "account",
                "cannot remove a mapping that does not exist",
            ));
        }
        let event = Self::event(1, command);
        Ok(Self::Applied(Self::record(
            1,
            true,
            event,
            Vec::new(),
            Vec::new(),
            command,
        )))
    }

    fn next(record: OwnershipRecord, command: &OwnershipCommand) -> PlatformResult<Self> {
        if command.action.is_add() {
            if !record.active {
                return Ok(Self::Applied(Self::advance(record, true, command)));
            }
            if record.owner != command.owner {
                return Err(PlatformError::auth_invalid_field(
                    "owner",
                    "account belongs to a different owner",
                ));
            }
            return Ok(Self::Unchanged(record));
        }
        if !record.active {
            return Ok(Self::Unchanged(record));
        }
        if record.owner != command.owner {
            return Err(PlatformError::auth_invalid_field(
                "owner",
                "account belongs to a different owner",
            ));
        }
        Ok(Self::Applied(Self::advance(record, false, command)))
    }

    fn advance(
        record: OwnershipRecord,
        active: bool,
        command: &OwnershipCommand,
    ) -> OwnershipRecord {
        let version = record.version.saturating_add(1);
        let event = Self::event(version, command);
        Self::record(
            version,
            active,
            event,
            record.audit,
            record.pending_events,
            command,
        )
    }

    fn record(
        version: u64,
        active: bool,
        event: OwnershipEvent,
        mut audit: Vec<OwnershipEvent>,
        mut pending_events: Vec<OwnershipEvent>,
        command: &OwnershipCommand,
    ) -> OwnershipRecord {
        audit.push(event.clone());
        pending_events.push(event.clone());
        OwnershipRecord {
            account: command.account.clone(),
            owner: command.owner.clone(),
            active,
            version,
            updated_at: command.occurred_at,
            audit,
            pending_events,
        }
    }

    fn event(version: u64, command: &OwnershipCommand) -> OwnershipEvent {
        OwnershipEvent::new(
            command.audit_id.clone(),
            command.owner.clone(),
            command.account.clone(),
            command.action,
            version,
            command.occurred_at,
        )
    }
}
