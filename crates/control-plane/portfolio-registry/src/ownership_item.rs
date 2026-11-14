use crate::OwnershipRecord;
use aws_sdk_dynamodb::types::AttributeValue;
use platform_errors::{BuildError, BuildResult};
use std::collections::HashMap;

pub const ACCOUNT_KEY: &str = "accountKey";
pub const OWNER: &str = "owner";
pub const ACTIVE: &str = "active";
pub const VERSION: &str = "version";
pub const RECORD: &str = "record";
pub const PENDING_EVENTS: &str = "pendingEvents";

pub struct OwnershipItem;

impl OwnershipItem {
    pub fn parse(item: &HashMap<String, AttributeValue>) -> BuildResult<OwnershipRecord> {
        let payload = Self::string(item, RECORD)?;
        let mut record: OwnershipRecord = serde_json::from_str(payload)?;
        record.pending_events = match item.get(PENDING_EVENTS) {
            Some(AttributeValue::S(payload)) => serde_json::from_str(payload)?,
            _ => Vec::new(),
        };
        Ok(record)
    }

    pub fn attributes(record: &OwnershipRecord) -> BuildResult<HashMap<String, AttributeValue>> {
        let mut item = HashMap::from([
            (
                ACCOUNT_KEY.to_string(),
                AttributeValue::S(record.account.storage_key()),
            ),
            (OWNER.to_string(), AttributeValue::S(record.owner.clone())),
            (ACTIVE.to_string(), AttributeValue::Bool(record.active)),
            (
                VERSION.to_string(),
                AttributeValue::N(record.version.to_string()),
            ),
            (
                RECORD.to_string(),
                AttributeValue::S(serde_json::to_string(record)?),
            ),
        ]);
        if !record.pending_events.is_empty() {
            item.insert(
                PENDING_EVENTS.to_string(),
                AttributeValue::S(serde_json::to_string(&record.pending_events)?),
            );
        }
        Ok(item)
    }

    fn string<'a>(
        item: &'a HashMap<String, AttributeValue>,
        name: &'static str,
    ) -> BuildResult<&'a str> {
        match item.get(name) {
            Some(AttributeValue::S(value)) => Ok(value),
            _ => Err(BuildError::bad_value(
                "ownership item",
                name,
                "missing string attribute",
            )),
        }
    }
}
