use crate::run_status::RunStatus;
use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{DateTime, SecondsFormat, Utc};
use platform_errors::{BpError, BpResult};
use std::collections::HashMap;

pub const ALGORITHM_ID: &str = "algorithmId";
pub const SCHEDULED_AT: &str = "scheduledAt";
pub const CWASM_KEY: &str = "cwasmKey";
pub const CWASM_SHA256: &str = "cwasmSha256";
pub const CWASM_SIZE: &str = "cwasmSize";
pub const WASMTIME_VERSION: &str = "wasmtimeVersion";
pub const STRATEGY_NAME: &str = "strategyName";
pub const STATUS: &str = "status";
pub const COMPLETED_AT: &str = "completedAt";
pub const ERROR: &str = "error";

pub type Item = HashMap<String, AttributeValue>;

/// A scheduled algorithm run together with everything the runner needs to
/// execute it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecord {
    pub algorithm_id: String,
    pub scheduled_at: DateTime<Utc>,
    pub cwasm_key: String,
    pub cwasm_sha256: String,
    pub cwasm_size: u64,
    pub wasmtime_version: String,
    pub strategy_name: String,
    pub status: RunStatus,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

impl RunRecord {
    pub fn new(
        algorithm_id: impl Into<String>,
        scheduled_at: DateTime<Utc>,
        cwasm_key: impl Into<String>,
        cwasm_sha256: impl Into<String>,
        cwasm_size: u64,
        wasmtime_version: impl Into<String>,
        strategy_name: impl Into<String>,
    ) -> Self {
        Self {
            algorithm_id: algorithm_id.into(),
            scheduled_at,
            cwasm_key: cwasm_key.into(),
            cwasm_sha256: cwasm_sha256.into(),
            cwasm_size,
            wasmtime_version: wasmtime_version.into(),
            strategy_name: strategy_name.into(),
            status: RunStatus::Pending,
            completed_at: None,
            error: None,
        }
    }

    pub(crate) fn to_item(&self) -> Item {
        let mut item = HashMap::new();
        item.insert(
            ALGORITHM_ID.into(),
            AttributeValue::S(self.algorithm_id.clone()),
        );
        item.insert(
            SCHEDULED_AT.into(),
            AttributeValue::S(format_timestamp(self.scheduled_at)),
        );
        item.insert(CWASM_KEY.into(), AttributeValue::S(self.cwasm_key.clone()));
        item.insert(
            CWASM_SHA256.into(),
            AttributeValue::S(self.cwasm_sha256.clone()),
        );
        item.insert(
            CWASM_SIZE.into(),
            AttributeValue::N(self.cwasm_size.to_string()),
        );
        item.insert(
            WASMTIME_VERSION.into(),
            AttributeValue::S(self.wasmtime_version.clone()),
        );
        item.insert(
            STRATEGY_NAME.into(),
            AttributeValue::S(self.strategy_name.clone()),
        );
        item.insert(
            STATUS.into(),
            AttributeValue::S(self.status.as_str().to_string()),
        );
        if let Some(completed_at) = self.completed_at {
            item.insert(
                COMPLETED_AT.into(),
                AttributeValue::S(format_timestamp(completed_at)),
            );
        }
        if let Some(error) = &self.error {
            item.insert(ERROR.into(), AttributeValue::S(error.clone()));
        }
        item
    }

    pub(crate) fn from_item(item: &Item) -> BpResult<Self> {
        Ok(Self {
            algorithm_id: string(item, ALGORITHM_ID)?,
            scheduled_at: timestamp(item, SCHEDULED_AT)?,
            cwasm_key: string(item, CWASM_KEY)?,
            cwasm_sha256: string(item, CWASM_SHA256)?,
            cwasm_size: number(item, CWASM_SIZE)?,
            wasmtime_version: string(item, WASMTIME_VERSION)?,
            strategy_name: string(item, STRATEGY_NAME)?,
            status: RunStatus::parse(&string(item, STATUS)?)
                .ok_or_else(|| invalid_attribute(STATUS, "is not a run status"))?,
            completed_at: match item.get(COMPLETED_AT) {
                Some(value) => Some(parse_timestamp(&string_value(COMPLETED_AT, value)?)?),
                None => None,
            },
            error: match item.get(ERROR) {
                Some(value) => Some(string_value(ERROR, value)?),
                None => None,
            },
        })
    }
}

pub(crate) fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub(crate) fn parse_timestamp(value: &str) -> BpResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|source| invalid_attribute_source(SCHEDULED_AT, &source))
}

fn string(item: &Item, name: &str) -> BpResult<String> {
    let value = item.get(name).ok_or_else(|| missing_attribute(name))?;
    string_value(name, value)
}

fn string_value(name: &str, value: &AttributeValue) -> BpResult<String> {
    value
        .as_s()
        .map_err(|_| invalid_attribute(name, "is not a string"))
        .and_then(|value| {
            if value.is_empty() {
                Err(invalid_attribute(name, "is empty"))
            } else {
                Ok(value.clone())
            }
        })
}

fn number(item: &Item, name: &str) -> BpResult<u64> {
    let value = item.get(name).ok_or_else(|| missing_attribute(name))?;
    let value = value
        .as_n()
        .map_err(|_| invalid_attribute(name, "is not a number"))?;
    value
        .parse::<u64>()
        .map_err(|_| invalid_attribute(name, "is not an unsigned integer"))
}

fn timestamp(item: &Item, name: &str) -> BpResult<DateTime<Utc>> {
    let value = item.get(name).ok_or_else(|| missing_attribute(name))?;
    parse_timestamp(&string_value(name, value)?)
}

fn missing_attribute(name: &str) -> BpError {
    invalid_attribute(name, "is missing")
}

fn invalid_attribute(name: &str, reason: &str) -> BpError {
    BpError::external(format!("run record attribute {name} {reason}"))
}

fn invalid_attribute_source(name: &str, source: &chrono::ParseError) -> BpError {
    BpError::external(format!(
        "run record attribute {name} is not a valid timestamp: {source}"
    ))
}
