use crate::token_usage_reservation::TokenUsageReservation;
use platform_errors::{PlatformError, PlatformResult};
use redis::{ErrorKind, RedisError, Value};

const PAYLOAD_FIELD: &str = "payload";
const TARGET: &str = "token-usage-queue";

/// Parses Redis stream replies and maps client failures into platform errors.
pub struct ValkeyTokenUsageReply;

impl ValkeyTokenUsageReply {
    pub fn parse(reply: Value) -> PlatformResult<Vec<TokenUsageReservation>> {
        let Value::Array(streams) = reply else {
            return Ok(Vec::new());
        };
        let mut reservations = Vec::new();
        for stream in streams {
            let Value::Array(parts) = stream else {
                return Err(Self::malformed("stream entry is not an array"));
            };
            let Some(Value::Array(entries)) = parts.get(1) else {
                return Err(Self::malformed("stream entry is missing records"));
            };
            for entry in entries {
                let Value::Array(fields) = entry else {
                    return Err(Self::malformed("stream record is not an array"));
                };
                let id = fields
                    .first()
                    .and_then(Self::string_value)
                    .ok_or_else(|| Self::malformed("stream record is missing an id"))?;
                let payload = Self::payload(fields.get(1))?;
                let usage = serde_json::from_str(&payload)
                    .map_err(|error| Self::decode_error(error.to_string()))?;
                reservations.push(TokenUsageReservation::new(id, usage));
            }
        }
        Ok(reservations)
    }

    pub fn error(operation: &'static str, error: RedisError) -> PlatformError {
        let retryable = error.is_io_error()
            || error.is_timeout()
            || error.is_connection_dropped()
            || error.is_cluster_error()
            || matches!(
                error.kind(),
                ErrorKind::BusyLoadingError
                    | ErrorKind::TryAgain
                    | ErrorKind::ClusterDown
                    | ErrorKind::MasterDown
                    | ErrorKind::ReadOnly
            );
        PlatformError::auth_store(operation, TARGET, retryable, error)
    }

    pub fn malformed(detail: &'static str) -> PlatformError {
        PlatformError::auth_store("reserve", TARGET, false, detail.to_string())
    }

    pub fn decode_error(detail: String) -> PlatformError {
        PlatformError::auth_store("reserve", TARGET, false, detail)
    }

    fn payload(fields: Option<&Value>) -> PlatformResult<String> {
        let Some(Value::Array(fields)) = fields else {
            return Err(Self::malformed("stream record is missing fields"));
        };
        for pair in fields.chunks(2) {
            let [name, value] = pair else {
                continue;
            };
            if Self::string_value(name).as_deref() == Some(PAYLOAD_FIELD) {
                return Self::string_value(value)
                    .ok_or_else(|| Self::malformed("stream payload is not a string"));
            }
        }
        Err(Self::malformed("stream record is missing its payload"))
    }

    fn string_value(value: &Value) -> Option<String> {
        match value {
            Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
            Value::SimpleString(text) => Some(text.clone()),
            Value::VerbatimString { text, .. } => Some(text.clone()),
            _ => None,
        }
    }
}
