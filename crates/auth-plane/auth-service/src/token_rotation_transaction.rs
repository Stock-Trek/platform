use crate::{ApiToken, TokenAuditEvent, token_rotation::TokenRotationIdentity};
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};
use serde_json::{Value, json};

pub const ROTATION_TRANSACTION_FIELD: &str = "__rotation_transaction__";
pub const ROTATION_TRANSACTION_PREFIX: &str = "rotation/transactions/";

/// The durable intent to rotate a token.
///
/// Vault cannot update the old and new token keys atomically. Instead this
/// record is written with one compare-and-swap create, and a relay later
/// materializes the retirement and replacement idempotently.
#[derive(Clone, Debug)]
pub struct TokenRotationTransaction {
    transaction_id: String,
    original: ApiToken,
    retired: ApiToken,
    replacement: ApiToken,
    events: Vec<TokenAuditEvent>,
}

impl TokenRotationTransaction {
    pub fn new(
        original: ApiToken,
        retired: ApiToken,
        replacement: ApiToken,
        mut events: Vec<TokenAuditEvent>,
    ) -> PlatformResult<Self> {
        if events.len() != 2 {
            return Err(PlatformError::auth_invalid_field(
                "audit_events",
                "a rotation transaction requires exactly two events",
            ));
        }
        if retired.id() != original.id()
            || retired.metadata().replaced_by() != Some(replacement.id())
            || replacement.metadata().rotated_from() != Some(original.id())
        {
            return Err(PlatformError::auth_invalid_field(
                "rotation",
                "does not describe a consistent token rotation",
            ));
        }
        let transaction_id = TokenRotationIdentity::transaction_id(original.id(), replacement.id());
        for (index, event) in events.iter_mut().enumerate() {
            event.event_id = Some(TokenRotationIdentity::event_id(
                &transaction_id,
                index,
                event,
            ));
        }
        Ok(Self {
            transaction_id,
            original,
            retired,
            replacement,
            events,
        })
    }

    pub fn has_same_rotation(&self, other: &Self) -> bool {
        self.original.id() == other.original.id() && self.replacement.id() == other.replacement.id()
    }

    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    pub fn original(&self) -> &ApiToken {
        &self.original
    }

    pub fn retired(&self) -> &ApiToken {
        &self.retired
    }

    pub fn replacement(&self) -> &ApiToken {
        &self.replacement
    }

    pub fn events(&self) -> &[TokenAuditEvent] {
        &self.events
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.events[0].timestamp
    }

    pub fn is_storage_key(id: &str) -> bool {
        id.starts_with(ROTATION_TRANSACTION_PREFIX)
    }

    pub fn is_document(document: &Value) -> bool {
        document
            .get(ROTATION_TRANSACTION_FIELD)
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    pub fn storage_key(&self) -> String {
        format!("{ROTATION_TRANSACTION_PREFIX}{}", self.transaction_id)
    }

    pub fn to_value(&self) -> Value {
        json!({
            ROTATION_TRANSACTION_FIELD: true,
            "transaction_id": self.transaction_id,
            "original": self.original.to_value(),
            "retired": self.retired.to_value(),
            "replacement": self.replacement.to_value(),
            "events": self.events,
        })
    }

    pub fn from_value(value: &Value) -> PlatformResult<Self> {
        if !value
            .get(ROTATION_TRANSACTION_FIELD)
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err(Self::malformed("missing rotation transaction marker"));
        }
        let transaction_id = value
            .get("transaction_id")
            .and_then(Value::as_str)
            .ok_or_else(|| Self::malformed("missing transaction id"))?
            .to_string();
        let original = Self::token(value, "original")?;
        let retired = Self::token(value, "retired")?;
        let replacement = Self::token(value, "replacement")?;
        let events: Vec<TokenAuditEvent> = value
            .get("events")
            .cloned()
            .map(serde_json::from_value::<Vec<TokenAuditEvent>>)
            .transpose()
            .map_err(|_| Self::malformed("invalid audit events"))?
            .ok_or_else(|| Self::malformed("missing audit events"))?;
        if events.len() != 2 {
            return Err(Self::malformed("expected two audit events"));
        }
        Ok(Self {
            transaction_id,
            original,
            retired,
            replacement,
            events,
        })
    }

    fn token(value: &Value, field: &'static str) -> PlatformResult<ApiToken> {
        let token_value = value
            .get(field)
            .ok_or_else(|| Self::malformed("missing token value"))?;
        let id = token_value
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| Self::malformed("missing token id"))?;
        ApiToken::from_value(id, token_value)
            .map_err(|error| Self::malformed(error.to_string().as_str()))
    }

    fn malformed(detail: &str) -> PlatformError {
        PlatformError::auth_store("read", "rotation-transaction", false, detail.to_string())
    }
}
