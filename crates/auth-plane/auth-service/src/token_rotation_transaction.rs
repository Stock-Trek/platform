use crate::{ApiToken, TokenAuditEvent, token_audit_outbox::TokenAuditOutbox};
use chrono::Utc;
use platform_errors::{BpError, BpResult};
use rand::{Rng, distributions::Alphanumeric};
use serde_json::{Value, json};

pub(crate) const ROTATION_TRANSACTION_FIELD: &str = "__rotation_transaction__";
// The '@' is not valid in a token id, so rotation records can never collide
// with, or hide, a real token in the same Vault mount.
pub(crate) const ROTATION_TRANSACTION_PREFIX: &str = "__rotation_transaction__@";

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
    ) -> BpResult<Self> {
        if events.len() != 2 {
            return Err(BpError::auth_invalid_field(
                "audit_events",
                "a rotation transaction requires exactly two events",
            ));
        }
        if retired.id() != original.id()
            || retired.metadata().replaced_by() != Some(replacement.id())
            || replacement.metadata().rotated_from() != Some(original.id())
        {
            return Err(BpError::auth_invalid_field(
                "rotation",
                "does not describe a consistent token rotation",
            ));
        }
        TokenAuditOutbox::ensure_events_have_ids(&mut events);
        Ok(Self {
            transaction_id: Self::generate_id(),
            original,
            retired,
            replacement,
            events,
        })
    }

    fn generate_id() -> String {
        let random: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();
        format!("{}-{random}", Utc::now().timestamp_micros())
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

    pub fn timestamp(&self) -> chrono::DateTime<Utc> {
        self.events[0].timestamp
    }

    pub(crate) fn is_storage_key(id: &str) -> bool {
        id.starts_with(ROTATION_TRANSACTION_PREFIX)
    }

    pub(crate) fn is_document(document: &Value) -> bool {
        document
            .get(ROTATION_TRANSACTION_FIELD)
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    pub(crate) fn storage_key(&self) -> String {
        format!("{ROTATION_TRANSACTION_PREFIX}{}", self.transaction_id)
    }

    pub(crate) fn to_value(&self) -> Value {
        json!({
            ROTATION_TRANSACTION_FIELD: true,
            "transaction_id": self.transaction_id,
            "original": self.original.to_value(),
            "retired": self.retired.to_value(),
            "replacement": self.replacement.to_value(),
            "events": self.events,
        })
    }

    pub(crate) fn from_value(value: &Value) -> BpResult<Self> {
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

    fn token(value: &Value, field: &'static str) -> BpResult<ApiToken> {
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

    fn malformed(detail: &str) -> BpError {
        BpError::auth_store("read", "rotation-transaction", false, detail.to_string())
    }
}
