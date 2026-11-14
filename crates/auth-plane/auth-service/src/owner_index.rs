use crate::{ApiToken, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const OWNER_INDEX_PREFIX: &str = "index/owners/";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerIndexEntry {
    id: String,
    owner: String,
    created_at: DateTime<Utc>,
    last_used_at: Option<DateTime<Utc>>,
    expires_at: DateTime<Utc>,
    scopes: Vec<TokenScope>,
    revoked_at: Option<DateTime<Utc>>,
    replaced_by: Option<String>,
    rotation_overlap_until: Option<DateTime<Utc>>,
}

impl OwnerIndexEntry {
    pub fn from_token(token: &ApiToken) -> Self {
        let metadata = token.metadata();
        Self {
            id: token.id().to_string(),
            owner: token.owner().to_string(),
            created_at: token.created_at(),
            last_used_at: metadata.last_used_at(),
            expires_at: metadata.expires_at(),
            scopes: metadata.scopes().to_vec(),
            revoked_at: metadata.revoked_at(),
            replaced_by: metadata.replaced_by().map(str::to_string),
            rotation_overlap_until: metadata.rotation_overlap_until(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn last_used_at(&self) -> Option<DateTime<Utc>> {
        self.last_used_at
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.created_at <= now
            && self.revoked_at.is_none()
            && self.expires_at > now
            && self.rotation_overlap_until.is_none_or(|until| until > now)
    }

    pub fn has_scope(&self, required: TokenScope) -> bool {
        self.scopes.iter().any(|scope| scope.matches(required))
    }

    pub fn matches_token(&self, token: &ApiToken) -> bool {
        self.id == token.id() && self.owner == token.owner() && *self == Self::from_token(token)
    }

    pub fn compare(left: &Self, right: &Self) -> std::cmp::Ordering {
        left.replaced_by
            .is_some()
            .cmp(&right.replaced_by.is_some())
            .then_with(|| right.last_used_at.cmp(&left.last_used_at))
            .then_with(|| right.created_at.cmp(&left.created_at))
            .then_with(|| left.id.cmp(&right.id))
    }

    fn to_value(&self) -> Value {
        json!({
            "id": self.id,
            "owner": self.owner,
            "created_at": self.created_at.to_rfc3339(),
            "last_used_at": self.last_used_at.map(|value| value.to_rfc3339()),
            "expires_at": self.expires_at.to_rfc3339(),
            "scopes": self.scopes.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "revoked_at": self.revoked_at.map(|value| value.to_rfc3339()),
            "replaced_by": self.replaced_by,
            "rotation_overlap_until": self.rotation_overlap_until.map(|value| value.to_rfc3339()),
        })
    }

    fn from_value(value: &Value) -> PlatformResult<Self> {
        let scopes = Self::string_array(value, "scopes")?
            .into_iter()
            .map(|scope| {
                scope
                    .parse::<TokenScope>()
                    .map_err(|_| Self::malformed(format!("{scope:?} is not a valid token scope")))
            })
            .collect::<PlatformResult<Vec<_>>>()?;
        Ok(Self {
            id: Self::required_string(value, "id")?,
            owner: Self::required_string(value, "owner")?,
            created_at: Self::required_timestamp(value, "created_at")?,
            last_used_at: Self::timestamp(value, "last_used_at")?,
            expires_at: Self::required_timestamp(value, "expires_at")?,
            scopes,
            revoked_at: Self::timestamp(value, "revoked_at")?,
            replaced_by: Self::optional_string(value, "replaced_by")?,
            rotation_overlap_until: Self::timestamp(value, "rotation_overlap_until")?,
        })
    }

    fn required_string(value: &Value, field: &'static str) -> PlatformResult<String> {
        Self::optional_string(value, field)?
            .ok_or_else(|| Self::malformed(format!("{field} is required")))
    }

    fn optional_string(value: &Value, field: &'static str) -> PlatformResult<Option<String>> {
        match value.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(Self::malformed(format!("{field} must be a string or null"))),
        }
    }

    fn string_array(value: &Value, field: &'static str) -> PlatformResult<Vec<String>> {
        value
            .get(field)
            .and_then(Value::as_array)
            .ok_or_else(|| Self::malformed(format!("{field} must be an array")))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| Self::malformed(format!("{field} must contain strings")))
            })
            .collect()
    }

    fn required_timestamp(value: &Value, field: &'static str) -> PlatformResult<DateTime<Utc>> {
        Self::timestamp(value, field)?
            .ok_or_else(|| Self::malformed(format!("{field} is required")))
    }

    fn timestamp(value: &Value, field: &'static str) -> PlatformResult<Option<DateTime<Utc>>> {
        let Some(value) = Self::optional_string(value, field)? else {
            return Ok(None);
        };
        DateTime::parse_from_rfc3339(&value)
            .map(|value| Some(value.with_timezone(&Utc)))
            .map_err(|error| {
                Self::malformed(format!("{field} is not an RFC 3339 timestamp: {error}"))
            })
    }

    fn malformed(detail: String) -> PlatformError {
        PlatformError::auth_store("read", "owner-index", false, detail)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerIndex {
    owner: String,
    tokens: Vec<OwnerIndexEntry>,
}

impl OwnerIndex {
    pub fn new(owner: impl Into<String>) -> Self {
        Self {
            owner: owner.into(),
            tokens: Vec::new(),
        }
    }

    pub fn from_tokens(owner: &str, tokens: &[ApiToken]) -> Self {
        let mut entries: Vec<_> = tokens
            .iter()
            .filter(|token| token.owner() == owner)
            .map(OwnerIndexEntry::from_token)
            .collect();
        entries.sort_by(|left, right| left.id.cmp(&right.id));
        entries.dedup_by(|left, right| left.id == right.id);
        Self {
            owner: owner.to_string(),
            tokens: entries,
        }
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn tokens(&self) -> &[OwnerIndexEntry] {
        &self.tokens
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    pub fn upsert(&mut self, entry: OwnerIndexEntry) {
        if let Some(existing) = self.tokens.iter_mut().find(|item| item.id == entry.id) {
            *existing = entry;
        } else {
            self.tokens.push(entry);
        }
        self.tokens.sort_by(|left, right| left.id.cmp(&right.id));
    }

    pub fn remove(&mut self, id: &str) {
        self.tokens.retain(|entry| entry.id != id);
    }

    pub fn to_value(&self) -> Value {
        json!({
            "owner": self.owner,
            "tokens": self.tokens.iter().map(OwnerIndexEntry::to_value).collect::<Vec<_>>(),
        })
    }

    pub fn from_value(value: &Value) -> PlatformResult<Self> {
        let owner = value
            .get("owner")
            .and_then(Value::as_str)
            .ok_or_else(|| Self::malformed("owner is required".to_string()))?
            .to_string();
        let tokens = value
            .get("tokens")
            .and_then(Value::as_array)
            .ok_or_else(|| Self::malformed("tokens must be an array".to_string()))?
            .iter()
            .map(OwnerIndexEntry::from_value)
            .collect::<PlatformResult<Vec<_>>>()?;
        if tokens.iter().any(|entry| entry.owner != owner) {
            return Err(Self::malformed(
                "owner index contains an entry for another owner".to_string(),
            ));
        }
        Ok(Self { owner, tokens })
    }

    pub fn storage_key(owner: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(owner.as_bytes());
        format!(
            "{OWNER_INDEX_PREFIX}{}",
            data_encoding::HEXLOWER.encode(&hasher.finalize())
        )
    }

    fn malformed(detail: String) -> PlatformError {
        PlatformError::auth_store("read", "owner-index", false, detail)
    }
}
