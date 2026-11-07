use platform_errors::{BpError, BpResult};

pub struct TokenScope;

impl TokenScope {
    pub const ALL: &'static str = "*";
    pub const ORDERS_READ: &'static str = "orders:read";
    pub const ORDERS_WRITE: &'static str = "orders:write";
    pub const TOKENS_READ: &'static str = "tokens:read";
    pub const TOKENS_WRITE: &'static str = "tokens:write";
    pub const ADMIN: &'static str = "admin";

    pub const DEFAULT: &'static [&'static str] = &[
        Self::ORDERS_READ,
        Self::ORDERS_WRITE,
        Self::TOKENS_READ,
        Self::TOKENS_WRITE,
        Self::ADMIN,
    ];

    pub fn validate(scope: &str) -> BpResult<()> {
        if scope == Self::ALL {
            return Ok(());
        }
        let base = scope.strip_suffix(":*").unwrap_or(scope);
        let wildcard = scope.ends_with(":*");
        let segments = base.split(':').collect::<Vec<_>>();
        let valid = !segments.is_empty()
            && segments.iter().all(|segment| Self::valid_segment(segment))
            && scope.matches('*').count() <= usize::from(wildcard);
        if valid {
            Ok(())
        } else {
            Err(BpError::auth_invalid_field(
                "scope",
                format!("{scope:?} is not a valid token scope"),
            ))
        }
    }

    pub fn validate_all<S: AsRef<str>>(scopes: &[S]) -> BpResult<()> {
        for scope in scopes {
            Self::validate(scope.as_ref())?;
        }
        Ok(())
    }

    pub fn allows(granted: &str, required: &str) -> bool {
        Self::matches(granted, required)
    }

    pub fn matches(granted: &str, required: &str) -> bool {
        if granted == Self::ALL || granted == required {
            return true;
        }
        let Some(prefix) = granted.strip_suffix(":*") else {
            return false;
        };
        required
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with(':'))
    }

    pub fn default_scopes() -> Vec<String> {
        Self::DEFAULT
            .iter()
            .map(|scope| scope.to_string())
            .collect()
    }

    fn valid_segment(segment: &str) -> bool {
        !segment.is_empty()
            && segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    }
}
