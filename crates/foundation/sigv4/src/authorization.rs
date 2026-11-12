use crate::headers::{ALGORITHM, AUTHORIZATION};
use platform_errors::{BuildError, BuildResult};
use std::collections::BTreeSet;

pub(crate) struct ParsedAuthorization {
    pub(crate) access_key_id: String,
    pub(crate) date: String,
    pub(crate) region: String,
    pub(crate) service: String,
    pub(crate) signed_headers: Vec<String>,
    pub(crate) signature: String,
}

impl ParsedAuthorization {
    pub(crate) fn parse(value: &str) -> BuildResult<Self> {
        let rest = value
            .strip_prefix(ALGORITHM)
            .and_then(|value| value.strip_prefix(' '))
            .ok_or_else(|| Self::invalid("unsupported authorization algorithm"))?;
        let mut credential = None;
        let mut signed_headers = None;
        let mut signature = None;
        for field in rest.split(',') {
            let field = field.trim();
            if let Some(value) = field.strip_prefix("Credential=") {
                if credential.replace(value).is_some() {
                    return Err(Self::invalid("duplicate credential"));
                }
            } else if let Some(value) = field.strip_prefix("SignedHeaders=") {
                if signed_headers.replace(value).is_some() {
                    return Err(Self::invalid("duplicate signed headers"));
                }
            } else if let Some(value) = field.strip_prefix("Signature=") {
                if signature.replace(value).is_some() {
                    return Err(Self::invalid("duplicate signature"));
                }
            } else {
                return Err(Self::invalid("authorization contains an unknown field"));
            }
        }
        let credential =
            credential.ok_or_else(|| Self::invalid("authorization is missing a credential"))?;
        let signed_headers = signed_headers
            .ok_or_else(|| Self::invalid("authorization is missing signed headers"))?;
        let signature =
            signature.ok_or_else(|| Self::invalid("authorization is missing a signature"))?;
        let parts = credential.split('/').collect::<Vec<_>>();
        if parts.len() != 5 || parts[4] != "aws4_request" {
            return Err(Self::invalid("credential scope is invalid"));
        }
        if parts[1].len() != 8 || !parts[1].bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Self::invalid("credential date is invalid"));
        }
        if signature.len() != 64 || !signature.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(Self::invalid("signature is not valid hex"));
        }
        Ok(Self {
            access_key_id: parts[0].to_string(),
            date: parts[1].to_string(),
            region: parts[2].to_string(),
            service: parts[3].to_string(),
            signed_headers: Self::signed_headers(signed_headers)?,
            signature: signature.to_ascii_lowercase(),
        })
    }

    pub(crate) fn invalid(message: impl Into<String>) -> BuildError {
        BuildError::auth_invalid_jwt(message)
    }

    fn signed_headers(value: &str) -> BuildResult<Vec<String>> {
        let headers = value
            .split(';')
            .map(str::to_string)
            .collect::<Vec<String>>();
        if headers.is_empty() || headers.iter().any(String::is_empty) {
            return Err(Self::invalid("signed headers are empty"));
        }
        let mut unique = BTreeSet::new();
        for header in &headers {
            if header == AUTHORIZATION
                || header != &header.to_ascii_lowercase()
                || !unique.insert(header.clone())
            {
                return Err(Self::invalid("signed headers must be lowercase and unique"));
            }
        }
        if headers.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Self::invalid("signed headers must be sorted"));
        }
        Ok(headers)
    }
}
