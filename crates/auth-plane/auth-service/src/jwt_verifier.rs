use crate::{
    ApiToken, OrderJwtClaims, jwt_signing_key::JWT_SIGNING_ALGORITHM,
    jwt_signing_key::JwtSigningKey,
};
use chrono::{DateTime, Utc};
use jsonwebtoken::{
    DecodingKey, Header, TokenData, Validation, decode, decode_header,
    jwk::{Jwk, JwkSet, KeyAlgorithm},
};
use platform_errors::{PlatformError, PlatformResult};
use std::sync::Arc;

#[derive(Clone)]
pub struct JwtVerifier {
    keys: Arc<JwkSet>,
}

impl JwtVerifier {
    pub fn new(keys: JwkSet) -> Self {
        Self {
            keys: Arc::new(keys),
        }
    }

    pub fn from_signing_key(signing_key: &JwtSigningKey) -> Self {
        Self::new(signing_key.jwks().clone())
    }

    pub fn jwks(&self) -> &JwkSet {
        &self.keys
    }

    pub fn verify(&self, jwt: &str, now: DateTime<Utc>) -> PlatformResult<OrderJwtClaims> {
        let header = decode_header(jwt)
            .map_err(|error| PlatformError::auth_invalid_jwt(error.to_string()))?;
        self.verify_with_header(&header, jwt, now)
    }

    pub fn has_key_for_header(&self, header: &Header) -> bool {
        self.resolve_key(header).is_some()
    }

    pub fn verify_with_header(
        &self,
        header: &Header,
        jwt: &str,
        now: DateTime<Utc>,
    ) -> PlatformResult<OrderJwtClaims> {
        if header.alg != JWT_SIGNING_ALGORITHM {
            return Err(PlatformError::auth_invalid_jwt(
                "must use the EdDSA signing algorithm",
            ));
        }
        let jwk = self
            .resolve_key(header)
            .ok_or_else(|| PlatformError::auth_invalid_jwt("references an unknown signing key"))?;
        if let Some(algorithm) = jwk.common.key_algorithm
            && algorithm != KeyAlgorithm::EdDSA
        {
            return Err(PlatformError::auth_invalid_jwt(
                "signing key does not support EdDSA",
            ));
        }
        let key = DecodingKey::from_jwk(jwk)
            .map_err(|error| PlatformError::auth_invalid_jwt(error.to_string()))?;
        let mut validation = Validation::new(JWT_SIGNING_ALGORITHM);
        validation.validate_exp = false;
        validation.validate_aud = false;
        validation.leeway = 0;
        let mut verified: TokenData<OrderJwtClaims> = decode(jwt, &key, &validation)
            .map_err(|error| PlatformError::auth_invalid_jwt(error.to_string()))?;
        ApiToken::validate_id(verified.claims.token_id())?;
        verified.claims.normalize_owner()?;
        verified.claims.validate_at(now)?;
        Ok(verified.claims)
    }

    fn resolve_key<'a>(&'a self, header: &Header) -> Option<&'a Jwk> {
        match header.kid.as_deref() {
            Some(key_id) => self.keys.find(key_id),
            None if self.keys.keys.len() == 1 => self.keys.keys.first(),
            None => None,
        }
    }
}
