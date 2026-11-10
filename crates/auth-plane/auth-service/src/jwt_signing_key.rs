use crate::{IssuedJwt, OrderJwtClaims};
use jsonwebtoken::{
    Algorithm, EncodingKey, Header, encode,
    jwk::{Jwk, JwkSet, KeyAlgorithm, PublicKeyUse},
};
use p256::{
    SecretKey,
    elliptic_curve::Generate,
    pkcs8::{DecodePrivateKey, EncodePrivateKey},
};
use platform_errors::{BpError, BpResult};
use std::fmt;

pub const JWT_SIGNING_ALGORITHM: Algorithm = Algorithm::ES256;
pub const DEFAULT_JWT_KEY_ID: &str = "auth-service";

pub struct JwtSigningKey {
    key_id: String,
    encoding: EncodingKey,
    jwks: JwkSet,
}

impl JwtSigningKey {
    pub fn generate(key_id: impl Into<String>) -> BpResult<Self> {
        let secret = SecretKey::try_generate().map_err(BpError::external)?;
        Self::from_secret_key(key_id.into(), &secret)
    }

    pub fn from_pkcs8_der(key_id: impl Into<String>, der: &[u8]) -> BpResult<Self> {
        let key_id = key_id.into();
        if key_id.trim().is_empty() {
            return Err(BpError::auth_invalid_field(
                "jwt_key_id",
                "must not be empty",
            ));
        }
        let encoding = EncodingKey::from_ec_der(der);
        let mut jwk = Jwk::from_encoding_key(&encoding, JWT_SIGNING_ALGORITHM)
            .map_err(|error| BpError::auth_invalid_field("jwt_private_key", error.to_string()))?;
        jwk.common.key_id = Some(key_id.clone());
        jwk.common.key_algorithm = Some(KeyAlgorithm::ES256);
        jwk.common.public_key_use = Some(PublicKeyUse::Signature);
        Ok(Self {
            key_id,
            encoding,
            jwks: JwkSet { keys: vec![jwk] },
        })
    }

    pub fn from_pkcs8_pem(key_id: impl Into<String>, pem: &str) -> BpResult<Self> {
        let pem = pem.replace("\\n", "\n");
        let secret = SecretKey::from_pkcs8_pem(&pem)
            .map_err(|error| BpError::auth_invalid_field("jwt_private_key", error.to_string()))?;
        Self::from_secret_key(key_id.into(), &secret)
    }

    pub fn sign(&self, claims: &OrderJwtClaims) -> BpResult<IssuedJwt> {
        let mut header = Header::new(JWT_SIGNING_ALGORITHM);
        header.kid = Some(self.key_id.clone());
        let token = encode(&header, claims, &self.encoding)
            .map_err(|error| BpError::auth_invalid_jwt(error.to_string()))?;
        Ok(IssuedJwt::new(token, claims.expires_at()))
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn jwks(&self) -> &JwkSet {
        &self.jwks
    }

    fn from_secret_key(key_id: String, secret: &SecretKey) -> BpResult<Self> {
        let der = secret
            .to_pkcs8_der()
            .map_err(|error| BpError::auth_invalid_field("jwt_private_key", error.to_string()))?;
        Self::from_pkcs8_der(key_id, der.as_bytes())
    }
}

impl fmt::Debug for JwtSigningKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwtSigningKey")
            .field("key_id", &self.key_id)
            .field("algorithm", &JWT_SIGNING_ALGORITHM)
            .field("encoding", &"<redacted>")
            .finish()
    }
}
