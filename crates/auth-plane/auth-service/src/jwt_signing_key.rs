use crate::{IssuedJwt, OrderJwtClaims};
use ed25519_compact::KeyPair;
use jsonwebtoken::{
    Algorithm, EncodingKey, Header, encode,
    jwk::{
        AlgorithmParameters, CommonParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm,
        OctetKeyPairParameters, OctetKeyPairType, PublicKeyUse,
    },
};
use platform_errors::{BpError, BpResult};
use std::fmt;

pub const JWT_SIGNING_ALGORITHM: Algorithm = Algorithm::EdDSA;
pub const DEFAULT_JWT_KEY_ID: &str = "auth-service";
const JWT_KEY_FIELD: &str = "jwt_private_key";

pub struct JwtSigningKey {
    key_id: String,
    encoding: EncodingKey,
    jwks: JwkSet,
}

impl JwtSigningKey {
    pub fn generate(key_id: impl Into<String>) -> BpResult<Self> {
        let key_pair = KeyPair::generate();
        Self::from_key_pair(key_id.into(), &key_pair)
    }

    pub fn from_pkcs8_der(key_id: impl Into<String>, der: &[u8]) -> BpResult<Self> {
        let key_pair = KeyPair::from_der(der)
            .map_err(|error| BpError::auth_invalid_field(JWT_KEY_FIELD, error.to_string()))?;
        Self::from_key_pair(key_id.into(), &key_pair)
    }

    pub fn from_pkcs8_pem(key_id: impl Into<String>, pem: &str) -> BpResult<Self> {
        let pem = pem.replace("\\n", "\n");
        let key_pair = KeyPair::from_pem(&pem)
            .map_err(|error| BpError::auth_invalid_field(JWT_KEY_FIELD, error.to_string()))?;
        Self::from_key_pair(key_id.into(), &key_pair)
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

    fn from_key_pair(key_id: String, key_pair: &KeyPair) -> BpResult<Self> {
        if key_id.trim().is_empty() {
            return Err(BpError::auth_invalid_field(
                "jwt_key_id",
                "must not be empty",
            ));
        }
        let encoding = EncodingKey::from_ed_der(&key_pair.sk.to_der());
        let public_key = data_encoding::BASE64URL_NOPAD.encode(&key_pair.pk[..]);
        let jwk = Jwk {
            common: CommonParameters {
                public_key_use: Some(PublicKeyUse::Signature),
                key_algorithm: Some(KeyAlgorithm::EdDSA),
                key_id: Some(key_id.clone()),
                ..Default::default()
            },
            algorithm: AlgorithmParameters::OctetKeyPair(OctetKeyPairParameters {
                key_type: OctetKeyPairType::OctetKeyPair,
                curve: EllipticCurve::Ed25519,
                x: public_key,
            }),
        };
        Ok(Self {
            key_id,
            encoding,
            jwks: JwkSet { keys: vec![jwk] },
        })
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
