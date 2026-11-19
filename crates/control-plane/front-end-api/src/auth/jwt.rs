use crate::auth::Scope;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use ed25519_compact::{KeyPair, PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub const ISSUER: &str = "platform-front-end-api";
pub const AUDIENCE: &str = "platform-api";

const HEADER: &str = "{\"alg\":\"EdDSA\",\"typ\":\"JWT\",\"crv\":\"Ed25519\"}";
const SIGNATURE_LENGTH: usize = 64;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AccessClaims<'a> {
    iss: &'a str,
    sub: &'a str,
    owner: &'a str,
    aud: &'a str,
    sid: &'a str,
    scope: Vec<Scope>,
    iat: i64,
    exp: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerifiedAccess {
    iss: String,
    aud: String,
    pub sub: String,
    pub owner: String,
    pub sid: String,
    pub scope: Vec<Scope>,
    iat: i64,
    pub exp: i64,
}

pub struct JwtSigner {
    secret: SecretKey,
    public: PublicKey,
}

impl JwtSigner {
    pub fn generate() -> Self {
        let key_pair = KeyPair::generate();
        Self {
            secret: key_pair.sk,
            public: key_pair.pk,
        }
    }

    pub fn mint<T: Serialize>(&self, claims: &T) -> String {
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).expect("claims json"));
        let signing_input = format!("{HEADER}.{payload}");
        let signature = self.secret.sign(signing_input.as_bytes(), None);
        format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature))
    }

    pub fn verify<T: DeserializeOwned>(&self, token: &str) -> Option<T> {
        let (header, payload, signature) = Self::split(token)?;
        self.public
            .verify(format!("{header}.{payload}").as_bytes(), &signature)
            .ok()?;
        let claims = URL_SAFE_NO_PAD.decode(payload).ok()?;
        serde_json::from_slice(&claims).ok()
    }

    pub fn mint_access_token(
        &self,
        owner: &str,
        scopes: &[Scope],
        session_id: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> String {
        let claims = AccessClaims {
            iss: ISSUER,
            sub: owner,
            owner,
            aud: AUDIENCE,
            sid: session_id,
            scope: scopes.to_vec(),
            iat: now.timestamp(),
            exp: expires_at.timestamp(),
        };
        self.mint(&claims)
    }

    pub fn verify_access(&self, token: &str) -> Option<VerifiedAccess> {
        let claims = self.verify::<VerifiedAccess>(token)?;
        (claims.iss == ISSUER
            && claims.aud == AUDIENCE
            && claims.iat <= claims.exp
            && claims.owner == claims.sub)
            .then_some(claims)
    }

    pub fn verify_session_id(&self, token: &str) -> Option<String> {
        self.verify_access(token).map(|claims| claims.sid)
    }

    fn split(token: &str) -> Option<(&str, &str, Signature)> {
        let mut segments = token.split('.');
        let header = segments.next()?;
        let payload = segments.next()?;
        let signature = segments.next()?;
        if segments.next().is_some() || header != HEADER {
            return None;
        }
        let signature = Self::decode_signature(signature)?;
        Some((header, payload, signature))
    }

    fn decode_signature(encoded: &str) -> Option<Signature> {
        let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
        (bytes.len() == SIGNATURE_LENGTH)
            .then(|| Signature::from_slice(&bytes).ok())
            .flatten()
    }
}
