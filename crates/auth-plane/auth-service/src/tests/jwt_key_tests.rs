use super::support_tests::{OWNER, TestFixture};
use crate::{
    JWT_SIGNING_ALGORITHM, JwksCache, JwtServer, JwtSigningKey, JwtVerifier, OrderJwtClaims,
};
use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, decode_header, jwk::JwkSet};

struct Fixture;

impl Fixture {
    fn claims(now: chrono::DateTime<Utc>) -> OrderJwtClaims {
        OrderJwtClaims::new("token-a", OWNER, now, now + Duration::minutes(5)).unwrap()
    }

    fn signing_key(key_id: &str) -> JwtSigningKey {
        JwtSigningKey::generate(key_id).unwrap()
    }

    async fn issued_token() -> (crate::AuthService, String, OrderJwtClaims) {
        let service = TestFixture::service();
        TestFixture::add_token(&service, "token-a", OWNER).await;
        let now = Utc::now();
        let issued = service.issue_jwt_for_owner(OWNER, now).await.unwrap();
        let claims = service.verify_jwt(issued.token(), now).await.unwrap();
        (service, issued.token().to_string(), claims)
    }
}

#[tokio::test]
async fn signs_tokens_with_es256_and_a_key_id() {
    let (_, token, _) = Fixture::issued_token().await;
    let header = decode_header(&token).unwrap();

    assert_eq!(header.alg, Algorithm::ES256);
    assert_eq!(header.kid.as_deref(), Some(crate::DEFAULT_JWT_KEY_ID));
}

#[tokio::test]
async fn publishes_only_public_key_material() {
    let (service, _, _) = Fixture::issued_token().await;
    let jwks = serde_json::to_value(service.jwks()).unwrap();
    let serialized = jwks.to_string();
    let keys = jwks["keys"].as_array().unwrap();

    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0]["kty"], "EC");
    assert_eq!(keys[0]["crv"], "P-256");
    assert_eq!(keys[0]["alg"], "ES256");
    assert_eq!(keys[0]["use"], "sig");
    assert!(keys[0].get("x").is_some());
    assert!(keys[0].get("y").is_some());
    assert!(!serialized.contains("\"d\""));
}

#[tokio::test]
async fn verifies_a_token_locally_from_public_jwks() {
    let (service, token, expected) = Fixture::issued_token().await;
    let verifier = JwtVerifier::new(service.jwks().clone());

    let claims = verifier.verify(&token, Utc::now()).unwrap();

    assert_eq!(claims, expected);
}

#[test]
fn rejects_a_token_signed_by_another_key() {
    let now = Utc::now();
    let signer = Fixture::signing_key("issuer-a");
    let other = Fixture::signing_key("issuer-b");
    let issued = signer.sign(&Fixture::claims(now)).unwrap();
    let verifier = JwtVerifier::from_signing_key(&other);

    assert!(verifier.verify(issued.token(), now).is_err());
}

#[test]
fn rejects_a_token_with_a_missing_key_id_when_multiple_keys_are_cached() {
    let now = Utc::now();
    let signer = Fixture::signing_key("issuer-a");
    let issued = signer.sign(&Fixture::claims(now)).unwrap();
    let mut keys = signer.jwks().clone();
    keys.keys
        .extend(Fixture::signing_key("issuer-b").jwks().keys.clone());
    for key in &mut keys.keys {
        key.common.key_id = None;
    }
    let verifier = JwtVerifier::new(keys);

    assert!(verifier.verify(issued.token(), now).is_err());
}

#[tokio::test]
async fn static_jwks_cache_verifies_without_a_token_store() {
    let signer = Fixture::signing_key("issuer-a");
    let now = Utc::now();
    let issued = signer.sign(&Fixture::claims(now)).unwrap();
    let cache = JwksCache::from_jwks(JwkSet {
        keys: signer.jwks().keys.clone(),
    });

    let claims = cache.verify(issued.token(), now).await.unwrap();

    assert_eq!(claims.token_id(), "token-a");
    assert_eq!(claims.owner(), OWNER);
}

#[test]
fn signs_with_an_imported_pkcs8_pem_key() {
    use p256::{
        SecretKey,
        elliptic_curve::Generate,
        pkcs8::{EncodePrivateKey, LineEnding},
    };

    let secret = SecretKey::try_generate().unwrap();
    let pem = secret.to_pkcs8_pem(LineEnding::LF).unwrap();
    let signer = JwtSigningKey::from_pkcs8_pem("imported", pem.as_ref()).unwrap();
    let now = Utc::now();
    let issued = signer.sign(&Fixture::claims(now)).unwrap();
    let verifier = JwtVerifier::from_signing_key(&signer);
    let header = decode_header(issued.token()).unwrap();

    assert_eq!(header.alg, JWT_SIGNING_ALGORITHM);
    assert_eq!(header.kid.as_deref(), Some("imported"));
    assert_eq!(verifier.verify(issued.token(), now).unwrap().owner(), OWNER);
}

#[tokio::test]
async fn serves_jwks_at_the_well_known_endpoint() {
    let service = TestFixture::service();
    let server = JwtServer::bind("127.0.0.1:0".parse().unwrap(), service.clone())
        .await
        .unwrap();
    let address = server.local_addr().unwrap();
    let task = tokio::spawn(server.serve());

    let response = reqwest::get(format!("http://{address}/.well-known/jwks.json"))
        .await
        .unwrap();
    let jwks: JwkSet = response.json().await.unwrap();
    task.abort();

    assert_eq!(jwks, service.jwks().clone());
}
