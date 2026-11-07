use crate::{ApiToken, AuthService, MemoryTokenStore, OrderRequestSigner, VaultTokenStoreConfig};
use chrono::{Duration as ChronoDuration, Utc};
use platform_errors::{AuthError, BpError};
use secrecy::{ExposeSecret, SecretString};
use serde_json::json;
use std::sync::Arc;

const ORDER_REQUEST: &[u8] = br#"{"limit":{"base_dto":"BTC","quote_dto":"USDC","side":"buy"}}"#;

fn token(id: &str) -> ApiToken {
    ApiToken::generate(id, "demo-owner").unwrap()
}

fn service() -> AuthService {
    AuthService::new(Arc::new(MemoryTokenStore::new()))
}

fn tampered_signature(signature: &str) -> String {
    let mut tampered = signature.to_string();
    let replacement = if signature.ends_with('0') { "1" } else { "0" };
    tampered.replace_range(tampered.len() - 1.., replacement);
    tampered
}

#[tokio::test]
async fn signs_order_requests_with_the_stored_token_secret() {
    let service = service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();

    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await
        .unwrap();

    assert_eq!(signed.token_id(), token.id());
    assert!(signed.signature().starts_with("v1."));
    assert_eq!(signed.raw_signature().len(), 64);
}

#[tokio::test]
async fn signs_order_requests_with_the_secret_after_the_request() {
    let service = service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();

    let signed = service
        .sign_order_request(token.id(), ORDER_REQUEST, token.secret())
        .await
        .unwrap();

    assert_eq!(signed.token_id(), token.id());
}

#[tokio::test]
async fn signs_the_rfc4231_reference_vector() {
    let signer = OrderRequestSigner::new(&SecretString::from("key")).unwrap();

    let signature = signer
        .sign(b"The quick brown fox jumps over the lazy dog")
        .unwrap();

    assert_eq!(
        signature,
        "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
    );
}

#[tokio::test]
async fn verifies_a_matching_signature() {
    let service = service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await
        .unwrap();

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature())
        .await
        .unwrap();

    assert!(verified);
}

#[tokio::test]
async fn rejects_a_tampered_signature() {
    for _ in 0..32 {
        let service = service();
        let token = service.add_token("token-a", "demo-owner").await.unwrap();
        let signed = service
            .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
            .await
            .unwrap();
        let tampered = tampered_signature(signed.signature());

        let verified = service
            .verify_order_request(token.id(), ORDER_REQUEST, &tampered)
            .await
            .unwrap();

        assert!(!verified);
    }
}

#[tokio::test]
async fn rejects_a_tampered_request() {
    let service = service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await
        .unwrap();

    let verified = service
        .verify_order_request(token.id(), br#"{"side":"sell"}"#, signed.signature())
        .await
        .unwrap();

    assert!(!verified);
}

#[tokio::test]
async fn rejects_a_replayed_signature() {
    let service = service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await
        .unwrap();

    let first = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature())
        .await
        .unwrap();
    let second = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature())
        .await
        .unwrap();

    assert!(first);
    assert!(!second);
}

#[tokio::test]
async fn rejects_an_expired_signature() {
    let service = service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    let issued_at = Utc::now() - ChronoDuration::minutes(10);
    let signed = service
        .sign_order_request_at(
            token.id(),
            token.secret().expose_secret(),
            ORDER_REQUEST,
            issued_at,
        )
        .await
        .unwrap();

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.signature())
        .await
        .unwrap();

    assert!(!verified);
}

#[tokio::test]
async fn signing_requires_the_token_secret() {
    let service = service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();

    let error = service
        .sign_order_request(token.id(), "wrong-secret", ORDER_REQUEST)
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[tokio::test]
async fn signing_an_unknown_token_fails() {
    let service = service();

    let error = service
        .sign_order_request("ghost", "secret", ORDER_REQUEST)
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn signing_after_deletion_fails() {
    let service = service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    service.delete_token(token.id()).await.unwrap();

    let error = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST)
        .await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::UnknownToken { .. }))
    ));
}

#[tokio::test]
async fn rejects_path_traversal_token_ids_before_store_access() {
    let service = service();

    for id in ["../metadata", "../../../sys/health", "not/a/path", ""] {
        let error = service
            .sign_order_request(id, "secret", ORDER_REQUEST)
            .await;
        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField { .. }))
        ));
        let error = service.token(id).await;
        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField { .. }))
        ));
        let error = service.delete_token(id).await;
        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField { .. }))
        ));
        let error = service
            .verify_order_request(id, ORDER_REQUEST, "v1.0.0.nonce.token.signature")
            .await;
        assert!(matches!(
            error,
            Err(BpError::Auth(AuthError::InvalidField { .. }))
        ));
    }
}

#[tokio::test]
async fn adding_a_duplicate_token_fails_without_replacing_it() {
    let service = service();
    let original = service.add_token("token-a", "demo-owner").await.unwrap();

    let error = service.add_token("token-a", "other-owner").await;

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::TokenExists { .. }))
    ));
    let stored = service.token("token-a").await.unwrap();
    assert_eq!(stored.owner(), original.owner());
    assert_eq!(
        stored.secret().expose_secret(),
        original.secret().expose_secret()
    );
}

#[tokio::test]
async fn deleting_a_missing_token_is_idempotent() {
    let service = service();

    service.delete_token("ghost").await.unwrap();
}

#[tokio::test]
async fn token_ids_are_listed_sorted() {
    let service = service();
    service.add_token("token-b", "demo-owner").await.unwrap();
    service.add_token("token-a", "demo-owner").await.unwrap();

    let ids = service.token_ids().await.unwrap();

    assert_eq!(ids, vec!["token-a".to_string(), "token-b".to_string()]);
}

#[tokio::test]
async fn each_token_gets_a_distinct_alphanumeric_secret() {
    let first = token("token-a");
    let second = token("token-b");

    let first_secret = first.secret().expose_secret();
    let second_secret = second.secret().expose_secret();
    assert_ne!(first_secret, second_secret);
    assert_eq!(first_secret.len(), 32);
    assert!(first_secret.chars().all(|c| c.is_ascii_alphanumeric()));
}

#[test]
fn secrets_are_redacted_in_debug_output() {
    let token = token("token-a");

    assert!(!format!("{token:?}").contains(token.secret().expose_secret()));
}

#[test]
fn round_trips_through_a_vault_value() {
    let token = token("token-a");
    let value = token.to_value();

    let restored = ApiToken::from_value(token.id(), &value).unwrap();

    assert_eq!(restored.id(), token.id());
    assert_eq!(restored.owner(), token.owner());
    assert_eq!(restored.created_at(), token.created_at());
    assert_eq!(
        restored.secret().expose_secret(),
        token.secret().expose_secret()
    );
}

#[test]
fn rejects_invalid_token_ids() {
    let error = ApiToken::generate("not/a/path", "demo-owner");

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[test]
fn rejects_missing_vault_value_fields() {
    let error = ApiToken::from_value("token-a", &json!({"owner": "demo-owner"}));

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField { .. }))
    ));
}

#[test]
fn vault_config_defaults_to_a_ten_second_timeout() {
    let config = VaultTokenStoreConfig::new(
        "https://vault.example.com",
        SecretString::from("vault-token"),
        "platform-tokens",
    );

    assert_eq!(config.timeout().as_secs(), 10);
    assert_eq!(config.address(), "https://vault.example.com");
    assert_eq!(config.mount(), "platform-tokens");
}

#[test]
fn vault_config_rejects_invalid_addresses_and_mounts() {
    let invalid_address = VaultTokenStoreConfig::new(
        "not a url",
        SecretString::from("vault-token"),
        "platform-tokens",
    );
    assert!(invalid_address.validate().is_err());

    let invalid_mount = VaultTokenStoreConfig::new(
        "https://vault.example.com",
        SecretString::from("vault-token"),
        "../sys",
    );
    assert!(invalid_mount.validate().is_err());
}

#[test]
fn created_at_is_recorded() {
    let before = Utc::now();
    let token = token("token-a");
    let after = Utc::now();

    assert!(token.created_at() >= before && token.created_at() <= after);
}
