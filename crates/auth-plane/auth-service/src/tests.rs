use crate::{ApiToken, AuthService, MemoryTokenStore, OrderRequestSigner, VaultTokenStoreConfig};
use chrono::Utc;
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

#[tokio::test]
async fn signs_order_requests_with_the_stored_token_secret() {
    let service = service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();

    let signed = service
        .sign_order_request(token.id(), ORDER_REQUEST)
        .await
        .unwrap();

    assert_eq!(signed.token_id(), token.id());
    assert_eq!(signed.signature().len(), 64);
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
        .sign_order_request(token.id(), ORDER_REQUEST)
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
    let service = service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    let signed = service
        .sign_order_request(token.id(), ORDER_REQUEST)
        .await
        .unwrap();
    let tampered = format!("0{}", &signed.signature()[1..]);

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, &tampered)
        .await
        .unwrap();

    assert!(!verified);
}

#[tokio::test]
async fn signing_an_unknown_token_fails() {
    let service = service();

    let error = service.sign_order_request("ghost", ORDER_REQUEST).await;

    assert!(matches!(error, Err(BpError::Auth(AuthError::UnknownToken { .. }))));
}

#[tokio::test]
async fn signing_after_deletion_fails() {
    let service = service();
    let token = service.add_token("token-a", "demo-owner").await.unwrap();
    service.delete_token(token.id()).await.unwrap();

    let error = service
        .sign_order_request(token.id(), ORDER_REQUEST)
        .await;

    assert!(matches!(error, Err(BpError::Auth(AuthError::UnknownToken { .. }))));
}

#[tokio::test]
async fn adding_a_duplicate_token_fails() {
    let service = service();
    service.add_token("token-a", "demo-owner").await.unwrap();

    let error = service.add_token("token-a", "demo-owner").await;

    assert!(matches!(error, Err(BpError::Auth(AuthError::TokenExists { .. }))));
}

#[tokio::test]
async fn deleting_a_missing_token_fails() {
    let service = service();

    let error = service.delete_token("ghost").await;

    assert!(matches!(error, Err(BpError::Auth(AuthError::UnknownToken { .. }))));
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

    assert!(matches!(error, Err(BpError::Auth(AuthError::InvalidField { .. }))));
}

#[test]
fn rejects_missing_vault_value_fields() {
    let error = ApiToken::from_value("token-a", &json!({"owner": "demo-owner"}));

    assert!(matches!(error, Err(BpError::Auth(AuthError::InvalidField { .. }))));
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
fn created_at_is_recorded() {
    let before = Utc::now();
    let token = token("token-a");
    let after = Utc::now();

    assert!(token.created_at() >= before && token.created_at() <= after);
}
