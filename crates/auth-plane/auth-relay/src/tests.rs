use crate::{AuthRelay, AuthRelayConfig, runner::AuthRelayRunner};
use async_trait::async_trait;
use chrono::Utc;
use platform_auth_service::{
    MemoryTokenStore, TokenAuditAction, TokenAuditEvent, TokenAuditLog, TokenStore,
    VaultTokenStoreConfig,
};
use platform_config::env_names::{AWS_CONNECT_TIMEOUT_SECS, AWS_OPERATION_TIMEOUT_SECS, RUST_LOG};
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_time::timeouts::Timeouts;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

const AUDIT_QUEUE_URL: &str = "AUTH_RELAY_AUDIT_QUEUE_URL";
const AUDIT_TIMEOUT_SECS: &str = "AUTH_RELAY_AUDIT_TIMEOUT_SECS";
const POLL_INTERVAL_SECS: &str = "AUTH_RELAY_POLL_INTERVAL_SECS";
const VAULT_ADDRESS: &str = "AUTH_RELAY_VAULT_ADDRESS";
const VAULT_MOUNT: &str = "AUTH_RELAY_VAULT_MOUNT";
const VAULT_TIMEOUT_SECS: &str = "AUTH_RELAY_VAULT_TIMEOUT_SECS";
const VAULT_TOKEN: &str = "AUTH_RELAY_VAULT_TOKEN";

const ALL_VARS: [(&str, &str); 10] = [
    (RUST_LOG, "info"),
    (AWS_CONNECT_TIMEOUT_SECS, "5"),
    (AWS_OPERATION_TIMEOUT_SECS, "15"),
    (VAULT_ADDRESS, "https://vault.example.com"),
    (VAULT_TOKEN, "vault-token"),
    (VAULT_MOUNT, "platform-tokens"),
    (VAULT_TIMEOUT_SECS, "10"),
    (AUDIT_QUEUE_URL, "https://sqs.example.com/123/audit"),
    (AUDIT_TIMEOUT_SECS, "5"),
    (POLL_INTERVAL_SECS, "30"),
];

struct ConfigFixture;

impl ConfigFixture {
    fn config(vars: &[(&str, &str)]) -> BpResult<AuthRelayConfig> {
        AuthRelayConfig::from_vars(&EnvVars::from_map(
            vars.iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
        ))
    }

    fn with(name: &str, value: &str) -> BpResult<AuthRelayConfig> {
        let vars: Vec<(&str, &str)> = ALL_VARS
            .iter()
            .copied()
            .map(|(variable, old)| {
                if variable == name {
                    (variable, value)
                } else {
                    (variable, old)
                }
            })
            .collect();
        Self::config(&vars)
    }
}

struct ToggleAudit {
    failing: AtomicBool,
    events: Mutex<Vec<TokenAuditEvent>>,
}

impl ToggleAudit {
    fn new() -> Self {
        Self {
            failing: AtomicBool::new(false),
            events: Mutex::new(Vec::new()),
        }
    }

    fn fail(&self) {
        self.failing.store(true, Ordering::SeqCst);
    }

    fn succeed(&self) {
        self.failing.store(false, Ordering::SeqCst);
    }

    fn events(&self) -> Vec<TokenAuditEvent> {
        self.events.lock().expect("audit lock").clone()
    }
}

#[async_trait]
impl TokenAuditLog for ToggleAudit {
    async fn record(&self, event: TokenAuditEvent) -> BpResult<()> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(BpError::auth_store(
                "audit",
                "relay-test",
                true,
                "sink unavailable".to_string(),
            ));
        }
        self.events.lock().expect("audit lock").push(event);
        Ok(())
    }
}

struct RelayFixture;

impl RelayFixture {
    async fn store() -> Arc<MemoryTokenStore> {
        let store = Arc::new(MemoryTokenStore::default());
        store
            .append_audit(
                TokenAuditEvent::new(TokenAuditAction::Issued, Utc::now())
                    .with_event_id("event-a")
                    .with_token("token-a", Some("owner-a")),
            )
            .await
            .expect("pending audit");
        store
    }

    fn config() -> AuthRelayConfig {
        AuthRelayConfig {
            log_filter: "info".to_string(),
            vault: VaultTokenStoreConfig::new(
                "https://vault.example.com",
                "vault-token",
                "platform-tokens",
                Duration::from_secs(10),
            )
            .expect("valid vault config"),
            audit_queue_url: "https://sqs.example.com/123/audit".to_string(),
            audit_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_secs(30),
            aws_timeouts: Timeouts::from_durations(Duration::from_secs(5), Duration::from_secs(15)),
        }
    }
}

#[test]
fn reads_every_value_from_the_environment() {
    let config = ConfigFixture::config(&ALL_VARS).expect("valid config");

    assert_eq!(config.log_filter, "info");
    assert_eq!(config.vault.address(), "https://vault.example.com");
    assert_eq!(config.vault.mount(), "platform-tokens");
    assert_eq!(config.vault.timeout(), Duration::from_secs(10));
    assert_eq!(config.audit_queue_url, "https://sqs.example.com/123/audit");
    assert_eq!(config.audit_timeout, Duration::from_secs(5));
    assert_eq!(config.poll_interval, Duration::from_secs(30));
    assert_eq!(
        config.aws_timeouts.connect.duration(),
        Some(Duration::from_secs(5))
    );
    assert_eq!(
        config.aws_timeouts.operation.duration(),
        Some(Duration::from_secs(15))
    );
}

#[test]
fn rejects_invalid_service_configuration() {
    for (name, value) in [
        (VAULT_ADDRESS, "not a url"),
        (VAULT_ADDRESS, "file:///tmp/vault"),
        (VAULT_MOUNT, "../sys"),
        (AUDIT_QUEUE_URL, "not a url"),
        (AUDIT_QUEUE_URL, "file:///tmp/audit"),
        (VAULT_TIMEOUT_SECS, "0"),
        (AUDIT_TIMEOUT_SECS, "0"),
        (POLL_INTERVAL_SECS, "0"),
        (AWS_CONNECT_TIMEOUT_SECS, "0"),
        (AWS_OPERATION_TIMEOUT_SECS, "0"),
    ] {
        assert!(ConfigFixture::with(name, value).is_err(), "{name}={value}");
    }
}

#[test]
fn requires_every_variable() {
    for (name, _) in ALL_VARS {
        let vars: Vec<(&str, &str)> = ALL_VARS
            .iter()
            .copied()
            .filter(|(variable, _)| *variable != name)
            .collect();
        let error = ConfigFixture::config(&vars).expect_err("missing variable");

        assert!(
            matches!(
                error,
                BpError::Config(platform_errors::ConfigError::Missing { name: missing })
                    if missing == name
            ),
            "{name}"
        );
    }
}

#[test]
fn rejects_empty_values() {
    for (name, _) in ALL_VARS {
        let vars: Vec<(&str, &str)> = ALL_VARS
            .iter()
            .copied()
            .map(|(variable, value)| {
                if variable == name {
                    (variable, "")
                } else {
                    (variable, value)
                }
            })
            .collect();
        let error = ConfigFixture::config(&vars).expect_err("empty variable");

        assert!(
            matches!(
                error,
                BpError::Config(platform_errors::ConfigError::Empty { name: empty })
                    if empty == name
            ),
            "{name}"
        );
    }
}

#[tokio::test]
async fn drains_pending_events_after_a_later_successful_attempt() {
    let store = RelayFixture::store().await;
    let audit = Arc::new(ToggleAudit::new());
    audit.fail();
    let relay = AuthRelay::new(store.clone(), audit.clone());

    assert!(relay.drain().await.is_err());
    assert_eq!(store.pending_audits().await.unwrap().len(), 1);

    audit.succeed();
    assert_eq!(relay.drain().await.unwrap(), 1);
    assert!(store.pending_audits().await.unwrap().is_empty());
    assert_eq!(audit.events().len(), 1);
    assert_eq!(audit.events()[0].event_id(), Some("event-a"));
}

#[tokio::test]
async fn runner_drains_once_without_waiting_for_the_poll_interval() {
    let store = RelayFixture::store().await;
    let audit = Arc::new(ToggleAudit::new());
    let runner = AuthRelayRunner::new(
        RelayFixture::config(),
        AuthRelay::new(store.clone(), audit.clone()),
    );

    runner.drain_once().await;

    assert!(store.pending_audits().await.unwrap().is_empty());
    assert_eq!(audit.events().len(), 1);
}
