use crate::{
    AuthService, AuthServiceConfig, MemoryReplayStore, MemoryTokenAuditLog, MemoryTokenStore,
};
use chrono::Duration;
use platform_errors::{AuthError, BpError};
use std::sync::Arc;

struct ConfigFixture;

impl ConfigFixture {
    fn config() -> AuthServiceConfig {
        AuthServiceConfig {
            token_lifetime: Duration::days(30),
            rotation_overlap: Duration::days(7),
            signature_validity: Duration::minutes(5),
            audit: Arc::new(MemoryTokenAuditLog::new(16)),
        }
    }
}

#[test]
fn rejects_sub_second_signature_validity() {
    let mut config = ConfigFixture::config();
    config.signature_validity = Duration::milliseconds(500);

    let error = AuthService::new(
        Arc::new(MemoryTokenStore::default()),
        config,
        Arc::new(MemoryReplayStore::default()),
    );

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "signature_validity",
            ..
        }))
    ));
}

#[test]
fn accepts_one_second_signature_validity() {
    let mut config = ConfigFixture::config();
    config.signature_validity = Duration::seconds(1);

    assert!(
        AuthService::new(
            Arc::new(MemoryTokenStore::default()),
            config,
            Arc::new(MemoryReplayStore::default()),
        )
        .is_ok()
    );
}
