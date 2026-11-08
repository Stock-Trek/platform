use crate::{AuthService, AuthServiceConfig, MemoryTokenAuditLog, MemoryTokenStore};
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

    let error = AuthService::new(Arc::new(MemoryTokenStore::default()), config);

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

    assert!(AuthService::new(Arc::new(MemoryTokenStore::default()), config).is_ok());
}

#[test]
fn rejects_rotation_overlap_longer_than_the_token_lifetime() {
    let mut config = ConfigFixture::config();
    config.rotation_overlap = config.token_lifetime + Duration::seconds(1);

    let error = AuthService::new(Arc::new(MemoryTokenStore::default()), config);

    assert!(matches!(
        error,
        Err(BpError::Auth(AuthError::InvalidField {
            field: "rotation_overlap",
            ..
        }))
    ));
}

#[test]
fn accepts_rotation_overlap_equal_to_the_token_lifetime() {
    let mut config = ConfigFixture::config();
    config.rotation_overlap = config.token_lifetime;

    assert!(AuthService::new(Arc::new(MemoryTokenStore::default()), config).is_ok());
}
