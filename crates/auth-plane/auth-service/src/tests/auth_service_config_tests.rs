use crate::tests::support_tests::AuthServiceBuilder;
use crate::{AuthService, AuthServiceConfig};
use chrono::Duration;
use platform_errors::{AuthError, BuildError, BuildResult};
struct ConfigFixture;

impl ConfigFixture {
    fn config() -> AuthServiceConfig {
        AuthServiceConfig {
            token_lifetime: Duration::days(30),
            rotation_overlap: Duration::days(7),
            signature_validity: Duration::minutes(5),
        }
    }

    fn service(config: AuthServiceConfig) -> BuildResult<AuthService> {
        AuthServiceBuilder::new().config(config).try_build()
    }
}

#[test]
fn rejects_rotation_overlap_longer_than_token_lifetime() {
    let mut config = ConfigFixture::config();
    config.rotation_overlap = config.token_lifetime + Duration::seconds(1);

    let error = ConfigFixture::service(config);

    assert!(matches!(
        error,
        Err(BuildError::Auth(AuthError::InvalidField {
            field: "rotation_overlap",
            ..
        }))
    ));
}

#[test]
fn accepts_rotation_overlap_equal_to_token_lifetime() {
    let mut config = ConfigFixture::config();
    config.rotation_overlap = config.token_lifetime;

    assert!(ConfigFixture::service(config).is_ok());
}

#[test]
fn rejects_sub_second_signature_validity() {
    let error = AuthServiceBuilder::new()
        .signature_validity(Duration::milliseconds(500))
        .try_build();

    assert!(matches!(
        error,
        Err(BuildError::Auth(AuthError::InvalidField {
            field: "signature_validity",
            ..
        }))
    ));
}

#[test]
fn accepts_one_second_signature_validity() {
    let result = AuthServiceBuilder::new()
        .signature_validity(Duration::seconds(1))
        .try_build();

    assert!(result.is_ok());
}
