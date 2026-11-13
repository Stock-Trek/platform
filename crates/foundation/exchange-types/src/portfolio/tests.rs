use crate::portfolio::{AuthError, Credential};
use std::time::{Duration, Instant};

#[test]
fn credential_reports_expiration_and_remaining_lifetime() {
    let now = Instant::now();
    let credential = Credential::new("token", now, Some(now + Duration::from_secs(10)));
    assert!(!credential.is_expired(now));
    assert!(credential.is_expired(now + Duration::from_secs(10)));
    assert!(credential.expires_within(now, Duration::from_secs(10)));
    assert!(!credential.expires_within(now, Duration::from_secs(9)));
    assert_eq!(
        credential.lifetime_remaining(now),
        Some(Duration::from_secs(10))
    );
    assert!(credential.renewable);
}

#[test]
fn credential_without_expiry_never_expires() {
    let now = Instant::now();
    let credential = Credential::new("token", now, None).with_renewable(false);
    assert!(!credential.is_expired(now + Duration::from_hours(1)));
    assert!(!credential.expires_within(now, Duration::ZERO));
    assert_eq!(credential.lifetime_remaining(now), None);
    assert!(!credential.renewable);
}

#[test]
fn auth_errors_keep_their_message_and_category() {
    let error = AuthError::Bind("missing token".to_string());
    assert_eq!(error.message(), "missing token");
    assert_eq!(
        error.to_string(),
        "failed to bind credential: missing token"
    );
    assert_eq!(AuthError::Expired.to_string(), "credential expired");
}
