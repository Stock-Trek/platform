use super::support_tests::TestFixture;
use crate::ApiToken;
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BpError};
use serde_json::{Value, json};

struct MetadataInvariantFixture;

impl MetadataInvariantFixture {
    fn value() -> Value {
        TestFixture::token("token-a").to_value()
    }

    fn assert_invalid(value: &Value, expected: &'static str) {
        let error = ApiToken::from_value("token-a", value).expect_err("value should be rejected");

        assert!(matches!(
            error,
            BpError::Auth(AuthError::InvalidField { field, .. }) if field == expected
        ));
    }
}

#[test]
fn rejects_revocation_metadata_without_a_timestamp() {
    let mut value = MetadataInvariantFixture::value();
    value["revocation_reason"] = json!("compromised");

    MetadataInvariantFixture::assert_invalid(&value, "revoked_at");
}

#[test]
fn rejects_revocation_metadata_without_a_reason() {
    let mut value = MetadataInvariantFixture::value();
    value["revoked_at"] = json!(Utc::now().to_rfc3339());

    MetadataInvariantFixture::assert_invalid(&value, "revocation_reason");
}

#[test]
fn rejects_rotation_metadata_without_an_overlap() {
    let mut value = MetadataInvariantFixture::value();
    value["replaced_by"] = json!("token-b");

    MetadataInvariantFixture::assert_invalid(&value, "rotation_overlap_until");
}

#[test]
fn rejects_rotation_overlap_without_a_replacement() {
    let mut value = MetadataInvariantFixture::value();
    value["rotation_overlap_until"] = json!((Utc::now() + Duration::minutes(1)).to_rfc3339());

    MetadataInvariantFixture::assert_invalid(&value, "replaced_by");
}

#[test]
fn rejects_invalid_rotation_lineage_ids() {
    let mut rotated_from = MetadataInvariantFixture::value();
    rotated_from["rotated_from"] = json!("../token-b");
    MetadataInvariantFixture::assert_invalid(&rotated_from, "rotated_from");

    let mut replaced_by = MetadataInvariantFixture::value();
    replaced_by["replaced_by"] = json!("../token-b");
    replaced_by["rotation_overlap_until"] = json!((Utc::now() + Duration::minutes(1)).to_rfc3339());
    MetadataInvariantFixture::assert_invalid(&replaced_by, "replaced_by");
}

#[test]
fn rejects_rotation_lineage_cycles() {
    let mut value = MetadataInvariantFixture::value();
    value["rotated_from"] = json!("token-b");
    value["replaced_by"] = json!("token-b");
    value["rotation_overlap_until"] = json!((Utc::now() + Duration::minutes(1)).to_rfc3339());
    MetadataInvariantFixture::assert_invalid(&value, "rotated_from");
}

#[test]
fn rejects_rotation_lineage_pointing_at_the_token_itself() {
    let mut replaced_by = MetadataInvariantFixture::value();
    replaced_by["replaced_by"] = json!("token-a");
    replaced_by["rotation_overlap_until"] = json!((Utc::now() + Duration::minutes(1)).to_rfc3339());
    MetadataInvariantFixture::assert_invalid(&replaced_by, "replaced_by");

    let mut rotated_from = MetadataInvariantFixture::value();
    rotated_from["rotated_from"] = json!("token-a");
    MetadataInvariantFixture::assert_invalid(&rotated_from, "rotated_from");
}
