use super::*;

impl PresignedPost {
    pub(crate) fn for_test(
        url: String,
        fields: BTreeMap<String, String>,
        max_artifact_size_bytes: NonZeroU64,
    ) -> Self {
        Self {
            url,
            fields,
            max_artifact_size_bytes,
        }
    }
}

use serde_json::{Value, json};

#[test]
fn signs_post_policy_with_session_token() {
    let credentials = Credentials::new(
        "AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        Some("token/with+chars=".to_string()),
        None,
        "test",
    );
    let signer = PresignedPostSigner::new(
        "stock-trek-user-artifacts".to_string(),
        "us-east-1".to_string(),
        None,
        NonZeroU64::new(123).unwrap(),
    );
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_704_164_645);

    let post = signer
        .sign(&credentials, "artifacts/42/", now, Duration::from_secs(900))
        .unwrap();

    assert_eq!(
        post.url,
        "https://stock-trek-user-artifacts.s3.us-east-1.amazonaws.com/"
    );
    assert_eq!(post.fields["key"], "artifacts/42/${filename}");
    assert_eq!(post.fields["x-amz-date"], "20240102T030405Z");
    assert_eq!(
        post.fields["x-amz-credential"],
        "AKIAIOSFODNN7EXAMPLE/20240102/us-east-1/s3/aws4_request"
    );
    assert_eq!(post.fields["x-amz-security-token"], "token/with+chars=");
    assert_eq!(post.fields["x-amz-signature"].len(), 64);
    assert_eq!(post.max_artifact_size_bytes().get(), 123);

    let policy: Value =
        serde_json::from_slice(&BASE64.decode(&post.fields["policy"]).unwrap()).unwrap();
    assert_eq!(policy["expiration"], "2024-01-02T03:19:05.000Z");
    assert_eq!(
        policy["conditions"][5],
        json!(["content-length-range", 0, 123])
    );
    assert_eq!(
        policy["conditions"][6],
        json!({ "x-amz-security-token": "token/with+chars=" })
    );
}
