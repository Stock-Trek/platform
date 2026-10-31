use super::*;
use aws_credential_types::Credentials;
use platform_aws::Region;
use platform_time::timeouts::Timeout;

fn s3_client() -> (S3, Credentials) {
    let credentials = Credentials::new(
        "AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        Some("token/with+chars=".to_string()),
        None,
        "test",
    );
    let sdk_config = SdkConfig::builder()
        .region(Region::new("us-east-1"))
        .credentials_provider(SharedCredentialsProvider::new(credentials.clone()))
        .build();
    let s3 = S3::from_sdk_config(
        &sdk_config,
        Timeouts {
            connect: Timeout::Disabled,
            operation: Timeout::Disabled,
        },
        S3Config {
            user_artifacts_bucket: "stock-trek-user-artifacts".to_string(),
            max_source_archive_size_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
            max_artifact_size_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
            signed_url_expiry: Duration::from_secs(900),
            endpoint_url: None,
        },
    )
    .unwrap();
    (s3, credentials)
}

#[tokio::test]
async fn presigns_get_url_with_sdk() {
    let (s3, _) = s3_client();
    let url = s3
        .signed_get_url(
            "github/github.com/42/7/heads/main/abc123/source/algorithms.zip",
            Deadline::new(
                std::time::SystemTime::now() + Duration::from_secs(30),
                Duration::ZERO,
            ),
        )
        .await
        .unwrap();

    assert!(url.starts_with(concat!(
        "https://stock-trek-user-artifacts.s3.us-east-1.amazonaws.com/",
        "github/github.com/42/7/heads/main/abc123/source/algorithms.zip?"
    )));
    assert!(url.contains("X-Amz-Expires=900"));
    assert!(url.contains("X-Amz-Signature="));
}
