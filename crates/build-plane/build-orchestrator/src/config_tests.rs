use crate::{
    aws_ops::s3_client::S3Config,
    config::{GitHubConfig, OrchestratorConfig},
};
use platform_config::DirectoryLimits;
use platform_github::HostAllowlist;
use platform_time::Timeouts;
use secrecy::SecretString;
use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

const PRIVATE_KEY: &str =
    "-----BEGIN RSA PRIVATE KEY-----\nsuper-secret-key\n-----END RSA PRIVATE KEY-----";

struct Fixture;

impl Fixture {
    fn orchestrator_config() -> OrchestratorConfig {
        let timeouts = Timeouts::from_durations(Duration::from_secs(5), Duration::from_secs(15));
        OrchestratorConfig {
            log_filter: "info".to_string(),
            lambda_function_name: "orchestrator".to_string(),
            deadline_buffer: Duration::from_secs(10),
            aws_timeouts: timeouts,
            github: GitHubConfig {
                app_id: 42,
                private_key: SecretString::from(PRIVATE_KEY),
                timeouts,
                allowed_hosts: HostAllowlist::from_hosts(["github.com"]).unwrap(),
                max_cached_installations: NonZeroUsize::new(10).unwrap(),
                directory_limits: DirectoryLimits::default(),
            },
            sqs_queue_url: "https://sqs.example.com/123/queue".to_string(),
            sqs_dedup_max_messages: NonZeroUsize::new(100).unwrap(),
            sqs_dedup_ttl: Duration::from_secs(60),
            sqs_process_concurrency: NonZeroUsize::new(4).unwrap(),
            s3: S3Config {
                user_artifacts_bucket: "artifacts".to_string(),
                max_source_archive_size_bytes: NonZeroU64::new(1024).unwrap(),
                max_artifact_size_bytes: NonZeroU64::new(2048).unwrap(),
                signed_url_expiry: Duration::from_secs(600),
                endpoint_url: None,
            },
            build_agent_function_name: "build-agent".to_string(),
        }
    }
}

#[test]
fn debug_redacts_the_github_app_private_key() {
    let debug = format!("{:?}", Fixture::orchestrator_config());

    assert!(!debug.contains("super-secret-key"));
    assert!(debug.contains("private_key: \"<redacted>\""));
}

#[test]
fn github_app_config_carries_the_private_key() {
    let app_config = Fixture::orchestrator_config().github_app_config();

    assert_eq!(app_config.app_id, 42);
    assert_eq!(app_config.private_key, PRIVATE_KEY);
}
