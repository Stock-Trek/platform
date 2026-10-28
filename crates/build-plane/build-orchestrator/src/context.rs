use crate::{
    aws_ops::{
        lambda::{Lambda, LambdaConfig},
        s3::S3,
    },
    config::OrchestratorConfig,
};
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_sqs::Client as SqsClient;
use platform_errors::BpResult;
use platform_github::github_auth::GitHubAuth;
use platform_sqs::{sqs::Sqs, sqs::SqsConfig, sqs_dedup::SqsDedup};
use std::time::Duration;

pub struct RsOrchestratorContext {
    pub lambda: Lambda,
    pub s3: S3,
    pub sqs: Sqs,
    pub github_auth: GitHubAuth,
    pub dedup: SqsDedup,
    pub sqs_process_concurrency: usize,
    pub deadline_buffer: Duration,
}

impl RsOrchestratorContext {
    pub async fn new(config: &OrchestratorConfig) -> BpResult<Self> {
        let aws_timeouts = config.aws_timeouts;
        let sdk_config = platform_aws::load_sdk_config(aws_timeouts).await;
        let lambda = Lambda::new(
            LambdaClient::new(&sdk_config),
            aws_timeouts,
            LambdaConfig::new(config.build_agent_function_name.clone()),
        );
        let s3 = S3::from_sdk_config(&sdk_config, aws_timeouts, config.s3.clone())?;
        let sqs = Sqs::new(
            SqsClient::new(&sdk_config),
            aws_timeouts,
            SqsConfig::new(config.sqs_queue_url.clone()),
        );
        let github_auth = GitHubAuth::with_directory_limits(
            config.github_app_config(),
            config.github.timeouts,
            config.github.allowed_hosts.clone(),
            config.github.max_cached_installations,
            config.github.directory_limits,
        )?;
        let dedup = SqsDedup::new(config.sqs_dedup_max_messages, config.sqs_dedup_ttl);
        Ok(Self {
            lambda,
            s3,
            sqs,
            github_auth,
            dedup,
            sqs_process_concurrency: config.sqs_process_concurrency.get(),
            deadline_buffer: config.deadline_buffer,
        })
    }
}
