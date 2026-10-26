use crate::{artifacts::Artifacts, s3::SignedPost, source::SourceLimits};
use platform_errors::{BpError, BpResult, TimeoutError};
use platform_messages::build_request::BuildRequest;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::time::timeout;

#[derive(Clone)]
pub struct Handler {
    build_timeout: Duration,
    failure_publish_budget: Duration,
    source_dir: PathBuf,
    build_cache_dir: PathBuf,
    source_limits: SourceLimits,
}

impl Handler {
    pub fn with_source_limits(
        build_timeout: Duration,
        failure_publish_budget: Duration,
        source_dir: PathBuf,
        build_cache_dir: PathBuf,
        source_limits: SourceLimits,
    ) -> Self {
        Self {
            build_timeout,
            failure_publish_budget,
            source_dir,
            build_cache_dir,
            source_limits,
        }
    }

    pub async fn handle(&self, request: BuildRequest, deadline: Instant) -> BpResult<()> {
        tracing::info!(
            git_host = %request.git_host,
            git_provider = %request.git_provider,
            repo_id = %request.repo_id,
            ref_name = %request.ref_name,
            ref_type = %request.ref_type,
            commit_hash = request.commit_hash.as_deref().unwrap_or("unknown"),
            "starting build"
        );
        let signed_post = SignedPost::from_build_request(&request.signed_post, self.build_timeout)?;
        let artifacts = Artifacts::with_source_limits(
            signed_post,
            request.signed_get_url,
            self.source_dir.clone(),
            self.build_cache_dir.clone(),
            self.source_limits,
        )?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let failure_budget = self.failure_publish_budget.min(remaining / 4);
        let build_budget = self
            .build_timeout
            .min(remaining.saturating_sub(failure_budget));
        let result = timeout(build_budget, async {
            let cwasm = artifacts.compile().await?;
            artifacts.publish(&cwasm).await
        })
        .await
        .unwrap_or(Err(BpError::Timeout(TimeoutError::BuildTimeout(
            self.build_timeout,
        ))));

        if let Err(error) = &result {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                tracing::error!(
                    error = %error,
                    "no time remaining to publish build failure"
                );
            } else {
                match timeout(remaining, artifacts.publish_failure(error)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(publish_error)) => tracing::error!(
                        error = %publish_error,
                        "failed to publish build failure artifacts"
                    ),
                    Err(_) => tracing::error!(
                        error = %error,
                        "timed out publishing build failure artifacts"
                    ),
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_messages::{build_request::BuildRequestSignedPost, sqs_message::SqsRefType};
    use std::collections::BTreeMap;

    fn request(signed_post: BuildRequestSignedPost) -> BuildRequest {
        BuildRequest {
            git_host: "github.com".to_string(),
            git_provider: "github".to_string(),
            repo_id: "42".to_string(),
            ref_name: "main".to_string(),
            ref_type: SqsRefType::Branch,
            commit_hash: None,
            signed_get_url: "https://example.com/source.zip".to_string(),
            signed_post,
        }
    }

    fn handler(timeout: Duration, failure_publish_budget: Duration) -> Handler {
        Handler::with_source_limits(
            timeout,
            failure_publish_budget,
            PathBuf::from("/unused-source"),
            PathBuf::from("/unused-cache"),
            SourceLimits::default(),
        )
    }

    #[tokio::test]
    async fn handle_rejects_invalid_signed_post_before_building() {
        let signed_post = BuildRequestSignedPost {
            url: "https://example.com/upload".to_string(),
            fields: BTreeMap::new(),
            max_artifact_size_bytes: None,
        };

        let timeout = Duration::from_secs(1);
        let failure_publish_budget = Duration::from_secs(30);
        let deadline = Instant::now() + timeout + failure_publish_budget;
        let error = handler(timeout, failure_publish_budget)
            .handle(request(signed_post), deadline)
            .await
            .unwrap_err();

        assert!(matches!(error, BpError::Config(_)));
    }

    #[test]
    fn failure_publication_budget_is_reserved_from_the_build_budget() {
        let remaining = Duration::from_secs(40);
        let failure_publish_budget = Duration::from_secs(30);
        let failure_budget = failure_publish_budget.min(remaining / 4);
        let build_budget = Duration::from_secs(300).min(remaining.saturating_sub(failure_budget));

        assert_eq!(failure_budget, Duration::from_secs(10));
        assert_eq!(build_budget, Duration::from_secs(30));
    }
}
