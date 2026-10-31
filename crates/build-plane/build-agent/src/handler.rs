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
mod tests;
