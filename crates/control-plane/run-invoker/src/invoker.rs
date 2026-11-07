use crate::config::RunInvokerConfig;
use crate::run_request::RunRequest;
use crate::runner::Runner;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::presigning::PresigningConfig;
use chrono::Utc;
use platform_aws::presigned_post::{PresignedPost, PresignedPostSigner};
use platform_config::env_names::S3_SIGNED_URL_EXPIRY_SECS;
use platform_errors::{BpError, BpResult, StorageError};
use platform_run_store::{RunRecord, RunStore};
use serde_json::json;
use std::time::SystemTime;
use tracing::{error, info};

pub struct RunInvoker {
    config: RunInvokerConfig,
    runner: Runner,
    store: RunStore,
    s3: S3Client,
    signer: PresignedPostSigner,
    credentials_provider: SharedCredentialsProvider,
}

impl RunInvoker {
    pub fn new(
        config: RunInvokerConfig,
        store: RunStore,
        s3: S3Client,
        runner: Runner,
        signer: PresignedPostSigner,
        credentials_provider: SharedCredentialsProvider,
    ) -> Self {
        Self {
            runner,
            config,
            store,
            s3,
            signer,
            credentials_provider,
        }
    }

    pub async fn run(self) -> BpResult<()> {
        info!(
            table = %self.config.table,
            artifact_bucket = %self.config.artifact_bucket,
            logs_bucket = %self.config.logs_bucket,
            runner_function = %self.config.runner_function_name,
            "run invoker cycle started"
        );
        self.run_cycle().await
    }

    async fn run_cycle(&self) -> BpResult<()> {
        let due = self
            .store
            .due_runs(Utc::now(), self.config.max_batch)
            .await?;
        info!(runs = due.len(), "checking the run store");
        for record in due {
            self.invoke(record).await;
        }
        Ok(())
    }

    async fn invoke(&self, record: RunRecord) {
        match self.store.claim(&record).await {
            Ok(true) => {}
            Ok(false) => {
                info!(
                    algorithm_id = %record.algorithm_id,
                    "skipped run claimed by another invoker"
                );
                return;
            }
            Err(error) => {
                error!(algorithm_id = %record.algorithm_id, %error, "failed to claim run");
                return;
            }
        }
        if let Err(error) = self.invoke_claimed(&record).await {
            error!(
                algorithm_id = %record.algorithm_id,
                %error,
                "failed to invoke the runner"
            );
        }
    }

    async fn invoke_claimed(&self, record: &RunRecord) -> BpResult<()> {
        let cwasm_get_url = self.presigned_get_url(&record.cwasm_key).await?;
        let logs_post = self.presigned_logs_post(record).await?;
        let request = RunRequest::from_record(
            record,
            cwasm_get_url,
            self.config.order_request_url.clone(),
            logs_post,
        );
        self.runner.invoke(&request.to_json()?).await?;
        info!(
            algorithm_id = %record.algorithm_id,
            log_key = %RunRequest::log_key(record),
            "runner invoked"
        );
        Ok(())
    }

    async fn presigned_get_url(&self, key: &str) -> BpResult<String> {
        let target = format!("s3://{}/{key}", self.config.artifact_bucket);
        let presigning_config = PresigningConfig::expires_in(self.config.signed_url_expiry)
            .map_err(|error| BpError::config_invalid_source(S3_SIGNED_URL_EXPIRY_SECS, error))?;
        let request = self
            .s3
            .get_object()
            .bucket(&self.config.artifact_bucket)
            .key(key)
            .presigned(presigning_config)
            .await
            .map_err(|error| BpError::s3("presign artifact download", target, error))?;
        Ok(request.uri().to_string())
    }

    async fn presigned_logs_post(&self, record: &RunRecord) -> BpResult<PresignedPost> {
        let credentials = self
            .credentials_provider
            .provide_credentials()
            .await
            .map_err(|error| BpError::Storage(StorageError::Credentials(error)))?;
        let log_key = RunRequest::log_key(record);
        self.signer.sign(
            &credentials,
            &log_key,
            json!({ "key": log_key.clone() }),
            &[],
            SystemTime::now(),
            self.config.signed_url_expiry,
        )
    }
}
