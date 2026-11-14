use platform_aws::PresignedPost;
use platform_errors::{PlatformError, PlatformResult};
use platform_run_store::RunRecord;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRequest {
    pub cwasm_get_url: String,
    pub cwasm_sha256: String,
    pub cwasm_size: u64,
    pub wasmtime_version: String,
    pub order_request_url: String,
    pub auth_service_url: String,
    pub owner: String,
    #[serde(flatten)]
    pub command: RunCommand,
    pub logs_post: PresignedPost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(
    tag = "command",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum RunCommand {
    Algorithm { strategy_name: String },
}

impl RunRequest {
    pub fn from_record(
        record: &RunRecord,
        cwasm_get_url: String,
        order_request_url: String,
        auth_service_url: String,
        logs_post: PresignedPost,
    ) -> Self {
        Self {
            cwasm_get_url,
            cwasm_sha256: record.cwasm_sha256.clone(),
            cwasm_size: record.cwasm_size,
            wasmtime_version: record.wasmtime_version.clone(),
            order_request_url,
            auth_service_url,
            owner: record.owner.clone(),
            command: RunCommand::Algorithm {
                strategy_name: record.strategy_name.clone(),
            },
            logs_post,
        }
    }

    pub fn log_key(record: &RunRecord) -> String {
        format!(
            "logs/{}/{}.log",
            record.algorithm_id,
            record.scheduled_at.format("%Y%m%dT%H%M%S%.3fZ")
        )
    }

    pub fn to_json(&self) -> PlatformResult<String> {
        serde_json::to_string(self).map_err(PlatformError::external)
    }
}
