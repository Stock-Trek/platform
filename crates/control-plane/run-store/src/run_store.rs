use crate::RunRecord;
use crate::run_record::{ALGORITHM_ID, COMPLETED_AT, ERROR, SCHEDULED_AT, STATUS};
use crate::run_status::RunStatus;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{DateTime, Utc};
use platform_errors::{BuildError, BuildResult};

/// Name of the global secondary index that makes runs searchable by status.
pub const RUN_STATUS_INDEX: &str = "status-index";

/// DynamoDB-backed store of scheduled algorithm runs.
pub struct RunStore {
    client: Client,
    table: String,
}

impl RunStore {
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }

    /// Stores a new pending run, refusing to overwrite an existing one.
    pub async fn schedule(&self, record: &RunRecord) -> BuildResult<()> {
        let request = self
            .client
            .put_item()
            .table_name(&self.table)
            .set_item(Some(record.to_item()))
            .condition_expression("attribute_not_exists(#algorithm_id)")
            .expression_attribute_names("#algorithm_id", ALGORITHM_ID);
        match request.send().await {
            Ok(_) => Ok(()),
            Err(error) => {
                let conflict = error
                    .as_service_error()
                    .is_some_and(PutItemError::is_conditional_check_failed_exception);
                if conflict {
                    Err(BuildError::external(format!(
                        "run of algorithm {} at {} is already scheduled",
                        record.algorithm_id,
                        RunRecord::format_timestamp(record.scheduled_at),
                    )))
                } else {
                    Err(BuildError::external(error))
                }
            }
        }
    }

    /// Returns pending runs scheduled at or before `now`, oldest first.
    pub async fn due_runs(&self, now: DateTime<Utc>, limit: usize) -> BuildResult<Vec<RunRecord>> {
        let limit = i32::try_from(limit)
            .map_err(|_| BuildError::config_invalid("limit", "must fit in an i32"))?;
        let output = self
            .client
            .query()
            .table_name(&self.table)
            .index_name(RUN_STATUS_INDEX)
            .key_condition_expression("#status = :pending AND #scheduled_at <= :now")
            .expression_attribute_names("#status", STATUS)
            .expression_attribute_names("#scheduled_at", SCHEDULED_AT)
            .expression_attribute_values(
                ":pending",
                AttributeValue::S(RunStatus::Pending.as_str().to_string()),
            )
            .expression_attribute_values(
                ":now",
                AttributeValue::S(RunRecord::format_timestamp(now)),
            )
            .limit(limit)
            .send()
            .await
            .map_err(|error: SdkError<QueryError>| BuildError::external(error))?;
        output.items().iter().map(RunRecord::from_item).collect()
    }

    /// Marks a pending run as running, returning `false` when another
    /// invocation claimed it first.
    pub async fn claim(&self, record: &RunRecord) -> BuildResult<bool> {
        let request = self
            .client
            .update_item()
            .table_name(&self.table)
            .key(ALGORITHM_ID, AttributeValue::S(record.algorithm_id.clone()))
            .key(
                SCHEDULED_AT,
                AttributeValue::S(RunRecord::format_timestamp(record.scheduled_at)),
            )
            .update_expression("SET #status = :running")
            .condition_expression("#status = :pending")
            .expression_attribute_names("#status", STATUS)
            .expression_attribute_values(
                ":pending",
                AttributeValue::S(RunStatus::Pending.as_str().to_string()),
            )
            .expression_attribute_values(
                ":running",
                AttributeValue::S(RunStatus::Running.as_str().to_string()),
            );
        match request.send().await {
            Ok(_) => Ok(true),
            Err(error) => {
                let claimed = error
                    .as_service_error()
                    .is_some_and(UpdateItemError::is_conditional_check_failed_exception);
                if claimed {
                    Ok(false)
                } else {
                    Err(BuildError::external(error))
                }
            }
        }
    }

    /// Marks a claimed run as completed.
    pub async fn complete(&self, record: &RunRecord, completed_at: DateTime<Utc>) -> BuildResult<()> {
        self.finish(record, RunStatus::Completed, None, completed_at)
            .await
    }

    /// Marks a claimed run as failed, recording `error` as the reason.
    pub async fn fail(
        &self,
        record: &RunRecord,
        error: &str,
        completed_at: DateTime<Utc>,
    ) -> BuildResult<()> {
        self.finish(record, RunStatus::Failed, Some(error), completed_at)
            .await
    }

    async fn finish(
        &self,
        record: &RunRecord,
        status: RunStatus,
        error: Option<&str>,
        completed_at: DateTime<Utc>,
    ) -> BuildResult<()> {
        let update_expression = match error {
            Some(_) => "SET #status = :status, #completed_at = :at, #error = :error",
            None => "SET #status = :status, #completed_at = :at REMOVE #error",
        };
        let mut request = self
            .client
            .update_item()
            .table_name(&self.table)
            .key(ALGORITHM_ID, AttributeValue::S(record.algorithm_id.clone()))
            .key(
                SCHEDULED_AT,
                AttributeValue::S(RunRecord::format_timestamp(record.scheduled_at)),
            )
            .update_expression(update_expression)
            .expression_attribute_names("#status", STATUS)
            .expression_attribute_names("#completed_at", COMPLETED_AT)
            .expression_attribute_names("#error", ERROR)
            .expression_attribute_values(":status", AttributeValue::S(status.as_str().to_string()))
            .expression_attribute_values(
                ":at",
                AttributeValue::S(RunRecord::format_timestamp(completed_at)),
            );
        if let Some(error) = error {
            request =
                request.expression_attribute_values(":error", AttributeValue::S(error.to_string()));
        }
        request.send().await.map_err(BuildError::external)?;
        Ok(())
    }
}
