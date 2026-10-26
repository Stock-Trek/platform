#![recursion_limit = "256"]

use lambda_runtime::{Error, LambdaEvent, service_fn};
use metrics_cloudwatch_embedded::lambda::service::run;
use platform_config::tracing_setup::TracingSetup;
use platform_market_data_old::{env_config::EnvConfig, job::Job, job_executor::JobExecutor};
use platform_metrics::{descriptions::DescriptionsMetrics, market_data::MarketDataMetrics};
use platform_time::deadline::Deadline;
use serde_json::{Value, json};
use std::time::Instant;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let env_config = EnvConfig::from_env()?;
    TracingSetup::setup(&env_config.log_filter)?;
    let recorder = metrics_cloudwatch_embedded::Builder::new()
        .cloudwatch_namespace("platform/market-data")
        .with_dimension("function", env_config.lambda_function_name.clone())
        .lambda_cold_start_metric(platform_metrics::descriptions::COLD_STARTS_TOTAL)
        .init()?;
    DescriptionsMetrics::describe();
    info!("set up tracing");
    run(
        recorder,
        service_fn(move |event| {
            let env_config = env_config.clone();
            async move { function_handler(event, env_config).await }
        }),
    )
    .await
}

async fn function_handler(
    event: LambdaEvent<Value>,
    env_config: EnvConfig,
) -> Result<Value, Error> {
    let deadline = Deadline::new(event.context.deadline(), env_config.deadline_buffer);
    let job = Job::from_value(event.payload)?;
    info!(
        "running lambda function {} with {} job",
        env_config.lambda_function_name,
        job.mode()
    );
    let started_at = Instant::now();
    let result = JobExecutor::run_job(&env_config, &job, deadline).await;
    match &result {
        Ok(outcome) => MarketDataMetrics::record_job_success(
            job.mode(),
            outcome.is_incomplete(),
            started_at.elapsed(),
        ),
        Err(error) => MarketDataMetrics::record_job_error(
            job.mode(),
            error.error_type(),
            started_at.elapsed(),
        ),
    }
    let outcome = result?;
    Ok(json!({
        "status": outcome.status(),
        "mode": job.mode(),
        "windows_ingested": outcome.windows_ingested,
        "windows_compacted": outcome.windows_compacted,
        "windows_incomplete": outcome.windows_incomplete,
        "windows_deadline": outcome.windows_deadline,
        "compaction_targets": outcome.compaction_targets,
    }))
}
