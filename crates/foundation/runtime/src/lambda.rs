//! Lambda-specific bootstrap shared by the Lambda services.

use metrics_cloudwatch_embedded::Collector;
use platform_errors::BpResult;
use platform_metrics::descriptions::{COLD_STARTS_TOTAL, DescriptionsMetrics};

/// Installs the CloudWatch metrics recorder used by Lambda services.
pub struct LambdaMetrics;

impl LambdaMetrics {
    /// Creates the embedded CloudWatch recorder, enables the cold-start metric,
    /// and describes every platform metric.
    pub fn init(namespace: &str, function_name: impl Into<String>) -> BpResult<&'static Collector> {
        let recorder = metrics_cloudwatch_embedded::Builder::new()
            .cloudwatch_namespace(namespace.to_owned())
            .with_dimension("function", function_name.into())
            .lambda_cold_start_metric(COLD_STARTS_TOTAL)
            .init()?;
        DescriptionsMetrics::describe();
        Ok(recorder)
    }
}
