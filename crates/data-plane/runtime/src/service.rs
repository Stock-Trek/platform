//! Stable service identity and logging bootstrap.
//!
//! Individual services still own their configuration and wiring, but every
//! process starts with the same service name and logging setup.

use crate::{Runtime, lambda::LambdaMetrics};
use metrics_cloudwatch_embedded::Collector;
use platform_errors::BpResult;
use tracing::info;

/// The identity and shared bootstrap of a running service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Service {
    name: &'static str,
}

impl Service {
    /// Creates a service identity from its stable process name.
    pub const fn new(name: &'static str) -> Self {
        Self { name }
    }

    /// Returns the stable service name used for logging and client identities.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Installs the global tracing subscriber for this service.
    pub fn init_logging(&self, log_filter: &str) -> BpResult<()> {
        Runtime::init_logging(log_filter)?;
        info!(service = self.name, "service initialised");
        Ok(())
    }

    /// Installs the Lambda CloudWatch metrics recorder for this service.
    pub fn init_lambda_metrics(
        &self,
        namespace: &str,
        function_name: impl Into<String>,
    ) -> BpResult<&'static Collector> {
        LambdaMetrics::init(namespace, function_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_the_configured_service_name() {
        let service = Service::new("platform-test");

        assert_eq!(service.name(), "platform-test");
    }
}
