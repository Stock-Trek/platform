use metrics_cloudwatch_embedded::{Builder, Collector};
use platform_errors::BuildResult;
use platform_metrics::descriptions::DescriptionsMetrics;
use std::io;
use std::ops::Deref;

pub struct ProcessMetrics {
    collector: &'static Collector,
}

impl ProcessMetrics {
    pub fn init(namespace: &str, service: &'static str) -> BuildResult<Self> {
        let builder = Builder::new()
            .cloudwatch_namespace(namespace.to_owned())
            .with_dimension("service", service);
        let builder = if tokio::runtime::Handle::try_current().is_ok() {
            builder.with_auto_flush()
        } else {
            builder
        };
        let collector = builder.init()?;
        DescriptionsMetrics::describe();
        Ok(Self { collector })
    }

    pub fn collector(&self) -> &'static Collector {
        self.collector
    }

    pub fn flush_to_stdout(&self) {
        if let Err(error) = self.collector.flush(io::stdout()) {
            tracing::error!(error = %error, "metrics flush failed");
        }
    }
}

impl Deref for ProcessMetrics {
    type Target = Collector;

    fn deref(&self) -> &Self::Target {
        self.collector
    }
}

impl Drop for ProcessMetrics {
    fn drop(&mut self) {
        self.flush_to_stdout();
    }
}
