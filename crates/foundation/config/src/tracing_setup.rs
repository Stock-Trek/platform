use platform_errors::{PlatformError, PlatformResult};
use tracing_subscriber::{EnvFilter, fmt::Subscriber};

pub struct TracingSetup;

impl TracingSetup {
    /// Installs the global tracing subscriber using an already parsed filter.
    ///
    /// The process configuration validates `RUST_LOG` before calling this, so
    /// startup fails fast when the value is missing or invalid.
    pub fn setup(filter: &str) -> PlatformResult<()> {
        let filter = EnvFilter::try_new(filter)
            .map_err(|error| PlatformError::config_invalid_source(EnvFilter::DEFAULT_ENV, error))?;
        let subscriber = Subscriber::builder()
            .with_ansi(false)
            .with_env_filter(filter)
            .finish();
        tracing::subscriber::set_global_default(subscriber)?;
        Ok(())
    }
}
