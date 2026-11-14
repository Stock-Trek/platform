use crate::{HealthConfig, HealthServer, HealthState};
use platform_errors::{PlatformError, PlatformResult};
use std::future::Future;

pub struct HealthRunner {
    state: HealthState,
    server: HealthServer,
}

impl HealthRunner {
    pub async fn from_env() -> PlatformResult<Self> {
        Self::from_service(None).await
    }

    pub async fn from_service_env(service: &str) -> PlatformResult<Self> {
        Self::from_service(Some(service)).await
    }

    pub fn state(&self) -> &HealthState {
        &self.state
    }

    pub fn mark_ready(&self) {
        self.state.mark_ready();
    }

    pub fn mark_not_ready(&self) {
        self.state.mark_not_ready();
    }

    pub async fn run<F>(self, work: F) -> PlatformResult<()>
    where
        F: Future<Output = PlatformResult<()>>,
    {
        let Self { state, server } = self;
        let mut server = tokio::spawn(server.serve());
        tokio::pin!(work);
        let result = tokio::select! {
            result = &mut work => result,
            joined = &mut server => match joined {
                Ok(Ok(())) => work.await,
                Ok(Err(error)) => Err(error),
                Err(error) => Err(PlatformError::external(error)),
            },
        };
        server.abort();
        let _ = server.await;
        state.mark_not_ready();
        result
    }

    async fn from_service(service: Option<&str>) -> PlatformResult<Self> {
        let state = HealthState::new();
        let config = HealthConfig::lookup(service)?;
        let server = HealthServer::from_config(config, state.clone()).await?;
        Ok(Self { state, server })
    }
}
