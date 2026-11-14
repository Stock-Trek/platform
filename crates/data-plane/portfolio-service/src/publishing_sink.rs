use crate::{PORTFOLIO_UPDATE_SUBJECT, PortfolioService, PortfolioUpdate};
use async_trait::async_trait;
use platform_errors::PlatformResult;
use platform_exchange_gateway::{
    ConnectorPortfolioUpdate, PortfolioUpdateOutcome, PortfolioUpdateSink,
};
use platform_nats::EventPublisher;
use std::sync::Arc;

pub struct PublishingPortfolioUpdateSink {
    publisher: Arc<dyn EventPublisher>,
    service: Arc<PortfolioService>,
    subject: String,
}

impl PublishingPortfolioUpdateSink {
    pub fn new(publisher: Arc<dyn EventPublisher>, service: Arc<PortfolioService>) -> Self {
        Self {
            publisher,
            service,
            subject: PORTFOLIO_UPDATE_SUBJECT.to_string(),
        }
    }

    pub fn with_subject(
        publisher: Arc<dyn EventPublisher>,
        service: Arc<PortfolioService>,
        subject: impl Into<String>,
    ) -> Self {
        Self {
            publisher,
            service,
            subject: subject.into(),
        }
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }
}

#[async_trait]
impl PortfolioUpdateSink for PublishingPortfolioUpdateSink {
    async fn apply_update(
        &self,
        update: ConnectorPortfolioUpdate,
    ) -> PlatformResult<PortfolioUpdateOutcome> {
        let update = PortfolioUpdate::try_from(update)?;
        self.publisher
            .publish(&self.subject, &update.to_bytes()?)
            .await?;
        Ok(self.service.apply_portfolio_update(update))
    }
}
