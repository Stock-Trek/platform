use crate::{PORTFOLIO_UPDATE_SUBJECT, PortfolioService, PortfolioUpdate, PortfolioUpdateOutcome};
use async_trait::async_trait;
use platform_errors::BuildResult;
use platform_exchange_gateway::connector::{
    ConnectorPortfolioUpdate, PortfolioUpdateApply, PortfolioUpdateSink,
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

    fn apply_outcome(outcome: PortfolioUpdateOutcome) -> PortfolioUpdateApply {
        match outcome {
            PortfolioUpdateOutcome::Applied => PortfolioUpdateApply::Applied,
            PortfolioUpdateOutcome::IgnoredUntracked => PortfolioUpdateApply::IgnoredUntracked,
            PortfolioUpdateOutcome::IgnoredStale => PortfolioUpdateApply::IgnoredStale,
        }
    }
}

#[async_trait]
impl PortfolioUpdateSink for PublishingPortfolioUpdateSink {
    async fn apply_update(
        &self,
        update: ConnectorPortfolioUpdate,
    ) -> BuildResult<PortfolioUpdateApply> {
        let update = PortfolioUpdate::try_from(update)?;
        self.publisher
            .publish(&self.subject, &update.to_bytes()?)
            .await?;
        Ok(Self::apply_outcome(
            self.service.apply_portfolio_update(update),
        ))
    }
}
