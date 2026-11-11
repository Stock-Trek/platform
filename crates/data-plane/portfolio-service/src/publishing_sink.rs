use crate::{
    PORTFOLIO_UPDATE_SUBJECT,
    feed::{PortfolioUpdateApply, PortfolioUpdateSink},
    portfolio::PortfolioUpdate,
};
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_nats::EventPublisher;
use std::sync::Arc;

pub struct PublishingPortfolioUpdateSink {
    publisher: Arc<dyn EventPublisher>,
    inner: Arc<dyn PortfolioUpdateSink>,
    subject: String,
}

impl PublishingPortfolioUpdateSink {
    pub fn new(publisher: Arc<dyn EventPublisher>, inner: Arc<dyn PortfolioUpdateSink>) -> Self {
        Self {
            publisher,
            inner,
            subject: PORTFOLIO_UPDATE_SUBJECT.to_string(),
        }
    }

    pub fn with_subject(
        publisher: Arc<dyn EventPublisher>,
        inner: Arc<dyn PortfolioUpdateSink>,
        subject: impl Into<String>,
    ) -> Self {
        Self {
            publisher,
            inner,
            subject: subject.into(),
        }
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }
}

#[async_trait]
impl PortfolioUpdateSink for PublishingPortfolioUpdateSink {
    async fn apply_update(&self, update: PortfolioUpdate) -> BpResult<PortfolioUpdateApply> {
        self.publisher
            .publish(&self.subject, &update.to_bytes()?)
            .await?;
        self.inner.apply_update(update).await
    }
}
