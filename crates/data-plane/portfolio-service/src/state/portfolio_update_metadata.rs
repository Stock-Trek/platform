use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct PortfolioUpdateMetadata {
    version: u64,
    occurred_at: DateTime<Utc>,
    exchange_time: Option<DateTime<Utc>>,
    applied_at: DateTime<Utc>,
}

impl PortfolioUpdateMetadata {
    pub fn new(
        version: u64,
        occurred_at: DateTime<Utc>,
        exchange_time: Option<DateTime<Utc>>,
        applied_at: DateTime<Utc>,
    ) -> Self {
        Self {
            version,
            occurred_at,
            exchange_time,
            applied_at,
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    pub fn exchange_time(&self) -> Option<DateTime<Utc>> {
        self.exchange_time
    }

    pub fn applied_at(&self) -> DateTime<Utc> {
        self.applied_at
    }
}
