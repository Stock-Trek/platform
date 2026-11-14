use crate::source_record::SourceRecord;
use async_trait::async_trait;
use platform_errors::PlatformResult;

#[async_trait]
pub trait MarketSource: Send {
    async fn next_input(&mut self) -> PlatformResult<Option<SourceRecord>>;

    async fn ack(&mut self, _record: &SourceRecord) -> PlatformResult<()> {
        Ok(())
    }
}
