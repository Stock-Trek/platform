use crate::source::SourceMessage;
use async_trait::async_trait;
use platform_errors::PlatformResult;

#[async_trait]
pub trait MessageSource<T: Send + Sync>: Send + Sync {
    async fn next(&self) -> PlatformResult<Option<SourceMessage<T>>>;

    async fn checkpoint(&self, sequence: u64) -> PlatformResult<()>;

    async fn acknowledge(&self, message: &SourceMessage<T>) -> PlatformResult<()>;
}
