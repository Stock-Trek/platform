use crate::writer_lease::WriterLease;
use async_trait::async_trait;
use platform_errors::BuildResult;

#[async_trait]
pub(crate) trait LeaseWatchStream: Send {
    async fn next(&mut self) -> BuildResult<Option<WriterLease>>;
}

pub struct LeaseWatch {
    inner: Box<dyn LeaseWatchStream>,
}

impl LeaseWatch {
    pub(crate) fn new(inner: impl LeaseWatchStream + 'static) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }

    pub async fn next(&mut self) -> BuildResult<Option<WriterLease>> {
        self.inner.next().await
    }
}
