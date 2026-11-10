use crate::{
    ApiToken, MemoryTokenStore, TokenAuditEvent, TokenMutation, TokenStore,
    sqs_token_audit_log::AuditQueue,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::{BpResult, SqsError};
use platform_time::deadline::Deadline;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub(crate) struct MockAuditQueue {
    batches: Mutex<Vec<Vec<TokenAuditEvent>>>,
    fail: AtomicBool,
}

impl MockAuditQueue {
    pub(crate) fn new() -> Self {
        Self {
            batches: Mutex::new(Vec::new()),
            fail: AtomicBool::new(false),
        }
    }

    pub(crate) fn fail(&self) {
        self.fail.store(true, Ordering::SeqCst);
    }

    pub(crate) fn batches(&self) -> Vec<Vec<TokenAuditEvent>> {
        self.batches.lock().clone()
    }

    pub(crate) fn events(&self) -> Vec<TokenAuditEvent> {
        self.batches.lock().iter().flatten().cloned().collect()
    }
}

#[async_trait]
impl AuditQueue for MockAuditQueue {
    async fn deliver(&self, events: &[TokenAuditEvent], _deadline: Deadline) -> BpResult<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(SqsError::MissingMessageBody.into());
        }
        self.batches.lock().push(events.to_vec());
        Ok(())
    }
}

pub(crate) struct RecreateOnSecondGet {
    inner: MemoryTokenStore,
    replacement: ApiToken,
    gets: AtomicUsize,
}

impl RecreateOnSecondGet {
    pub(crate) fn new(replacement: ApiToken) -> Self {
        Self {
            inner: MemoryTokenStore::default(),
            replacement,
            gets: AtomicUsize::new(0),
        }
    }

    pub(crate) async fn put(&self, token: &ApiToken) -> BpResult<()> {
        self.inner.put(token).await
    }

    async fn recreate_if_due(&self, id: &str) -> BpResult<()> {
        if self.gets.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
            self.inner.delete(id).await?;
            self.inner.put(&self.replacement).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl TokenStore for RecreateOnSecondGet {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        self.inner.put(token).await
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        self.recreate_if_due(id).await?;
        self.inner.get(id).await
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        self.inner.delete(id).await
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        self.inner.ids().await
    }

    async fn update(&self, expected: &ApiToken, token: &ApiToken) -> BpResult<()> {
        self.inner.update(expected, token).await
    }

    async fn commit(&self, mutation: TokenMutation, event: TokenAuditEvent) -> BpResult<bool> {
        match mutation {
            TokenMutation::Create(token) => {
                self.inner.commit(TokenMutation::Create(token), event).await
            }
            TokenMutation::Update { expected, token } => {
                self.update(&expected, &token).await?;
                self.inner.append_audit(event).await?;
                Ok(true)
            }
            TokenMutation::Delete { expected } => {
                if self.delete_if(&expected).await? {
                    self.inner.append_audit(event).await?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    async fn append_audit(&self, event: TokenAuditEvent) -> BpResult<()> {
        self.inner.append_audit(event).await
    }

    async fn pending_audits(&self) -> BpResult<Vec<TokenAuditEvent>> {
        self.inner.pending_audits().await
    }

    async fn acknowledge_audit(&self, event_id: &str) -> BpResult<()> {
        self.inner.acknowledge_audit(event_id).await
    }
}
