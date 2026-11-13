use crate::connector::portfolio::{PortfolioEventStream, PortfolioTransport};
use platform_errors::BuildResult;
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    portfolio::{
        AccountEvent, AccountSnapshot, Credential, ExchangeAdapter, RenewalSchedule,
        SessionLifecycle, WsConnection,
    },
};
use std::{sync::Arc, time::Instant};
use stock_trek_types::cex::account_id::AccountId;

pub struct PortfolioSession {
    account: AccountId,
    credentials: ApiKeyCredentials,
    credential: Option<Credential>,
    adapter: Box<dyn ExchangeAdapter>,
    stream: Box<dyn PortfolioEventStream>,
    transport: Arc<dyn PortfolioTransport>,
}

impl PortfolioSession {
    pub async fn open(
        transport: Arc<dyn PortfolioTransport>,
        account: &AccountId,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<Self> {
        Self::open_with_credential(transport, account, credentials, None).await
    }

    pub async fn open_with_credential(
        transport: Arc<dyn PortfolioTransport>,
        account: &AccountId,
        credentials: &ApiKeyCredentials,
        previous: Option<Credential>,
    ) -> BuildResult<Self> {
        let adapter = transport.adapter(credentials).await?;
        let lifecycle = adapter.lifecycle();
        let credential = match previous {
            Some(credential)
                if lifecycle.reconnect_reuses_credential()
                    && !credential.is_expired(Instant::now()) =>
            {
                Some(credential)
            }
            _ => adapter.credential_provider().acquire().await?,
        };
        let mut request = transport.connect_request()?;
        adapter.auth_binder().prepare_connect(
            &mut request,
            credential.as_ref(),
            adapter.signer(),
        )?;
        let stream = match transport.open_stream(request).await {
            Ok(stream) => stream,
            Err(error) => {
                if let Some(credential) = credential.as_ref()
                    && lifecycle.revoke_on_close()
                {
                    let _ = adapter.credential_provider().revoke(credential).await;
                }
                return Err(error);
            }
        };
        let connection: &dyn WsConnection = stream.as_ref();
        if let Err(error) = adapter
            .auth_binder()
            .bind(connection, credential.as_ref(), adapter.signer())
            .await
        {
            if let Some(credential) = credential.as_ref()
                && lifecycle.revoke_on_close()
            {
                let _ = adapter.credential_provider().revoke(credential).await;
            }
            return Err(error.into());
        }
        Ok(Self {
            account: account.clone(),
            credentials: credentials.clone(),
            credential,
            adapter,
            stream,
            transport,
        })
    }

    pub async fn snapshot(&self) -> BuildResult<AccountSnapshot> {
        self.transport.account_snapshot(&self.credentials).await
    }

    pub async fn refresh(&mut self) -> BuildResult<()> {
        let Some(credential) = self.credential.as_ref() else {
            return Ok(());
        };
        if !credential.renewable {
            return Ok(());
        }
        self.credential = Some(self.adapter.credential_provider().renew(credential).await?);
        Ok(())
    }

    pub async fn close(&mut self) -> BuildResult<()> {
        let Some(credential) = self.credential.take() else {
            return Ok(());
        };
        if self.adapter.lifecycle().revoke_on_close() {
            self.adapter
                .credential_provider()
                .revoke(&credential)
                .await?;
        }
        Ok(())
    }

    pub async fn next(&mut self) -> BuildResult<Option<AccountEvent>> {
        self.stream.next().await
    }

    pub fn credential(&self) -> Option<&Credential> {
        self.credential.as_ref()
    }

    pub fn renewal_schedule(&self) -> Option<RenewalSchedule> {
        self.adapter.lifecycle().scheduled_renewal()
    }

    pub fn max_session_lifetime(&self) -> Option<std::time::Duration> {
        self.adapter.lifecycle().max_session_lifetime()
    }

    pub fn reconnect_reuses_credential(&self) -> bool {
        self.adapter.lifecycle().reconnect_reuses_credential()
    }

    pub fn lifecycle(&self) -> SessionLifecycle {
        self.adapter.lifecycle()
    }
}

impl std::fmt::Debug for PortfolioSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PortfolioSession")
            .field("account", &self.account)
            .field(
                "credential",
                &self.credential.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}
