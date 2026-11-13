use crate::connector::portfolio::{PortfolioExchange, PortfolioStream};
use platform_errors::BuildResult;
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials,
    portfolio::{
        AccountEvent, AccountSnapshot, Credential, PortfolioAuth, RenewalSchedule, SessionLifecycle,
    },
};
use std::{sync::Arc, time::Instant};
use stock_trek_types::cex::account_id::AccountId;

pub struct PortfolioSession {
    account: AccountId,
    credentials: ApiKeyCredentials,
    credential: Option<Credential>,
    auth: PortfolioAuth,
    stream: PortfolioStream,
    exchange: Arc<dyn PortfolioExchange>,
}

impl PortfolioSession {
    pub async fn open(
        exchange: Arc<dyn PortfolioExchange>,
        account: &AccountId,
        credentials: &ApiKeyCredentials,
    ) -> BuildResult<Self> {
        Self::open_with_credential(exchange, account, credentials, None).await
    }

    pub async fn open_with_credential(
        exchange: Arc<dyn PortfolioExchange>,
        account: &AccountId,
        credentials: &ApiKeyCredentials,
        previous: Option<Credential>,
    ) -> BuildResult<Self> {
        let auth = exchange.portfolio_auth(credentials).await?;
        let lifecycle = auth.lifecycle();
        let credential = match previous {
            Some(credential)
                if lifecycle.reconnect_reuses_credential()
                    && !credential.is_expired(Instant::now()) =>
            {
                Some(credential)
            }
            _ => auth.credential_provider().acquire().await?,
        };
        let stream = match exchange.open_stream(&auth, credential.as_ref()).await {
            Ok(stream) => stream,
            Err(error) => {
                Self::revoke(&auth, credential.as_ref()).await;
                return Err(error);
            }
        };
        if let Err(error) = auth
            .auth_binder()
            .bind(stream.client(), credential.as_ref(), auth.signer())
            .await
        {
            let _ = stream.disconnect().await;
            Self::revoke(&auth, credential.as_ref()).await;
            return Err(error.into());
        }
        Ok(Self {
            account: account.clone(),
            credentials: credentials.clone(),
            credential,
            auth,
            stream,
            exchange,
        })
    }

    async fn revoke(auth: &PortfolioAuth, credential: Option<&Credential>) {
        if let Some(credential) = credential
            && auth.lifecycle().revoke_on_close()
        {
            let _ = auth.credential_provider().revoke(credential).await;
        }
    }

    pub async fn snapshot(&self) -> BuildResult<AccountSnapshot> {
        self.exchange.account_snapshot(&self.credentials).await
    }

    pub async fn refresh(&mut self) -> BuildResult<()> {
        let Some(credential) = self.credential.as_ref() else {
            return Ok(());
        };
        if !credential.renewable {
            return Ok(());
        }
        self.credential = Some(self.auth.credential_provider().renew(credential).await?);
        Ok(())
    }

    pub async fn disconnect(&self) -> BuildResult<()> {
        self.stream.disconnect().await
    }

    pub async fn close(&mut self) -> BuildResult<()> {
        let credential = self.credential.take();
        let disconnect = self.stream.disconnect().await;
        if let Some(credential) = credential
            && self.auth.lifecycle().revoke_on_close()
        {
            self.auth.credential_provider().revoke(&credential).await?;
        }
        disconnect
    }

    pub async fn next(&mut self) -> BuildResult<Option<AccountEvent>> {
        self.stream.next().await
    }

    pub fn credential(&self) -> Option<&Credential> {
        self.credential.as_ref()
    }

    pub fn renewal_schedule(&self) -> Option<RenewalSchedule> {
        self.auth.lifecycle().scheduled_renewal()
    }

    pub fn max_session_lifetime(&self) -> Option<std::time::Duration> {
        self.auth.lifecycle().max_session_lifetime()
    }

    pub fn reconnect_reuses_credential(&self) -> bool {
        self.auth.lifecycle().reconnect_reuses_credential()
    }

    pub fn lifecycle(&self) -> SessionLifecycle {
        self.auth.lifecycle()
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
