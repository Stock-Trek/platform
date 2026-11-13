use crate::{
    portfolio::{AuthBinder, CredentialProvider, SessionLifecycle},
    signer::Signer,
};

pub trait ExchangeAdapter: Send + Sync {
    fn credential_provider(&self) -> &dyn CredentialProvider;
    fn auth_binder(&self) -> &dyn AuthBinder;
    fn lifecycle(&self) -> SessionLifecycle;
    fn signer(&self) -> &Signer;
}

pub struct PortfolioAuth<P, B> {
    credential_provider: P,
    auth_binder: B,
    lifecycle: SessionLifecycle,
    signer: Signer,
}

impl<P, B> PortfolioAuth<P, B> {
    pub fn new(
        credential_provider: P,
        auth_binder: B,
        lifecycle: SessionLifecycle,
        signer: Signer,
    ) -> Self {
        Self {
            credential_provider,
            auth_binder,
            lifecycle,
            signer,
        }
    }
}

impl<P, B> ExchangeAdapter for PortfolioAuth<P, B>
where
    P: CredentialProvider,
    B: AuthBinder,
{
    fn credential_provider(&self) -> &dyn CredentialProvider {
        &self.credential_provider
    }

    fn auth_binder(&self) -> &dyn AuthBinder {
        &self.auth_binder
    }

    fn lifecycle(&self) -> SessionLifecycle {
        self.lifecycle
    }

    fn signer(&self) -> &Signer {
        &self.signer
    }
}
