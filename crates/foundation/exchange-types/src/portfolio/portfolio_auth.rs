use crate::{
    portfolio::{AuthBinder, CredentialProvider, SessionLifecycle},
    signer::Signer,
};

pub struct PortfolioAuth {
    credential_provider: Box<dyn CredentialProvider>,
    auth_binder: Box<dyn AuthBinder>,
    lifecycle: SessionLifecycle,
    signer: Signer,
}

impl PortfolioAuth {
    pub fn new(
        credential_provider: impl CredentialProvider + 'static,
        auth_binder: impl AuthBinder + 'static,
        lifecycle: SessionLifecycle,
        signer: Signer,
    ) -> Self {
        Self {
            credential_provider: Box::new(credential_provider),
            auth_binder: Box::new(auth_binder),
            lifecycle,
            signer,
        }
    }

    pub fn credential_provider(&self) -> &dyn CredentialProvider {
        self.credential_provider.as_ref()
    }

    pub fn auth_binder(&self) -> &dyn AuthBinder {
        self.auth_binder.as_ref()
    }

    pub fn lifecycle(&self) -> SessionLifecycle {
        self.lifecycle
    }

    pub fn signer(&self) -> &Signer {
        &self.signer
    }
}
