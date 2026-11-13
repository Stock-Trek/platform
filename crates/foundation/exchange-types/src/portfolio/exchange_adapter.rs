use crate::portfolio::{AuthBinder, CredentialProvider, SessionLifecyclePolicy, Signer};

pub struct ExchangeAdapter {
    pub credential_provider: Box<dyn CredentialProvider>,
    pub auth_binder: Box<dyn AuthBinder>,
    pub lifecycle: Box<dyn SessionLifecyclePolicy>,
    pub signer: Box<dyn Signer>,
}

impl ExchangeAdapter {
    pub fn new(
        credential_provider: Box<dyn CredentialProvider>,
        auth_binder: Box<dyn AuthBinder>,
        lifecycle: Box<dyn SessionLifecyclePolicy>,
        signer: Box<dyn Signer>,
    ) -> Self {
        Self {
            credential_provider,
            auth_binder,
            lifecycle,
            signer,
        }
    }
}
