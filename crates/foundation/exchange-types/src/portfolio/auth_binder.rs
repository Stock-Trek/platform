use crate::portfolio::{
    AuthError, ConnectRequest, Credential, IncomingFrame, Signer, WsConnection,
};
use async_trait::async_trait;

pub type ConfirmationPredicate = Box<dyn Fn(&IncomingFrame) -> bool + Send + Sync>;

#[async_trait]
pub trait AuthBinder: Send + Sync {
    fn prepare_connect(
        &self,
        _request: &mut ConnectRequest,
        _credential: Option<&Credential>,
        _signer: &dyn Signer,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    async fn bind(
        &self,
        _connection: &dyn WsConnection,
        _credential: Option<&Credential>,
        _signer: &dyn Signer,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    fn confirmation_predicate(&self) -> Option<ConfirmationPredicate> {
        None
    }
}
