use crate::portfolio::AuthError;

pub trait Signer: Send + Sync {
    fn api_key(&self) -> &str;
    fn sign(&self, payload: &[u8]) -> Result<Vec<u8>, AuthError>;
}

pub trait PortfolioSigner: Signer {}

impl<T> PortfolioSigner for T where T: Signer + ?Sized {}
