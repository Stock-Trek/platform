use crate::OrderRequestContext;
use hmac::{Hmac, KeyInit, Mac};
use platform_errors::{BpError, BpResult};
use secrecy::{ExposeSecret, SecretString};
use sha2::Sha256;
use subtle::ConstantTimeEq;

pub struct OrderRequestSigner {
    mac: Hmac<Sha256>,
}

impl OrderRequestSigner {
    pub fn new(secret: &SecretString) -> BpResult<Self> {
        if secret.expose_secret().is_empty() {
            return Err(BpError::auth_invalid_field("secret", "must not be empty"));
        }
        let mac =
            Hmac::<Sha256>::new_from_slice(secret.expose_secret().as_bytes()).map_err(|e| {
                BpError::auth_invalid_field("secret", format!("rejected as an HMAC key: {e}"))
            })?;
        Ok(Self { mac })
    }

    pub fn sign(&self, order_request: &[u8]) -> BpResult<String> {
        let mut mac = self.mac.clone();
        mac.update(order_request);
        Ok(data_encoding::HEXLOWER.encode(&mac.finalize().into_bytes()))
    }

    pub fn sign_order_request(
        &self,
        context: &OrderRequestContext,
        order_request: &[u8],
    ) -> BpResult<String> {
        let mut mac = self.mac.clone();
        mac.update(&context.canonical(order_request));
        Ok(data_encoding::HEXLOWER.encode(&mac.finalize().into_bytes()))
    }

    pub fn verify_order_request(
        &self,
        context: &OrderRequestContext,
        order_request: &[u8],
        signature: &str,
    ) -> BpResult<bool> {
        let expected = self.sign_order_request(context, order_request)?;
        Ok(expected.as_bytes().ct_eq(signature.as_bytes()).into())
    }
}
