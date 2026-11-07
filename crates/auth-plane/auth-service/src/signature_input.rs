use crate::{ApiToken, SignedOrderRequest};
use secrecy::{ExposeSecret, SecretString};

pub trait SignatureInput {
    fn signature_input(&self) -> &str;
}

impl SignatureInput for str {
    fn signature_input(&self) -> &str {
        self
    }
}

impl SignatureInput for String {
    fn signature_input(&self) -> &str {
        self.as_str()
    }
}

impl SignatureInput for SecretString {
    fn signature_input(&self) -> &str {
        self.expose_secret()
    }
}

impl SignatureInput for ApiToken {
    fn signature_input(&self) -> &str {
        self.secret().expose_secret()
    }
}

impl SignatureInput for SignedOrderRequest {
    fn signature_input(&self) -> &str {
        self.signature()
    }
}

impl<T> SignatureInput for &T
where
    T: SignatureInput + ?Sized,
{
    fn signature_input(&self) -> &str {
        (*self).signature_input()
    }
}
