use crate::{ApiToken, SignedOrderRequest};
use platform_errors::{BpError, BpResult};
use secrecy::{ExposeSecret, SecretString};

pub enum SignatureArgument<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
}

pub trait SignatureInput {
    fn signature_argument(&self) -> SignatureArgument<'_>;
}

impl SignatureInput for str {
    fn signature_argument(&self) -> SignatureArgument<'_> {
        SignatureArgument::Text(self)
    }
}

impl SignatureInput for String {
    fn signature_argument(&self) -> SignatureArgument<'_> {
        SignatureArgument::Text(self.as_str())
    }
}

impl SignatureInput for SecretString {
    fn signature_argument(&self) -> SignatureArgument<'_> {
        SignatureArgument::Text(self.expose_secret())
    }
}

impl SignatureInput for ApiToken {
    fn signature_argument(&self) -> SignatureArgument<'_> {
        SignatureArgument::Text(self.secret().expose_secret())
    }
}

impl SignatureInput for SignedOrderRequest {
    fn signature_argument(&self) -> SignatureArgument<'_> {
        SignatureArgument::Text(self.signature())
    }
}

impl SignatureInput for [u8] {
    fn signature_argument(&self) -> SignatureArgument<'_> {
        SignatureArgument::Bytes(self)
    }
}

impl SignatureInput for Vec<u8> {
    fn signature_argument(&self) -> SignatureArgument<'_> {
        SignatureArgument::Bytes(self.as_slice())
    }
}

impl<const N: usize> SignatureInput for [u8; N] {
    fn signature_argument(&self) -> SignatureArgument<'_> {
        SignatureArgument::Bytes(self.as_slice())
    }
}

impl<T> SignatureInput for &T
where
    T: SignatureInput + ?Sized,
{
    fn signature_argument(&self) -> SignatureArgument<'_> {
        (*self).signature_argument()
    }
}

pub struct SignatureArguments;

impl SignatureArguments {
    pub fn split_sign<'a, A, B>(first: &'a A, second: &'a B) -> BpResult<(&'a str, &'a [u8])>
    where
        A: SignatureInput + ?Sized,
        B: SignatureInput + ?Sized,
    {
        match (first.signature_argument(), second.signature_argument()) {
            (SignatureArgument::Text(secret), SignatureArgument::Bytes(request))
            | (SignatureArgument::Bytes(request), SignatureArgument::Text(secret)) => {
                Ok((secret, request))
            }
            _ => Err(BpError::auth_invalid_field(
                "order_request",
                "expected one token secret and one request byte payload",
            )),
        }
    }

    pub fn split_verify<'a, A, B>(first: &'a A, second: &'a B) -> BpResult<(&'a [u8], &'a str)>
    where
        A: SignatureInput + ?Sized,
        B: SignatureInput + ?Sized,
    {
        match (first.signature_argument(), second.signature_argument()) {
            (SignatureArgument::Bytes(request), SignatureArgument::Text(signature))
            | (SignatureArgument::Text(signature), SignatureArgument::Bytes(request)) => {
                Ok((request, signature))
            }
            _ => Err(BpError::auth_invalid_field(
                "signature",
                "expected one request byte payload and one signature string",
            )),
        }
    }
}
