use crate::SignedOrderRequest;

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
