pub trait RevocationReason {
    fn into_reason(self) -> Option<String>;
}

impl RevocationReason for &str {
    fn into_reason(self) -> Option<String> {
        (!self.trim().is_empty()).then(|| self.to_string())
    }
}

impl RevocationReason for String {
    fn into_reason(self) -> Option<String> {
        (!self.trim().is_empty()).then_some(self)
    }
}

impl RevocationReason for &String {
    fn into_reason(self) -> Option<String> {
        self.as_str().into_reason()
    }
}

impl<T: RevocationReason> RevocationReason for Option<T> {
    fn into_reason(self) -> Option<String> {
        self.and_then(RevocationReason::into_reason)
    }
}
