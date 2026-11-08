use secrecy::{ExposeSecret, SecretString};

pub trait SecretInput {
    fn secret_input(&self) -> &str;
}

impl SecretInput for str {
    fn secret_input(&self) -> &str {
        self
    }
}

impl SecretInput for String {
    fn secret_input(&self) -> &str {
        self.as_str()
    }
}

impl SecretInput for SecretString {
    fn secret_input(&self) -> &str {
        self.expose_secret()
    }
}

impl<T> SecretInput for &T
where
    T: SecretInput + ?Sized,
{
    fn secret_input(&self) -> &str {
        (*self).secret_input()
    }
}
