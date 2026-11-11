use aws_credential_types::Credentials;
use aws_smithy_runtime_api::client::identity::Identity;
use secrecy::ExposeSecret;

#[derive(Clone)]
pub struct SigV4Credentials {
    access_key_id: String,
    secret_access_key: secrecy::SecretString,
    session_token: Option<secrecy::SecretString>,
}

impl SigV4Credentials {
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> platform_errors::BpResult<Self> {
        let access_key_id = access_key_id.into();
        let secret_access_key = secret_access_key.into();
        Self::validate_value("accessKeyId", &access_key_id)?;
        Self::validate_value("secretAccessKey", &secret_access_key)?;
        if let Some(token) = &session_token {
            Self::validate_value("sessionToken", token)?;
        }
        Ok(Self {
            access_key_id,
            secret_access_key: secrecy::SecretString::from(secret_access_key),
            session_token: session_token.map(secrecy::SecretString::from),
        })
    }

    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    pub(crate) fn secret_access_key(&self) -> &str {
        self.secret_access_key.expose_secret()
    }

    pub(crate) fn session_token(&self) -> Option<&str> {
        self.session_token
            .as_ref()
            .map(|token| token.expose_secret())
    }

    pub(crate) fn identity(&self) -> Identity {
        let credentials = Credentials::new(
            self.access_key_id.clone(),
            self.secret_access_key().to_string(),
            self.session_token().map(str::to_string),
            None,
            "platform-sigv4",
        );
        Identity::new(credentials, None)
    }

    fn validate_value(field: &'static str, value: &str) -> platform_errors::BpResult<()> {
        if value.trim().is_empty() {
            return Err(platform_errors::BpError::auth_invalid_field(
                field,
                "must not be empty",
            ));
        }
        if value != value.trim() || value.chars().any(char::is_control) {
            return Err(platform_errors::BpError::auth_invalid_field(
                field,
                "must not contain surrounding whitespace or control characters",
            ));
        }
        Ok(())
    }
}

impl std::fmt::Debug for SigV4Credentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SigV4Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"[redacted]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}
