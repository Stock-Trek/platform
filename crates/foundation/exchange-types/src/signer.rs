use crate::{
    api_key_credential::ApiKeyCredentials,
    encode::ByteEncoder,
    encrypt::{EncryptionAlgorithm, Encryptor},
};
use platform_errors::BpResult;

pub struct Signer {
    api_key: String,
    encryptor: Encryptor,
    encoder: ByteEncoder,
}

impl Signer {
    pub fn new(api_key: String, encryptor: Encryptor, encoder: ByteEncoder) -> Self {
        Self {
            api_key,
            encryptor,
            encoder,
        }
    }
    pub fn new_unencrypted(encoder: ByteEncoder) -> Self {
        Self {
            api_key: "".into(),
            encryptor: Encryptor::Null,
            encoder,
        }
    }
    pub fn try_new(
        credentials: ApiKeyCredentials,
        algorithm: EncryptionAlgorithm,
        encoder: ByteEncoder,
    ) -> BpResult<Self> {
        let api_key = credentials.api_key.clone();
        let encryptor = algorithm.encryptor(credentials)?;
        Ok(Self {
            api_key,
            encryptor,
            encoder,
        })
    }
    pub fn api_key(&self) -> String {
        self.api_key.clone()
    }
    pub fn signature(&self, bytes: &[u8]) -> BpResult<String> {
        let encrypted = self.encryptor.encrypt(bytes)?;
        Ok(self.encoder.encode(&encrypted))
    }
}
