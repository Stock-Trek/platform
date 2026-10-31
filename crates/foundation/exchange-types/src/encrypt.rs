use crate::api_key_credential::ApiKeyCredentials;
use hmac::{Hmac, KeyInit, Mac};
use p256::ecdsa::signature::Signer as SignerTrait;
use platform_errors::{BpError, BpResult, ExchangeError};
use rsa::{
    RsaPrivateKey, pkcs1::DecodeRsaPrivateKey, pkcs1v15::SigningKey as RsaPkcs1v15SigningKey,
    pkcs8::DecodePrivateKey, sha2::Sha256,
};
use secrecy::{ExposeSecret, SecretSlice};
use sha2::Sha512;
use std::str::FromStr;
use strum::Display;

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EncryptionAlgorithm {
    Null,
    EcdsaP256,
    EcdsaP384,
    Ed25519,
    HmacSha256,
    HmacSha512,
    RsaSha256,
}

#[derive(Display)]
pub enum Encryptor {
    Null,
    EcdsaP256(p256::ecdsa::SigningKey),
    EcdsaP384(p384::ecdsa::SigningKey),
    Ed25519(ed25519_compact::SecretKey),
    HmacSha256(secrecy::SecretSlice<u8>),
    HmacSha512(secrecy::SecretSlice<u8>),
    RsaSha256(Box<RsaPkcs1v15SigningKey<Sha256>>),
}

impl std::fmt::Debug for Encryptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Encryptor::Null => write!(f, "Null"),
            Encryptor::EcdsaP256(_) => write!(f, "EcdsaP256"),
            Encryptor::EcdsaP384(_) => write!(f, "EcdsaP384"),
            Encryptor::Ed25519(_) => write!(f, "Ed25519"),
            Encryptor::HmacSha256(_) => write!(f, "HmacSha256"),
            Encryptor::HmacSha512(_) => write!(f, "HmacSha512"),
            Encryptor::RsaSha256(_) => write!(f, "RsaSha256"),
        }
    }
}

impl FromStr for EncryptionAlgorithm {
    type Err = ExchangeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let algorithm = match value.trim().to_ascii_lowercase().as_str() {
            "null" => Self::Null,
            "ecdsa_p256" => Self::EcdsaP256,
            "ecdsa_p384" => Self::EcdsaP384,
            "ed25519" => Self::Ed25519,
            "hmac_sha256" => Self::HmacSha256,
            "hmac_sha512" => Self::HmacSha512,
            "rsa_sha256" => Self::RsaSha256,
            _ => {
                return Err(ExchangeError::BadValue {
                    name: "signing algorithm".to_string(),
                    value: value.to_string(),
                    reason: "expected one of null, ecdsa_p256, ecdsa_p384, ed25519, hmac_sha256, hmac_sha512, rsa_sha256".to_string(),
                });
            }
        };
        Ok(algorithm)
    }
}

impl EncryptionAlgorithm {
    pub fn encryptor(&self, api_key_credentials: ApiKeyCredentials) -> BpResult<Encryptor> {
        let secret_key_bytes = api_key_credentials.secret.expose_secret().as_bytes();
        match self {
            Self::Null => Ok(Encryptor::Null),
            Self::EcdsaP256 => {
                let signing_key =
                    p256::ecdsa::SigningKey::from_slice(secret_key_bytes).map_err(|e| {
                        BpError::Exchange(ExchangeError::CryptoKey(format!(
                            "ECDSA P-256 key error: {e}"
                        )))
                    })?;
                Ok(Encryptor::EcdsaP256(signing_key))
            }
            Self::EcdsaP384 => {
                let signing_key =
                    p384::ecdsa::SigningKey::from_slice(secret_key_bytes).map_err(|e| {
                        BpError::Exchange(ExchangeError::CryptoKey(format!(
                            "ECDSA P-384 key error: {e}"
                        )))
                    })?;
                Ok(Encryptor::EcdsaP384(signing_key))
            }
            Self::Ed25519 => {
                let seed = ed25519_compact::Seed::from_slice(secret_key_bytes).map_err(|_| {
                    BpError::Exchange(ExchangeError::CryptoKey(
                        "Ed25519 key must be exactly 32 bytes".to_string(),
                    ))
                })?;
                if seed.iter().all(|byte| *byte == 0) {
                    return Err(BpError::Exchange(ExchangeError::CryptoKey(
                        "Ed25519 key must not be all zeros".to_string(),
                    )));
                }
                let signing_key = ed25519_compact::KeyPair::from_seed(seed).sk;
                Ok(Encryptor::Ed25519(signing_key))
            }
            Self::HmacSha256 => {
                let signing_slice = SecretSlice::from(secret_key_bytes.to_vec());
                Ok(Encryptor::HmacSha256(signing_slice))
            }
            Self::HmacSha512 => {
                let signing_slice = SecretSlice::from(secret_key_bytes.to_vec());
                Ok(Encryptor::HmacSha512(signing_slice))
            }
            Self::RsaSha256 => {
                let signing_key = Self::rsa_signing_key(api_key_credentials.secret.expose_secret())
                    .map_err(|e| {
                        BpError::Exchange(ExchangeError::CryptoKey(format!("RSA key error: {e}")))
                    })?;
                Ok(Encryptor::RsaSha256(Box::new(signing_key)))
            }
        }
    }

    fn rsa_signing_key(secret: &str) -> Result<RsaPkcs1v15SigningKey<Sha256>, String> {
        let secret = secret.trim();
        let private_key = if secret.starts_with("-----BEGIN") {
            if secret.contains("RSA PRIVATE KEY") {
                RsaPrivateKey::from_pkcs1_pem(secret)
                    .map_err(|e| format!("invalid PKCS#1 PEM: {e}"))?
            } else {
                RsaPrivateKey::from_pkcs8_pem(secret)
                    .map_err(|e| format!("invalid PKCS#8 PEM: {e}"))?
            }
        } else {
            let bytes = secret.as_bytes();
            RsaPrivateKey::from_pkcs8_der(bytes)
                .or_else(|_| RsaPrivateKey::from_pkcs1_der(bytes))
                .map_err(|e| format!("invalid PKCS#8 or PKCS#1 DER: {e}"))?
        };
        Ok(RsaPkcs1v15SigningKey::<Sha256>::new(private_key))
    }
}

impl Encryptor {
    pub fn encrypt(&self, bytes: &[u8]) -> BpResult<Vec<u8>> {
        match self {
            Self::Null => Ok(bytes.to_vec()),
            Self::EcdsaP256(signing_key) => {
                let signature: p256::ecdsa::Signature = signing_key.sign(bytes);
                Ok(signature.to_der().to_bytes().to_vec())
            }
            Self::EcdsaP384(signing_key) => {
                let signature: p384::ecdsa::Signature = signing_key.sign(bytes);
                Ok(signature.to_der().to_bytes().to_vec())
            }
            Self::Ed25519(signing_key) => {
                let signature = signing_key.sign(bytes, None);
                Ok(signature.to_vec())
            }
            Self::HmacSha256(signing_slice) => {
                let mut mac = Hmac::<sha2::Sha256>::new_from_slice(signing_slice.expose_secret())
                    .map_err(|e| {
                    BpError::Exchange(ExchangeError::CryptoKey(format!(
                        "HMAC-SHA256 key error: {e}"
                    )))
                })?;
                mac.update(bytes);
                Ok(mac.finalize().into_bytes().to_vec())
            }
            Self::HmacSha512(signing_slice) => {
                let mut mac = Hmac::<Sha512>::new_from_slice(signing_slice.expose_secret())
                    .map_err(|e| {
                        BpError::Exchange(ExchangeError::CryptoKey(format!(
                            "HMAC-SHA512 key error: {e}"
                        )))
                    })?;
                mac.update(bytes);
                Ok(mac.finalize().into_bytes().to_vec())
            }
            Self::RsaSha256(signing_key) => {
                let signature: rsa::pkcs1v15::Signature = signing_key.sign(bytes);
                let signature: Box<[u8]> = signature.into();
                Ok(signature.to_vec())
            }
        }
    }
}
