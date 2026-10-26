use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials, binance::exchange::BinanceExchange, encode::ByteEncoder,
    encrypt::EncryptionAlgorithm, signer::Signer,
};
use data_encoding::{BASE64, HEXLOWER};
use ed25519_compact::{KeyPair, Seed, Signature as Ed25519Signature};
use hmac::{Hmac, KeyInit, Mac};
use rsa::{
    RsaPrivateKey,
    pkcs1v15::{Signature as RsaSignature, VerifyingKey as RsaVerifyingKey},
    pkcs8::{EncodePrivateKey, LineEnding},
    sha2::Sha256 as RsaSha256,
};
use secrecy::SecretString;
use sha2::Sha256;

fn credentials(api_key: &str, secret: &str) -> ApiKeyCredentials {
    ApiKeyCredentials {
        api_key: api_key.into(),
        secret: SecretString::from(secret),
    }
}

#[test]
fn hmac_sha256_signature_matches_direct_computation() {
    let secret = "shared-secret";
    let signer = BinanceExchange::signer_hmac_sha256(credentials("api-key", secret)).unwrap();
    let message = b"symbol=BTCUSDT&side=BUY";
    let signature = signer.signature(message).unwrap();
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(message);
    assert_eq!(signature, HEXLOWER.encode(&mac.finalize().into_bytes()));
}

#[test]
fn ed25519_signature_verifies_with_the_public_key() {
    let seed = "0123456789abcdef0123456789abcdef";
    let signer = BinanceExchange::signer_ed25519(credentials("api-key", seed)).unwrap();
    let message = b"payload";
    let signature = signer.signature(message).unwrap();
    let signature_bytes = BASE64.decode(signature.as_bytes()).unwrap();
    let signature = Ed25519Signature::from_slice(&signature_bytes).unwrap();
    let key_pair = KeyPair::from_seed(Seed::from_slice(seed.as_bytes()).unwrap());
    assert!(key_pair.pk.verify(message, &signature).is_ok());
    assert!(key_pair.pk.verify(b"tampered", &signature).is_err());
}

#[test]
fn ecdsa_p256_signature_verifies_with_the_verifying_key() {
    let secret = "0123456789abcdef0123456789abcdef";
    let encryptor = EncryptionAlgorithm::EcdsaP256
        .encryptor(credentials("api-key", secret))
        .unwrap();
    let signer = Signer::new("api-key".into(), encryptor, ByteEncoder::HexLower);
    let message = b"payload";
    let signature = signer.signature(message).unwrap();
    let der = HEXLOWER.decode(signature.as_bytes()).unwrap();
    let signature = p256::ecdsa::Signature::from_der(&der).unwrap();
    let signing_key = p256::ecdsa::SigningKey::from_slice(secret.as_bytes()).unwrap();
    use p256::ecdsa::signature::Verifier;
    assert!(
        signing_key
            .verifying_key()
            .verify(message, &signature)
            .is_ok()
    );
    assert!(
        signing_key
            .verifying_key()
            .verify(b"tampered", &signature)
            .is_err()
    );
}

#[test]
fn ecdsa_p384_signature_verifies_with_the_verifying_key() {
    let secret = "0123456789abcdef0123456789abcdef0123456789abcdef";
    let encryptor = EncryptionAlgorithm::EcdsaP384
        .encryptor(credentials("api-key", secret))
        .unwrap();
    let signer = Signer::new("api-key".into(), encryptor, ByteEncoder::HexLower);
    let message = b"payload";
    let signature = signer.signature(message).unwrap();
    let der = HEXLOWER.decode(signature.as_bytes()).unwrap();
    let signature = p384::ecdsa::Signature::from_der(&der).unwrap();
    let signing_key = p384::ecdsa::SigningKey::from_slice(secret.as_bytes()).unwrap();
    use p384::ecdsa::signature::Verifier;
    assert!(
        signing_key
            .verifying_key()
            .verify(message, &signature)
            .is_ok()
    );
    assert!(
        signing_key
            .verifying_key()
            .verify(b"tampered", &signature)
            .is_err()
    );
}

#[test]
fn rsa_sha256_signature_verifies_with_the_public_key() {
    let mut rng = rand::thread_rng();
    let private_key = RsaPrivateKey::new(&mut rng, 1024).unwrap();
    let pem = private_key.to_pkcs8_pem(LineEnding::LF).unwrap();
    let signer = BinanceExchange::signer_rsa_sha256(credentials("api-key", pem.as_str())).unwrap();
    let message = b"payload";
    let signature = signer.signature(message).unwrap();
    let signature_bytes = BASE64.decode(signature.as_bytes()).unwrap();
    let signature = RsaSignature::try_from(signature_bytes.as_slice()).unwrap();
    let verifying_key = RsaVerifyingKey::<RsaSha256>::new(private_key.to_public_key());
    use rsa::signature::Verifier;
    assert!(verifying_key.verify(message, &signature).is_ok());
}
