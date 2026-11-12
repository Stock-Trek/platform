//! JSON envelope codec for messages published on the market data subjects.

use platform_errors::BuildResult;
use serde::{Serialize, de::DeserializeOwned};

/// Shared encode/decode methods for market data envelope payloads.
pub trait EnvelopeCodec: Serialize + DeserializeOwned + Sized {
    /// Encodes a market data message into the JSON bytes carried by NATS.
    fn encode(&self) -> BuildResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Decodes the JSON payload of a market data message.
    fn decode(payload: &[u8]) -> BuildResult<Self> {
        Ok(serde_json::from_slice(payload)?)
    }
}

impl<T> EnvelopeCodec for T where T: Serialize + DeserializeOwned {}
