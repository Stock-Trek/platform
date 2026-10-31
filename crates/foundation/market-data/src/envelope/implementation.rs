use platform_errors::BpResult;
use serde::{Serialize, de::DeserializeOwned};

/// Shared encode/decode methods for market data envelope payloads.
pub trait EnvelopeCodec: Serialize + DeserializeOwned + Sized {
    /// Encodes a market data message into the JSON bytes carried by NATS.
    fn encode(&self) -> BpResult<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Decodes the JSON payload of a market data message.
    fn decode(payload: &[u8]) -> BpResult<Self> {
        Ok(serde_json::from_slice(payload)?)
    }
}

impl<T> EnvelopeCodec for T where T: Serialize + DeserializeOwned {}
