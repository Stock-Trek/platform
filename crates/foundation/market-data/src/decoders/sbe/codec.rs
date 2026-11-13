use crate::decoders::{
    frame::{DecodedFrame, FrameBytes},
    sbe::frame::SbeFrame,
};
use iris::{Codec, ConnectionError};
use platform_errors::IngesterError;
use platform_exchange_types::market_data::{SbeSchemaDefinition, SbeTemplateKind};
use std::marker::PhantomData;
use tokio_websockets::Message;

pub struct SbeCodec<S> {
    _schema: PhantomData<S>,
}

impl<S> SbeCodec<S> {
    pub const fn new() -> Self {
        Self {
            _schema: PhantomData,
        }
    }
}

impl<S> Default for SbeCodec<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Clone for SbeCodec<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for SbeCodec<S> {}

impl<S: SbeSchemaDefinition> SbeCodec<S> {
    pub fn is_sbe_frame(frame: &[u8]) -> bool {
        SbeFrame::is_schema::<S>(frame)
    }

    pub fn decode_trades(&self, frame: &[u8]) -> Result<Vec<S::Trade>, IngesterError> {
        let mut frame = SbeFrame::decode::<S>(frame)?;
        match frame.template().kind {
            SbeTemplateKind::Trades => self.trade_entries(&mut frame),
            SbeTemplateKind::BestBidAsk
            | SbeTemplateKind::DepthSnapshot
            | SbeTemplateKind::DepthDiff => Ok(Vec::new()),
        }
    }

    pub fn decode_best_bid_ask(&self, frame: &[u8]) -> Result<S::BestBidAsk, IngesterError> {
        let mut frame = SbeFrame::decode::<S>(frame)?;
        if frame.template().kind != SbeTemplateKind::BestBidAsk {
            return Err(IngesterError::malformed_sbe(format!(
                "expected best bid/ask template, got {:?}",
                frame.template().kind
            )));
        }
        let (_, best_bid_ask) = self.best_bid_ask_fields(&mut frame)?;
        Ok(best_bid_ask)
    }
}

impl<S> Codec<(), DecodedFrame> for SbeCodec<S>
where
    S: SbeSchemaDefinition + Send + Sync + 'static,
{
    fn encode(&self, _request: &()) -> Result<Vec<u8>, ConnectionError> {
        Err(ConnectionError::Codec(
            "the ingester does not send websocket messages".to_string(),
        ))
    }

    fn decode(&self, message: Message) -> Result<DecodedFrame, ConnectionError> {
        if !message.is_binary() {
            return Err(ConnectionError::BadMessagePayload);
        }

        let frame = FrameBytes::from_message(message)?;
        Ok(DecodedFrame::new(self.decode_frame(&frame)))
    }
}
