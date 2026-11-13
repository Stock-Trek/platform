use super::codec::BinanceSbeCodec;
use platform_errors::IngesterError;
use platform_market_data::binary_cursor::BinaryCursor;

pub(super) const BINANCE_SBE_SCHEMA_ID: u16 = 1;
pub(super) const BINANCE_SBE_VERSION: u16 = 0;

pub(super) const BINANCE_TRADES_TEMPLATE_ID: u16 = 10_000;
pub(super) const BINANCE_BEST_BID_ASK_TEMPLATE_ID: u16 = 10_001;
pub(super) const BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID: u16 = 10_002;
pub(super) const BINANCE_DEPTH_DIFF_TEMPLATE_ID: u16 = 10_003;

pub(super) const BINANCE_SBE_HEADER_LENGTH: usize = 8;
const BINANCE_TRADES_BLOCK_LENGTH: u16 = 18;
const BINANCE_BEST_BID_ASK_BLOCK_LENGTH: u16 = 50;
const BINANCE_DEPTH_SNAPSHOT_BLOCK_LENGTH: u16 = 18;
const BINANCE_DEPTH_DIFF_BLOCK_LENGTH: u16 = 26;

impl BinanceSbeCodec {
    pub fn is_sbe_frame(frame: &[u8]) -> bool {
        if frame.len() < BINANCE_SBE_HEADER_LENGTH {
            return false;
        }
        let template_id = u16::from_le_bytes([frame[2], frame[3]]);
        let schema_id = u16::from_le_bytes([frame[4], frame[5]]);
        schema_id == BINANCE_SBE_SCHEMA_ID
            && matches!(
                template_id,
                BINANCE_TRADES_TEMPLATE_ID
                    | BINANCE_BEST_BID_ASK_TEMPLATE_ID
                    | BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID
                    | BINANCE_DEPTH_DIFF_TEMPLATE_ID
            )
    }

    pub(super) fn frame_cursor<'a>(
        &self,
        frame: &'a [u8],
    ) -> Result<(u16, BinaryCursor<'a>), IngesterError> {
        if frame.len() < BINANCE_SBE_HEADER_LENGTH {
            return Err(IngesterError::malformed_sbe(format!(
                "SBE header requires {BINANCE_SBE_HEADER_LENGTH} bytes, got {}",
                frame.len()
            )));
        }
        let mut cursor = BinaryCursor::new(frame);
        let block_length = cursor.read_u16()?;
        let template_id = cursor.read_u16()?;
        let schema_id = cursor.read_u16()?;
        let version = cursor.read_u16()?;
        if schema_id != BINANCE_SBE_SCHEMA_ID {
            return Err(IngesterError::malformed_sbe(format!(
                "expected schema id {BINANCE_SBE_SCHEMA_ID}, got {schema_id}"
            )));
        }
        let expected_block_length = Self::block_length(template_id)?;
        if version != BINANCE_SBE_VERSION {
            return Err(IngesterError::malformed_sbe(format!(
                "unsupported schema version {version}, expected {BINANCE_SBE_VERSION}"
            )));
        }
        if block_length != expected_block_length {
            return Err(IngesterError::malformed_sbe(format!(
                "expected block length {expected_block_length} for template id {template_id}, got {block_length}"
            )));
        }
        Ok((template_id, cursor))
    }

    fn block_length(template_id: u16) -> Result<u16, IngesterError> {
        match template_id {
            BINANCE_TRADES_TEMPLATE_ID => Ok(BINANCE_TRADES_BLOCK_LENGTH),
            BINANCE_BEST_BID_ASK_TEMPLATE_ID => Ok(BINANCE_BEST_BID_ASK_BLOCK_LENGTH),
            BINANCE_DEPTH_SNAPSHOT_TEMPLATE_ID => Ok(BINANCE_DEPTH_SNAPSHOT_BLOCK_LENGTH),
            BINANCE_DEPTH_DIFF_TEMPLATE_ID => Ok(BINANCE_DEPTH_DIFF_BLOCK_LENGTH),
            template_id => Err(IngesterError::unknown_sbe_template(template_id)),
        }
    }
}
