use crate::decoders::binary_cursor::BinaryCursor;
use platform_errors::IngesterError;
use platform_exchange_types::market_data::{SbeSchemaDefinition, SbeTemplate};

const HEADER_LENGTH: usize = 8;

#[derive(Debug, Clone, Copy)]
pub struct SbeFrame<'a> {
    template: SbeTemplate,
    cursor: BinaryCursor<'a>,
}

impl<'a> SbeFrame<'a> {
    pub fn decode<S: SbeSchemaDefinition>(frame: &'a [u8]) -> Result<Self, IngesterError> {
        if frame.len() < HEADER_LENGTH {
            return Err(IngesterError::malformed_sbe(format!(
                "SBE header requires {HEADER_LENGTH} bytes, got {}",
                frame.len()
            )));
        }

        let schema = S::SCHEMA;
        let mut cursor = BinaryCursor::new(frame);
        let block_length = cursor.read_u16()?;
        let template_id = cursor.read_u16()?;
        let schema_id = cursor.read_u16()?;
        let version = cursor.read_u16()?;

        if schema_id != schema.schema_id {
            return Err(IngesterError::malformed_sbe(format!(
                "expected schema id {}, got {schema_id}",
                schema.schema_id
            )));
        }
        if version != schema.version {
            return Err(IngesterError::malformed_sbe(format!(
                "unsupported schema version {version}, expected {}",
                schema.version
            )));
        }

        let template = schema
            .templates
            .iter()
            .find(|template| template.template_id == template_id)
            .copied()
            .ok_or_else(|| IngesterError::unknown_sbe_template(template_id))?;

        if block_length != template.block_length {
            return Err(IngesterError::malformed_sbe(format!(
                "expected block length {} for template id {template_id}, got {block_length}",
                template.block_length
            )));
        }

        Ok(Self { template, cursor })
    }

    pub fn is_schema<S: SbeSchemaDefinition>(frame: &[u8]) -> bool {
        if frame.len() < HEADER_LENGTH {
            return false;
        }
        let schema = S::SCHEMA;
        let template_id = u16::from_le_bytes([frame[2], frame[3]]);
        let schema_id = u16::from_le_bytes([frame[4], frame[5]]);
        schema_id == schema.schema_id
            && schema
                .templates
                .iter()
                .any(|template| template.template_id == template_id)
    }

    pub fn template(&self) -> SbeTemplate {
        self.template
    }

    pub fn cursor(&mut self) -> &mut BinaryCursor<'a> {
        &mut self.cursor
    }
}
