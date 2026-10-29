use super::ArchiveBatch;
use crate::record::ArchiveKind;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::{collections::HashMap, sync::Arc};

impl ArchiveBatch {
    fn field(id: i32, name: &str, data_type: DataType, nullable: bool) -> Field {
        Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    pub fn schema(kind: ArchiveKind) -> SchemaRef {
        match kind {
            ArchiveKind::Events => Arc::new(Schema::new(vec![
                Self::field(1, "exchange", DataType::Utf8, false),
                Self::field(2, "stream", DataType::Utf8, false),
                Self::field(3, "sequence", DataType::UInt64, true),
                Self::field(4, "exchange_timestamp", DataType::Int64, true),
                Self::field(5, "received_at", DataType::Int64, false),
                Self::field(6, "payload", DataType::Utf8, false),
            ])),
            ArchiveKind::Candles => Arc::new(Schema::new(vec![
                Self::field(1, "exchange", DataType::Utf8, false),
                Self::field(2, "base", DataType::Utf8, false),
                Self::field(3, "quote", DataType::Utf8, false),
                Self::field(4, "interval", DataType::Utf8, false),
                Self::field(5, "start_time_millis_inc", DataType::Int64, false),
                Self::field(6, "end_time_millis_exc", DataType::Int64, false),
                Self::field(7, "duration_millis", DataType::Int64, false),
                Self::field(8, "is_candle_closed", DataType::Boolean, false),
                Self::field(9, "open", DataType::Float64, false),
                Self::field(10, "high", DataType::Float64, false),
                Self::field(11, "low", DataType::Float64, false),
                Self::field(12, "close", DataType::Float64, false),
                Self::field(13, "volume", DataType::Float64, false),
                Self::field(14, "quote_volume", DataType::Float64, false),
                Self::field(15, "vwap", DataType::Float64, false),
                Self::field(16, "trade_count", DataType::UInt64, false),
                Self::field(17, "revision", DataType::UInt64, false),
            ])),
        }
    }
}
