use super::*;

#[test]
fn tables_are_named_per_layer_and_kind() {
    assert_eq!(
        ArchiveIcebergTable::raw(ArchiveKind::Events).name(),
        "raw/events"
    );
    assert_eq!(
        ArchiveIcebergTable::raw(ArchiveKind::Candles).name(),
        "raw/candles"
    );
    assert_eq!(
        ArchiveIcebergTable::compacted(ArchiveKind::Events).name(),
        "compacted/events"
    );
    assert_eq!(
        ArchiveIcebergTable::compacted(ArchiveKind::Candles).name(),
        "compacted/candles"
    );
    assert_eq!(
        ArchiveIcebergTable::new(ArchiveTableLayer::Compacted, ArchiveKind::Candles).kind(),
        ArchiveKind::Candles
    );
}

#[test]
fn event_schema_matches_the_archive_columns() {
    let schema = ArchiveIcebergTable::schema(ArchiveKind::Events).unwrap();
    let fields = schema.as_struct().fields();

    assert_eq!(fields.len(), 8);
    assert_eq!(fields[0].name, "exchange");
    assert_eq!(
        fields[0].field_type.as_ref(),
        &Type::Primitive(PrimitiveType::String)
    );
    assert_eq!(fields[2].name, "sequence");
    assert_eq!(
        fields[2].field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert!(!fields[2].required);
    assert_eq!(fields[4].name, "received_at");
    assert!(fields[4].required);
    assert_eq!(fields[6].name, "state_epoch");
    assert!(fields[6].required);
    assert_eq!(fields[7].name, "routing_key");
    assert_eq!(
        fields[7].field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert!(fields[7].required);
}

#[test]
fn candle_schema_matches_the_archive_columns() {
    let schema = ArchiveIcebergTable::schema(ArchiveKind::Candles).unwrap();
    let fields = schema.as_struct().fields();

    assert_eq!(fields.len(), 23);
    assert_eq!(fields[8].name, "open");
    assert_eq!(
        fields[8].field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Double)
    );
    assert_eq!(fields[16].name, "close_watermark");
    assert_eq!(fields[17].name, "source_offset");
    assert_eq!(fields[18].name, "state_epoch");
    assert_eq!(fields[19].name, "revision_id");
    assert_eq!(fields[20].name, "revision");
    assert_eq!(fields[21].name, "is_complete");
    assert_eq!(
        fields[21].field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Boolean)
    );
    assert_eq!(fields[22].name, "routing_key");
    assert_eq!(
        fields[22].field_type.as_ref(),
        &Type::Primitive(PrimitiveType::Long)
    );
    assert!(fields[22].required);
}
