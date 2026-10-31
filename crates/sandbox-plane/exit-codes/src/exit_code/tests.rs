use super::*;

#[test]
fn from_code_returns_known_variants() {
    assert_eq!(ExitCode::from_code(0), Some(ExitCode::Success));
    assert_eq!(
        ExitCode::from_code(108),
        Some(ExitCode::ServerEntryInvalidArgument)
    );
    assert_eq!(ExitCode::from_code(204), Some(ExitCode::BotErrorPanic));
}

#[test]
fn from_code_rejects_unknown_and_out_of_range_values() {
    assert_eq!(ExitCode::from_code(5), None);
    assert_eq!(ExitCode::from_code(-1), None);
    assert_eq!(ExitCode::from_code(i32::from(u8::MAX) + 1), None);
}
