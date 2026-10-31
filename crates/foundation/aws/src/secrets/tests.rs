use super::*;
use aws_sdk_ssm::operation::get_parameters_by_path::GetParametersByPathOutput;
use aws_sdk_ssm::types::Parameter;
use platform_errors::BpError;
use std::collections::HashMap;

fn page(names: &[(&str, Option<&str>)]) -> GetParametersByPathOutput {
    GetParametersByPathOutput::builder()
        .set_parameters(Some(
            names
                .iter()
                .map(|(name, value)| {
                    Parameter::builder()
                        .name(*name)
                        .set_value(value.map(str::to_string))
                        .build()
                })
                .collect::<Vec<_>>(),
        ))
        .build()
}

#[test]
fn path_is_normalised_and_validated() {
    assert_eq!(
        SsmConfig::new("platform/git-webhook/production".into(), true)
            .unwrap()
            .path,
        "/platform/git-webhook/production"
    );
    assert!(SsmConfig::new("platform/git-webhook/production/".into(), true).is_err());
    assert!(SsmConfig::new("   ".into(), true).is_err());
}

#[test]
fn parameters_are_keyed_by_their_final_path_segment() {
    let mut secrets = HashMap::new();
    Secrets::insert_page(
        &mut secrets,
        &page(&[
            (
                "/platform/git-webhook/production/GITHUB_WEBHOOK_SECRET",
                Some("value"),
            ),
            ("/platform/git-webhook/production/OTHER", Some("other")),
        ]),
    );

    assert_eq!(
        secrets.get("GITHUB_WEBHOOK_SECRET").map(String::as_str),
        Some("value")
    );
    assert_eq!(secrets.get("OTHER").map(String::as_str), Some("other"));
}

#[test]
fn parameters_without_a_name_or_value_are_handled() {
    let mut secrets = HashMap::new();
    Secrets::insert_page(
        &mut secrets,
        &page(&[
            ("/platform/git-webhook/production/", Some("ignored")),
            (
                "/platform/git-webhook/production/GITHUB_WEBHOOK_SECRET",
                None,
            ),
        ]),
    );

    assert_eq!(
        secrets.get("GITHUB_WEBHOOK_SECRET").map(String::as_str),
        Some("")
    );
    assert!(!secrets.contains_key(""));
}

#[test]
fn missing_secrets_are_a_configuration_error() {
    let secrets = Secrets::default();

    assert!(matches!(
        secrets.get("BACKFILLER_EXCHANGE_API_KEY"),
        Err(BpError::Config(_))
    ));
}
