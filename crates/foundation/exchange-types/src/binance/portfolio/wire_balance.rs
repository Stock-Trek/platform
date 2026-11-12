use serde::Deserialize;

#[allow(non_snake_case)]
#[derive(Deserialize)]
pub(super) struct WireBalance {
    #[serde(rename = "a")]
    pub(super) asset: String,
    #[serde(rename = "f")]
    pub(super) free: String,
    #[serde(rename = "l")]
    pub(super) locked: String,
}
