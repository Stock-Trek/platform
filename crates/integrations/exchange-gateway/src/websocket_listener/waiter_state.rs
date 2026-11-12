use platform_errors::BuildError;
use std::task::Waker;

#[derive(Default)]
pub(crate) struct WaiterState {
    pub(super) filtered_response: Option<serde_json::Value>,
    pub(super) error: Option<BuildError>,
    pub(super) waker: Option<Waker>,
}
