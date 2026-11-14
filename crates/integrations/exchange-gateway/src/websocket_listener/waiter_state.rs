use platform_errors::BuildError;
use std::task::Waker;

#[derive(Default)]
pub struct WaiterState {
    pub filtered_response: Option<serde_json::Value>,
    pub error: Option<BuildError>,
    pub waker: Option<Waker>,
}
