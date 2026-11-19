use serde::Serialize;

#[derive(Serialize)]
pub(super) struct PublicErrorBody {
    error: &'static str,
    message: &'static str,
}

impl PublicErrorBody {
    pub(super) fn new(error: &'static str, message: &'static str) -> Self {
        Self { error, message }
    }
}
