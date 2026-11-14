use serde::Serialize;

#[derive(Serialize)]
pub struct PublicErrorBody {
    error: &'static str,
    message: &'static str,
}

impl PublicErrorBody {
    pub fn new(error: &'static str, message: &'static str) -> Self {
        Self { error, message }
    }
}
