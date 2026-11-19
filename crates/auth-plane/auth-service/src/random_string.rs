use rand::{Rng, distributions::Alphanumeric};

pub(crate) struct RandomString;

impl RandomString {
    pub(crate) fn generate(length: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }
}
