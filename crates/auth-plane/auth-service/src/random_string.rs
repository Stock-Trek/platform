use rand::{Rng, distributions::Alphanumeric};

pub struct RandomString;

impl RandomString {
    pub fn generate(length: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }
}
