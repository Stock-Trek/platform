use crate::random_string::RandomString;

struct RandomStringFixture;

impl RandomStringFixture {
    fn assert_valid(value: &str, expected_length: usize) {
        assert_eq!(value.len(), expected_length);
        assert!(
            value
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        );
    }
}

#[test]
fn generates_the_requested_number_of_alphanumeric_characters() {
    let value = RandomString::generate(64);

    RandomStringFixture::assert_valid(&value, 64);
}

#[test]
fn generates_distinct_values() {
    let first = RandomString::generate(32);
    let second = RandomString::generate(32);

    assert_ne!(first, second);
}
