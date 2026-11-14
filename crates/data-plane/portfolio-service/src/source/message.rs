use platform_nats::Message;

#[derive(Debug, Clone)]
pub struct SourceMessage<T> {
    value: T,
    stream_sequence: Option<u64>,
    raw: Option<Message>,
}

impl<T> SourceMessage<T> {
    pub fn new(value: T, stream_sequence: Option<u64>, raw: Option<Message>) -> Self {
        Self {
            value,
            stream_sequence,
            raw,
        }
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn stream_sequence(&self) -> Option<u64> {
        self.stream_sequence
    }

    pub fn raw(&self) -> Option<&Message> {
        self.raw.as_ref()
    }
}
