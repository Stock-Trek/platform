//! NATS message representation shared by core and JetStream flows.

/// Header pairs carried by a NATS/1.0 header block.
pub type Headers = Vec<(String, String)>;

/// A message delivered by a NATS subscription.
///
/// Messages read through JetStream also carry the source position metadata
/// required by checkpointed consumers. Core NATS messages, and messages created
/// directly in tests, leave the JetStream fields unset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Message {
    /// The subject the message was published to.
    pub subject: String,
    pub reply_to: Option<String>,
    pub status: Option<u16>,
    pub headers: Headers,
    pub payload: Vec<u8>,
    /// Name of the JetStream stream that stored the message.
    pub stream: Option<String>,
    /// Monotonic sequence assigned by the source stream.
    ///
    /// This is the position checkpointed by consumers so they can resume from
    /// the first unprocessed message after a restart.
    pub stream_sequence: Option<u64>,
    /// Sequence assigned by the durable consumer that delivered the message.
    pub consumer_sequence: Option<u64>,
}

impl Message {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn is_status(&self, code: u16) -> bool {
        self.status == Some(code)
    }
}

impl From<async_nats::Message> for Message {
    fn from(message: async_nats::Message) -> Self {
        let headers = message
            .headers
            .as_ref()
            .map(|headers| {
                headers
                    .iter()
                    .flat_map(|(name, values)| {
                        values
                            .iter()
                            .map(|value| (name.to_string(), value.as_str().to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        Self {
            subject: message.subject.to_string(),
            reply_to: message.reply.map(|reply| reply.to_string()),
            status: message.status.map(u16::from),
            headers,
            payload: message.payload.to_vec(),
            stream: None,
            stream_sequence: None,
            consumer_sequence: None,
        }
    }
}

impl From<async_nats::jetstream::Message> for Message {
    fn from(message: async_nats::jetstream::Message) -> Self {
        let metadata = message.info().ok().map(|info| {
            (
                info.stream.to_string(),
                info.stream_sequence,
                info.consumer_sequence,
            )
        });
        let mut message = Message::from(message.message);
        if let Some((stream, stream_sequence, consumer_sequence)) = metadata {
            message.stream = Some(stream);
            message.stream_sequence = Some(stream_sequence);
            message.consumer_sequence = Some(consumer_sequence);
        }
        message
    }
}

#[cfg(test)]
mod tests;
