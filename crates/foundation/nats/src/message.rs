//! NATS message representation shared by core and JetStream flows.

/// Header pairs carried by a NATS/1.0 header block.
pub type Headers = Vec<(String, String)>;

/// A message delivered by a NATS subscription.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Message {
    pub subject: String,
    pub reply_to: Option<String>,
    pub status: Option<u16>,
    pub headers: Headers,
    pub payload: Vec<u8>,
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_async_nats_messages() {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", "market-events.1");
        let message = async_nats::Message {
            subject: "market.events.binance.btcusdt.trade".to_string().into(),
            reply: Some("_INBOX.1".to_string().into()),
            payload: b"payload".to_vec().into(),
            headers: Some(headers),
            status: async_nats::StatusCode::from_u16(404).ok(),
            description: Some("No Messages".to_string()),
            length: 7,
        };

        let message = Message::from(message);

        assert_eq!(message.subject, "market.events.binance.btcusdt.trade");
        assert_eq!(message.reply_to.as_deref(), Some("_INBOX.1"));
        assert_eq!(message.status, Some(404));
        assert_eq!(message.header("nats-msg-id"), Some("market-events.1"));
        assert_eq!(message.payload, b"payload");
    }
}
