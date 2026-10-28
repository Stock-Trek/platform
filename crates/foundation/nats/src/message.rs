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

/// Formats a NATS/1.0 header block, including the terminating blank line.
pub fn format_header_block(headers: &[(String, String)]) -> Vec<u8> {
    let mut block = String::from("NATS/1.0\r\n");
    for (name, value) in headers {
        block.push_str(name);
        block.push_str(": ");
        block.push_str(value);
        block.push_str("\r\n");
    }
    block.push_str("\r\n");
    block.into_bytes()
}

/// Parses a NATS/1.0 header block into a status code and header pairs.
pub fn parse_header_block(block: &[u8]) -> Result<(Option<u16>, Headers), String> {
    let text = std::str::from_utf8(block)
        .map_err(|error| format!("header block is not valid UTF-8: {error}"))?;
    let mut lines = text.split("\r\n");
    let first = lines.next().unwrap_or_default();
    let mut tokens = first.split_whitespace();
    let version = tokens.next().unwrap_or_default();
    if !version.starts_with("NATS/") {
        return Err(format!("unexpected header block version {version:?}"));
    }
    let status = tokens.next().and_then(|code| code.parse::<u16>().ok());
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(format!("malformed header line {line:?}"));
        };
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }
    Ok((status, headers))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_and_parses_a_header_block_round_trip() {
        let headers = vec![
            ("Nats-Msg-Id".to_string(), "market-events.1".to_string()),
            ("X-Trace".to_string(), "abc".to_string()),
        ];
        let block = format_header_block(&headers);
        let (status, parsed) = parse_header_block(&block).unwrap();

        assert_eq!(status, None);
        assert_eq!(parsed, headers);
    }

    #[test]
    fn parses_status_header_blocks() {
        let block = b"NATS/1.0 404\r\nDescription: No Messages\r\n\r\n";
        let (status, headers) = parse_header_block(block).unwrap();

        assert_eq!(status, Some(404));
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].0, "Description");
        assert_eq!(headers[0].1, "No Messages");
    }

    #[test]
    fn rejects_header_blocks_without_a_version_line() {
        assert!(parse_header_block(b"HTTP/1.1 200\r\n\r\n").is_err());
        assert!(parse_header_block(&[0xff, 0xfe]).is_err());
    }
}
