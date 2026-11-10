use std::collections::BTreeMap;

pub struct SigV4Request<'a> {
    method: &'a str,
    url: &'a str,
    headers: &'a BTreeMap<String, String>,
    body: &'a [u8],
}

impl<'a> SigV4Request<'a> {
    pub fn new(
        method: &'a str,
        url: &'a str,
        headers: &'a BTreeMap<String, String>,
        body: &'a [u8],
    ) -> Self {
        Self {
            method,
            url,
            headers,
            body,
        }
    }

    pub fn method(&self) -> &str {
        self.method
    }

    pub fn url(&self) -> &str {
        self.url
    }

    pub fn headers(&self) -> &BTreeMap<String, String> {
        self.headers
    }

    pub fn body(&self) -> &[u8] {
        self.body
    }
}
