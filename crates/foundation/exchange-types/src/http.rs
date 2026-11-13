#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub path: String,
    pub query: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpMethod {
    GET,
    DELETE,
    PATCH,
    POST,
    PUT,
}

#[async_trait::async_trait]
pub trait HttpClient: Send + Sync {
    async fn send(
        &self,
        request: HttpRequest,
        timeout: std::time::Duration,
    ) -> platform_errors::BuildResult<HttpResponse>;
}
