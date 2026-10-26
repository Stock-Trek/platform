use crate::{
    encode::ByteEncoder,
    http::HttpRequest,
    request::{ETHttpRequest, ETWebsocketRequest, WebsocketResponseMatcher},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use platform_errors::{BpError, BpResult, ExchangeError};
use serde::Serialize;
use serde_with::skip_serializing_none;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
struct WebsocketRequest<R> {
    id: ETWebsocketId,
    method: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<BinanceWebsocketParams<R>>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
struct BinanceWebsocketParams<R> {
    #[serde(flatten)]
    request: R,
    signature: Option<String>,
}

pub(crate) struct BinanceRequestFactory;

impl BinanceRequestFactory {
    pub fn try_into_http<R>(mut request: R, signer: &Signer) -> BpResult<HttpRequest>
    where
        R: ETHttpRequest,
    {
        let method = request.http_method();
        let endpoint = request.http_endpoint();
        let is_signed = request.is_signed();
        request.set_api_key(None);
        let query_params = request.query_params(true);
        let (query_params, headers) = if is_signed {
            let signature = signer.signature(query_params.as_bytes())?;
            (
                format!(
                    "{}&signature={}",
                    query_params,
                    ByteEncoder::Percent.encode(signature.as_bytes())
                ),
                vec![("X-MBX-APIKEY".into(), signer.api_key().clone())],
            )
        } else {
            (query_params, vec![])
        };
        let query = (!query_params.is_empty()).then_some(query_params);
        let body = None;
        Ok(HttpRequest {
            method,
            path: endpoint.to_string(),
            query,
            headers,
            body,
        })
    }
    pub fn try_into_websocket<R>(
        mut request: R,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)>
    where
        R: ETWebsocketRequest,
    {
        let method = request.websocket_method_name();
        let is_signed = request.is_signed();
        if is_signed {
            request.set_api_key(Some(signer.api_key()));
        }
        let query_params = request.query_params(false);
        let params = if query_params.is_empty() {
            None
        } else {
            let signature = if is_signed {
                Some(signer.signature(query_params.as_bytes())?)
            } else {
                None
            };
            Some(BinanceWebsocketParams { request, signature })
        };
        let websocket_request = WebsocketRequest {
            id: id.clone(),
            method,
            params,
        };
        let message = serde_json::to_string(&websocket_request)
            .map_err(|error| BpError::Exchange(ExchangeError::SerializeRequest(error)))?;
        let response_matcher: WebsocketResponseMatcher =
            Arc::new(move |value| match value.get("id") {
                None => false,
                Some(id_value) => match &id {
                    ETWebsocketId::Int(int_id) => match id_value.as_i64() {
                        None => false,
                        Some(int_id_value) => int_id_value == *int_id,
                    },
                    ETWebsocketId::Str(str_id) => match id_value.as_str() {
                        None => false,
                        Some(str_id_value) => str_id_value == *str_id,
                    },
                },
            });
        Ok((message, response_matcher))
    }
}
