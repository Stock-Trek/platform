use super::RequestHandler;
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::watch};

const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct ApiConnection;

impl ApiConnection {
    pub(crate) async fn serve(
        stream: TcpStream,
        handler: Arc<dyn RequestHandler>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut connection = Box::pin(
            http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_READ_TIMEOUT)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |request| Self::handle(handler.clone(), request)),
                ),
        );
        tokio::select! {
            result = connection.as_mut() => Self::log(result),
            _ = Self::wait_for_shutdown(&mut shutdown) => {
                connection.as_mut().graceful_shutdown();
                Self::log(connection.as_mut().await);
            }
        }
    }

    async fn handle(
        handler: Arc<dyn RequestHandler>,
        request: hyper::Request<hyper::body::Incoming>,
    ) -> Result<hyper::Response<http_body_util::Full<hyper::body::Bytes>>, Infallible> {
        Ok(handler.handle(request).await)
    }

    async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
        let _ = shutdown.wait_for(|shutdown| *shutdown).await;
    }

    fn log(result: Result<(), hyper::Error>) {
        if let Err(error) = result {
            tracing::warn!("portfolio registry connection failed: {error}");
        }
    }
}
