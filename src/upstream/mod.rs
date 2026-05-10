use std::convert::Infallible;
use std::time::Duration;

use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::header::HOST;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use url::Url;

use crate::config::UpstreamConfig;

pub type ProxyError = Box<dyn std::error::Error + Send + Sync>;
pub type ProxyBody = UnsyncBoxBody<Bytes, ProxyError>;

#[derive(Clone)]
pub struct UpstreamClient {
    client: Client<HttpConnector, ProxyBody>,
    connect_timeout: Duration,
    read_timeout: Duration,
}

#[derive(Debug)]
pub enum UpstreamError {
    InvalidBackendUrl(String),
    InvalidUri(hyper::http::uri::InvalidUri),
    ConnectTimeout(Duration),
    Request(hyper_util::client::legacy::Error),
}

impl std::fmt::Display for UpstreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBackendUrl(err) => write!(f, "invalid backend URL: {err}"),
            Self::InvalidUri(err) => write!(f, "invalid upstream request URI: {err}"),
            Self::ConnectTimeout(duration) => {
                write!(f, "upstream connect/read-header timeout after {} ms", duration.as_millis())
            }
            Self::Request(err) => write!(f, "upstream request failed: {err}"),
        }
    }
}

impl std::error::Error for UpstreamError {}

impl UpstreamClient {
    pub fn new(config: &UpstreamConfig) -> Self {
        let mut connector = HttpConnector::new();
        let connect_timeout = Duration::from_millis(config.connect_timeout_ms);
        let read_timeout = Duration::from_millis(config.read_timeout_ms);
        connector.set_connect_timeout(Some(connect_timeout));
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self {
            client,
            connect_timeout,
            read_timeout,
        }
    }

    pub async fn forward<B>(
        &self,
        backend: &str,
        request: Request<B>,
    ) -> Result<Response<ProxyBody>, UpstreamError>
    where
        B: hyper::body::Body<Data = Bytes> + Send + Sync + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let upstream_uri = build_upstream_uri(backend, request.uri())?;
        let (parts, body) = request.into_parts();

        let mut builder = Request::builder()
            .method(parts.method)
            .uri(upstream_uri)
            .version(parts.version);

        if let Some(headers) = builder.headers_mut() {
            for (name, value) in &parts.headers {
                if name != HOST { // HOST is rewritten to match the upstream address
                    headers.insert(name, value.clone());
                }
            }
        }

        let upstream_request = builder
            .body(body.map_err(|err| Box::new(err) as ProxyError).boxed_unsync())
            .expect("invalid upstream request");

        // covers TCP connect and reading response headers
        let upstream_response = timeout(self.connect_timeout, self.client.request(upstream_request))
            .await
            .map_err(|_| UpstreamError::ConnectTimeout(self.connect_timeout))?
            .map_err(UpstreamError::Request)?;

        let (parts, body): (_, Incoming) = upstream_response.into_parts();
        let read_timeout = self.read_timeout;
        // per-chunk idle timeout: fires if no data arrives within read_timeout
        let stream = body
            .into_data_stream()
            .timeout(read_timeout)
            .map(move |result| match result {
                Ok(Ok(bytes)) => Ok(Frame::data(bytes)),
                Ok(Err(err)) => Err(Box::new(err) as ProxyError),
                Err(_) => Err(timeout_error(read_timeout)),
            });

        let mut response = Response::builder()
            .status(parts.status)
            .version(parts.version);

        if let Some(headers) = response.headers_mut() {
            for (name, value) in &parts.headers {
                headers.insert(name, value.clone());
            }
        }

        Ok(response
            .body(StreamBody::new(stream).boxed_unsync())
            .expect("invalid upstream response"))
    }
}

impl Default for UpstreamClient {
    fn default() -> Self {
        Self::new(&UpstreamConfig::default())
    }
}

pub fn bad_gateway_response(err: &UpstreamError) -> Response<ProxyBody> {
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .body(full_body(format!("bad gateway: {err}\n")))
        .expect("invalid bad gateway response")
}

pub fn full_body(body: impl Into<Bytes>) -> ProxyBody {
    Full::new(body.into())
        .map_err(|never: Infallible| match never {})
        .boxed_unsync()
}

fn build_upstream_uri(backend: &str, request_uri: &Uri) -> Result<Uri, UpstreamError> {
    let mut url = Url::parse(backend)
        .map_err(|err| UpstreamError::InvalidBackendUrl(err.to_string()))?;
    url.set_path(request_uri.path());
    url.set_query(request_uri.query());
    url.as_str().parse().map_err(UpstreamError::InvalidUri)
}

fn timeout_error(duration: Duration) -> ProxyError {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("upstream idle timeout: no data received for {} ms", duration.as_millis()),
    ))
}

#[cfg(test)]
mod tests {
    use http_body_util::{BodyExt, Empty, StreamBody};
    use hyper::Uri;
    use hyper::body::{Bytes, Frame, Incoming};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, sleep};
    use tokio_stream::wrappers::ReceiverStream;

    use crate::config::UpstreamConfig;

    use super::{ProxyError, UpstreamClient, build_upstream_uri};

    #[test]
    fn builds_upstream_uri_from_backend_and_request_path() {
        let request_uri: Uri = "/api/users?page=2".parse().unwrap();
        let upstream_uri = build_upstream_uri("http://127.0.0.1:3001", &request_uri).unwrap();

        assert_eq!(upstream_uri.to_string(), "http://127.0.0.1:3001/api/users?page=2");
    }

    #[tokio::test]
    async fn times_out_when_upstream_response_headers_stall() {
        let backend = spawn_header_stall_server().await;
        let client = UpstreamClient::new(&UpstreamConfig {
            connect_timeout_ms: 20,
            read_timeout_ms: 100,
        });
        let request = Request::builder()
            .uri("/api/users")
            .body(Empty::<Bytes>::new())
            .unwrap();

        let err = client.forward(backend.as_str(), request).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("upstream connect/read-header timeout"));
    }

    #[tokio::test]
    async fn times_out_when_upstream_response_body_stalls() {
        let backend = spawn_body_stall_server().await;
        let client = UpstreamClient::new(&UpstreamConfig {
            connect_timeout_ms: 100,
            read_timeout_ms: 20,
        });
        let request = Request::builder()
            .uri("/api/users")
            .body(Empty::<Bytes>::new())
            .unwrap();

        let response = client.forward(backend.as_str(), request).await.unwrap();
        let err = response.into_body().collect().await.unwrap_err();
        let err_text = err.to_string();

        assert!(err_text.contains("upstream idle timeout"));
    }

    async fn spawn_header_stall_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            sleep(Duration::from_millis(100)).await;
            drop(stream);
        });

        format!("http://{addr}")
    }

    async fn spawn_body_stall_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);

            let service = service_fn(|_request: Request<Incoming>| async move {
                let (tx, rx) = mpsc::channel(1);

                tokio::spawn(async move {
                    sleep(Duration::from_millis(100)).await;
                    let _ = tx.send(Ok::<Frame<Bytes>, ProxyError>(Frame::data(Bytes::from_static(b"late")))).await;
                });

                let body = StreamBody::new(ReceiverStream::new(rx)).boxed_unsync();
                Ok::<_, std::convert::Infallible>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(body)
                        .unwrap(),
                )
            });

            let result = http1::Builder::new()
                .serve_connection(io, service)
                .await;
            result.unwrap();
        });

        format!("http://{addr}")
    }
}
