use std::collections::HashSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::header::{
    CONNECTION, HOST, HeaderMap, HeaderName, HeaderValue, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION,
    TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use url::Url;

use crate::config::UpstreamConfig;

pub type ProxyError = Box<dyn std::error::Error + Send + Sync>;
pub type ProxyBody = UnsyncBoxBody<Bytes, ProxyError>;

const X_FORWARDED_FOR: &str = "x-forwarded-for";
const X_FORWARDED_PROTO: &str = "x-forwarded-proto";
const FORWARDED: &str = "forwarded";
const PROXY_CONNECTION: &str = "proxy-connection";
const KEEP_ALIVE: &str = "keep-alive";

#[derive(Clone)]
pub struct UpstreamClient {
    client: Client<HttpConnector, ProxyBody>,
    connect_timeout: Duration,
    read_timeout: Duration,
    max_request_body_bytes: u64,
    max_response_body_bytes: u64,
}

#[derive(Debug)]
pub enum UpstreamError {
    InvalidBackendUrl(String),
    InvalidUri(hyper::http::uri::InvalidUri),
    ConnectTimeout(Duration),
    Request(hyper_util::client::legacy::Error),
    ResponseTooLarge { limit: u64, content_length: u64 },
}

impl UpstreamError {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::InvalidBackendUrl(_) => "invalid_backend_url",
            Self::InvalidUri(_) => "invalid_upstream_uri",
            Self::ConnectTimeout(_) => "upstream_connect_timeout",
            Self::Request(_) => "upstream_request_failed",
            Self::ResponseTooLarge { .. } => "response_body_too_large",
        }
    }
}

impl std::fmt::Display for UpstreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBackendUrl(err) => write!(f, "invalid backend URL: {err}"),
            Self::InvalidUri(err) => write!(f, "invalid upstream request URI: {err}"),
            Self::ConnectTimeout(duration) => {
                write!(
                    f,
                    "upstream connect/read-header timeout after {} ms",
                    duration.as_millis()
                )
            }
            Self::Request(err) => write!(f, "upstream request failed: {err}"),
            Self::ResponseTooLarge {
                limit,
                content_length,
            } => {
                write!(
                    f,
                    "upstream response body exceeds limit: content-length {content_length} bytes over {limit} byte limit"
                )
            }
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
            max_request_body_bytes: config.max_request_body_bytes,
            max_response_body_bytes: config.max_response_body_bytes,
        }
    }

    pub fn max_request_body_bytes(&self) -> u64 {
        self.max_request_body_bytes
    }

    pub async fn forward<B>(
        &self,
        backend: &str,
        request: Request<B>,
        client_addr: Option<SocketAddr>,
        client_body_timeout: Duration,
    ) -> Result<Response<ProxyBody>, UpstreamError>
    where
        B: hyper::body::Body<Data = Bytes> + Send + Sync + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let backend_url =
            Url::parse(backend).map_err(|err| UpstreamError::InvalidBackendUrl(err.to_string()))?;
        let upstream_uri = build_upstream_uri(&backend_url, request.uri())?;
        let (parts, body) = request.into_parts();

        let mut builder = Request::builder()
            .method(parts.method)
            .uri(upstream_uri)
            .version(parts.version);

        if let Some(headers) = builder.headers_mut() {
            *headers = parts.headers.clone();
            sanitize_hop_by_hop_headers(headers);
            apply_forwarding_headers(headers, &backend_url, client_addr);
        }

        let upstream_request = builder
            .body(limit_body_stream(
                body,
                self.max_request_body_bytes,
                client_body_timeout,
                "client request body",
            ))
            .expect("invalid upstream request");

        let upstream_response =
            timeout(self.connect_timeout, self.client.request(upstream_request))
                .await
                .map_err(|_| UpstreamError::ConnectTimeout(self.connect_timeout))?
                .map_err(UpstreamError::Request)?;

        let (parts, body): (_, Incoming) = upstream_response.into_parts();
        if let Some(content_length) =
            content_length_exceeds(&parts.headers, self.max_response_body_bytes)
        {
            return Err(UpstreamError::ResponseTooLarge {
                limit: self.max_response_body_bytes,
                content_length,
            });
        }

        let mut response = Response::builder()
            .status(parts.status)
            .version(parts.version);

        if let Some(headers) = response.headers_mut() {
            *headers = parts.headers.clone();
            sanitize_hop_by_hop_headers(headers);
        }

        Ok(response
            .body(limit_body_stream(
                body,
                self.max_response_body_bytes,
                self.read_timeout,
                "upstream response body",
            ))
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

fn build_upstream_uri(backend: &Url, request_uri: &Uri) -> Result<Uri, UpstreamError> {
    let mut url = backend.clone();
    url.set_path(request_uri.path());
    url.set_query(request_uri.query());
    url.as_str().parse().map_err(UpstreamError::InvalidUri)
}

fn apply_forwarding_headers(
    headers: &mut HeaderMap<HeaderValue>,
    backend_url: &Url,
    client_addr: Option<SocketAddr>,
) {
    headers.remove(HOST);
    headers.remove(X_FORWARDED_FOR);
    headers.remove(X_FORWARDED_PROTO);
    headers.remove(FORWARDED);

    if let Some(authority) = backend_authority(backend_url) {
        if let Ok(host) = HeaderValue::from_str(&authority) {
            headers.insert(HOST, host);
        }
    }

    if let Some(client_addr) = client_addr {
        let client_ip = client_addr.ip().to_string();
        if let Ok(value) = HeaderValue::from_str(&client_ip) {
            headers.insert(HeaderName::from_static(X_FORWARDED_FOR), value);
        }

        headers.insert(
            HeaderName::from_static(X_FORWARDED_PROTO),
            HeaderValue::from_static("http"),
        );

        let forwarded = format!("for={};proto=http", format_forwarded_for(&client_ip));
        if let Ok(value) = HeaderValue::from_str(&forwarded) {
            headers.insert(HeaderName::from_static(FORWARDED), value);
        }
    }
}

fn sanitize_hop_by_hop_headers(headers: &mut HeaderMap<HeaderValue>) {
    let mut strip_names = HashSet::new();
    strip_names.insert(CONNECTION);
    strip_names.insert(HeaderName::from_static(KEEP_ALIVE));
    strip_names.insert(PROXY_AUTHENTICATE);
    strip_names.insert(PROXY_AUTHORIZATION);
    strip_names.insert(TE);
    strip_names.insert(TRAILER);
    strip_names.insert(TRANSFER_ENCODING);
    strip_names.insert(UPGRADE);
    strip_names.insert(HeaderName::from_static(PROXY_CONNECTION));

    if let Some(value) = headers.get(CONNECTION).cloned() {
        if let Ok(value) = value.to_str() {
            for token in value.split(',') {
                let token = token.trim().to_ascii_lowercase();
                if let Ok(name) = HeaderName::from_bytes(token.as_bytes()) {
                    strip_names.insert(name);
                }
            }
        }
    }

    for name in strip_names {
        headers.remove(name);
    }
}

fn content_length_exceeds(headers: &HeaderMap<HeaderValue>, limit: u64) -> Option<u64> {
    headers
        .get(hyper::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
        .filter(|content_length| *content_length > limit)
}

fn limit_body_stream<B>(
    body: B,
    max_bytes: u64,
    idle_timeout: Duration,
    context: &'static str,
) -> ProxyBody
where
    B: hyper::body::Body<Data = Bytes> + Send + Sync + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let mut seen = 0u64;
    let stream = body
        .into_data_stream()
        .timeout(idle_timeout)
        .map(move |result| match result {
            Ok(Ok(bytes)) => {
                seen += bytes.len() as u64;
                if seen > max_bytes {
                    Err(size_limit_error(max_bytes, context))
                } else {
                    Ok(Frame::data(bytes))
                }
            }
            Ok(Err(err)) => Err(Box::new(err) as ProxyError),
            Err(_) => Err(timeout_error(idle_timeout, context)),
        });

    StreamBody::new(stream).boxed_unsync()
}

fn timeout_error(duration: Duration, context: &str) -> ProxyError {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!(
            "{context} idle timeout: no data received for {} ms",
            duration.as_millis()
        ),
    ))
}

fn size_limit_error(limit: u64, context: &str) -> ProxyError {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("{context} exceeded {limit} byte limit"),
    ))
}

fn format_forwarded_for(value: &str) -> String {
    if value.contains(':') {
        format!("\"[{value}]\"")
    } else {
        value.to_string()
    }
}

fn backend_authority(backend_url: &Url) -> Option<String> {
    let host = backend_url.host_str()?;
    Some(match backend_url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use http_body_util::{BodyExt, Empty, Full, StreamBody};
    use hyper::Uri;
    use hyper::body::{Bytes, Frame, Incoming};
    use hyper::header::{CONNECTION, HOST, HeaderMap, HeaderName, HeaderValue};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, sleep};
    use tokio_stream::wrappers::ReceiverStream;
    use url::Url;

    use crate::config::UpstreamConfig;

    use super::{
        KEEP_ALIVE, ProxyError, UpstreamClient, build_upstream_uri, sanitize_hop_by_hop_headers,
    };

    #[test]
    fn builds_upstream_uri_from_backend_and_request_path() {
        let request_uri: Uri = "/api/users?page=2".parse().unwrap();
        let backend = Url::parse("http://127.0.0.1:3001").unwrap();
        let upstream_uri = build_upstream_uri(&backend, &request_uri).unwrap();

        assert_eq!(
            upstream_uri.to_string(),
            "http://127.0.0.1:3001/api/users?page=2"
        );
    }

    #[test]
    fn strips_hop_by_hop_and_connection_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONNECTION,
            HeaderValue::from_static("keep-alive, x-remove-me"),
        );
        headers.insert(
            HeaderName::from_static(KEEP_ALIVE),
            HeaderValue::from_static("timeout=5"),
        );
        headers.insert(
            HeaderName::from_static("x-remove-me"),
            HeaderValue::from_static("1"),
        );
        headers.insert(HOST, HeaderValue::from_static("proxy.local"));

        sanitize_hop_by_hop_headers(&mut headers);

        assert!(!headers.contains_key(CONNECTION));
        assert!(!headers.contains_key(KEEP_ALIVE));
        assert!(!headers.contains_key("x-remove-me"));
        assert!(headers.contains_key(HOST));
    }

    #[tokio::test]
    async fn times_out_when_upstream_response_headers_stall() {
        let backend = spawn_header_stall_server().await;
        let client = UpstreamClient::new(&UpstreamConfig {
            connect_timeout_ms: 20,
            read_timeout_ms: 100,
            ..Default::default()
        });
        let request = Request::builder()
            .uri("/api/users")
            .body(Empty::<Bytes>::new())
            .unwrap();

        let err = client
            .forward(backend.as_str(), request, None, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("upstream connect/read-header timeout")
        );
    }

    #[tokio::test]
    async fn times_out_when_upstream_response_body_stalls() {
        let backend = spawn_body_stall_server().await;
        let client = UpstreamClient::new(&UpstreamConfig {
            connect_timeout_ms: 100,
            read_timeout_ms: 20,
            ..Default::default()
        });
        let request = Request::builder()
            .uri("/api/users")
            .body(Empty::<Bytes>::new())
            .unwrap();

        let response = client
            .forward(backend.as_str(), request, None, Duration::from_millis(50))
            .await
            .unwrap();
        let err = response.into_body().collect().await.unwrap_err();
        let err_text = err.to_string();

        assert!(err_text.contains("upstream response body idle timeout"));
    }

    #[tokio::test]
    async fn rejects_large_upstream_response_by_content_length() {
        let backend = spawn_large_response_server().await;
        let client = UpstreamClient::new(&UpstreamConfig {
            max_response_body_bytes: 4,
            ..Default::default()
        });
        let request = Request::builder()
            .uri("/api/users")
            .body(Empty::<Bytes>::new())
            .unwrap();

        let err = client
            .forward(backend.as_str(), request, None, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), "response_body_too_large");
    }

    #[tokio::test]
    async fn chunked_upstream_response_over_limit_errors_mid_stream() {
        let backend = spawn_chunked_large_response_server().await;
        let client = UpstreamClient::new(&UpstreamConfig {
            max_response_body_bytes: 4,
            ..Default::default()
        });
        let request = Request::builder()
            .uri("/api/users")
            .body(Empty::<Bytes>::new())
            .unwrap();

        let response = client
            .forward(backend.as_str(), request, None, Duration::from_millis(50))
            .await
            .unwrap();
        let err = response.into_body().collect().await.unwrap_err();

        assert!(
            err.to_string()
                .contains("upstream response body exceeded 4 byte limit")
        );
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
                    let _ = tx
                        .send(Ok::<Frame<Bytes>, ProxyError>(Frame::data(
                            Bytes::from_static(b"late"),
                        )))
                        .await;
                });

                let body = StreamBody::new(ReceiverStream::new(rx)).boxed_unsync();
                Ok::<_, std::convert::Infallible>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(body)
                        .unwrap(),
                )
            });

            let result = http1::Builder::new().serve_connection(io, service).await;
            result.unwrap();
        });

        format!("http://{addr}")
    }

    async fn spawn_large_response_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);

            let service = service_fn(|_request: Request<Incoming>| async move {
                Ok::<_, std::convert::Infallible>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(hyper::header::CONTENT_LENGTH, "10")
                        .body(Full::new(Bytes::from_static(b"0123456789")))
                        .unwrap(),
                )
            });

            http1::Builder::new()
                .serve_connection(io, service)
                .await
                .unwrap();
        });

        format!("http://{addr}")
    }

    async fn spawn_chunked_large_response_server() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);

            let service = service_fn(|_request: Request<Incoming>| async move {
                let (tx, rx) = mpsc::channel(2);

                tokio::spawn(async move {
                    let _ = tx
                        .send(Ok::<Frame<Bytes>, ProxyError>(Frame::data(
                            Bytes::from_static(b"12"),
                        )))
                        .await;
                    let _ = tx
                        .send(Ok::<Frame<Bytes>, ProxyError>(Frame::data(
                            Bytes::from_static(b"345"),
                        )))
                        .await;
                });

                Ok::<_, std::convert::Infallible>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(StreamBody::new(ReceiverStream::new(rx)).boxed_unsync())
                        .unwrap(),
                )
            });

            http1::Builder::new()
                .serve_connection(io, service)
                .await
                .unwrap();
        });

        format!("http://{addr}")
    }
}
