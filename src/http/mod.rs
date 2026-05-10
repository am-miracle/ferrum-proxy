use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::upstream::{ProxyBody, UpstreamClient, bad_gateway_response, full_body};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, Response, StatusCode};
use tokio_stream::StreamExt;

use crate::balancing::RoundRobinBalancer;
use crate::config::Config;
use crate::health::{HealthManager, spawn_active_checks};
use crate::routing::match_route;
use crate::telemetry::Telemetry;

#[derive(Clone)]
pub struct AppState {
    config: Arc<Config>,
    balancer: Arc<RoundRobinBalancer>,
    health: Arc<HealthManager>,
    telemetry: Arc<Telemetry>,
    upstream: UpstreamClient,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnectionInfo {
    pub remote_addr: SocketAddr,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        let upstream_config = config.upstream.clone();
        let telemetry = Arc::new(Telemetry::new(&config.routes));
        let balancer = Arc::new(RoundRobinBalancer::new(&config.routes));
        let health = Arc::new(HealthManager::with_telemetry(
            &config.routes,
            config.health_check.failure_threshold,
            config.health_check.recovery_threshold,
            Some(telemetry.clone()),
        ));

        Self {
            config: Arc::new(config),
            balancer,
            health,
            telemetry,
            upstream: UpstreamClient::new(&upstream_config),
        }
    }

    pub fn spawn_background_tasks(&self, shutdown: tokio::sync::watch::Receiver<bool>) {
        let handle = spawn_active_checks(self.config.clone(), self.health.clone(), shutdown);
        let telemetry = self.telemetry.clone();
        tokio::spawn(async move {
            if let Err(panic) = handle.await {
                telemetry.log_background_task_failure("health_checks", &panic.to_string());
            }
        });
    }

    pub fn telemetry(&self) -> &Telemetry {
        &self.telemetry
    }

    pub fn telemetry_handle(&self) -> Arc<Telemetry> {
        self.telemetry.clone()
    }

    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.config.server.graceful_shutdown_timeout_ms)
    }

    pub fn client_header_timeout(&self) -> Duration {
        Duration::from_millis(self.config.server.client_header_timeout_ms)
    }
}

pub async fn handle_request<B>(request: Request<B>, state: AppState) -> Response<ProxyBody>
where
    B: hyper::body::Body<Data = Bytes> + Send + Sync + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    state.telemetry.record_request();

    let method = request.method().clone();
    let path = request.uri().path().to_string();

    if let Some(response) = handle_internal_route(request.method(), request.uri().path(), &state) {
        return complete_request(&state, &method, &path, None, Instant::now(), response, None);
    }

    if let Some(content_length) =
        content_length_exceeds(request.headers(), state.upstream.max_request_body_bytes())
    {
        state.telemetry.record_proxy_error("request_body_too_large");
        return complete_request(
            &state,
            &method,
            &path,
            None,
            Instant::now(),
            text_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "request body too large: content-length {content_length} exceeds {} byte limit\n",
                    state.upstream.max_request_body_bytes()
                ),
            ),
            Some("request_body_too_large"),
        );
    }

    let Some(route) = match_route(&path, &state.config.routes) else {
        return complete_request(
            &state,
            &method,
            &path,
            None,
            Instant::now(),
            text_response(StatusCode::NOT_FOUND, "no route matched request path\n"),
            None,
        );
    };

    let healthy_backends = state.health.healthy_backends(route);
    let backend = match state
        .balancer
        .select_backend(&route.path_prefix, &healthy_backends)
    {
        Some(backend) => backend,
        None => {
            state.telemetry.record_proxy_error("no_healthy_backends");
            return complete_request(
                &state,
                &method,
                &path,
                None,
                Instant::now(),
                text_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "no healthy backends available\n",
                ),
                Some("no_healthy_backends"),
            );
        }
    };

    let client_addr = request
        .extensions()
        .get::<ConnectionInfo>()
        .map(|info| info.remote_addr);
    let started = Instant::now();

    let client_body_timeout = Duration::from_millis(state.config.server.client_body_timeout_ms);
    if should_prebuffer_request(&method) {
        match buffer_request_body(
            request,
            state.upstream.max_request_body_bytes(),
            client_body_timeout,
        )
        .await
        {
            Ok(request) => {
                forward_request(
                    &state,
                    &method,
                    &path,
                    backend,
                    request,
                    client_addr,
                    started,
                    client_body_timeout,
                )
                .await
            }
            Err(err) => {
                let kind = err.kind();
                state.telemetry.record_proxy_error(kind);
                complete_request(
                    &state,
                    &method,
                    &path,
                    Some(backend),
                    started,
                    err.into_response(),
                    Some(kind),
                )
            }
        }
    } else {
        forward_request(
            &state,
            &method,
            &path,
            backend,
            request,
            client_addr,
            started,
            client_body_timeout,
        )
        .await
    }
}

async fn forward_request<B>(
    state: &AppState,
    method: &Method,
    path: &str,
    backend: &str,
    request: Request<B>,
    client_addr: Option<SocketAddr>,
    started: Instant,
    client_body_timeout: Duration,
) -> Response<ProxyBody>
where
    B: hyper::body::Body<Data = Bytes> + Send + Sync + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    match state
        .upstream
        .forward(backend, request, client_addr, client_body_timeout)
        .await
    {
        Ok(response) => {
            state.telemetry.record_upstream_latency(started.elapsed());
            if response.status().is_server_error() {
                state.health.record_failure(backend);
            } else {
                state.health.record_success(backend);
            }

            complete_request(state, method, path, Some(backend), started, response, None)
        }
        Err(err) => {
            state.telemetry.record_upstream_latency(started.elapsed());
            state.health.record_failure(backend);
            state.telemetry.record_proxy_error(err.kind());
            complete_request(
                state,
                method,
                path,
                Some(backend),
                started,
                bad_gateway_response(&err),
                Some(err.kind()),
            )
        }
    }
}

async fn buffer_request_body<B>(
    request: Request<B>,
    max_bytes: u64,
    idle_timeout: Duration,
) -> Result<Request<Full<Bytes>>, RequestBufferError>
where
    B: hyper::body::Body<Data = Bytes> + Send + Sync + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let (parts, body) = request.into_parts();
    let stream = body.into_data_stream().timeout(idle_timeout);
    tokio::pin!(stream);
    let mut buffered = Vec::new();

    while let Some(result) = stream.next().await {
        match result {
            Ok(Ok(chunk)) => {
                let next_len = buffered.len() as u64 + chunk.len() as u64;
                if next_len > max_bytes {
                    return Err(RequestBufferError::TooLarge { limit: max_bytes });
                }
                buffered.extend_from_slice(&chunk);
            }
            Ok(Err(err)) => {
                return Err(RequestBufferError::ReadFailed(err.to_string()));
            }
            Err(_) => {
                return Err(RequestBufferError::TimedOut {
                    timeout: idle_timeout,
                });
            }
        }
    }

    Ok(Request::from_parts(parts, Full::new(Bytes::from(buffered))))
}

fn should_prebuffer_request(method: &Method) -> bool {
    !matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
    )
}

enum RequestBufferError {
    TooLarge { limit: u64 },
    TimedOut { timeout: Duration },
    ReadFailed(String),
}

impl RequestBufferError {
    fn kind(&self) -> &'static str {
        match self {
            Self::TooLarge { .. } => "request_body_too_large",
            Self::TimedOut { .. } => "client_body_timeout",
            Self::ReadFailed(_) => "request_body_read_failed",
        }
    }

    fn into_response(self) -> Response<ProxyBody> {
        match self {
            Self::TooLarge { limit } => text_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("request body exceeded {limit} byte limit\n"),
            ),
            Self::TimedOut { timeout } => text_response(
                StatusCode::REQUEST_TIMEOUT,
                format!(
                    "client request body idle timeout after {} ms\n",
                    timeout.as_millis()
                ),
            ),
            Self::ReadFailed(err) => text_response(
                StatusCode::BAD_REQUEST,
                format!("failed to read client request body: {err}\n"),
            ),
        }
    }
}

fn complete_request(
    state: &AppState,
    method: &Method,
    path: &str,
    backend: Option<&str>,
    started: Instant,
    response: Response<ProxyBody>,
    error_kind: Option<&'static str>,
) -> Response<ProxyBody> {
    let status = response.status();
    state.telemetry.record_response_status(status.as_u16());
    state.telemetry.log_request_complete(
        method.as_str(),
        path,
        backend,
        status.as_u16(),
        started.elapsed(),
        error_kind,
    );
    response
}

fn handle_internal_route(
    method: &Method,
    path: &str,
    state: &AppState,
) -> Option<Response<ProxyBody>> {
    match (method, path) {
        (&Method::GET, "/") => Some(root_response(state)),
        (&Method::GET, "/health") => Some(text_response(StatusCode::OK, "ok\n")),
        (&Method::GET, "/health/backends") => Some(backend_health_response(state)),
        (&Method::GET, "/metrics") => Some(metrics_response(state)),
        _ => None,
    }
}

fn root_response(state: &AppState) -> Response<ProxyBody> {
    let route_count = state.config.routes.len();
    text_response(
        StatusCode::OK,
        format!("ferrum-proxy is running with {route_count} configured route(s)\n"),
    )
}

fn backend_health_response(state: &AppState) -> Response<ProxyBody> {
    let body = state
        .health
        .backend_statuses()
        .into_iter()
        .map(|(backend, healthy)| {
            format!(
                "{backend} {}\n",
                if healthy { "healthy" } else { "unhealthy" }
            )
        })
        .collect::<String>();

    text_response(StatusCode::OK, body)
}

fn metrics_response(state: &AppState) -> Response<ProxyBody> {
    text_response(
        StatusCode::OK,
        state
            .telemetry
            .render_prometheus(&state.health.backend_statuses()),
    )
}

fn text_response(status: StatusCode, body: impl Into<Bytes>) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .body(full_body(body))
        .expect("invalid response")
}

fn content_length_exceeds(
    headers: &hyper::header::HeaderMap<hyper::header::HeaderValue>,
    limit: u64,
) -> Option<u64> {
    headers
        .get(hyper::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
        .filter(|content_length| *content_length > limit)
}

#[cfg(test)]
mod tests {
    use http_body_util::{BodyExt, Empty, Full, StreamBody};
    use hyper::body::{Bytes, Frame};
    use hyper::header::{CONNECTION, CONTENT_LENGTH, HOST, HeaderName, HeaderValue};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::{AppState, ConnectionInfo, handle_request, should_prebuffer_request};
    use crate::config::{Config, HealthCheckConfig, RouteConfig, ServerConfig, UpstreamConfig};

    fn sample_state() -> AppState {
        AppState::new(sample_config())
    }

    fn sample_config() -> Config {
        Config {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
                ..Default::default()
            },
            routes: vec![
                RouteConfig {
                    path_prefix: "/api".to_string(),
                    backends: vec![
                        "http://127.0.0.1:3001".to_string(),
                        "http://127.0.0.1:3002".to_string(),
                    ],
                },
                RouteConfig {
                    path_prefix: "/static".to_string(),
                    backends: vec!["http://127.0.0.1:4000".to_string()],
                },
            ],
            health_check: HealthCheckConfig {
                interval_sec: 10,
                endpoint: "/health".to_string(),
                ..Default::default()
            },
            upstream: UpstreamConfig::default(),
        }
    }

    #[tokio::test]
    async fn health_endpoint_responds_ok() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .body(empty_body())
            .unwrap();
        let response = handle_request(request, sample_state()).await;

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_text() {
        let state = sample_state();
        state.telemetry.record_request();
        state
            .telemetry
            .record_upstream_latency(std::time::Duration::from_millis(5));
        state.telemetry.record_response_status(200);
        state.health.record_failure("http://127.0.0.1:3001");
        state.health.record_failure("http://127.0.0.1:3001");
        state.health.record_failure("http://127.0.0.1:3001");

        let request = Request::builder()
            .method(Method::GET)
            .uri("/metrics")
            .body(empty_body())
            .unwrap();
        let response = handle_request(request, state).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = std::str::from_utf8(&body).unwrap();

        assert_eq!(status, StatusCode::OK);
        assert!(text.contains("# TYPE ferrum_proxy_requests_total counter"));
        assert!(text.contains("ferrum_proxy_requests_total 2"));
        assert!(text.contains("ferrum_proxy_backend_healthy"));
    }

    #[tokio::test]
    async fn proxy_rewrites_forwarding_headers_and_strips_connection_headers() {
        let upstream = spawn_test_upstream(|request| async move {
            let host = header_text(request.headers(), HOST);
            let xff = header_text(request.headers(), "x-forwarded-for");
            let xfp = header_text(request.headers(), "x-forwarded-proto");
            let forwarded = header_text(request.headers(), "forwarded");
            let connection = header_text(request.headers(), CONNECTION);
            let keep_alive = header_text(request.headers(), "keep-alive");

            Response::builder()
                .status(StatusCode::OK)
                .header(CONNECTION, "close")
                .body(Full::new(Bytes::from(format!(
                    "host={host} xff={xff} xfp={xfp} forwarded={forwarded} connection={connection} keep_alive={keep_alive}"
                ))))
                .unwrap()
        })
        .await;

        let mut request = Request::builder()
            .method(Method::GET)
            .uri("/api/users")
            .header(HOST, "proxy.local")
            .header(CONNECTION, "keep-alive, x-remove-me")
            .header(HeaderName::from_static("keep-alive"), "timeout=5")
            .header("x-remove-me", "1")
            .body(empty_body())
            .unwrap();
        request.extensions_mut().insert(ConnectionInfo {
            remote_addr: "203.0.113.10:1234".parse().unwrap(),
        });

        let response = handle_request(request, state_with_api_backends(&[upstream.as_str()])).await;
        let response_headers = response.headers().clone();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = std::str::from_utf8(&body).unwrap();

        assert!(text.contains("host=127.0.0.1:"));
        assert!(text.contains("xff=203.0.113.10"));
        assert!(text.contains("xfp=http"));
        assert!(text.contains("forwarded=for=203.0.113.10;proto=http"));
        assert!(text.contains("connection=missing"));
        assert!(text.contains("keep_alive=missing"));
        assert!(!response_headers.contains_key(CONNECTION));
    }

    #[tokio::test]
    async fn rejects_request_body_over_configured_limit() {
        let state = state_with_config(Config {
            upstream: UpstreamConfig {
                max_request_body_bytes: 4,
                ..Default::default()
            },
            ..sample_config()
        });
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/upload")
            .header(CONTENT_LENGTH, "10")
            .body(Full::new(Bytes::from_static(b"0123456789")))
            .unwrap();

        let response = handle_request(request, state).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains("request body too large")
        );
    }

    #[tokio::test]
    async fn chunked_request_body_over_limit_returns_payload_too_large_before_forwarding() {
        let seen_requests = Arc::new(AtomicUsize::new(0));
        let upstream = spawn_test_upstream({
            let seen_requests = seen_requests.clone();
            move |_request| {
                let seen_requests = seen_requests.clone();
                async move {
                    seen_requests.fetch_add(1, Ordering::Relaxed);
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::from_static(b"upstream")))
                        .unwrap()
                }
            }
        })
        .await;

        let state = state_with_config(Config {
            routes: vec![RouteConfig {
                path_prefix: "/api".to_string(),
                backends: vec![upstream],
            }],
            upstream: UpstreamConfig {
                max_request_body_bytes: 4,
                ..Default::default()
            },
            ..sample_config()
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/upload")
            .body(chunked_body(&[b"12".as_slice(), b"345".as_slice()]))
            .unwrap();

        let response = handle_request(request, state).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = std::str::from_utf8(&body).unwrap();

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(text.contains("request body exceeded 4 byte limit"));
        assert_eq!(seen_requests.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn prebuffers_only_unsafe_methods() {
        assert!(!should_prebuffer_request(&Method::GET));
        assert!(!should_prebuffer_request(&Method::HEAD));
        assert!(!should_prebuffer_request(&Method::OPTIONS));
        assert!(!should_prebuffer_request(&Method::TRACE));
        assert!(should_prebuffer_request(&Method::POST));
        assert!(should_prebuffer_request(&Method::PUT));
        assert!(should_prebuffer_request(&Method::PATCH));
        assert!(should_prebuffer_request(&Method::DELETE));
    }

    #[tokio::test]
    async fn rejects_upstream_response_over_configured_limit() {
        let upstream = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_LENGTH, "10")
                .body(Full::new(Bytes::from_static(b"0123456789")))
                .unwrap()
        })
        .await;

        let state = state_with_config(Config {
            routes: vec![RouteConfig {
                path_prefix: "/api".to_string(),
                backends: vec![upstream],
            }],
            upstream: UpstreamConfig {
                max_response_body_bytes: 4,
                ..Default::default()
            },
            ..sample_config()
        });

        let response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state,
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn unknown_route_returns_not_found() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/unknown")
            .body(empty_body())
            .unwrap();
        let response = handle_request(request, sample_state()).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, Bytes::from_static(b"no route matched request path\n"));
    }

    fn header_text(
        headers: &hyper::header::HeaderMap<HeaderValue>,
        name: impl hyper::header::AsHeaderName,
    ) -> String {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("missing")
            .to_string()
    }

    fn empty_body() -> Empty<Bytes> {
        Empty::new()
    }

    fn chunked_body(
        chunks: &[&[u8]],
    ) -> StreamBody<ReceiverStream<Result<Frame<Bytes>, std::convert::Infallible>>> {
        let (tx, rx) = mpsc::channel(chunks.len());
        let owned_chunks: Vec<Bytes> = chunks
            .iter()
            .map(|chunk| Bytes::copy_from_slice(chunk))
            .collect();

        tokio::spawn(async move {
            for chunk in owned_chunks {
                let _ = tx
                    .send(Ok::<Frame<Bytes>, std::convert::Infallible>(Frame::data(
                        chunk,
                    )))
                    .await;
            }
        });

        StreamBody::new(ReceiverStream::new(rx))
    }

    fn state_with_api_backends(backends: &[&str]) -> AppState {
        state_with_config(Config {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
                ..Default::default()
            },
            routes: vec![
                RouteConfig {
                    path_prefix: "/api".to_string(),
                    backends: backends
                        .iter()
                        .map(|backend| (*backend).to_string())
                        .collect(),
                },
                RouteConfig {
                    path_prefix: "/static".to_string(),
                    backends: vec!["http://127.0.0.1:4000".to_string()],
                },
            ],
            health_check: HealthCheckConfig {
                interval_sec: 10,
                endpoint: "/health".to_string(),
                ..Default::default()
            },
            upstream: UpstreamConfig::default(),
        })
    }

    fn state_with_config(config: Config) -> AppState {
        AppState::new(config)
    }

    async fn spawn_test_upstream<F, Fut>(handler: F) -> String
    where
        F: Fn(Request<hyper::body::Incoming>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Response<Full<Bytes>>> + Send + 'static,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);
            let service = service_fn(move |request| {
                let future = handler(request);
                async move { Ok::<_, std::convert::Infallible>(future.await) }
            });

            http1::Builder::new()
                .serve_connection(io, service)
                .await
                .unwrap();
        });

        format!("http://{addr}")
    }
}
