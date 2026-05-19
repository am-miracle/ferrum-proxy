use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::upstream::{ProxyBody, UpstreamClient, bad_gateway_response, full_body};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use tokio_stream::StreamExt;

static REQUEST_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_request_id() -> String {
    let id = REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{id:016x}")
}

use crate::balancing::LoadBalancer;
use crate::config::{Config, RouteConfig};
use crate::health::{HealthManager, run_active_check_pass, spawn_active_checks};
use crate::routing::match_route;
use crate::telemetry::Telemetry;

#[derive(Clone)]
pub struct AppState {
    config: Arc<Config>,
    balancer: Arc<LoadBalancer>,
    health: Arc<HealthManager>,
    telemetry: Arc<Telemetry>,
    upstream: UpstreamClient,
    buffer_semaphore: Arc<tokio::sync::Semaphore>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnectionInfo {
    pub remote_addr: SocketAddr,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        let upstream_config = config.upstream.clone();
        let max_buffered_bodies = config.upstream.max_buffered_bodies;
        let telemetry = Arc::new(Telemetry::new(&config.routes));
        let balancer = Arc::new(LoadBalancer::new(&config.routes));
        let health = Arc::new(HealthManager::with_telemetry(
            &config.routes,
            config.health_check.failure_threshold,
            config.health_check.recovery_threshold,
            Duration::from_millis(config.health_check.ejection_duration_ms),
            config.health_check.active_success_status_min,
            config.health_check.active_success_status_max,
            config.health_check.passive_failure_status_min,
            config.health_check.passive_failure_status_max,
            Some(telemetry.clone()),
        ));

        Self {
            config: Arc::new(config),
            balancer,
            health,
            telemetry,
            upstream: UpstreamClient::new(&upstream_config),
            buffer_semaphore: Arc::new(tokio::sync::Semaphore::new(max_buffered_bodies)),
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

    pub fn route_client_body_timeout(&self, route: &RouteConfig) -> Duration {
        Duration::from_millis(
            route
                .client_body_timeout_ms
                .unwrap_or(self.config.server.client_body_timeout_ms),
        )
    }

    pub fn route_connect_timeout(&self, route: &RouteConfig) -> Duration {
        Duration::from_millis(
            route
                .connect_timeout_ms
                .unwrap_or(self.config.upstream.connect_timeout_ms),
        )
    }

    pub fn route_read_timeout(&self, route: &RouteConfig) -> Duration {
        Duration::from_millis(
            route
                .read_timeout_ms
                .unwrap_or(self.config.upstream.read_timeout_ms),
        )
    }

    pub fn should_retry_status(&self, route: &RouteConfig, status: StatusCode) -> bool {
        let retry_on_statuses = if route.retry_on_statuses.is_empty() {
            &self.config.retry.retry_on_statuses
        } else {
            &route.retry_on_statuses
        };
        retry_on_statuses.contains(&status.as_u16())
    }

    pub fn is_passive_failure_status(&self, route: &RouteConfig, status: StatusCode) -> bool {
        if !route.passive_failure_statuses.is_empty() {
            return route.passive_failure_statuses.contains(&status.as_u16());
        }

        self.health.is_passive_failure_status(status)
    }

    pub async fn run_startup_health_check(&self) -> Result<(), Box<dyn std::error::Error>> {
        run_active_check_pass(&self.config, &self.health).await;
        let dead_pools = self.health.startup_dead_pools(&self.config.routes);
        for pool in &dead_pools {
            self.telemetry
                .log_startup_warning(pool.as_str(), "all_backends_unavailable");
        }
        if self.config.server.fail_on_startup_dead_pool && !dead_pools.is_empty() {
            return Err(format!(
                "startup aborted: {} route(s) have no reachable backends: {}",
                dead_pools.len(),
                dead_pools.join(", ")
            )
            .into());
        }
        Ok(())
    }
}

pub async fn handle_request<B>(mut request: Request<B>, state: AppState) -> Response<ProxyBody>
where
    B: hyper::body::Body<Data = Bytes> + Send + Sync + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    state.telemetry.record_request();

    // Preserve an existing X-Request-ID from the client, or generate a new one. The header is
    // forwarded to upstreams so proxy logs and backend logs share a correlation handle.
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .unwrap_or_else(next_request_id);
    if let Ok(val) = HeaderValue::from_str(&request_id) {
        request
            .headers_mut()
            .insert(HeaderName::from_static("x-request-id"), val);
    }

    let method = request.method().clone();
    let path = request.uri().path().to_string();

    if let Some(response) = handle_internal_route(&request, &state) {
        return complete_request(
            &state,
            &method,
            &path,
            None,
            Instant::now(),
            response,
            None,
            &request_id,
        );
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
            &request_id,
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
            &request_id,
        );
    };

    let client_addr = request
        .extensions()
        .get::<ConnectionInfo>()
        .map(|info| info.remote_addr);
    let started = Instant::now();

    let client_body_timeout = state.route_client_body_timeout(route);
    if should_retry_request(&state, &method) {
        let _permit = match state.buffer_semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                state
                    .telemetry
                    .record_proxy_error("buffer_capacity_exceeded");
                return complete_request(
                    &state,
                    &method,
                    &path,
                    None,
                    started,
                    text_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "too many concurrent buffered requests\n",
                    ),
                    Some("buffer_capacity_exceeded"),
                    &request_id,
                );
            }
        };
        match buffer_request_body(
            request,
            state.upstream.max_request_body_bytes(),
            client_body_timeout,
        )
        .await
        {
            Ok(buffered) => {
                drop(_permit);
                forward_with_retries(
                    &state,
                    route,
                    &method,
                    &path,
                    buffered,
                    client_addr,
                    started,
                    client_body_timeout,
                    &request_id,
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
                    None,
                    started,
                    err.into_response(),
                    Some(kind),
                    &request_id,
                )
            }
        }
    } else if should_prebuffer_request(&method) {
        let backend = match select_backend(&state, route, None) {
            Some(backend) => backend,
            None => return no_healthy_backends_response(&state, &method, &path, &request_id),
        };

        let _permit = match state.buffer_semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                state
                    .telemetry
                    .record_proxy_error("buffer_capacity_exceeded");
                return complete_request(
                    &state,
                    &method,
                    &path,
                    Some(backend.as_str()),
                    started,
                    text_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "too many concurrent buffered requests\n",
                    ),
                    Some("buffer_capacity_exceeded"),
                    &request_id,
                );
            }
        };
        match buffer_request_body(
            request,
            state.upstream.max_request_body_bytes(),
            client_body_timeout,
        )
        .await
        {
            Ok(buffered) => {
                drop(_permit);
                forward_request(
                    &state,
                    &method,
                    &path,
                    route,
                    backend.as_str(),
                    buffered.map(Full::new),
                    client_addr,
                    started,
                    client_body_timeout,
                    &request_id,
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
                    Some(backend.as_str()),
                    started,
                    err.into_response(),
                    Some(kind),
                    &request_id,
                )
            }
        }
    } else {
        let backend = match select_backend(&state, route, None) {
            Some(backend) => backend,
            None => return no_healthy_backends_response(&state, &method, &path, &request_id),
        };

        forward_request(
            &state,
            &method,
            &path,
            route,
            backend.as_str(),
            request,
            client_addr,
            started,
            client_body_timeout,
            &request_id,
        )
        .await
    }
}

async fn forward_with_retries(
    state: &AppState,
    route: &RouteConfig,
    method: &Method,
    path: &str,
    request: Request<Bytes>,
    client_addr: Option<SocketAddr>,
    started: Instant,
    client_body_timeout: Duration,
    request_id: &str,
) -> Response<ProxyBody> {
    let replayable = ReplayableRequest::from(request);
    let max_attempts = state.config.retry.max_attempts;
    let total_timeout = Duration::from_millis(state.config.retry.total_timeout_ms);
    let base_backoff_ms = state.config.retry.backoff_ms;
    let mut attempted_backends = HashSet::new();
    let mut attempt = 1usize;

    loop {
        if started.elapsed() >= total_timeout {
            state.telemetry.record_proxy_error("retry_budget_exhausted");
            return complete_request(
                state,
                method,
                path,
                None,
                started,
                text_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    "retry budget exhausted before a successful upstream response\n",
                ),
                Some("retry_budget_exhausted"),
                request_id,
            );
        }

        let backend = match select_backend(state, route, Some(&attempted_backends)) {
            Some(backend) => backend,
            None => {
                if attempt == 1 {
                    return no_healthy_backends_response(state, method, path, request_id);
                }

                state.telemetry.record_proxy_error("retry_exhausted");
                return complete_request(
                    state,
                    method,
                    path,
                    None,
                    started,
                    text_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "retry attempts exhausted with no healthy backends remaining\n",
                    ),
                    Some("retry_exhausted"),
                    request_id,
                );
            }
        };
        attempted_backends.insert(backend.clone());

        let response = forward_request(
            state,
            method,
            path,
            route,
            backend.as_str(),
            replayable.to_request(),
            client_addr,
            started,
            client_body_timeout,
            request_id,
        )
        .await;

        let status = response.status();
        if !should_retry_response(state, route, status)
            || attempt >= max_attempts
            || started.elapsed() >= total_timeout
        {
            return response;
        }

        state.telemetry.log_retry_attempt(
            method.as_str(),
            path,
            backend.as_str(),
            attempt + 1,
            "retryable_status",
        );

        // exponential backoff: base * 2^(attempt-1), capped by the remaining timeout budget.
        if base_backoff_ms > 0 {
            let backoff = Duration::from_millis(
                base_backoff_ms.saturating_mul(1u64 << (attempt - 1).min(10)),
            );
            let remaining = total_timeout.saturating_sub(started.elapsed());
            if backoff >= remaining {
                return response;
            }
            tokio::time::sleep(backoff).await;
        }

        attempt += 1;
    }
}

async fn forward_request<B>(
    state: &AppState,
    method: &Method,
    path: &str,
    route: &RouteConfig,
    backend: &str,
    request: Request<B>,
    client_addr: Option<SocketAddr>,
    started: Instant,
    client_body_timeout: Duration,
    request_id: &str,
) -> Response<ProxyBody>
where
    B: hyper::body::Body<Data = Bytes> + Send + Sync + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let request_error_hook: Arc<dyn Fn(&'static str) + Send + Sync> = {
        let telemetry = state.telemetry_handle();
        Arc::new(move |kind| telemetry.record_proxy_error(kind))
    };
    let response_error_hook: Arc<dyn Fn(&'static str) + Send + Sync> = {
        let telemetry = state.telemetry_handle();
        let health = state.health.clone();
        let backend = backend.to_string();
        Arc::new(move |kind| {
            telemetry.record_proxy_error(kind);
            health.record_failure(backend.as_str());
        })
    };

    match state
        .upstream
        .forward(
            backend,
            request,
            client_addr,
            state.route_connect_timeout(route),
            state.route_read_timeout(route),
            client_body_timeout,
            Some(request_error_hook),
            Some(response_error_hook),
        )
        .await
    {
        Ok(response) => {
            state.telemetry.record_upstream_latency(started.elapsed());
            if state.is_passive_failure_status(route, response.status()) {
                state.health.record_failure(backend);
            } else {
                state.health.record_success(backend);
            }

            complete_request(
                state,
                method,
                path,
                Some(backend),
                started,
                response,
                None,
                request_id,
            )
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
                request_id,
            )
        }
    }
}

async fn buffer_request_body<B>(
    request: Request<B>,
    max_bytes: u64,
    idle_timeout: Duration,
) -> Result<Request<Bytes>, RequestBufferError>
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

    Ok(Request::from_parts(parts, Bytes::from(buffered)))
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
            Self::ReadFailed(_) => "client_request_body_read_failed",
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

struct ReplayableRequest {
    method: Method,
    uri: hyper::Uri,
    version: hyper::Version,
    headers: hyper::HeaderMap<HeaderValue>,
    body: Bytes,
}

impl ReplayableRequest {
    fn to_request(&self) -> Request<Full<Bytes>> {
        let mut builder = Request::builder()
            .method(self.method.clone())
            .uri(self.uri.clone())
            .version(self.version);

        if let Some(headers) = builder.headers_mut() {
            *headers = self.headers.clone();
            if let Ok(content_length) = HeaderValue::from_str(&self.body.len().to_string()) {
                headers.insert(CONTENT_LENGTH, content_length);
            }
        }

        builder
            .body(Full::new(self.body.clone()))
            .expect("invalid replay request")
    }
}

impl From<Request<Bytes>> for ReplayableRequest {
    fn from(request: Request<Bytes>) -> Self {
        let (parts, body) = request.into_parts();

        Self {
            method: parts.method,
            uri: parts.uri,
            version: parts.version,
            headers: parts.headers,
            body,
        }
    }
}

fn complete_request(
    state: &AppState,
    method: &Method,
    path: &str,
    backend: Option<&str>,
    started: Instant,
    mut response: Response<ProxyBody>,
    error_kind: Option<&'static str>,
    request_id: &str,
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
        request_id,
    );
    if let Ok(val) = HeaderValue::from_str(request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-request-id"), val);
    }
    response
}

fn handle_internal_route(
    request: &Request<impl hyper::body::Body>,
    state: &AppState,
) -> Option<Response<ProxyBody>> {
    let method = request.method();
    let path = request.uri().path();

    match (method, path) {
        (&Method::GET, "/") => Some(root_response(state)),
        (&Method::GET, "/health") => Some(text_response(StatusCode::OK, "ok\n")),
        (&Method::GET, "/health/backends") => {
            if !state.config.debug.expose_backend_health {
                None
            } else if !debug_request_authorized(request, state) {
                Some(unauthorized_debug_response())
            } else {
                Some(backend_health_response(state))
            }
        }
        (&Method::GET, "/metrics") => {
            if !state.config.debug.expose_metrics {
                None
            } else if !debug_request_authorized(request, state) {
                Some(unauthorized_debug_response())
            } else {
                Some(metrics_response(state))
            }
        }
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
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(full_body(
            state
                .telemetry
                .render_prometheus(&state.health.backend_statuses()),
        ))
        .expect("invalid metrics response")
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
        .get(CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
        .filter(|content_length| *content_length > limit)
}

fn no_healthy_backends_response(
    state: &AppState,
    method: &Method,
    path: &str,
    request_id: &str,
) -> Response<ProxyBody> {
    state.telemetry.record_proxy_error("no_healthy_backends");
    complete_request(
        state,
        method,
        path,
        None,
        Instant::now(),
        text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "no healthy backends available\n",
        ),
        Some("no_healthy_backends"),
        request_id,
    )
}

fn select_backend<'a>(
    state: &'a AppState,
    route: &'a RouteConfig,
    attempted_backends: Option<&HashSet<String>>,
) -> Option<String> {
    let healthy_backends = state.health.healthy_backends(route);
    let eligible: Vec<_> = match attempted_backends {
        Some(attempted_backends) => healthy_backends
            .into_iter()
            .filter(|backend| !attempted_backends.contains(*backend))
            .collect(),
        None => healthy_backends,
    };

    state
        .balancer
        .select_backend(route, &eligible)
        .map(str::to_string)
}

fn should_retry_request(state: &AppState, method: &Method) -> bool {
    state.config.retry.max_attempts > 1
        && is_retryable_method(method, state.config.retry.retry_idempotent_methods)
}

// GET/HEAD/OPTIONS/TRACE are always safe to replay. PUT/DELETE are idempotent per spec but
// not always safe to replay in practice (they may have partial side effects before a 5xx).
// Opt in via retry.retry_idempotent_methods.
fn is_retryable_method(method: &Method, retry_idempotent: bool) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
    ) || (retry_idempotent && matches!(*method, Method::PUT | Method::DELETE))
}

fn should_retry_response(state: &AppState, route: &RouteConfig, status: StatusCode) -> bool {
    state.should_retry_status(route, status)
}

fn debug_request_authorized(request: &Request<impl hyper::body::Body>, state: &AppState) -> bool {
    let Some(expected_token) = &state.config.debug.auth_token else {
        return true;
    };

    let expected = format!("Bearer {expected_token}");
    request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some(expected.as_str())
}

fn unauthorized_debug_response() -> Response<ProxyBody> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("www-authenticate", HeaderValue::from_static("Bearer"))
        .body(full_body("debug endpoint requires bearer authentication\n"))
        .expect("invalid debug auth response")
}

#[cfg(test)]
mod tests {
    use http_body_util::{BodyExt, Empty, Full, StreamBody};
    use hyper::body::{Bytes, Frame};
    use hyper::header::{AUTHORIZATION, CONNECTION, CONTENT_LENGTH, HOST, HeaderName, HeaderValue};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::{AppState, ConnectionInfo, handle_request, should_prebuffer_request};
    use crate::config::{
        BalancingStrategy, Config, HealthCheckConfig, RouteConfig, ServerConfig, UpstreamConfig,
    };

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
                    balancing: BalancingStrategy::RoundRobin,
                    retry_on_statuses: vec![],
                    passive_failure_statuses: vec![],
                    health_check_endpoint: None,
                    connect_timeout_ms: None,
                    read_timeout_ms: None,
                    client_body_timeout_ms: None,
                },
                RouteConfig {
                    path_prefix: "/static".to_string(),
                    backends: vec!["http://127.0.0.1:4000".to_string()],
                    balancing: BalancingStrategy::RoundRobin,
                    retry_on_statuses: vec![],
                    passive_failure_statuses: vec![],
                    health_check_endpoint: None,
                    connect_timeout_ms: None,
                    read_timeout_ms: None,
                    client_body_timeout_ms: None,
                },
            ],
            health_check: HealthCheckConfig {
                interval_sec: 10,
                endpoint: "/health".to_string(),
                ..Default::default()
            },
            upstream: UpstreamConfig::default(),
            retry: crate::config::RetryConfig::default(),
            debug: crate::config::DebugConfig::default(),
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
    async fn metrics_endpoint_requires_bearer_token_when_configured() {
        let state = state_with_config(Config {
            debug: crate::config::DebugConfig {
                auth_token: Some("secret-token".to_string()),
                ..Default::default()
            },
            ..sample_config()
        });

        let unauthorized = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .header(AUTHORIZATION, "Bearer secret-token")
                .body(empty_body())
                .unwrap(),
            state,
        )
        .await;
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_endpoint_can_be_disabled() {
        let state = state_with_config(Config {
            debug: crate::config::DebugConfig {
                expose_metrics: false,
                ..Default::default()
            },
            ..sample_config()
        });

        let response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .body(empty_body())
                .unwrap(),
            state,
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
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
                balancing: BalancingStrategy::RoundRobin,
                retry_on_statuses: vec![],
                passive_failure_statuses: vec![],
                health_check_endpoint: None,
                connect_timeout_ms: None,
                read_timeout_ms: None,
                client_body_timeout_ms: None,
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
                balancing: BalancingStrategy::RoundRobin,
                retry_on_statuses: vec![],
                passive_failure_statuses: vec![],
                health_check_endpoint: None,
                connect_timeout_ms: None,
                read_timeout_ms: None,
                client_body_timeout_ms: None,
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
    async fn retries_safe_request_on_retryable_status() {
        let first = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(Full::new(Bytes::from_static(b"retry me")))
                .unwrap()
        })
        .await;
        let second = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from_static(b"healthy-backend")))
                .unwrap()
        })
        .await;

        let state = state_with_config(Config {
            routes: vec![RouteConfig {
                path_prefix: "/api".to_string(),
                backends: vec![first, second],
                balancing: BalancingStrategy::RoundRobin,
                retry_on_statuses: vec![],
                passive_failure_statuses: vec![],
                health_check_endpoint: None,
                connect_timeout_ms: None,
                read_timeout_ms: None,
                client_body_timeout_ms: None,
            }],
            retry: crate::config::RetryConfig {
                max_attempts: 2,
                retry_on_statuses: vec![503],
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
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, Bytes::from_static(b"healthy-backend"));
    }

    #[tokio::test]
    async fn does_not_retry_post_request_even_when_retry_is_enabled() {
        let seen_requests = Arc::new(AtomicUsize::new(0));
        let first = spawn_test_upstream({
            let seen_requests = seen_requests.clone();
            move |_| {
                let seen_requests = seen_requests.clone();
                async move {
                    seen_requests.fetch_add(1, Ordering::Relaxed);
                    Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(Full::new(Bytes::from_static(b"retry me")))
                        .unwrap()
                }
            }
        })
        .await;
        let second = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from_static(b"should-not-run")))
                .unwrap()
        })
        .await;

        let state = state_with_config(Config {
            routes: vec![RouteConfig {
                path_prefix: "/api".to_string(),
                backends: vec![first, second],
                balancing: BalancingStrategy::RoundRobin,
                retry_on_statuses: vec![],
                passive_failure_statuses: vec![],
                health_check_endpoint: None,
                connect_timeout_ms: None,
                read_timeout_ms: None,
                client_body_timeout_ms: None,
            }],
            retry: crate::config::RetryConfig {
                max_attempts: 2,
                retry_on_statuses: vec![503],
                ..Default::default()
            },
            ..sample_config()
        });

        let response = handle_request(
            Request::builder()
                .method(Method::POST)
                .uri("/api/users")
                .body(Full::new(Bytes::from_static(b"small")))
                .unwrap(),
            state,
        )
        .await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(seen_requests.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn route_specific_retry_statuses_override_global_retry_policy() {
        let first = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Full::new(Bytes::from_static(b"retry me")))
                .unwrap()
        })
        .await;
        let second = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from_static(b"healthy-backend")))
                .unwrap()
        })
        .await;

        let state = state_with_config(Config {
            routes: vec![RouteConfig {
                path_prefix: "/api".to_string(),
                backends: vec![first, second],
                balancing: BalancingStrategy::RoundRobin,
                retry_on_statuses: vec![502],
                passive_failure_statuses: vec![],
                health_check_endpoint: None,
                connect_timeout_ms: None,
                read_timeout_ms: None,
                client_body_timeout_ms: None,
            }],
            retry: crate::config::RetryConfig {
                max_attempts: 2,
                retry_on_statuses: vec![503],
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
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, Bytes::from_static(b"healthy-backend"));
    }

    #[tokio::test]
    async fn route_specific_passive_failure_statuses_override_global_health_policy() {
        let first = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body(Full::new(Bytes::from_static(b"rate-limited")))
                .unwrap()
        })
        .await;
        let second = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from_static(b"healthy-backend")))
                .unwrap()
        })
        .await;

        let state = state_with_config(Config {
            health_check: HealthCheckConfig {
                failure_threshold: 2,
                ..sample_config().health_check
            },
            routes: vec![RouteConfig {
                path_prefix: "/api".to_string(),
                backends: vec![first, second],
                balancing: BalancingStrategy::RoundRobin,
                retry_on_statuses: vec![],
                passive_failure_statuses: vec![429],
                health_check_endpoint: None,
                connect_timeout_ms: None,
                read_timeout_ms: None,
                client_body_timeout_ms: None,
            }],
            ..sample_config()
        });

        let first = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        let second = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        let third = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        let fourth = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state,
        )
        .await;

        assert_eq!(first.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(third.status(), StatusCode::TOO_MANY_REQUESTS);
        let body = fourth.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(b"healthy-backend"));
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
                    balancing: BalancingStrategy::RoundRobin,
                    retry_on_statuses: vec![],
                    passive_failure_statuses: vec![],
                    health_check_endpoint: None,
                    connect_timeout_ms: None,
                    read_timeout_ms: None,
                    client_body_timeout_ms: None,
                },
                RouteConfig {
                    path_prefix: "/static".to_string(),
                    backends: vec!["http://127.0.0.1:4000".to_string()],
                    balancing: BalancingStrategy::RoundRobin,
                    retry_on_statuses: vec![],
                    passive_failure_statuses: vec![],
                    health_check_endpoint: None,
                    connect_timeout_ms: None,
                    read_timeout_ms: None,
                    client_body_timeout_ms: None,
                },
            ],
            health_check: HealthCheckConfig {
                interval_sec: 10,
                endpoint: "/health".to_string(),
                ..Default::default()
            },
            upstream: UpstreamConfig::default(),
            retry: crate::config::RetryConfig::default(),
            debug: crate::config::DebugConfig::default(),
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
