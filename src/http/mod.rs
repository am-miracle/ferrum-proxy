use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Request, Response, StatusCode};

use crate::balancing::RoundRobinBalancer;
use crate::config::Config;
use crate::health::{spawn_active_checks, HealthManager};
use crate::routing::match_route;
use crate::upstream::{bad_gateway_response, UpstreamClient};

#[derive(Clone)]
pub struct AppState {
    config: Arc<Config>,
    balancer: Arc<RoundRobinBalancer>,
    health: Arc<HealthManager>,
    upstream: UpstreamClient,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        let balancer = Arc::new(RoundRobinBalancer::new(&config.routes));
        let health = Arc::new(HealthManager::new(&config.routes));

        Self {
            config: Arc::new(config),
            balancer,
            health,
            upstream: UpstreamClient::new(),
        }
    }

    pub fn spawn_background_tasks(&self) {
        spawn_active_checks(self.config.clone(), self.health.clone());
    }
}

pub async fn handle_request<B>(request: Request<B>, state: AppState) -> Response<Full<Bytes>>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    if let Some(response) = handle_internal_route(request.method(), request.uri().path(), &state) {
        return response;
    }

    let path = request.uri().path();
    let Some(route) = match_route(path, &state.config.routes) else {
        return text_response(StatusCode::NOT_FOUND, "no route matched request path\n");
    };

    let healthy_backends = state.health.healthy_backends(route);
    let backend = match state
        .balancer
        .select_backend(&route.path_prefix, &healthy_backends)
    {
        Some(backend) => backend,
        None => {
            return text_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "no healthy backends available\n",
            );
        }
    };

    match state.upstream.forward(backend, request).await {
        Ok(response) => {
            if response.status().is_server_error() {
                state.health.record_failure(backend);
            } else {
                state.health.record_success(backend);
            }

            response
        }
        Err(err) => {
            state.health.record_failure(backend);
            bad_gateway_response(&err)
        }
    }
}

fn handle_internal_route(
    method: &Method,
    path: &str,
    state: &AppState,
) -> Option<Response<Full<Bytes>>> {
    match (method, path) {
        (&Method::GET, "/") => Some(root_response(state)),
        (&Method::GET, "/health") => Some(text_response(StatusCode::OK, "ok\n")),
        (&Method::GET, "/health/backends") => Some(backend_health_response(state)),
        _ => None,
    }
}

fn root_response(state: &AppState) -> Response<Full<Bytes>> {
    let route_count = state.config.routes.len();
    text_response(
        StatusCode::OK,
        format!("ferrum-proxy is running with {route_count} configured route(s)\n"),
    )
}

fn backend_health_response(state: &AppState) -> Response<Full<Bytes>> {
    let body = state
        .health
        .backend_statuses()
        .into_iter()
        .map(|(backend, healthy)| {
            format!("{backend} {}\n", if healthy { "healthy" } else { "unhealthy" })
        })
        .collect::<String>();

    text_response(StatusCode::OK, body)
}

fn text_response(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(body.into()))
        .expect("response should be constructible")
}

#[cfg(test)]
mod tests {
    use http_body_util::{BodyExt, Empty, Full};
    use hyper::body::Bytes;
    use hyper::header::{HeaderValue, CONTENT_TYPE, HOST};
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;

    use super::{handle_request, AppState};
    use crate::config::{Config, HealthCheckConfig, RouteConfig, ServerConfig};

    fn sample_state() -> AppState {
        AppState::new(sample_config())
    }

    fn sample_config() -> Config {
        Config {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
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
            },
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
    async fn backend_health_endpoint_reports_current_status() {
        let state = state_with_api_backends(&["http://127.0.0.1:3002", "http://127.0.0.1:3001"]);
        state.health.record_failure("http://127.0.0.1:3002");
        state.health.record_failure("http://127.0.0.1:3002");
        state.health.record_failure("http://127.0.0.1:3002");

        let request = Request::builder()
            .method(Method::GET)
            .uri("/health/backends")
            .body(empty_body())
            .unwrap();
        let response = handle_request(request, state).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            Bytes::from_static(
                b"http://127.0.0.1:3001 healthy\nhttp://127.0.0.1:3002 unhealthy\nhttp://127.0.0.1:4000 healthy\n"
            )
        );
    }

    #[tokio::test]
    async fn api_route_forwards_request_to_upstream() {
        let upstream: String = spawn_test_upstream(|request| async move {
            let body = format!(
                "upstream {} {}",
                request.method(),
                request.uri().path_and_query().map(|pq| pq.as_str()).unwrap_or("/")
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, HeaderValue::from_static("text/plain"))
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        })
        .await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/users")
            .body(empty_body())
            .unwrap();
        let response = handle_request(request, state_with_api_backends(&[upstream.as_str()])).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, Bytes::from_static(b"upstream GET /api/users"));
    }

    #[tokio::test]
    async fn proxy_preserves_query_headers_and_body() {
        let upstream: String = spawn_test_upstream(|request| async move {
            let custom_header = request
                .headers()
                .get("x-trace-id")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing")
                .to_string();
            let host_header = request
                .headers()
                .get(HOST)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing")
                .to_string();
            let body = request.into_body().collect().await.unwrap().to_bytes();

            let response_body = format!(
                "path={} query={} x-trace-id={} host={} body={}",
                "/api/upload",
                "source=mobile",
                custom_header,
                host_header,
                std::str::from_utf8(&body).unwrap()
            );

            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, HeaderValue::from_static("text/plain"))
                .body(Full::new(Bytes::from(response_body)))
                .unwrap()
        })
        .await;

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/upload?source=mobile")
            .header("x-trace-id", "trace-123")
            .header(HOST, "proxy.local")
            .body(Full::new(Bytes::from_static(b"hello upstream")))
            .unwrap();
        let response = handle_request(request, state_with_api_backends(&[upstream.as_str()])).await;
        let status = response.status();
        let content_type = response.headers().get(CONTENT_TYPE).cloned();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type, Some(HeaderValue::from_static("text/plain")));
        let body_text = std::str::from_utf8(&body).unwrap();
        assert!(body_text.contains("path=/api/upload"));
        assert!(body_text.contains("query=source=mobile"));
        assert!(body_text.contains("x-trace-id=trace-123"));
        assert!(body_text.contains("body=hello upstream"));
        assert!(body_text.contains("host=127.0.0.1:"));
        assert!(!body_text.contains("host=proxy.local"));
    }

    #[tokio::test]
    async fn root_endpoint_reports_loaded_routes() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/")
            .body(empty_body())
            .unwrap();
        let response = handle_request(request, sample_state()).await;
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(
            body,
            Bytes::from_static(b"ferrum-proxy is running with 2 configured route(s)\n")
        );
    }

    #[tokio::test]
    async fn static_route_matches_single_backend() {
        let upstream: String = spawn_test_upstream(|request| async move {
            let body = format!(
                "asset {}",
                request.uri().path_and_query().map(|pq| pq.as_str()).unwrap_or("/")
            );
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        })
        .await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/static/logo.png")
            .body(empty_body())
            .unwrap();
        let response = handle_request(request, state_with_static_backend(upstream.as_str())).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, Bytes::from_static(b"asset /static/logo.png"));
    }

    #[tokio::test]
    async fn api_route_uses_round_robin_across_backends() {
        let first: String = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from_static(b"backend-1")))
                .unwrap()
        })
        .await;
        let second: String = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from_static(b"backend-2")))
                .unwrap()
        })
        .await;

        let state = state_with_api_backends(&[first.as_str(), second.as_str()]);

        let first_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        let first_body = first_response.into_body().collect().await.unwrap().to_bytes();

        let second_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state,
        )
        .await;
        let second_body = second_response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(first_body, Bytes::from_static(b"backend-1"));
        assert_eq!(second_body, Bytes::from_static(b"backend-2"));
    }

    #[tokio::test]
    async fn passive_failures_mark_backend_unhealthy_and_skip_it() {
        let healthy: String = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from_static(b"healthy-backend")))
                .unwrap()
        })
        .await;

        let failing_backend = "http://127.0.0.1:9";
        let state = state_with_api_backends(&[failing_backend, healthy.as_str()]);

        let first_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        assert_eq!(first_response.status(), StatusCode::BAD_GATEWAY);

        let second_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        let second_body = second_response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(second_body, Bytes::from_static(b"healthy-backend"));

        let third_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        assert_eq!(third_response.status(), StatusCode::BAD_GATEWAY);

        let fourth_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        let fourth_body = fourth_response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(fourth_body, Bytes::from_static(b"healthy-backend"));

        let fifth_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        assert_eq!(fifth_response.status(), StatusCode::BAD_GATEWAY);

        let sixth_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state,
        )
        .await;
        let sixth_body = sixth_response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(sixth_body, Bytes::from_static(b"healthy-backend"));
    }

    #[tokio::test]
    async fn passive_server_errors_mark_backend_unhealthy_and_skip_it() {
        let failing: String = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::from_static(b"boom")))
                .unwrap()
        })
        .await;
        let healthy: String = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from_static(b"healthy-backend")))
                .unwrap()
        })
        .await;

        let state = state_with_api_backends(&[failing.as_str(), healthy.as_str()]);

        let first_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        assert_eq!(first_response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let second_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        let second_body = second_response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(second_body, Bytes::from_static(b"healthy-backend"));

        let third_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        assert_eq!(third_response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let fourth_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        let fourth_body = fourth_response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(fourth_body, Bytes::from_static(b"healthy-backend"));

        let fifth_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state.clone(),
        )
        .await;
        assert_eq!(fifth_response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let sixth_response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state,
        )
        .await;
        let sixth_body = sixth_response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(sixth_body, Bytes::from_static(b"healthy-backend"));
    }

    #[tokio::test]
    async fn skips_unhealthy_backends_during_round_robin() {
        let first: String = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from_static(b"backend-1")))
                .unwrap()
        })
        .await;
        let second: String = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from_static(b"backend-2")))
                .unwrap()
        })
        .await;

        let state = state_with_api_backends(&[first.as_str(), second.as_str()]);
        state.health.record_failure(first.as_str());
        state.health.record_failure(first.as_str());
        state.health.record_failure(first.as_str());

        let response = handle_request(
            Request::builder()
                .method(Method::GET)
                .uri("/api/users")
                .body(empty_body())
                .unwrap(),
            state,
        )
        .await;
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(body, Bytes::from_static(b"backend-2"));
    }

    #[tokio::test]
    async fn returns_service_unavailable_when_no_healthy_backends_exist() {
        let first: String = spawn_test_upstream(|_| async move {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from_static(b"backend-1")))
                .unwrap()
        })
        .await;

        let state = state_with_api_backends(&[first.as_str()]);
        state.health.record_failure(first.as_str());
        state.health.record_failure(first.as_str());
        state.health.record_failure(first.as_str());

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

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, Bytes::from_static(b"no healthy backends available\n"));
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

    #[tokio::test]
    async fn upstream_failures_return_bad_gateway() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/users")
            .body(empty_body())
            .unwrap();
        let response = handle_request(request, state_with_api_backends(&["http://127.0.0.1:9"])).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(std::str::from_utf8(&body)
            .unwrap()
            .starts_with("bad gateway: upstream request failed:"));
    }

    fn empty_body() -> Empty<Bytes> {
        Empty::new()
    }

    fn state_with_api_backends(backends: &[&str]) -> AppState {
        state_with_config(Config {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
            },
            routes: vec![
                RouteConfig {
                    path_prefix: "/api".to_string(),
                    backends: backends.iter().map(|backend| (*backend).to_string()).collect(),
                },
                RouteConfig {
                    path_prefix: "/static".to_string(),
                    backends: vec!["http://127.0.0.1:4000".to_string()],
                },
            ],
            health_check: HealthCheckConfig {
                interval_sec: 10,
                endpoint: "/health".to_string(),
            },
        })
    }

    fn state_with_static_backend(backend: &str) -> AppState {
        state_with_config(Config {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
            },
            routes: vec![
                RouteConfig {
                    path_prefix: "/api".to_string(),
                    backends: vec!["http://127.0.0.1:3001".to_string()],
                },
                RouteConfig {
                    path_prefix: "/static".to_string(),
                    backends: vec![backend.to_string()],
                },
            ],
            health_check: HealthCheckConfig {
                interval_sec: 10,
                endpoint: "/health".to_string(),
            },
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
