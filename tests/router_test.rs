use ferrum_proxy::config::{
    BalancingStrategy, Config, HealthCheckConfig, RouteConfig, ServerConfig, UpstreamConfig,
};
use ferrum_proxy::http::{AppState, handle_request};
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;

fn config(routes: Vec<RouteConfig>) -> Config {
    Config {
        server: ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 8080,
            ..Default::default()
        },
        routes,
        health_check: HealthCheckConfig {
            interval_sec: 10,
            endpoint: "/health".to_string(),
            ..Default::default()
        },
        upstream: UpstreamConfig::default(),
        retry: ferrum_proxy::config::RetryConfig::default(),
        debug: ferrum_proxy::config::DebugConfig::default(),
    }
}

fn route(path_prefix: &str, backends: &[String]) -> RouteConfig {
    RouteConfig {
        path_prefix: path_prefix.to_string(),
        backends: backends.to_vec(),
        balancing: BalancingStrategy::RoundRobin,
        retry_on_statuses: vec![],
        passive_failure_statuses: vec![],
        health_check_endpoint: None,
        connect_timeout_ms: None,
        read_timeout_ms: None,
        client_body_timeout_ms: None,
    }
}

#[tokio::test]
async fn routes_request_to_matching_prefix_backend() {
    let api_backend = spawn_upstream("api-service").await;
    let static_backend = spawn_upstream("static-service").await;
    let state = AppState::new(config(vec![
        route("/api", std::slice::from_ref(&api_backend)),
        route("/static", std::slice::from_ref(&static_backend)),
    ]));

    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/users")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = handle_request(request, state).await;
    let body = response.into_body().collect().await.unwrap().to_bytes();

    assert_eq!(body, Bytes::from_static(b"api-service"));
}

#[tokio::test]
async fn prefers_more_specific_route_when_multiple_prefixes_match() {
    let api_backend = spawn_upstream("generic-api").await;
    let admin_backend = spawn_upstream("admin-api").await;
    let reports_backend = spawn_upstream("reports-api").await;
    let state = AppState::new(config(vec![
        route("/api", std::slice::from_ref(&api_backend)),
        route("/api/admin", std::slice::from_ref(&admin_backend)),
        route("/api/admin/reports", std::slice::from_ref(&reports_backend)),
    ]));

    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/admin/reports/daily")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = handle_request(request, state).await;
    let body = response.into_body().collect().await.unwrap().to_bytes();

    assert_eq!(body, Bytes::from_static(b"reports-api"));
}

#[tokio::test]
async fn returns_not_found_when_no_route_matches() {
    let api_backend = spawn_upstream("api-service").await;
    let state = AppState::new(config(vec![route(
        "/api",
        std::slice::from_ref(&api_backend),
    )]));

    let request = Request::builder()
        .method(Method::GET)
        .uri("/unknown")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = handle_request(request, state).await;
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, Bytes::from_static(b"no route matched request path\n"));
}

async fn spawn_upstream(payload: &'static str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);

            tokio::spawn(async move {
                let service = service_fn(move |_request: Request<Incoming>| async move {
                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from_static(payload.as_bytes())))
                            .unwrap(),
                    )
                });

                http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                    .unwrap();
            });
        }
    });

    format!("http://{addr}")
}
