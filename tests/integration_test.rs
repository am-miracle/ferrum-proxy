use std::convert::Infallible;
use std::net::TcpListener;
use std::time::Duration;

use ferrum_proxy::config::{Config, HealthCheckConfig, RouteConfig, ServerConfig, UpstreamConfig};
use ferrum_proxy::server;
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn serves_internal_health_endpoint_over_real_http() {
    let backend = spawn_upstream("api-backend", StatusCode::OK).await;
    let server = spawn_proxy_server(config(
        pick_unused_port(),
        vec![route("/api", std::slice::from_ref(&backend))],
    ))
    .await;

    let response = get(&server.base_url, "/health").await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body, "ok\n");

    server.shutdown();
}

#[tokio::test]
async fn proxies_requests_and_rotates_backends_over_real_http() {
    let first = spawn_upstream("backend-1", StatusCode::OK).await;
    let second = spawn_upstream("backend-2", StatusCode::OK).await;
    let server = spawn_proxy_server(config(
        pick_unused_port(),
        vec![route("/api", &[first, second])],
    ))
    .await;

    let first_response = get(&server.base_url, "/api/users").await;
    let second_response = get(&server.base_url, "/api/users").await;
    let third_response = get(&server.base_url, "/api/users").await;

    assert_eq!(first_response.status, StatusCode::OK);
    assert_eq!(second_response.status, StatusCode::OK);
    assert_eq!(third_response.status, StatusCode::OK);
    assert_eq!(first_response.body, "backend-1");
    assert_eq!(second_response.body, "backend-2");
    assert_eq!(third_response.body, "backend-1");

    server.shutdown();
}

#[tokio::test]
async fn reports_backend_health_and_metrics_after_passive_failures() {
    let failing = "http://127.0.0.1:9".to_string();
    let healthy = spawn_upstream("healthy-backend", StatusCode::OK).await;
    let server = spawn_proxy_server(config(
        pick_unused_port(),
        vec![route("/api", &[failing.clone(), healthy.clone()])],
    ))
    .await;

    let first = get(&server.base_url, "/api/users").await;
    let second = get(&server.base_url, "/api/users").await;
    let third = get(&server.base_url, "/api/users").await;
    let fourth = get(&server.base_url, "/api/users").await;
    let fifth = get(&server.base_url, "/api/users").await;
    let sixth = get(&server.base_url, "/api/users").await;

    let statuses = [
        first.status,
        second.status,
        third.status,
        fourth.status,
        fifth.status,
    ];
    assert!(statuses.contains(&StatusCode::BAD_GATEWAY));
    assert_eq!(sixth.status, StatusCode::OK);
    assert_eq!(sixth.body, "healthy-backend");

    let backend_health = get(&server.base_url, "/health/backends").await;
    let metrics = get(&server.base_url, "/metrics").await;

    assert_eq!(backend_health.status, StatusCode::OK);
    assert!(
        backend_health
            .body
            .contains(&format!("{failing} unhealthy"))
    );
    assert!(backend_health.body.contains(&format!("{healthy} healthy")));

    assert_eq!(metrics.status, StatusCode::OK);
    assert!(metric_value(&metrics.body, "ferrum_proxy_requests_total") >= 8);
    assert_eq!(
        metric_value(
            &metrics.body,
            "ferrum_proxy_upstream_request_duration_microseconds_count"
        ),
        6
    );
    assert!(metrics.body.contains(&format!(
        "ferrum_proxy_backend_failures_total{{backend=\"{failing}\"}}"
    )));
    assert!(
        metrics
            .body
            .contains("ferrum_proxy_health_transitions_total")
    );

    server.shutdown();
}

#[tokio::test]
async fn drains_in_flight_request_during_graceful_shutdown() {
    let backend = spawn_slow_upstream(
        "drained-backend",
        StatusCode::OK,
        Duration::from_millis(100),
    )
    .await;
    let config = config(
        pick_unused_port(),
        vec![route("/api", std::slice::from_ref(&backend))],
    );
    let base_url = format!("http://{}:{}", config.server.host, config.server.port);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let task = tokio::spawn(async move {
        let _ = server::run_with_shutdown(config, async move {
            let _ = shutdown_rx.await;
        })
        .await;
    });

    wait_until_ready(&base_url).await;

    let request_task = tokio::spawn({
        let base_url = base_url.clone();
        async move { get(&base_url, "/api/users").await }
    });

    sleep(Duration::from_millis(20)).await;
    let _ = shutdown_tx.send(());

    let response = timeout(Duration::from_secs(1), request_task)
        .await
        .expect("request should finish before drain timeout")
        .expect("request task should not panic");
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body, "drained-backend");

    timeout(Duration::from_secs(1), task)
        .await
        .expect("server should shut down after draining")
        .expect("server task should not panic");
}

struct TestServer {
    base_url: String,
    task: JoinHandle<()>,
}

impl TestServer {
    fn shutdown(self) {
        self.task.abort();
    }
}

struct TestResponse {
    status: StatusCode,
    body: String,
}

async fn spawn_proxy_server(config: Config) -> TestServer {
    let base_url = format!("http://{}:{}", config.server.host, config.server.port);
    let task = tokio::spawn(async move {
        let _ = server::run(config).await;
    });

    wait_until_ready(&base_url).await;

    TestServer { base_url, task }
}

async fn wait_until_ready(base_url: &str) {
    for _ in 0..50 {
        if try_get(base_url, "/health").await.is_some() {
            return;
        }

        sleep(Duration::from_millis(20)).await;
    }

    panic!("proxy server did not become ready at {base_url}");
}

async fn get(base_url: &str, path: &str) -> TestResponse {
    try_get(base_url, path)
        .await
        .unwrap_or_else(|| panic!("request to {base_url}{path} failed"))
}

async fn try_get(base_url: &str, path: &str) -> Option<TestResponse> {
    let client = test_client();
    let uri: Uri = format!("{base_url}{path}").parse().unwrap();
    let request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Empty::<Bytes>::new())
        .unwrap();

    let response = timeout(Duration::from_secs(1), client.request(request))
        .await
        .ok()?
        .ok()?;
    let status = response.status();
    let body = response.into_body().collect().await.ok()?.to_bytes();

    Some(TestResponse {
        status,
        body: String::from_utf8(body.to_vec()).unwrap(),
    })
}

fn test_client() -> Client<HttpConnector, Empty<Bytes>> {
    let connector = HttpConnector::new();
    Client::builder(TokioExecutor::new()).build(connector)
}

fn config(port: u16, routes: Vec<RouteConfig>) -> Config {
    Config {
        server: ServerConfig {
            host: "127.0.0.1".to_string(),
            port,
            ..Default::default()
        },
        routes,
        health_check: HealthCheckConfig {
            interval_sec: 60,
            endpoint: "/health".to_string(),
            ..Default::default()
        },
        upstream: UpstreamConfig::default(),
    }
}

fn route(path_prefix: &str, backends: &[String]) -> RouteConfig {
    RouteConfig {
        path_prefix: path_prefix.to_string(),
        backends: backends.to_vec(),
    }
}

fn pick_unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

async fn spawn_upstream(payload: &'static str, status: StatusCode) -> String {
    spawn_slow_upstream(payload, status, Duration::from_millis(0)).await
}

async fn spawn_slow_upstream(
    payload: &'static str,
    status: StatusCode,
    response_delay: Duration,
) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);

            tokio::spawn(async move {
                let service = service_fn(move |_request: Request<Incoming>| async move {
                    if !response_delay.is_zero() {
                        sleep(response_delay).await;
                    }
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
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

fn metric_value(report: &str, key: &str) -> u64 {
    report
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(' ')?;
            (name == key).then(|| value.parse().ok()).flatten()
        })
        .unwrap_or_else(|| panic!("missing metric '{key}' in report:\n{report}"))
}
