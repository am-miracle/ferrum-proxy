use std::convert::Infallible;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ferrum_proxy::config::{
    BalancingStrategy, Config, HealthCheckConfig, RouteConfig, ServerConfig, UpstreamConfig,
};
use ferrum_proxy::server;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::ReceiverStream;

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

#[tokio::test]
async fn route_specific_client_body_timeout_rejects_slow_upload() {
    let backend = spawn_upstream("upload-backend", StatusCode::OK).await;
    let mut api_route = route("/api", &[backend]);
    api_route.client_body_timeout_ms = Some(30);

    let server = spawn_proxy_server(config(pick_unused_port(), vec![api_route])).await;
    let response = raw_http_exchange(
        &server.base_url,
        "POST /api/upload HTTP/1.1\r\nHost: proxy.local\r\nContent-Length: 5\r\n\r\n",
        Some(Duration::from_millis(80)),
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 408"));
    assert!(response.contains("client request body idle timeout"));

    server.shutdown();
}

#[tokio::test]
async fn client_disconnect_during_buffering_does_not_forward_partial_upload() {
    let forwarded = Arc::new(AtomicUsize::new(0));
    let backend = spawn_counting_upstream("upload-backend", StatusCode::OK, forwarded.clone()).await;
    let api_route = route("/api", std::slice::from_ref(&backend));
    let server = spawn_proxy_server(config(pick_unused_port(), vec![api_route])).await;
    let baseline = forwarded.load(Ordering::Relaxed);

    let mut stream = connect_raw(&server.base_url);
    stream.write_all(
        b"POST /api/upload HTTP/1.1\r\nHost: proxy.local\r\nContent-Length: 5\r\n\r\n12",
    ).unwrap();
    sleep(Duration::from_millis(50)).await;
    drop(stream);

    sleep(Duration::from_millis(100)).await;
    assert_eq!(forwarded.load(Ordering::Relaxed), baseline);

    let health = get(&server.base_url, "/health").await;
    assert_eq!(health.status, StatusCode::OK);

    server.shutdown();
}

#[tokio::test]
async fn client_disconnect_during_streaming_response_keeps_proxy_healthy() {
    let (backend, _cancelled_rx) = spawn_cancellable_streaming_upstream().await;
    let mut api_route = route("/api", std::slice::from_ref(&backend));
    api_route.health_check_endpoint = Some("/ready".to_string());
    let server = spawn_proxy_server(config(pick_unused_port(), vec![api_route])).await;

    let mut stream = connect_raw(&server.base_url);
    stream
        .write_all(b"GET /api/stream HTTP/1.1\r\nHost: proxy.local\r\nConnection: close\r\n\r\n")
        .unwrap();
    std::thread::sleep(Duration::from_millis(120));
    drop(stream);
    sleep(Duration::from_millis(100)).await;

    let health = get(&server.base_url, "/health").await;
    assert_eq!(health.status, StatusCode::OK);

    server.shutdown();
}

#[tokio::test]
async fn route_specific_read_timeout_is_reported_in_metrics() {
    let backend = spawn_stalling_response_upstream().await;
    let mut api_route = route("/api", &[backend]);
    api_route.read_timeout_ms = Some(30);
    api_route.health_check_endpoint = Some("/ready".to_string());

    let server = spawn_proxy_server(config(pick_unused_port(), vec![api_route])).await;
    let response = request(Method::GET, &server.base_url, "/api/stream", Empty::<Bytes>::new()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.into_body().collect().await.is_err());

    let metrics = get(&server.base_url, "/metrics").await;
    assert!(
        metrics
            .body
            .contains("ferrum_proxy_errors_total{kind=\"upstream_response_body_timeout\"} 1")
    );

    server.shutdown();
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
    let response = request(Method::GET, base_url, path, Empty::<Bytes>::new()).await;
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();

    TestResponse {
        status,
        body: String::from_utf8(body.to_vec()).unwrap(),
    }
}

async fn try_get(base_url: &str, path: &str) -> Option<TestResponse> {
    let response = try_request(Method::GET, base_url, path, Empty::<Bytes>::new())
        .await
        .ok()?;
    let status = response.status();
    let body = response.into_body().collect().await.ok()?.to_bytes();

    Some(TestResponse {
        status,
        body: String::from_utf8(body.to_vec()).unwrap(),
    })
}

async fn request<B>(
    method: Method,
    base_url: &str,
    path: &str,
    body: B,
) -> Response<Incoming>
where
    B: hyper::body::Body<Data = Bytes> + Send + Sync + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    try_request(method, base_url, path, body)
        .await
        .unwrap_or_else(|err| panic!("request to {base_url}{path} failed: {err}"))
}

async fn try_request<B>(
    method: Method,
    base_url: &str,
    path: &str,
    body: B,
) -> Result<Response<Incoming>, hyper_util::client::legacy::Error>
where
    B: hyper::body::Body<Data = Bytes> + Send + Sync + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let connector = HttpConnector::new();
    let client = Client::builder(TokioExecutor::new()).build(connector);
    let uri: Uri = format!("{base_url}{path}").parse().unwrap();
    let request = Request::builder().method(method).uri(uri).body(body).unwrap();

    timeout(Duration::from_secs(1), client.request(request))
        .await
        .unwrap()
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

async fn spawn_counting_upstream(
    payload: &'static str,
    status: StatusCode,
    forwarded: Arc<AtomicUsize>,
) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);
            let forwarded = forwarded.clone();

            tokio::spawn(async move {
                let service = service_fn(move |_request: Request<Incoming>| {
                    let forwarded = forwarded.clone();
                    async move {
                        forwarded.fetch_add(1, Ordering::Relaxed);
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(status)
                                .body(Full::new(Bytes::from_static(payload.as_bytes())))
                                .unwrap(),
                        )
                    }
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

async fn spawn_cancellable_streaming_upstream() -> (String, oneshot::Receiver<bool>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (cancelled_tx, cancelled_rx) = oneshot::channel();

    tokio::spawn(async move {
        let cancelled_tx = Arc::new(std::sync::Mutex::new(Some(cancelled_tx)));
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);
            let cancelled_tx = cancelled_tx.clone();

            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let cancelled_tx = cancelled_tx.clone();
                    async move {
                        if request.uri().path() == "/ready" {
                            return Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .body(Full::new(Bytes::from_static(b"ready")).boxed())
                                    .unwrap(),
                            );
                        }

                        let (tx, rx) = mpsc::channel(2);

                        tokio::spawn(async move {
                            let _ = tx
                                .send(Ok::<Frame<Bytes>, Infallible>(Frame::data(Bytes::from_static(
                                    b"chunk-1",
                                ))))
                                .await;
                            sleep(Duration::from_millis(100)).await;
                            let send_result = tx
                                .send(Ok::<Frame<Bytes>, Infallible>(Frame::data(Bytes::from_static(
                                    b"chunk-2",
                                ))))
                                .await;
                            if let Some(cancelled_tx) = cancelled_tx.lock().unwrap().take() {
                                let _ = cancelled_tx.send(send_result.is_err());
                            }
                        });

                        let body = StreamBody::new(ReceiverStream::new(rx));
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(body.boxed())
                                .unwrap(),
                        )
                    }
                });

                let _ = http1::Builder::new().serve_connection(io, service).await;
            });
        }
    });

    (format!("http://{addr}"), cancelled_rx)
}

async fn spawn_stalling_response_upstream() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);

            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| async move {
                    if request.uri().path() == "/ready" {
                        return Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::from_static(b"ready")).boxed())
                                .unwrap(),
                        );
                    }

                    let (tx, rx) = mpsc::channel(2);

                    tokio::spawn(async move {
                        let _ = tx
                            .send(Ok::<Frame<Bytes>, Infallible>(Frame::data(Bytes::from_static(
                                b"chunk-1",
                            ))))
                            .await;
                        sleep(Duration::from_millis(100)).await;
                        let _ = tx
                            .send(Ok::<Frame<Bytes>, Infallible>(Frame::data(Bytes::from_static(
                                b"chunk-2",
                            ))))
                            .await;
                    });

                    let body = StreamBody::new(ReceiverStream::new(rx));
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(body.boxed())
                            .unwrap(),
                    )
                });

                let _ = http1::Builder::new().serve_connection(io, service).await;
            });
        }
    });

    format!("http://{addr}")
}

fn connect_raw(base_url: &str) -> std::net::TcpStream {
    let addr = base_url.trim_start_matches("http://");
    std::net::TcpStream::connect(addr).unwrap()
}

async fn raw_http_exchange(base_url: &str, request: &str, wait_before_read: Option<Duration>) -> String {
    let mut stream = connect_raw(base_url);
    stream.write_all(request.as_bytes()).unwrap();
    if let Some(wait) = wait_before_read {
        sleep(wait).await;
    }
    let mut response = Vec::new();
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    stream.read_to_end(&mut response).unwrap();
    String::from_utf8(response).unwrap()
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
