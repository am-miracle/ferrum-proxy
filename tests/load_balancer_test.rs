use ferrum_proxy::config::{Config, HealthCheckConfig, RouteConfig, ServerConfig, UpstreamConfig};
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
        },
        routes,
        health_check: HealthCheckConfig {
            interval_sec: 10,
            endpoint: "/health".to_string(),
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

#[tokio::test]
async fn rotates_requests_across_backends_end_to_end() {
    let first = spawn_upstream("backend-1", StatusCode::OK).await;
    let second = spawn_upstream("backend-2", StatusCode::OK).await;
    let state = AppState::new(config(vec![route("/api", &[first, second])]));

    let first_body = request_body("/api/users", state.clone()).await;
    let second_body = request_body("/api/users", state.clone()).await;
    let third_body = request_body("/api/users", state).await;

    assert_eq!(first_body, Bytes::from_static(b"backend-1"));
    assert_eq!(second_body, Bytes::from_static(b"backend-2"));
    assert_eq!(third_body, Bytes::from_static(b"backend-1"));
}

#[tokio::test]
async fn maintains_independent_rotation_per_route() {
    let api_a = spawn_upstream("api-a", StatusCode::OK).await;
    let api_b = spawn_upstream("api-b", StatusCode::OK).await;
    let static_a = spawn_upstream("static-a", StatusCode::OK).await;
    let static_b = spawn_upstream("static-b", StatusCode::OK).await;
    let state = AppState::new(config(vec![
        route("/api", &[api_a, api_b]),
        route("/static", &[static_a, static_b]),
    ]));

    let api_first = request_body("/api/users", state.clone()).await;
    let static_first = request_body("/static/logo.png", state.clone()).await;
    let api_second = request_body("/api/users", state.clone()).await;
    let static_second = request_body("/static/logo.png", state).await;

    assert_eq!(api_first, Bytes::from_static(b"api-a"));
    assert_eq!(api_second, Bytes::from_static(b"api-b"));
    assert_eq!(static_first, Bytes::from_static(b"static-a"));
    assert_eq!(static_second, Bytes::from_static(b"static-b"));
}

#[tokio::test]
async fn skips_backend_after_repeated_passive_failures() {
    let failing = "http://127.0.0.1:9".to_string();
    let healthy = spawn_upstream("healthy-backend", StatusCode::OK).await;
    let state = AppState::new(config(vec![route("/api", &[failing, healthy])]));

    let first = request_response("/api/users", state.clone()).await;
    let second = request_response("/api/users", state.clone()).await;
    let third = request_response("/api/users", state.clone()).await;
    let fourth = request_response("/api/users", state.clone()).await;
    let fifth = request_response("/api/users", state.clone()).await;
    let sixth = request_body("/api/users", state).await;

    assert_eq!(first.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(second.into_body().collect().await.unwrap().to_bytes(), Bytes::from_static(b"healthy-backend"));
    assert_eq!(third.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(fourth.into_body().collect().await.unwrap().to_bytes(), Bytes::from_static(b"healthy-backend"));
    assert_eq!(fifth.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(sixth, Bytes::from_static(b"healthy-backend"));
}

async fn request_body(path: &str, state: AppState) -> Bytes {
    request_response(path, state)
        .await
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
}

async fn request_response(path: &str, state: AppState) -> Response<ferrum_proxy::upstream::ProxyBody> {
    let request = Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Empty::<Bytes>::new())
        .unwrap();
    handle_request(request, state).await
}

async fn spawn_upstream(payload: &'static str, status: StatusCode) -> String {
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
