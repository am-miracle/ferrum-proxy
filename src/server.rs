use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;

use crate::config::Config;

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
}

pub async fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let addr = config.server.socket_addr()?;
    let route_count = config.routes.len();
    let state = AppState {
        config: Arc::new(config),
    };
    let listener = tokio::net::TcpListener::bind(addr).await?;

    println!("Listening on http://{addr} with {route_count} configured route(s)");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let state = state.clone();
                async move { Ok::<_, std::convert::Infallible>(handle_request(request, state).await) }
            });

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("connection error: {err}");
            }
        });
    }
}

async fn handle_request<B>(request: Request<B>, state: AppState) -> Response<Full<Bytes>> {
    match (request.method(), request.uri().path()) {
        (&Method::GET, "/") => root_response(&state),
        (&Method::GET, "/health") => text_response(StatusCode::OK, "ok\n"),
        _ => fallback_response(&state),
    }
}

fn root_response(state: &AppState) -> Response<Full<Bytes>> {
    let route_count = state.config.routes.len();
    text_response(
        StatusCode::OK,
        format!("ferrum-proxy is running with {route_count} configured route(s)\n"),
    )
}

fn fallback_response(state: &AppState) -> Response<Full<Bytes>> {
    let route_count = state.config.routes.len();
    text_response(
        StatusCode::NOT_IMPLEMENTED,
        format!(
            "proxy request handling is not implemented yet; {route_count} route(s) are loaded\n"
        ),
    )
}

fn text_response(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(body.into()))
        .expect("response should be constructible")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use http_body_util::{BodyExt, Empty};
    use hyper::body::Bytes;
    use hyper::{Method, Request, StatusCode};

    use crate::config::{Config, HealthCheckConfig, RouteConfig, ServerConfig};

    use super::{handle_request, AppState};

    fn sample_state() -> AppState {
        AppState {
            config: Arc::new(Config {
                server: ServerConfig {
                    host: "127.0.0.1".to_string(),
                    port: 8080,
                },
                routes: vec![RouteConfig {
                    path_prefix: "/api".to_string(),
                    backends: vec!["http://127.0.0.1:3001".to_string()],
                }],
                health_check: HealthCheckConfig {
                    interval_sec: 10,
                    endpoint: "/health".to_string(),
                },
            }),
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
    async fn unknown_route_returns_not_implemented() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/users")
            .body(empty_body())
            .unwrap();
        let response = handle_request(request, sample_state()).await;

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
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

        assert_eq!(body, Bytes::from_static(b"ferrum-proxy is running with 1 configured route(s)\n"));
    }

    fn empty_body() -> Empty<Bytes> {
        Empty::new()
    }
}
