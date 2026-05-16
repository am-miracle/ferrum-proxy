use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::time::sleep;

#[derive(Clone)]
struct BackendConfig {
    name: String,
    port: u16,
    status: StatusCode,
    failure_status: StatusCode,
    fail_every: u64,
    delay_ms: u64,
    response_body_bytes: usize,
    health_status: StatusCode,
}

struct AppState {
    config: BackendConfig,
    request_count: AtomicU64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = parse_args()?;
    let listener = TcpListener::bind(("127.0.0.1", config.port)).await?;
    let state = Arc::new(AppState {
        config: config.clone(),
        request_count: AtomicU64::new(0),
    });

    println!(
        "listening on http://127.0.0.1:{} name={}",
        config.port, config.name
    );

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(handle_request(request, state).await) }
            });

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                let err_text = err.to_string();
                if !is_expected_disconnect(&err_text) {
                    eprintln!("benchmark backend connection error: {err_text}");
                }
            }
        });
    }
}

async fn handle_request(
    request: Request<Incoming>,
    state: Arc<AppState>,
) -> Response<Full<Bytes>> {
    let config = &state.config;

    if request.uri().path() == "/health" {
        return text_response(config.health_status, b"ok\n".to_vec(), Some(&config.name));
    }

    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let received_bytes = read_request_body_size(request).await;
    let request_number = state.request_count.fetch_add(1, Ordering::Relaxed) + 1;

    if config.delay_ms > 0 {
        sleep(Duration::from_millis(config.delay_ms)).await;
    }

    let status = if config.fail_every > 0 && request_number % config.fail_every == 0 {
        config.failure_status
    } else {
        config.status
    };

    let body = build_response_body(
        &config.name,
        &method,
        &path,
        request_number,
        received_bytes,
        config.response_body_bytes,
    );

    text_response(status, body, Some(&config.name))
}

async fn read_request_body_size(request: Request<Incoming>) -> usize {
    request
        .into_body()
        .collect()
        .await
        .map(|collected| collected.to_bytes().len())
        .unwrap_or(0)
}

fn build_response_body(
    name: &str,
    method: &Method,
    path: &str,
    request_number: u64,
    received_bytes: usize,
    response_size: usize,
) -> Vec<u8> {
    let prefix = format!(
        "name={name} method={} path={path} request={request_number} received_bytes={received_bytes}\n",
        method.as_str()
    );
    let mut body = prefix.into_bytes();
    if body.len() >= response_size {
        body.truncate(response_size);
        return body;
    }

    body.resize(response_size, b'x');
    body
}

fn text_response(
    status: StatusCode,
    body: Vec<u8>,
    backend_name: Option<&str>,
) -> Response<Full<Bytes>> {
    let mut response = Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("content-length", body.len().to_string());

    if let Some(name) = backend_name {
        response = response.header("x-benchmark-backend", name);
    }

    response
        .body(Full::new(Bytes::from(body)))
        .expect("invalid benchmark backend response")
}

fn is_expected_disconnect(err_text: &str) -> bool {
    err_text.contains("broken pipe")
        || err_text.contains("connection reset")
        || err_text.contains("connection closed")
}

fn parse_args() -> Result<BackendConfig, String> {
    let mut port = None;
    let mut name = "benchmark-backend".to_string();
    let mut status = StatusCode::OK;
    let mut failure_status = StatusCode::SERVICE_UNAVAILABLE;
    let mut fail_every = 0u64;
    let mut delay_ms = 0u64;
    let mut response_body_bytes = 128usize;
    let mut health_status = StatusCode::OK;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => port = Some(parse_value(args.next(), "--port")?),
            "--name" => name = parse_value(args.next(), "--name")?,
            "--status" => status = parse_status(args.next(), "--status")?,
            "--failure-status" => {
                failure_status = parse_status(args.next(), "--failure-status")?
            }
            "--fail-every" => fail_every = parse_value(args.next(), "--fail-every")?,
            "--delay-ms" => delay_ms = parse_value(args.next(), "--delay-ms")?,
            "--response-body-bytes" => {
                response_body_bytes = parse_value(args.next(), "--response-body-bytes")?
            }
            "--health-status" => health_status = parse_status(args.next(), "--health-status")?,
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument: {other}\n\n{}", usage())),
        }
    }

    Ok(BackendConfig {
        name,
        port: port.ok_or_else(|| format!("missing required --port\n\n{}", usage()))?,
        status,
        failure_status,
        fail_every,
        delay_ms,
        response_body_bytes,
        health_status,
    })
}

fn parse_status(value: Option<String>, flag: &str) -> Result<StatusCode, String> {
    let value: u16 = parse_value(value, flag)?;
    StatusCode::from_u16(value).map_err(|_| format!("invalid status code for {flag}: {value}"))
}

fn parse_value<T>(value: Option<String>, flag: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let raw = value.ok_or_else(|| format!("missing value for {flag}"))?;
    raw.parse::<T>()
        .map_err(|err| format!("invalid value for {flag}: {err}"))
}

fn usage() -> String {
    "usage: benchmark_backend --port <port> [--name <name>] [--status <code>] [--failure-status <code>] [--fail-every <n>] [--delay-ms <ms>] [--response-body-bytes <n>] [--health-status <code>]".to_string()
}
