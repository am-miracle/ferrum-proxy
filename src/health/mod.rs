use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::{Request, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use tokio::task::JoinHandle;
use tokio::time;
use url::Url;

use crate::config::{Config, RouteConfig};
use crate::telemetry::Telemetry;

pub struct HealthManager {
    backend_health: HashMap<String, BackendHealth>,
    telemetry: Option<Arc<Telemetry>>,
    failure_threshold: usize,
    recovery_threshold: usize,
}

struct BackendHealth {
    healthy: AtomicBool,
    consecutive_failures: AtomicUsize,
    consecutive_successes: AtomicUsize,
}

impl HealthManager {
    #[cfg(test)]
    pub fn new(routes: &[RouteConfig]) -> Self {
        Self::with_telemetry(routes, 3, 2, None)
    }

    pub fn with_telemetry(
        routes: &[RouteConfig],
        failure_threshold: usize,
        recovery_threshold: usize,
        telemetry: Option<Arc<Telemetry>>,
    ) -> Self {
        let mut backend_health = HashMap::new();

        for route in routes {
            for backend in &route.backends {
                backend_health
                    .entry(backend.clone())
                    .or_insert_with(BackendHealth::healthy); // shared across routes if the same URL appears in multiple
            }
        }

        Self {
            backend_health,
            telemetry,
            failure_threshold,
            recovery_threshold,
        }
    }

    pub fn healthy_backends<'a>(&self, route: &'a RouteConfig) -> Vec<&'a str> {
        route
            .backends
            .iter()
            .map(String::as_str)
            .filter(|backend| self.is_backend_healthy(backend))
            .collect()
    }

    pub fn is_backend_healthy(&self, backend: &str) -> bool {
        self.backend_health
            .get(backend)
            .map(|state| state.healthy.load(Ordering::Relaxed))
            .unwrap_or(false)
    }

    pub fn backend_statuses(&self) -> Vec<(String, bool)> {
        let mut statuses: Vec<_> = self
            .backend_health
            .iter()
            .map(|(backend, state)| {
                (
                    backend.clone(),
                    state.healthy.load(Ordering::Relaxed),
                )
            })
            .collect();

        statuses.sort_by(|left, right| left.0.cmp(&right.0));
        statuses
    }

    pub fn record_success(&self, backend: &str) {
        if let Some(state) = self.backend_health.get(backend) {
            state.consecutive_failures.store(0, Ordering::Relaxed); // any success resets the failure streak
            let successes = state.consecutive_successes.fetch_add(1, Ordering::Relaxed) + 1;

            if !state.healthy.load(Ordering::Relaxed) && successes >= self.recovery_threshold {
                state.healthy.store(true, Ordering::Relaxed);
                state.consecutive_successes.store(0, Ordering::Relaxed);
                self.record_transition(backend, "unhealthy", "healthy", "success_threshold");
            }
        }
    }

    pub fn record_failure(&self, backend: &str) {
        if let Some(state) = self.backend_health.get(backend) {
            state.consecutive_successes.store(0, Ordering::Relaxed); // any failure resets the success streak
            if let Some(telemetry) = &self.telemetry {
                telemetry.record_backend_failure(backend);
            }
            let failures = state.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

            if state.healthy.load(Ordering::Relaxed) && failures >= self.failure_threshold {
                state.healthy.store(false, Ordering::Relaxed);
                state.consecutive_failures.store(0, Ordering::Relaxed);
                self.record_transition(backend, "healthy", "unhealthy", "failure_threshold");
            }
        }
    }

    fn record_transition(
        &self,
        backend: &str,
        from: &'static str,
        to: &'static str,
        reason: &'static str,
    ) {
        if let Some(telemetry) = &self.telemetry {
            telemetry.record_health_transition(backend, from, to, reason);
        }
    }
}

impl BackendHealth {
    fn healthy() -> Self {
        Self {
            healthy: AtomicBool::new(true),
            consecutive_failures: AtomicUsize::new(0),
            consecutive_successes: AtomicUsize::new(0),
        }
    }
}

pub fn spawn_active_checks(config: Arc<Config>, manager: Arc<HealthManager>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let check_timeout = Duration::from_millis(config.health_check.check_timeout_ms);
        let client = health_client();
        let mut interval = time::interval(Duration::from_secs(config.health_check.interval_sec));

        loop {
            interval.tick().await;
            run_active_check_pass_with_client(&client, &config, &manager, check_timeout).await;
        }
    })
}

#[cfg(test)]
pub async fn run_active_check_pass(config: &Config, manager: &HealthManager) {
    let check_timeout = Duration::from_millis(config.health_check.check_timeout_ms);
    let client = health_client();
    run_active_check_pass_with_client(&client, config, manager, check_timeout).await;
}

async fn run_active_check_pass_with_client(
    client: &Client<HttpConnector, Empty<Bytes>>,
    config: &Config,
    manager: &HealthManager,
    check_timeout: Duration,
) {
    for backend in unique_backends(&config.routes) {
        if check_backend(client, backend.as_str(), &config.health_check.endpoint, check_timeout)
            .await
            .is_success()
        {
            manager.record_success(backend.as_str());
        } else {
            manager.record_failure(backend.as_str());
        }
    }
}

async fn check_backend(
    client: &Client<HttpConnector, Empty<Bytes>>,
    backend: &str,
    endpoint: &str,
    check_timeout: Duration,
) -> StatusCode {
    let uri = match build_healthcheck_uri(backend, endpoint) {
        Ok(uri) => uri,
        Err(_) => return StatusCode::BAD_GATEWAY,
    };

    let request = match Request::builder().uri(uri).body(Empty::new()) {
        Ok(request) => request,
        Err(_) => return StatusCode::BAD_GATEWAY,
    };

    match time::timeout(check_timeout, client.request(request)).await {
        Ok(Ok(response)) => response.status(),
        _ => StatusCode::BAD_GATEWAY, // timeout or connection error = unhealthy
    }
}

fn health_client() -> Client<HttpConnector, Empty<Bytes>> {
    let connector = HttpConnector::new();
    Client::builder(TokioExecutor::new()).build(connector)
}

fn build_healthcheck_uri(backend: &str, endpoint: &str) -> Result<Uri, url::ParseError> {
    let mut url = Url::parse(backend)?;
    url.set_path(endpoint);
    url.set_query(None);
    Ok(url
        .as_str()
        .parse()
        .expect("invalid health check URI"))
}

// a backend may appear in multiple routes; check it once per cycle
fn unique_backends(routes: &[RouteConfig]) -> Vec<String> {
    let mut backends = HashSet::new();

    for route in routes {
        for backend in &route.backends {
            backends.insert(backend.clone());
        }
    }

    backends.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU16, Ordering};

    use crate::config::{Config, HealthCheckConfig, RouteConfig, ServerConfig};

    use super::{HealthManager, build_healthcheck_uri, run_active_check_pass};

    #[test]
    fn returns_only_healthy_backends_for_route() {
        let routes = vec![RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec![
                "http://127.0.0.1:3001".to_string(),
                "http://127.0.0.1:3002".to_string(),
            ],
        }];
        let manager = HealthManager::new(&routes);

        manager.record_failure("http://127.0.0.1:3001");
        manager.record_failure("http://127.0.0.1:3001");
        manager.record_failure("http://127.0.0.1:3001");

        let healthy = manager.healthy_backends(&routes[0]);
        assert_eq!(healthy, vec!["http://127.0.0.1:3002"]);
    }

    #[test]
    fn starts_with_all_backends_healthy() {
        let routes = vec![RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec!["http://127.0.0.1:3001".to_string()],
        }];
        let manager = HealthManager::new(&routes);

        assert!(manager.is_backend_healthy("http://127.0.0.1:3001"));
    }

    #[test]
    fn returns_sorted_backend_statuses() {
        let routes = vec![RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec![
                "http://127.0.0.1:3002".to_string(),
                "http://127.0.0.1:3001".to_string(),
            ],
        }];
        let manager = HealthManager::new(&routes);

        manager.record_failure("http://127.0.0.1:3002");
        manager.record_failure("http://127.0.0.1:3002");
        manager.record_failure("http://127.0.0.1:3002");

        assert_eq!(
            manager.backend_statuses(),
            vec![
                ("http://127.0.0.1:3001".to_string(), true),
                ("http://127.0.0.1:3002".to_string(), false),
            ]
        );
    }

    #[test]
    fn marks_backend_unhealthy_after_failure_threshold() {
        let routes = vec![RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec!["http://127.0.0.1:3001".to_string()],
        }];
        let manager = HealthManager::new(&routes);

        manager.record_failure("http://127.0.0.1:3001");
        assert!(manager.is_backend_healthy("http://127.0.0.1:3001"));

        manager.record_failure("http://127.0.0.1:3001");
        assert!(manager.is_backend_healthy("http://127.0.0.1:3001"));

        manager.record_failure("http://127.0.0.1:3001");
        assert!(!manager.is_backend_healthy("http://127.0.0.1:3001"));
    }

    #[test]
    fn marks_backend_healthy_after_recovery_threshold() {
        let routes = vec![RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec!["http://127.0.0.1:3001".to_string()],
        }];
        let manager = HealthManager::new(&routes);

        manager.record_failure("http://127.0.0.1:3001");
        manager.record_failure("http://127.0.0.1:3001");
        manager.record_failure("http://127.0.0.1:3001");
        manager.record_success("http://127.0.0.1:3001");
        assert!(!manager.is_backend_healthy("http://127.0.0.1:3001"));

        manager.record_success("http://127.0.0.1:3001");
        assert!(manager.is_backend_healthy("http://127.0.0.1:3001"));
    }

    #[tokio::test]
    async fn active_checks_mark_backend_unhealthy_after_failed_checks() {
        let backend = "http://127.0.0.1:9";
        let config = config_with_backends(vec![backend.to_string()]);
        let manager = HealthManager::new(&config.routes);

        run_active_check_pass(&config, &manager).await;
        assert!(manager.is_backend_healthy(backend));

        run_active_check_pass(&config, &manager).await;
        assert!(manager.is_backend_healthy(backend));

        run_active_check_pass(&config, &manager).await;
        assert!(!manager.is_backend_healthy(backend));
    }

    #[tokio::test]
    async fn active_checks_restore_backend_after_recovery() {
        let status = Arc::new(AtomicU16::new(StatusCode::INTERNAL_SERVER_ERROR.as_u16()));
        let backend = spawn_health_server(status.clone()).await;
        let config = config_with_backends(vec![backend.clone()]);
        let manager = HealthManager::new(&config.routes);

        run_active_check_pass(&config, &manager).await;
        run_active_check_pass(&config, &manager).await;
        run_active_check_pass(&config, &manager).await;
        assert!(!manager.is_backend_healthy(&backend));

        status.store(StatusCode::OK.as_u16(), Ordering::Relaxed);

        run_active_check_pass(&config, &manager).await;
        assert!(!manager.is_backend_healthy(&backend));

        run_active_check_pass(&config, &manager).await;
        assert!(manager.is_backend_healthy(&backend));
    }

    #[test]
    fn builds_healthcheck_uri_from_backend_and_endpoint() {
        let uri = build_healthcheck_uri("http://127.0.0.1:3001", "/health").unwrap();
        assert_eq!(uri.to_string(), "http://127.0.0.1:3001/health");
    }

    fn config_with_backends(backends: Vec<String>) -> Config {
        Config {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
            },
            routes: vec![RouteConfig {
                path_prefix: "/api".to_string(),
                backends,
            }],
            health_check: HealthCheckConfig {
                interval_sec: 1,
                endpoint: "/health".to_string(),
                ..Default::default()
            },
            upstream: crate::config::UpstreamConfig::default(),
        }
    }

    async fn spawn_health_server(status: Arc<AtomicU16>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let io = TokioIo::new(stream);
                let status = status.clone();

                tokio::spawn(async move {
                    let service = service_fn(move |_request: Request<hyper::body::Incoming>| {
                        let status = status.clone();
                        async move {
                            let response = Response::builder()
                                .status(
                                    StatusCode::from_u16(status.load(Ordering::Relaxed)).unwrap(),
                                )
                                .body(Full::new(Bytes::new()))
                                .unwrap();

                            Ok::<_, std::convert::Infallible>(response)
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
}
