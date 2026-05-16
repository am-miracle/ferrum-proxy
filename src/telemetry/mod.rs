use std::collections::{HashMap, VecDeque};
use std::fmt::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::RouteConfig;

const MAX_TRANSITIONS: usize = 32;

pub struct Telemetry {
    request_count: AtomicU64,
    upstream_latency_count: AtomicU64,
    upstream_latency_total_micros: AtomicU64,
    backend_failures: HashMap<String, AtomicU64>,
    health_transitions_total: AtomicU64,
    response_statuses: Mutex<HashMap<u16, u64>>,
    proxy_errors: Mutex<HashMap<&'static str, u64>>,
    health_transitions: Mutex<VecDeque<HealthTransition>>,
}

#[derive(Clone)]
struct HealthTransition {
    backend: String,
    from: &'static str,
    to: &'static str,
    reason: &'static str,
}

impl Telemetry {
    pub fn new(routes: &[RouteConfig]) -> Self {
        let mut backend_failures = HashMap::new();

        for route in routes {
            for backend in &route.backends {
                backend_failures
                    .entry(backend.clone())
                    .or_insert_with(|| AtomicU64::new(0));
            }
        }

        Self {
            request_count: AtomicU64::new(0),
            upstream_latency_count: AtomicU64::new(0),
            upstream_latency_total_micros: AtomicU64::new(0),
            backend_failures,
            health_transitions_total: AtomicU64::new(0),
            response_statuses: Mutex::new(HashMap::new()),
            proxy_errors: Mutex::new(HashMap::new()),
            health_transitions: Mutex::new(VecDeque::new()),
        }
    }

    pub fn record_request(&self) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_upstream_latency(&self, latency: Duration) {
        self.upstream_latency_count.fetch_add(1, Ordering::Relaxed);
        self.upstream_latency_total_micros
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
    }

    pub fn record_backend_failure(&self, backend: &str) {
        if let Some(counter) = self.backend_failures.get(backend) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_response_status(&self, status: u16) {
        let mut statuses = self
            .response_statuses
            .lock()
            .expect("response status lock poisoned");
        *statuses.entry(status).or_insert(0) += 1;
    }

    pub fn record_proxy_error(&self, kind: &'static str) {
        let mut errors = self.proxy_errors.lock().expect("proxy error lock poisoned");
        *errors.entry(kind).or_insert(0) += 1;
    }

    pub fn record_health_transition(
        &self,
        backend: &str,
        from: &'static str,
        to: &'static str,
        reason: &'static str,
    ) {
        self.health_transitions_total
            .fetch_add(1, Ordering::Relaxed);

        let transition = HealthTransition {
            backend: backend.to_string(),
            from,
            to,
            reason,
        };

        self.log(
            "INFO",
            "health_transition",
            &[
                ("backend", &transition.backend),
                ("from", transition.from),
                ("to", transition.to),
                ("reason", transition.reason),
            ],
        );

        let mut transitions = self
            .health_transitions
            .lock()
            .expect("transition log lock poisoned");
        if transitions.len() >= MAX_TRANSITIONS {
            transitions.pop_front();
        }
        transitions.push_back(transition);
    }

    pub fn log_server_start(&self, bind_addr: &str, route_count: usize) {
        let route_count = route_count.to_string();
        self.log(
            "INFO",
            "server_start",
            &[
                ("bind_addr", bind_addr),
                ("route_count", route_count.as_str()),
            ],
        );
    }

    pub fn log_shutdown_started(&self, timeout: Duration) {
        let timeout_ms = timeout.as_millis().to_string();
        self.log(
            "INFO",
            "shutdown_started",
            &[("drain_timeout_ms", timeout_ms.as_str())],
        );
    }

    pub fn log_shutdown_complete(&self, drained: bool, remaining_connections: usize) {
        let drained = if drained { "true" } else { "false" };
        let remaining = remaining_connections.to_string();
        self.log(
            "INFO",
            "shutdown_complete",
            &[
                ("drained", drained),
                ("remaining_connections", remaining.as_str()),
            ],
        );
    }

    pub fn log_connection_error(&self, remote_addr: &str, err: &str) {
        self.log(
            "ERROR",
            "connection_error",
            &[("remote_addr", remote_addr), ("error", err)],
        );
    }

    pub fn log_background_task_failure(&self, task: &str, err: &str) {
        self.log(
            "ERROR",
            "background_task_failed",
            &[("task", task), ("error", err)],
        );
    }

    pub fn log_startup_warning(&self, route: &str, reason: &str) {
        self.log(
            "WARN",
            "startup_warning",
            &[("route", route), ("reason", reason)],
        );
    }

    pub fn log_retry_attempt(
        &self,
        method: &str,
        path: &str,
        backend: &str,
        attempt: usize,
        reason: &str,
    ) {
        let attempt = attempt.to_string();
        self.log(
            "INFO",
            "retry_attempt",
            &[
                ("method", method),
                ("path", path),
                ("backend", backend),
                ("attempt", attempt.as_str()),
                ("reason", reason),
            ],
        );
    }

    pub fn log_control_signal(&self, signal: &str, action: &str) {
        self.log(
            "INFO",
            "control_signal",
            &[("signal", signal), ("action", action)],
        );
    }

    pub fn log_request_complete(
        &self,
        method: &str,
        path: &str,
        backend: Option<&str>,
        status: u16,
        latency: Duration,
        error_kind: Option<&'static str>,
        request_id: &str,
    ) {
        let status = status.to_string();
        let latency_ms = latency.as_millis().to_string();
        let backend = backend.unwrap_or("-");
        let error_kind = error_kind.unwrap_or("-");

        self.log(
            "INFO",
            "request_complete",
            &[
                ("request_id", request_id),
                ("method", method),
                ("path", path),
                ("backend", backend),
                ("status", status.as_str()),
                ("latency_ms", latency_ms.as_str()),
                ("error_kind", error_kind),
            ],
        );
    }

    pub fn render_prometheus(&self, backend_statuses: &[(String, bool)]) -> String {
        let mut output = String::new();
        let request_count = self.request_count.load(Ordering::Relaxed);
        let latency_count = self.upstream_latency_count.load(Ordering::Relaxed);
        let latency_total = self.upstream_latency_total_micros.load(Ordering::Relaxed);
        let health_transitions_total = self.health_transitions_total.load(Ordering::Relaxed);

        writeln!(
            output,
            "# HELP ferrum_proxy_requests_total Total HTTP requests handled by the proxy."
        )
        .unwrap();
        writeln!(output, "# TYPE ferrum_proxy_requests_total counter").unwrap();
        writeln!(output, "ferrum_proxy_requests_total {request_count}").unwrap();

        writeln!(
            output,
            "# HELP ferrum_proxy_upstream_request_duration_microseconds Total upstream request latency in microseconds."
        )
        .unwrap();
        writeln!(
            output,
            "# TYPE ferrum_proxy_upstream_request_duration_microseconds summary"
        )
        .unwrap();
        writeln!(
            output,
            "ferrum_proxy_upstream_request_duration_microseconds_sum {latency_total}"
        )
        .unwrap();
        writeln!(
            output,
            "ferrum_proxy_upstream_request_duration_microseconds_count {latency_count}"
        )
        .unwrap();

        writeln!(
            output,
            "# HELP ferrum_proxy_health_transitions_total Total health state transitions."
        )
        .unwrap();
        writeln!(
            output,
            "# TYPE ferrum_proxy_health_transitions_total counter"
        )
        .unwrap();
        writeln!(
            output,
            "ferrum_proxy_health_transitions_total {health_transitions_total}"
        )
        .unwrap();

        let mut backend_failures: Vec<_> = self
            .backend_failures
            .iter()
            .map(|(backend, count)| (backend.clone(), count.load(Ordering::Relaxed)))
            .collect();
        backend_failures.sort_by(|left, right| left.0.cmp(&right.0));

        writeln!(
            output,
            "# HELP ferrum_proxy_backend_failures_total Total passive or active backend failures."
        )
        .unwrap();
        writeln!(output, "# TYPE ferrum_proxy_backend_failures_total counter").unwrap();
        for (backend, count) in backend_failures {
            writeln!(
                output,
                "ferrum_proxy_backend_failures_total{{backend=\"{}\"}} {count}",
                escape_label_value(&backend)
            )
            .unwrap();
        }

        writeln!(
            output,
            "# HELP ferrum_proxy_backend_healthy Current backend health state."
        )
        .unwrap();
        writeln!(output, "# TYPE ferrum_proxy_backend_healthy gauge").unwrap();
        for (backend, healthy) in backend_statuses {
            writeln!(
                output,
                "ferrum_proxy_backend_healthy{{backend=\"{}\"}} {}",
                escape_label_value(backend),
                if *healthy { 1 } else { 0 }
            )
            .unwrap();
        }

        let mut statuses: Vec<_> = self
            .response_statuses
            .lock()
            .expect("response status lock poisoned")
            .iter()
            .map(|(status, count)| (*status, *count))
            .collect();
        statuses.sort_by_key(|(status, _)| *status);

        writeln!(
            output,
            "# HELP ferrum_proxy_responses_total Total responses sent by status code."
        )
        .unwrap();
        writeln!(output, "# TYPE ferrum_proxy_responses_total counter").unwrap();
        for (status, count) in statuses {
            writeln!(
                output,
                "ferrum_proxy_responses_total{{status_code=\"{status}\"}} {count}"
            )
            .unwrap();
        }

        let mut errors: Vec<_> = self
            .proxy_errors
            .lock()
            .expect("proxy error lock poisoned")
            .iter()
            .map(|(kind, count)| (*kind, *count))
            .collect();
        errors.sort_by_key(|(kind, _)| *kind);

        writeln!(
            output,
            "# HELP ferrum_proxy_errors_total Total proxy errors by classification."
        )
        .unwrap();
        writeln!(output, "# TYPE ferrum_proxy_errors_total counter").unwrap();
        for (kind, count) in errors {
            writeln!(
                output,
                "ferrum_proxy_errors_total{{kind=\"{}\"}} {count}",
                escape_label_value(kind)
            )
            .unwrap();
        }

        output
    }

    fn log(&self, level: &str, event: &str, fields: &[(&str, &str)]) {
        let ts_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        let mut line = format!("ts={ts_millis} level={level} event={event}");
        for (key, value) in fields {
            let _ = write!(line, " {key}={}", quote(value));
        }
        eprintln!("{line}");
    }
}

fn escape_label_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn quote(value: &str) -> String {
    format!("{value:?}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::config::RouteConfig;

    use super::Telemetry;

    #[test]
    fn renders_prometheus_metrics() {
        let routes = vec![RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec!["http://127.0.0.1:3001".to_string()],
        }];
        let telemetry = Telemetry::new(&routes);

        telemetry.record_request();
        telemetry.record_upstream_latency(Duration::from_millis(10));
        telemetry.record_backend_failure("http://127.0.0.1:3001");
        telemetry.record_response_status(200);
        telemetry.record_proxy_error("request_body_too_large");
        telemetry.record_health_transition(
            "http://127.0.0.1:3001",
            "healthy",
            "unhealthy",
            "passive_failure",
        );

        let report = telemetry.render_prometheus(&[("http://127.0.0.1:3001".to_string(), false)]);
        assert!(report.contains("ferrum_proxy_requests_total 1"));
        assert!(
            report.contains(
                "ferrum_proxy_backend_failures_total{backend=\"http://127.0.0.1:3001\"} 1"
            )
        );
        assert!(
            report.contains("ferrum_proxy_backend_healthy{backend=\"http://127.0.0.1:3001\"} 0")
        );
        assert!(report.contains("ferrum_proxy_responses_total{status_code=\"200\"} 1"));
        assert!(report.contains("ferrum_proxy_errors_total{kind=\"request_body_too_large\"} 1"));
    }
}
