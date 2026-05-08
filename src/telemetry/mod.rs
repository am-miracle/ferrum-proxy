use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use crate::config::RouteConfig;

const MAX_TRANSITIONS: usize = 32; // ring buffer cap

pub struct Telemetry {
    request_count: AtomicU64,
    upstream_latency_count: AtomicU64,
    upstream_latency_total_micros: AtomicU64,
    backend_failures: HashMap<String, AtomicU64>,
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
            health_transitions: Mutex::new(VecDeque::new()),
        }
    }

    pub fn record_request(&self) {
        self.request_count.fetch_add(1, Ordering::Relaxed); // Relaxed: counters are best-effort, no ordering needed
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

    pub fn record_health_transition(
        &self,
        backend: &str,
        from: &'static str,
        to: &'static str,
        reason: &'static str,
    ) {
        let transition = HealthTransition {
            backend: backend.to_string(),
            from,
            to,
            reason,
        };

        eprintln!(
            "health transition backend={} from={} to={} reason={}",
            transition.backend, transition.from, transition.to, transition.reason
        );

        let mut transitions = self.health_transitions.lock().expect("transition log lock poisoned");
        if transitions.len() >= MAX_TRANSITIONS {
            transitions.pop_front();
        }
        transitions.push_back(transition);
    }

    pub fn render_report(&self) -> String {
        let request_count = self.request_count.load(Ordering::Relaxed);
        let latency_count = self.upstream_latency_count.load(Ordering::Relaxed);
        let latency_total = self.upstream_latency_total_micros.load(Ordering::Relaxed);
        let latency_avg = if latency_count == 0 {
            0.0
        } else {
            latency_total as f64 / latency_count as f64 / 1000.0 // micros → ms
        };

        let mut lines = vec![
            format!("request_count {request_count}"),
            format!("upstream_latency_count {latency_count}"),
            format!("upstream_latency_avg_ms {latency_avg:.3}"),
        ];

        let mut backend_failures: Vec<_> = self
            .backend_failures
            .iter()
            .map(|(backend, count)| (backend.clone(), count.load(Ordering::Relaxed)))
            .collect();
        backend_failures.sort_by(|left, right| left.0.cmp(&right.0));

        for (backend, count) in backend_failures {
            lines.push(format!("backend_failure backend={backend} count={count}"));
        }

        let transitions = self.health_transitions.lock().expect("transition log lock poisoned");
        for transition in transitions.iter() {
            lines.push(format!(
                "health_transition backend={} from={} to={} reason={}",
                transition.backend, transition.from, transition.to, transition.reason
            ));
        }

        lines.push(String::new());
        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::config::RouteConfig;

    use super::Telemetry;

    #[test]
    fn renders_metrics_and_transition_log() {
        let routes = vec![RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec!["http://127.0.0.1:3001".to_string()],
        }];
        let telemetry = Telemetry::new(&routes);

        telemetry.record_request();
        telemetry.record_upstream_latency(Duration::from_millis(10));
        telemetry.record_backend_failure("http://127.0.0.1:3001");
        telemetry.record_health_transition(
            "http://127.0.0.1:3001",
            "healthy",
            "unhealthy",
            "passive_failure",
        );

        let report = telemetry.render_report();
        assert!(report.contains("request_count 1"));
        assert!(report.contains("upstream_latency_count 1"));
        assert!(report.contains("backend_failure backend=http://127.0.0.1:3001 count=1"));
        assert!(report.contains(
            "health_transition backend=http://127.0.0.1:3001 from=healthy to=unhealthy reason=passive_failure"
        ));
    }
}
