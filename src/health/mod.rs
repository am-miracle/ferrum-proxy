use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::RouteConfig;

pub struct HealthManager {
    backend_health: HashMap<String, AtomicBool>,
}

impl HealthManager {
    pub fn new(routes: &[RouteConfig]) -> Self {
        let mut backend_health = HashMap::new();

        for route in routes {
            for backend in &route.backends {
                backend_health
                    .entry(backend.clone())
                    .or_insert_with(|| AtomicBool::new(true));
            }
        }

        Self { backend_health }
    }

    pub fn healthy_backends<'a>(&self, route: &'a RouteConfig) -> Vec<&'a str> {
        route
            .backends
            .iter()
            .map(String::as_str)
            .filter(|backend| self.is_backend_healthy(backend))
            .collect()
    }

    #[allow(dead_code)]
    pub fn mark_backend_healthy(&self, backend: &str) {
        if let Some(state) = self.backend_health.get(backend) {
            state.store(true, Ordering::Relaxed);
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn mark_backend_unhealthy(&self, backend: &str) {
        if let Some(state) = self.backend_health.get(backend) {
            state.store(false, Ordering::Relaxed);
        }
    }

    pub fn is_backend_healthy(&self, backend: &str) -> bool {
        self.backend_health
            .get(backend)
            .map(|state| state.load(Ordering::Relaxed))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::RouteConfig;

    use super::HealthManager;

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

        manager.mark_backend_unhealthy("http://127.0.0.1:3001");

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
}
