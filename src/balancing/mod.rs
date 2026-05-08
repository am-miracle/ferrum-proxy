use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::config::RouteConfig;

pub struct RoundRobinBalancer {
    counters: HashMap<String, AtomicUsize>,
}

impl RoundRobinBalancer {
    pub fn new(routes: &[RouteConfig]) -> Self {
        let counters = routes
            .iter()
            .map(|route| (route.path_prefix.clone(), AtomicUsize::new(0)))
            .collect();

        Self { counters }
    }

    pub fn select_backend<'a>(&self, route_key: &str, backends: &'a [&'a str]) -> Option<&'a str> {
        if backends.is_empty() {
            return None;
        }

        let index = self
            .counters
            .get(route_key)
            .map(|counter| counter.fetch_add(1, Ordering::Relaxed) % backends.len())
            .unwrap_or(0); // unknown prefix, fall back to first backend

        backends.get(index).copied()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::RouteConfig;

    use super::RoundRobinBalancer;

    #[test]
    fn cycles_backends_in_round_robin_order() {
        let route = RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec![
                "http://127.0.0.1:3001".to_string(),
                "http://127.0.0.1:3002".to_string(),
                "http://127.0.0.1:3003".to_string(),
            ],
        };
        let backends = ["http://127.0.0.1:3001", "http://127.0.0.1:3002", "http://127.0.0.1:3003"];
        let balancer = RoundRobinBalancer::new(&[route]);

        assert_eq!(balancer.select_backend("/api", &backends), Some("http://127.0.0.1:3001"));
        assert_eq!(balancer.select_backend("/api", &backends), Some("http://127.0.0.1:3002"));
        assert_eq!(balancer.select_backend("/api", &backends), Some("http://127.0.0.1:3003"));
        assert_eq!(balancer.select_backend("/api", &backends), Some("http://127.0.0.1:3001"));
    }

    #[test]
    fn returns_none_when_no_eligible_backends_exist() {
        let route = RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec!["http://127.0.0.1:3001".to_string()],
        };
        let balancer = RoundRobinBalancer::new(&[route]);

        assert_eq!(balancer.select_backend("/api", &[]), None);
    }
}
