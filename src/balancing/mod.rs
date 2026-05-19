use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::{BalancingStrategy, RouteConfig};

pub struct LoadBalancer {
    routes: HashMap<String, RouteBalancer>,
}

struct RouteBalancer {
    strategy: BalancingStrategy,
    counter: AtomicU64,
}

impl LoadBalancer {
    pub fn new(routes: &[RouteConfig]) -> Self {
        let routes = routes
            .iter()
            .map(|route| {
                (
                    route.path_prefix.clone(),
                    RouteBalancer {
                        strategy: route.balancing,
                        counter: AtomicU64::new(0),
                    },
                )
            })
            .collect();

        Self { routes }
    }

    pub fn select_backend<'a>(&self, route: &RouteConfig, backends: &'a [&'a str]) -> Option<&'a str> {
        if backends.is_empty() {
            return None;
        }

        match self.routes.get(route.path_prefix.as_str()) {
            Some(route_balancer) => route_balancer.select(backends),
            None => backends.first().copied(),
        }
    }
}

impl RouteBalancer {
    fn select<'a>(&self, backends: &'a [&'a str]) -> Option<&'a str> {
        match self.strategy {
            BalancingStrategy::RoundRobin => {
                let index = self.counter.fetch_add(1, Ordering::Relaxed) as usize % backends.len();
                backends.get(index).copied()
            }
            BalancingStrategy::FirstHealthy => backends.first().copied(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{BalancingStrategy, RouteConfig};

    use super::LoadBalancer;

    #[test]
    fn cycles_backends_in_round_robin_order() {
        let route = RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec![
                "http://127.0.0.1:3001".to_string(),
                "http://127.0.0.1:3002".to_string(),
                "http://127.0.0.1:3003".to_string(),
            ],
            balancing: BalancingStrategy::RoundRobin,
            retry_on_statuses: vec![],
            passive_failure_statuses: vec![],
            health_check_endpoint: None,
            connect_timeout_ms: None,
            read_timeout_ms: None,
            client_body_timeout_ms: None,
        };
        let backends = [
            "http://127.0.0.1:3001",
            "http://127.0.0.1:3002",
            "http://127.0.0.1:3003",
        ];
        let balancer = LoadBalancer::new(&[route.clone()]);

        assert_eq!(
            balancer.select_backend(&route, &backends),
            Some("http://127.0.0.1:3001")
        );
        assert_eq!(
            balancer.select_backend(&route, &backends),
            Some("http://127.0.0.1:3002")
        );
        assert_eq!(
            balancer.select_backend(&route, &backends),
            Some("http://127.0.0.1:3003")
        );
        assert_eq!(
            balancer.select_backend(&route, &backends),
            Some("http://127.0.0.1:3001")
        );
    }

    #[test]
    fn first_healthy_strategy_picks_first_backend_without_rotation() {
        let route = RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec![
                "http://127.0.0.1:3001".to_string(),
                "http://127.0.0.1:3002".to_string(),
            ],
            balancing: BalancingStrategy::FirstHealthy,
            retry_on_statuses: vec![],
            passive_failure_statuses: vec![],
            health_check_endpoint: None,
            connect_timeout_ms: None,
            read_timeout_ms: None,
            client_body_timeout_ms: None,
        };
        let backends = ["http://127.0.0.1:3001", "http://127.0.0.1:3002"];
        let balancer = LoadBalancer::new(&[route.clone()]);

        assert_eq!(
            balancer.select_backend(&route, &backends),
            Some("http://127.0.0.1:3001")
        );
        assert_eq!(
            balancer.select_backend(&route, &backends),
            Some("http://127.0.0.1:3001")
        );
    }

    #[test]
    fn returns_none_when_no_eligible_backends_exist() {
        let route = RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec!["http://127.0.0.1:3001".to_string()],
            balancing: BalancingStrategy::RoundRobin,
            retry_on_statuses: vec![],
            passive_failure_statuses: vec![],
            health_check_endpoint: None,
            connect_timeout_ms: None,
            read_timeout_ms: None,
            client_body_timeout_ms: None,
        };
        let balancer = LoadBalancer::new(&[route.clone()]);

        assert_eq!(balancer.select_backend(&route, &[]), None);
    }
}
