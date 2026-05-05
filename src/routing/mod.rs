use crate::config::RouteConfig;

pub fn match_route<'a>(path: &str, routes: &'a [RouteConfig]) -> Option<&'a RouteConfig> {
    routes
        .iter()
        .filter(|route| path.starts_with(&route.path_prefix))
        .max_by_key(|route| route.path_prefix.len())
}

#[cfg(test)]
mod tests {
    use crate::config::RouteConfig;

    use super::match_route;

    #[test]
    fn prefers_more_specific_prefix() {
        let routes = vec![
            RouteConfig {
                path_prefix: "/api".to_string(),
                backends: vec!["http://127.0.0.1:3001".to_string()],
            },
            RouteConfig {
                path_prefix: "/api/admin".to_string(),
                backends: vec!["http://127.0.0.1:4001".to_string()],
            },
        ];

        let route = match_route("/api/admin/users", &routes).unwrap();
        assert_eq!(route.path_prefix, "/api/admin");
    }

    #[test]
    fn returns_none_when_no_route_matches() {
        let routes = vec![RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec!["http://127.0.0.1:3001".to_string()],
        }];

        assert!(match_route("/static/logo.png", &routes).is_none());
    }
}
