use crate::config::RouteConfig;

pub fn match_route<'a>(path: &str, routes: &'a [RouteConfig]) -> Option<&'a RouteConfig> {
    routes
        .iter()
        .filter(|route| {
            let p = route.path_prefix.as_str();
            path == p || path.starts_with(&format!("{p}/"))
        })
        .max_by_key(|route| route.path_prefix.len()) // longest prefix wins
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
    fn does_not_match_partial_path_segment() {
        let routes = vec![RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec!["http://127.0.0.1:3001".to_string()],
        }];

        assert!(match_route("/api-other/resource", &routes).is_none());
    }

    #[test]
    fn matches_exact_prefix() {
        let routes = vec![RouteConfig {
            path_prefix: "/api".to_string(),
            backends: vec!["http://127.0.0.1:3001".to_string()],
        }];

        assert!(match_route("/api", &routes).is_some());
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
