use std::collections::HashSet;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub routes: Vec<RouteConfig>,
    pub health_check: HealthCheckConfig,
    #[serde(default)]
    pub upstream: UpstreamConfig,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub debug: DebugConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
    pub host: String,
    #[serde(default = "default_graceful_shutdown_timeout_ms")]
    pub graceful_shutdown_timeout_ms: u64,
    #[serde(default = "default_client_header_timeout_ms")]
    pub client_header_timeout_ms: u64,
    #[serde(default = "default_client_body_timeout_ms")]
    pub client_body_timeout_ms: u64,
    /// abort startup if any route has no reachable backends after the initial health check pass.
    #[serde(default)]
    pub fail_on_startup_dead_pool: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RouteConfig {
    pub path_prefix: String,
    pub backends: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct HealthCheckConfig {
    pub interval_sec: u64,
    pub endpoint: String,
    #[serde(default = "default_check_timeout_ms")]
    pub check_timeout_ms: u64,
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: usize,
    #[serde(default = "default_recovery_threshold")]
    pub recovery_threshold: usize,
    #[serde(default = "default_ejection_duration_ms")]
    pub ejection_duration_ms: u64,
    #[serde(default = "default_active_success_status_min")]
    pub active_success_status_min: u16,
    #[serde(default = "default_active_success_status_max")]
    pub active_success_status_max: u16,
    #[serde(default = "default_passive_failure_status_min")]
    pub passive_failure_status_min: u16,
    #[serde(default = "default_passive_failure_status_max")]
    pub passive_failure_status_max: u16,
}

#[derive(Clone, Debug, Deserialize)]
pub struct UpstreamConfig {
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_read_timeout_ms")]
    pub read_timeout_ms: u64,
    #[serde(default = "default_max_request_body_bytes")]
    pub max_request_body_bytes: u64,
    #[serde(default = "default_max_response_body_bytes")]
    pub max_response_body_bytes: u64,
    /// maximum number of request bodies that may be buffered in memory simultaneously.
    /// requests that arrive when this limit is reached receive 503 immediately.
    #[serde(default = "default_max_buffered_bodies")]
    pub max_buffered_bodies: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_retry_max_attempts")]
    pub max_attempts: usize,
    #[serde(default = "default_retry_total_timeout_ms")]
    pub total_timeout_ms: u64,
    /// base backoff between retry attempts. actual delay doubles each attempt (exponential backoff).
    #[serde(default = "default_retry_backoff_ms")]
    pub backoff_ms: u64,
    #[serde(default = "default_retry_on_statuses")]
    pub retry_on_statuses: Vec<u16>,
    /// also retry PUT and DELETE. These are idempotent by spec but not always safe to replay
    /// in practice (partial state changes, side effects). Disabled by default.
    #[serde(default)]
    pub retry_idempotent_methods: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DebugConfig {
    #[serde(default = "default_expose_backend_health")]
    pub expose_backend_health: bool,
    #[serde(default = "default_expose_metrics")]
    pub expose_metrics: bool,
    #[serde(default)]
    pub auth_token: Option<String>,
}

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(serde_yaml::Error),
    Validation(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "failed to read config file: {err}"),
            Self::Parse(err) => write!(f, "failed to parse config file: {err}"),
            Self::Validation(err) => write!(f, "invalid config: {err}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<serde_yaml::Error> for ConfigError {
    fn from(err: serde_yaml::Error) -> Self {
        Self::Parse(err)
    }
}

impl Config {
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path)?;
        let config: Self = serde_yaml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        self.server.validate()?;

        if self.routes.is_empty() {
            return Err(ConfigError::Validation(
                "at least one route must be configured".to_string(),
            ));
        }

        let mut seen_prefixes = HashSet::new();
        for route in &self.routes {
            route.validate()?;
            if !seen_prefixes.insert(route.path_prefix.as_str()) {
                return Err(ConfigError::Validation(format!(
                    "duplicate route prefix '{}'",
                    route.path_prefix
                )));
            }
        }

        self.health_check.validate()?;
        self.upstream.validate()?;
        self.retry.validate()?;
        self.debug.validate()?;

        Ok(())
    }
}

impl ServerConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.port == 0 {
            return Err(ConfigError::Validation(
                "server.port must be greater than 0".to_string(),
            ));
        }

        if self.host.trim().is_empty() {
            return Err(ConfigError::Validation(
                "server.host must not be empty".to_string(),
            ));
        }

        if self.graceful_shutdown_timeout_ms == 0 {
            return Err(ConfigError::Validation(
                "server.graceful_shutdown_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.client_header_timeout_ms == 0 {
            return Err(ConfigError::Validation(
                "server.client_header_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.client_body_timeout_ms == 0 {
            return Err(ConfigError::Validation(
                "server.client_body_timeout_ms must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, ConfigError> {
        let addr = format!("{}:{}", self.host, self.port);
        addr.parse().map_err(|err| {
            ConfigError::Validation(format!(
                "server host/port must form a valid socket address: {err}"
            ))
        })
    }
}

impl RouteConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if !self.path_prefix.starts_with('/') {
            return Err(ConfigError::Validation(format!(
                "route prefix '{}' must start with '/'",
                self.path_prefix
            )));
        }

        if self.backends.is_empty() {
            return Err(ConfigError::Validation(format!(
                "route '{}' must define at least one backend",
                self.path_prefix
            )));
        }

        for backend in &self.backends {
            let parsed = Url::parse(backend).map_err(|err| {
                ConfigError::Validation(format!(
                    "route '{}' has invalid backend URL '{}': {err}",
                    self.path_prefix, backend
                ))
            })?;

            match parsed.scheme() {
                "http" | "https" => {}
                scheme => {
                    return Err(ConfigError::Validation(format!(
                        "route '{}' uses unsupported backend scheme '{}' in '{}'",
                        self.path_prefix, scheme, backend
                    )));
                }
            }

            if parsed.host_str().is_none() {
                return Err(ConfigError::Validation(format!(
                    "route '{}' backend '{}' must include a host",
                    self.path_prefix, backend
                )));
            }
        }

        Ok(())
    }
}

impl HealthCheckConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.interval_sec == 0 {
            return Err(ConfigError::Validation(
                "health_check.interval_sec must be greater than 0".to_string(),
            ));
        }

        if !self.endpoint.starts_with('/') {
            return Err(ConfigError::Validation(
                "health_check.endpoint must start with '/'".to_string(),
            ));
        }

        if self.check_timeout_ms == 0 {
            return Err(ConfigError::Validation(
                "health_check.check_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.failure_threshold == 0 {
            return Err(ConfigError::Validation(
                "health_check.failure_threshold must be greater than 0".to_string(),
            ));
        }

        if self.recovery_threshold == 0 {
            return Err(ConfigError::Validation(
                "health_check.recovery_threshold must be greater than 0".to_string(),
            ));
        }

        if self.ejection_duration_ms == 0 {
            return Err(ConfigError::Validation(
                "health_check.ejection_duration_ms must be greater than 0".to_string(),
            ));
        }

        if self.active_success_status_min == 0
            || self.active_success_status_min > self.active_success_status_max
        {
            return Err(ConfigError::Validation(
                "health_check active success status range must be valid".to_string(),
            ));
        }

        if self.passive_failure_status_min == 0
            || self.passive_failure_status_min > self.passive_failure_status_max
        {
            return Err(ConfigError::Validation(
                "health_check passive failure status range must be valid".to_string(),
            ));
        }

        Ok(())
    }
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval_sec: 10,
            endpoint: "/health".to_string(),
            check_timeout_ms: default_check_timeout_ms(),
            failure_threshold: default_failure_threshold(),
            recovery_threshold: default_recovery_threshold(),
            ejection_duration_ms: default_ejection_duration_ms(),
            active_success_status_min: default_active_success_status_min(),
            active_success_status_max: default_active_success_status_max(),
            passive_failure_status_min: default_passive_failure_status_min(),
            passive_failure_status_max: default_passive_failure_status_max(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            host: "127.0.0.1".to_string(),
            graceful_shutdown_timeout_ms: default_graceful_shutdown_timeout_ms(),
            client_header_timeout_ms: default_client_header_timeout_ms(),
            client_body_timeout_ms: default_client_body_timeout_ms(),
            fail_on_startup_dead_pool: false,
        }
    }
}

impl UpstreamConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.connect_timeout_ms == 0 {
            return Err(ConfigError::Validation(
                "upstream.connect_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.read_timeout_ms == 0 {
            return Err(ConfigError::Validation(
                "upstream.read_timeout_ms must be greater than 0".to_string(),
            ));
        }

        if self.max_request_body_bytes == 0 {
            return Err(ConfigError::Validation(
                "upstream.max_request_body_bytes must be greater than 0".to_string(),
            ));
        }

        if self.max_response_body_bytes == 0 {
            return Err(ConfigError::Validation(
                "upstream.max_response_body_bytes must be greater than 0".to_string(),
            ));
        }

        if self.max_buffered_bodies == 0 {
            return Err(ConfigError::Validation(
                "upstream.max_buffered_bodies must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: default_connect_timeout_ms(),
            read_timeout_ms: default_read_timeout_ms(),
            max_request_body_bytes: default_max_request_body_bytes(),
            max_response_body_bytes: default_max_response_body_bytes(),
            max_buffered_bodies: default_max_buffered_bodies(),
        }
    }
}

impl RetryConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.max_attempts == 0 {
            return Err(ConfigError::Validation(
                "retry.max_attempts must be greater than 0".to_string(),
            ));
        }

        if self.total_timeout_ms == 0 {
            return Err(ConfigError::Validation(
                "retry.total_timeout_ms must be greater than 0".to_string(),
            ));
        }

        for status in &self.retry_on_statuses {
            if *status < 100 {
                return Err(ConfigError::Validation(format!(
                    "retry.retry_on_statuses contains invalid status code '{}'",
                    status
                )));
            }
        }

        Ok(())
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_retry_max_attempts(),
            total_timeout_ms: default_retry_total_timeout_ms(),
            backoff_ms: default_retry_backoff_ms(),
            retry_on_statuses: default_retry_on_statuses(),
            retry_idempotent_methods: false,
        }
    }
}

impl DebugConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if let Some(token) = &self.auth_token {
            if token.trim().is_empty() {
                return Err(ConfigError::Validation(
                    "debug.auth_token must not be empty when provided".to_string(),
                ));
            }
        }

        Ok(())
    }
}

impl Default for DebugConfig {
    fn default() -> Self {
        Self {
            expose_backend_health: default_expose_backend_health(),
            expose_metrics: default_expose_metrics(),
            auth_token: None,
        }
    }
}

fn default_connect_timeout_ms() -> u64 {
    3_000
}

fn default_read_timeout_ms() -> u64 {
    15_000
}

fn default_graceful_shutdown_timeout_ms() -> u64 {
    30_000
}

fn default_client_header_timeout_ms() -> u64 {
    10_000
}

fn default_client_body_timeout_ms() -> u64 {
    15_000
}

fn default_max_request_body_bytes() -> u64 {
    16 * 1024 * 1024
}

fn default_max_response_body_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_max_buffered_bodies() -> usize {
    100
}

fn default_check_timeout_ms() -> u64 {
    5_000
}

fn default_failure_threshold() -> usize {
    3
}

fn default_recovery_threshold() -> usize {
    2
}

fn default_ejection_duration_ms() -> u64 {
    30_000
}

fn default_active_success_status_min() -> u16 {
    200
}

fn default_active_success_status_max() -> u16 {
    399
}

fn default_passive_failure_status_min() -> u16 {
    500
}

fn default_passive_failure_status_max() -> u16 {
    599
}

fn default_retry_max_attempts() -> usize {
    1
}

fn default_retry_total_timeout_ms() -> u64 {
    5_000
}

fn default_retry_backoff_ms() -> u64 {
    50
}

fn default_retry_on_statuses() -> Vec<u16> {
    vec![502, 503, 504]
}

fn default_expose_backend_health() -> bool {
    true
}

fn default_expose_metrics() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::Config;

    fn parse_config(input: &str) -> Config {
        serde_yaml::from_str(input).expect("config should parse")
    }

    #[test]
    fn accepts_valid_config() {
        let config = parse_config(
            r#"
server:
  port: 8080
  host: 0.0.0.0
routes:
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3001
health_check:
  interval_sec: 10
  endpoint: /health
"#,
        );

        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_duplicate_route_prefixes() {
        let config = parse_config(
            r#"
server:
  port: 8080
  host: 0.0.0.0
routes:
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3001
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3002
health_check:
  interval_sec: 10
  endpoint: /health
"#,
        );

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_backend_scheme() {
        let config = parse_config(
            r#"
server:
  port: 8080
  host: 0.0.0.0
routes:
  - path_prefix: /api
    backends:
      - tcp://127.0.0.1:3001
health_check:
  interval_sec: 10
  endpoint: /health
"#,
        );

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_health_endpoint() {
        let config = parse_config(
            r#"
server:
  port: 8080
  host: 0.0.0.0
routes:
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3001
health_check:
  interval_sec: 10
  endpoint: health
"#,
        );

        assert!(config.validate().is_err());
    }

    #[test]
    fn builds_socket_address_from_server_config() {
        let config = parse_config(
            r#"
server:
  port: 8080
  host: 127.0.0.1
routes:
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3001
health_check:
  interval_sec: 10
  endpoint: /health
"#,
        );

        let addr = config
            .server
            .socket_addr()
            .expect("socket addr should parse");
        assert_eq!(addr.to_string(), "127.0.0.1:8080");
    }

    #[test]
    fn rejects_zero_upstream_connect_timeout() {
        let config = parse_config(
            r#"
server:
  port: 8080
  host: 127.0.0.1
routes:
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3001
health_check:
  interval_sec: 10
  endpoint: /health
upstream:
  connect_timeout_ms: 0
  read_timeout_ms: 1000
"#,
        );

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_graceful_shutdown_timeout() {
        let config = parse_config(
            r#"
server:
  port: 8080
  host: 127.0.0.1
  graceful_shutdown_timeout_ms: 0
routes:
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3001
health_check:
  interval_sec: 10
  endpoint: /health
"#,
        );

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_upstream_body_limits() {
        let config = parse_config(
            r#"
server:
  port: 8080
  host: 127.0.0.1
routes:
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3001
health_check:
  interval_sec: 10
  endpoint: /health
upstream:
  max_request_body_bytes: 0
  max_response_body_bytes: 1
"#,
        );

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_retry_config() {
        let config = parse_config(
            r#"
server:
  port: 8080
  host: 127.0.0.1
routes:
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3001
health_check:
  interval_sec: 10
  endpoint: /health
retry:
  max_attempts: 0
"#,
        );

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_empty_debug_auth_token() {
        let config = parse_config(
            r#"
server:
  port: 8080
  host: 127.0.0.1
routes:
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3001
health_check:
  interval_sec: 10
  endpoint: /health
debug:
  auth_token: "   "
"#,
        );

        assert!(config.validate().is_err());
    }
}
