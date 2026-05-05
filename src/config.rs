use std::collections::HashSet;
use std::fs;
use std::path::Path;

use serde::Deserialize;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub routes: Vec<RouteConfig>,
    pub health_check: HealthCheckConfig,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
    pub host: String,
}

#[derive(Debug, Deserialize)]
pub struct RouteConfig {
    pub path_prefix: String,
    pub backends: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct HealthCheckConfig {
    pub interval_sec: u64,
    pub endpoint: String,
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

        Ok(())
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

        Ok(())
    }
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
}
