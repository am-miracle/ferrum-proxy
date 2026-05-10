# ferrum-proxy

`ferrum-proxy` is a Rust HTTP reverse proxy. It sits in front of your backend services, matches a request to a route, picks a healthy backend, forwards the request, and sends the response back to the client.

## What it does

- path-based routing
- round-robin load balancing
- active health checks
- passive failure tracking from real traffic
- request and response streaming
- upstream connect and read timeouts
- simple debug endpoints for backend health and metrics

## Current status

This project works and is useful for local testing, learning, and small internal environments.

It is not production-ready yet. The main gaps are documented in [docs/production-readiness.md](docs/production-readiness.md).

## Project layout

```text
src/
  config/      load and validate config
  server/      TCP listener and HTTP server wiring
  http/        request handling and proxy flow
  routing/     route matching
  balancing/   backend selection
  health/      active and passive health state
  upstream/    outbound HTTP client
  telemetry/   in-memory metrics and health transition logs
```

## Config

The proxy reads `config.yaml`.

Example:

```yaml
server:
  port: 8080
  host: 0.0.0.0

routes:
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3001
      - http://127.0.0.1:3002
      - http://127.0.0.1:3003

  - path_prefix: /static
    backends:
      - http://127.0.0.1:4000

health_check:
  interval_sec: 10
  endpoint: /health
```

How it works:

- requests starting with `/api` go to the `/api` backend pool
- requests starting with `/static` go to the `/static` backend pool
- the health checker probes each backend on `/health`
- only healthy backends stay in the load-balancing pool

## Run it

```bash
cargo run
```

The proxy starts on the configured host and port.

## Local test setup

This repo includes a tiny Python backend for local testing:

[scripts/test_backend.py](scripts/test_backend.py)

Run these in separate terminals:

```bash
python3 scripts/test_backend.py 3001
python3 scripts/test_backend.py 3002
python3 scripts/test_backend.py 4000
```

Leave `3003` down if you want to test unhealthy backend behavior.

Then start the proxy:

```bash
cargo run
```

## Try it

Check the proxy:

```bash
curl -i http://127.0.0.1:8080/health
curl -i http://127.0.0.1:8080/health/backends
curl -i http://127.0.0.1:8080/metrics
```

Send traffic through it:

```bash
curl -i http://127.0.0.1:8080/api/users
curl -i http://127.0.0.1:8080/api/users
curl -i http://127.0.0.1:8080/static/logo.png
```

The test backend responds with its port number, so it is easy to see which backend handled the request.

## Internal endpoints

- `GET /`
  Basic status message.

- `GET /health`
  Health of the proxy process itself.

- `GET /health/backends`
  Current backend health state.

- `GET /metrics`
  Simple in-memory metrics and recent health transitions.

## Tests

Run the full test suite:

```bash
cargo test
```

The repo has:

- unit tests for routing, balancing, health, config, upstream, and telemetry
- integration tests for request-path behavior
- black-box tests that start the real server and hit it over HTTP

## Docs

- [docs/architecture.md](docs/architecture.md)
- [docs/production-readiness.md](docs/production-readiness.md)
