# ferrum-proxy

`ferrum-proxy` is a Rust HTTP reverse proxy. It sits in front of your backend services, matches a request to a route, picks a healthy backend, forwards the request, and sends the response back to the client.

## What it does

- path-based routing
- per-route balancing (`round_robin` or `first_healthy`)
- active health checks
- passive failure tracking from real traffic
- request and response streaming
- graceful shutdown and connection draining
- hardened forwarding and hop-by-hop header handling
- request and response size limits
- upstream connect and read timeouts
- bounded retries for safe and idempotent requests
- temporary backend ejection for flapping upstreams
- Prometheus-style metrics export

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
  graceful_shutdown_timeout_ms: 30000
  client_header_timeout_ms: 10000
  client_body_timeout_ms: 15000

routes:
  - path_prefix: /api
    backends:
      - http://127.0.0.1:3001
      - http://127.0.0.1:3002
      - http://127.0.0.1:3003
    balancing: round_robin
    retry_on_statuses: [502, 503, 504]
    passive_failure_statuses: [500, 502, 503, 504]
    health_check_endpoint: /ready
    connect_timeout_ms: 1000
    read_timeout_ms: 5000
    client_body_timeout_ms: 3000

  - path_prefix: /static
    backends:
      - http://127.0.0.1:4000
    balancing: first_healthy

health_check:
  interval_sec: 10
  endpoint: /health
  failure_threshold: 3
  recovery_threshold: 2
  ejection_duration_ms: 30000
  active_success_status_min: 200
  active_success_status_max: 399
  passive_failure_status_min: 500
  passive_failure_status_max: 599

upstream:
  connect_timeout_ms: 3000
  read_timeout_ms: 15000
  max_request_body_bytes: 16777216
  max_response_body_bytes: 67108864

retry:
  max_attempts: 2
  total_timeout_ms: 5000
  backoff_ms: 50
  retry_on_statuses: [502, 503, 504]

debug:
  expose_backend_health: true
  expose_metrics: true
  auth_token: change-me
```

How it works:

- requests starting with `/api` go to the `/api` backend pool
- requests starting with `/static` go to the `/static` backend pool
- routes can override balancing strategy, retryable statuses, passive failure statuses, health endpoints, and connect/read/body timeouts
- the health checker probes each backend on the route override or the global `/health` endpoint
- only healthy backends stay in the load-balancing pool
- client headers and bodies are timed out independently from upstream reads
- request and response bodies are rejected once they exceed configured byte limits
- safe and idempotent requests can be retried within a bounded total timeout
- repeated backend failures trigger temporary ejection before active checks recover them
- slow client uploads, slow upstream response bodies, and partial client disconnects are surfaced through proxy error metrics
- debug endpoints can be hidden or protected with a bearer token
- `https://` upstream backends are rejected for now; terminate TLS in a trusted front proxy and forward plain HTTP to `ferrum-proxy`

## Run it

```bash
cargo run
```

The proxy starts on the configured host and port.
On Unix-like systems, `SIGHUP` triggers a graceful controlled restart workflow: the proxy drains connections and exits so a supervisor can start it again with fresh config.

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
  Current backend health state. Can be disabled or protected with a bearer token.

- `GET /metrics`
  Prometheus text exposition with request, latency, backend failure, backend health, and error counters. Can be disabled or protected with a bearer token.

## Tests

Run the full test suite:

```bash
cargo test
```

The repo has:

- unit tests for routing, balancing, health, config, upstream, and telemetry
- integration tests for request-path behavior
- black-box tests that start the real server and hit it over HTTP

## Benchmarks

This repo benchmarks the live proxy with `wrk`, not a function-level microbenchmark.

Install `wrk`, then run:

```bash
cargo run --release --bin benchmark_runner --
```

The runner builds the release binaries, starts dedicated local benchmark backends, runs warmup and measured `wrk` passes, and stores results under `benchmark-results/<timestamp>/`.

The default benchmark suite covers:

- `healthy_get`
  steady-state GET traffic through healthy backends
- `large_response`
  large streamed upstream responses
- `retry_get`
  one failing backend plus one healthy backend to measure retry cost
- `upload_post`
  `POST` requests with a 64 KiB request body

Useful commands:

```bash
cargo run --release --bin benchmark_runner -- --scenarios healthy_get,retry_get
cargo run --release --bin benchmark_runner -- --warmup 10s --duration 30s
cargo run --release --bin benchmark_runner -- --skip-build
```

The benchmark output is the native `wrk` report, for example:

```text
Running 30s test @ http://127.0.0.1:63580/api/users
  4 threads and 100 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     4.69ms    4.79ms 123.96ms   93.34%
    Req/Sec     6.24k     1.10k    7.79k    70.51%
  Latency Distribution
     50%    3.65ms
     75%    4.76ms
     90%    7.33ms
     99%   25.20ms
  747182 requests in 30.10s, 0.86GB read
Requests/sec:  24822.42
Transfer/sec:     29.43MB
```

Focus on:

- `Requests/sec` for throughput
- `Latency Distribution`, especially `90%` and `99%`
- `Transfer/sec` for body-heavy scenarios

For more detail, see [docs/benchmarking.md](docs/benchmarking.md).

## Docs

- [docs/architecture.md](docs/architecture.md)
- [docs/benchmarking.md](docs/benchmarking.md)
- [docs/production-readiness.md](docs/production-readiness.md)
