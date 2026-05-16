# Benchmarking

This project uses `wrk` against the live proxy process rather than a function-level microbenchmark.

That is deliberate. The meaningful performance costs in `ferrum-proxy` are in:

- accepting TCP connections
- parsing HTTP/1.1 requests
- route matching and backend selection
- forwarding headers and bodies
- retry behavior
- timeout enforcement
- in-process telemetry updates

## Prerequisites

- `wrk`
- Rust toolchain with `cargo`

## Run the benchmark suite

From the repo root:

```bash
cargo run --release --bin benchmark_runner --
```

That command:

1. builds `target/release/ferrum-proxy` and `target/release/benchmark_backend`
2. starts dedicated local Rust benchmark backends
3. generates a temporary `config.yaml` for the proxy
4. runs warmup and measured `wrk` passes
5. prints the native `wrk` report to stdout for each scenario
6. stores raw results, proxy metrics, backend health, and logs under `benchmark-results/<timestamp>/`

## Scenarios

The default suite runs four scenarios.

- `healthy_get`
  Healthy round-robin GET traffic with small responses. Use this as the primary steady-state baseline.

- `large_response`
  Large streamed responses. This exposes response-body accounting, body streaming, and upstream read-path overhead.

- `retry_get`
  One backend always returns `503` and one backend succeeds. This measures the cost of bounded retries without letting passive health ejection remove the failing backend mid-run.

- `upload_post`
  `POST` requests with a 64 KiB body. This measures request-body buffering and request forwarding overhead.

## Useful options

Run a subset of scenarios:

```bash
cargo run --release --bin benchmark_runner -- --scenarios healthy_get,retry_get
```

Increase duration:

```bash
cargo run --release --bin benchmark_runner -- --warmup 10s --duration 30s
```

Reuse an existing release build:

```bash
cargo run --release --bin benchmark_runner -- --skip-build
```

## Output format

The runner preserves the native `wrk` output format. A typical measured run looks like:

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

## Output layout

Each scenario gets its own result directory:

```text
benchmark-results/<timestamp>/<scenario>/
  backend-health.txt
  config.yaml
  metrics.prom
  summary.txt
  warmup.txt
  wrk.txt
```

`summary.txt` and `wrk.txt` both store the full `wrk` report for the measured pass.

The parent result directory also stores process logs under `logs/`.

## Reading results

Use `healthy_get` as the baseline for regressions in the main request path.

Use `retry_get` to estimate the additional latency and throughput cost introduced by retry logic.

Use `upload_post` and `large_response` to understand how request and response body size affect throughput and tail latency.

Compare:

- `Requests/sec`
- latency distribution from `--latency`, especially `90%` and `99%`
- `Transfer/sec`
- proxy metrics in `metrics.prom`

## Notes

- Run benchmarks on an otherwise idle machine when possible.
- Prefer repeated runs and compare medians rather than one-off results.
- The generated benchmark config keeps passive health ejection effectively disabled so the retry scenario stays stable and comparable between runs.
- The orchestration layer and the synthetic benchmark backend are both implemented in Rust. `wrk` remains the external load generator.
