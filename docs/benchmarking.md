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
3. allocates fresh local ports for the proxy and each backend
4. generates a temporary `config.yaml` for the proxy
5. starts one proxy process per scenario
6. runs warmup and measured `wrk` passes
7. prints the native `wrk` report to stdout for each scenario
8. stores raw results, proxy metrics, backend health, and logs under `benchmark-results/<timestamp>/`

By default the runner uses:

- `--warmup 5s`
- `--duration 15s`
- `--timeout 5s`
- `--results-dir benchmark-results`
- `--scenarios all`
- `--wrk-bin wrk`

## Scenarios

The default suite runs four scenarios.

- `healthy_get`
  Healthy round-robin GET traffic with small responses. Uses `128` connections and `4` threads. Use this as the primary steady-state baseline.

- `large_response`
  Large streamed responses. Uses `64` connections and `4` threads. This exposes response-body accounting, body streaming, and upstream read-path overhead.

- `retry_get`
  One backend always returns `503` and one backend succeeds. Uses `64` connections and `4` threads. This measures the cost of bounded retries without letting passive health ejection remove the failing backend mid-run.

- `upload_post`
  `POST` requests with a 64 KiB body. Uses `48` connections and `4` threads plus a generated Lua script passed to `wrk`. This measures request-body buffering and request forwarding overhead.

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

Change the `wrk` timeout passed to each run:

```bash
cargo run --release --bin benchmark_runner -- --timeout 10s
```

Write results somewhere else:

```bash
cargo run --release --bin benchmark_runner -- --results-dir tmp/benchmarks
```

Use a specific `wrk` binary:

```bash
cargo run --release --bin benchmark_runner -- --wrk-bin /opt/homebrew/bin/wrk
```

## Output format

The runner preserves the native `wrk` output format. A measured `healthy_get` run from May 24, 2026 in `benchmark-results/20260524-194445/healthy_get/wrk.txt` looked like:

```text
Running 15s test @ http://127.0.0.1:55845/api/users
  4 threads and 128 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     4.89ms    7.48ms 117.35ms   95.86%
    Req/Sec     8.45k     2.16k   11.10k    73.50%
  Latency Distribution
     50%    3.43ms
     75%    4.43ms
     90%    6.38ms
     99%   41.07ms
  504964 requests in 15.01s, 581.26MB read
Requests/sec:  33642.70
Transfer/sec:     38.73MB
```

## Latest snapshot

The most recent full suite in this repo was run on May 24, 2026 and wrote results to `benchmark-results/20260524-194445/`.

- `healthy_get`: `33642.70` requests/sec, `p90 6.38ms`, `p99 41.07ms`, `38.73MB/sec`
- `large_response`: `10430.62` requests/sec, `p90 8.87ms`, `p99 54.99ms`, `2.55GB/sec`
- `retry_get`: `21891.36` requests/sec, `p90 4.79ms`, `p99 10.80ms`, `6.49MB/sec`
- `upload_post`: `18625.54` requests/sec, `p90 3.87ms`, `p99 9.68ms`, `4.33MB/sec`

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
  warmup.stderr.txt     # only if wrk writes stderr
  wrk.stderr.txt        # only if wrk writes stderr
```

`summary.txt` and `wrk.txt` both store the full `wrk` report for the measured pass.

The parent result directory also stores process logs under `logs/`, including:

- `cargo-build.stdout.log` and `cargo-build.stderr.log` when the build step runs
- one stdout and stderr log per benchmark backend
- one stdout and stderr log per proxy scenario run

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
- The runner creates a temporary runtime directory for each scenario and removes it after that scenario finishes.
- The orchestration layer and the synthetic benchmark backend are both implemented in Rust. `wrk` remains the external load generator.
