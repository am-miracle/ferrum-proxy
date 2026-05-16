use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const REPO_ROOT: &str = env!("CARGO_MANIFEST_DIR");
const WRK_POST_LUA: &str = r#"wrk.method = "POST"
wrk.headers["Content-Type"] = "application/octet-stream"
wrk.headers["X-Benchmark-Scenario"] = "upload_post"
wrk.body = string.rep("x", 64 * 1024)
"#;

#[derive(Clone, Copy)]
struct BackendSpec {
    name: &'static str,
    response_body_bytes: usize,
    status: u16,
    failure_status: u16,
    fail_every: u64,
    delay_ms: u64,
}

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    path: &'static str,
    description: &'static str,
    connections: usize,
    threads: usize,
    lua_script: Option<&'static str>,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "healthy_get",
        path: "/api/users",
        description: "Small response, healthy round-robin fast path.",
        connections: 128,
        threads: 4,
        lua_script: None,
    },
    Scenario {
        name: "large_response",
        path: "/large/blob",
        description: "Large streamed response to surface response-body overhead.",
        connections: 64,
        threads: 4,
        lua_script: None,
    },
    Scenario {
        name: "retry_get",
        path: "/retry/items",
        description: "One failing backend plus one healthy backend to exercise retry cost.",
        connections: 64,
        threads: 4,
        lua_script: None,
    },
    Scenario {
        name: "upload_post",
        path: "/upload/object",
        description: "64 KiB POST body to measure request buffering and forwarding.",
        connections: 48,
        threads: 4,
        lua_script: Some("post_upload.lua"),
    },
];

const BACKEND_SPECS: &[(&str, BackendSpec)] = &[
    (
        "healthy_a",
        BackendSpec {
            name: "healthy-a",
            response_body_bytes: 1024,
            status: 200,
            failure_status: 503,
            fail_every: 0,
            delay_ms: 0,
        },
    ),
    (
        "healthy_b",
        BackendSpec {
            name: "healthy-b",
            response_body_bytes: 1024,
            status: 200,
            failure_status: 503,
            fail_every: 0,
            delay_ms: 0,
        },
    ),
    (
        "large_a",
        BackendSpec {
            name: "large-a",
            response_body_bytes: 256 * 1024,
            status: 200,
            failure_status: 503,
            fail_every: 0,
            delay_ms: 0,
        },
    ),
    (
        "retry_bad",
        BackendSpec {
            name: "retry-bad",
            response_body_bytes: 128,
            status: 200,
            failure_status: 503,
            fail_every: 1,
            delay_ms: 0,
        },
    ),
    (
        "retry_good",
        BackendSpec {
            name: "retry-good",
            response_body_bytes: 128,
            status: 200,
            failure_status: 503,
            fail_every: 0,
            delay_ms: 0,
        },
    ),
    (
        "upload_a",
        BackendSpec {
            name: "upload-a",
            response_body_bytes: 64,
            status: 200,
            failure_status: 503,
            fail_every: 0,
            delay_ms: 0,
        },
    ),
];

struct Cli {
    duration: String,
    warmup: String,
    timeout: String,
    results_dir: PathBuf,
    scenarios: Vec<&'static Scenario>,
    skip_build: bool,
    wrk_bin: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = parse_args()?;
    let results_root = prepare_results_root(&cli.results_dir)?;
    let logs_dir = results_root.join("logs");
    fs::create_dir_all(&logs_dir)?;

    let wrk_path = resolve_executable(&cli.wrk_bin)
        .ok_or_else(|| format!("wrk not found: {}", cli.wrk_bin))?;

    if !cli.skip_build {
        run_command_to_logs(
            Command::new("cargo")
                .arg("build")
                .arg("--release")
                .current_dir(REPO_ROOT),
            &logs_dir.join("cargo-build.stdout.log"),
            &logs_dir.join("cargo-build.stderr.log"),
        )?;
    }

    let proxy_binary = Path::new(REPO_ROOT).join("target/release/ferrum-proxy");
    let backend_binary = Path::new(REPO_ROOT).join("target/release/benchmark_backend");
    if !proxy_binary.exists() {
        return Err(format!("release proxy binary not found at {}", proxy_binary.display()).into());
    }
    if !backend_binary.exists() {
        return Err(
            format!("release benchmark backend binary not found at {}", backend_binary.display())
                .into(),
        );
    }

    let backend_ports = allocate_backend_ports();
    let proxy_port = pick_unused_port()?;
    let mut backend_processes = Vec::new();

    for (key, spec) in BACKEND_SPECS {
        let port = *backend_ports
            .get(*key)
            .ok_or_else(|| format!("missing backend port allocation for {key}"))?;
        backend_processes.push(spawn_backend(
            &backend_binary,
            spec,
            port,
            &logs_dir,
        )?);
    }

    let run_result = (|| -> Result<(), Box<dyn std::error::Error>> {
        for scenario in &cli.scenarios {
            println!("[scenario] {}: {}", scenario.name, scenario.description);
            let scenario_dir = results_root.join(scenario.name);
            fs::create_dir_all(&scenario_dir)?;
            run_scenario(
                scenario,
                &scenario_dir,
                &proxy_binary,
                proxy_port,
                &backend_ports,
                &wrk_path,
                &cli.duration,
                &cli.warmup,
                &cli.timeout,
                &logs_dir,
            )?;
        }
        Ok(())
    })();

    stop_children(&mut backend_processes);
    run_result?;

    println!("benchmark results written to {}", results_root.display());
    Ok(())
}

fn parse_args() -> Result<Cli, String> {
    let mut duration = "15s".to_string();
    let mut warmup = "5s".to_string();
    let mut timeout = "5s".to_string();
    let mut results_dir = PathBuf::from("benchmark-results");
    let mut scenario_arg = "all".to_string();
    let mut skip_build = false;
    let mut wrk_bin = "wrk".to_string();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--duration" => duration = parse_arg_value(args.next(), "--duration")?,
            "--warmup" => warmup = parse_arg_value(args.next(), "--warmup")?,
            "--timeout" => timeout = parse_arg_value(args.next(), "--timeout")?,
            "--results-dir" => {
                results_dir = PathBuf::from(parse_arg_value::<String>(args.next(), "--results-dir")?)
            }
            "--scenarios" => scenario_arg = parse_arg_value(args.next(), "--scenarios")?,
            "--skip-build" => skip_build = true,
            "--wrk-bin" => wrk_bin = parse_arg_value(args.next(), "--wrk-bin")?,
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument: {other}\n\n{}", usage())),
        }
    }

    Ok(Cli {
        duration,
        warmup,
        timeout,
        results_dir,
        scenarios: select_scenarios(&scenario_arg)?,
        skip_build,
        wrk_bin,
    })
}

fn select_scenarios(selection: &str) -> Result<Vec<&'static Scenario>, String> {
    if selection == "all" {
        return Ok(SCENARIOS.iter().collect());
    }

    let mut selected = Vec::new();
    for name in selection.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let scenario = SCENARIOS
            .iter()
            .find(|scenario| scenario.name == name)
            .ok_or_else(|| format!("unknown scenario: {name}"))?;
        selected.push(scenario);
    }

    if selected.is_empty() {
        return Err("no scenarios selected".to_string());
    }

    Ok(selected)
}

fn prepare_results_root(base_dir: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let timestamp = unix_timestamp_string()?;
    let root = Path::new(REPO_ROOT).join(base_dir).join(timestamp);
    fs::create_dir_all(&root)?;
    Ok(root)
}

fn unix_timestamp_string() -> Result<String, Box<dyn std::error::Error>> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let secs = now.as_secs();
    let tm = chrono_like_utc(secs)?;
    Ok(format!(
        "{:04}{:02}{:02}-{:02}{:02}{:02}",
        tm.year, tm.month, tm.day, tm.hour, tm.minute, tm.second
    ))
}

struct DateParts {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
}

fn chrono_like_utc(timestamp_secs: u64) -> Result<DateParts, Box<dyn std::error::Error>> {
    let secs = timestamp_secs as i64;
    let days = secs.div_euclid(86_400);
    let seconds_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days)?;
    Ok(DateParts {
        year,
        month,
        day,
        hour: (seconds_of_day / 3600) as u32,
        minute: ((seconds_of_day % 3600) / 60) as u32,
        second: (seconds_of_day % 60) as u32,
    })
}

fn civil_from_days(days_since_epoch: i64) -> Result<(i32, u32, u32), Box<dyn std::error::Error>> {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    Ok((year as i32, m as u32, d as u32))
}

fn allocate_backend_ports() -> std::collections::BTreeMap<&'static str, u16> {
    BACKEND_SPECS
        .iter()
        .map(|(key, _)| (*key, pick_unused_port().expect("failed to allocate port")))
        .collect()
}

fn pick_unused_port() -> Result<u16, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn spawn_backend(
    backend_binary: &Path,
    spec: &BackendSpec,
    port: u16,
    logs_dir: &Path,
) -> Result<Child, Box<dyn std::error::Error>> {
    let stdout = File::create(logs_dir.join(format!("{}.stdout.log", spec.name)))?;
    let stderr = File::create(logs_dir.join(format!("{}.stderr.log", spec.name)))?;

    let child = Command::new(backend_binary)
        .arg("--port")
        .arg(port.to_string())
        .arg("--name")
        .arg(spec.name)
        .arg("--status")
        .arg(spec.status.to_string())
        .arg("--failure-status")
        .arg(spec.failure_status.to_string())
        .arg("--fail-every")
        .arg(spec.fail_every.to_string())
        .arg("--delay-ms")
        .arg(spec.delay_ms.to_string())
        .arg("--response-body-bytes")
        .arg(spec.response_body_bytes.to_string())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()?;

    wait_until_ready(("127.0.0.1", port), "/health", Duration::from_secs(10))?;
    Ok(child)
}

fn run_scenario(
    scenario: &Scenario,
    scenario_dir: &Path,
    proxy_binary: &Path,
    proxy_port: u16,
    backend_ports: &std::collections::BTreeMap<&'static str, u16>,
    wrk_bin: &Path,
    duration: &str,
    warmup: &str,
    timeout: &str,
    logs_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime_dir = create_runtime_dir(scenario.name)?;
    let config_path = runtime_dir.join("config.yaml");
    fs::write(&config_path, render_config(proxy_port, backend_ports))?;
    if scenario.lua_script.is_some() {
        fs::write(runtime_dir.join("post_upload.lua"), WRK_POST_LUA)?;
    }

    let stdout = File::create(logs_dir.join(format!("{}.proxy.stdout.log", scenario.name)))?;
    let stderr = File::create(logs_dir.join(format!("{}.proxy.stderr.log", scenario.name)))?;

    let mut proxy = Command::new(proxy_binary)
        .current_dir(&runtime_dir)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()?;

    let run_result = (|| -> Result<(), Box<dyn std::error::Error>> {
        wait_until_ready(("127.0.0.1", proxy_port), "/health", Duration::from_secs(20))?;
        fs::copy(&config_path, scenario_dir.join("config.yaml"))?;

        run_wrk(
            wrk_bin,
            scenario,
            &runtime_dir,
            proxy_port,
            warmup,
            timeout,
            &scenario_dir.join("warmup.txt"),
        )?;
        run_wrk(
            wrk_bin,
            scenario,
            &runtime_dir,
            proxy_port,
            duration,
            timeout,
            &scenario_dir.join("wrk.txt"),
        )?;

        let wrk_output = fs::read_to_string(scenario_dir.join("wrk.txt"))?;
        fs::write(scenario_dir.join("summary.txt"), &wrk_output)?;
        println!("{wrk_output}");
        fs::write(
            scenario_dir.join("metrics.prom"),
            fetch_http(("127.0.0.1", proxy_port), "/metrics")?,
        )?;
        fs::write(
            scenario_dir.join("backend-health.txt"),
            fetch_http(("127.0.0.1", proxy_port), "/health/backends")?,
        )?;
        Ok(())
    })();

    stop_child(&mut proxy);
    let _ = fs::remove_dir_all(&runtime_dir);
    run_result
}

fn create_runtime_dir(label: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = std::env::temp_dir().join(format!("ferrum-proxy-bench-{label}-{nanos}"));
    fs::create_dir_all(&path)?;
    Ok(path)
}

fn run_wrk(
    wrk_bin: &Path,
    scenario: &Scenario,
    runtime_dir: &Path,
    proxy_port: u16,
    duration: &str,
    timeout: &str,
    output_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://127.0.0.1:{proxy_port}{}", scenario.path);
    let mut command = Command::new(wrk_bin);
    command
        .arg(format!("-t{}", scenario.threads))
        .arg(format!("-c{}", scenario.connections))
        .arg(format!("-d{duration}"))
        .arg("--timeout")
        .arg(timeout)
        .arg("--latency");

    if let Some(script) = scenario.lua_script {
        command.arg("-s").arg(runtime_dir.join(script));
    }

    let output = command.arg(url).output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("wrk failed: {stderr}").into());
    }

    fs::write(output_path, output.stdout)?;
    if !output.stderr.is_empty() {
        fs::write(output_path.with_extension("stderr.txt"), output.stderr)?;
    }
    Ok(())
}

fn wait_until_ready(
    address: (&str, u16),
    path: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(response) = fetch_http(address, path) {
            if response.starts_with("ok") || response.contains("ferrum_proxy_requests_total") {
                return Ok(());
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(format!("service did not become ready at {}:{}", address.0, address.1).into())
}

fn fetch_http(address: (&str, u16), path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let addr: SocketAddr = format!("{}:{}", address.0, address.1).parse()?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\n\r\n",
        address.0, address.1
    );
    stream.write_all(request.as_bytes())?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (_, body) = response
        .split_once("\r\n\r\n")
        .ok_or("invalid HTTP response from benchmark target")?;
    Ok(body.to_string())
}

fn render_config(
    proxy_port: u16,
    backend_ports: &std::collections::BTreeMap<&'static str, u16>,
) -> String {
    format!(
        "server:\n  port: {proxy_port}\n  host: 127.0.0.1\n  graceful_shutdown_timeout_ms: 30000\n  client_header_timeout_ms: 10000\n  client_body_timeout_ms: 15000\n  fail_on_startup_dead_pool: false\n\nroutes:\n  - path_prefix: /api\n    backends:\n      - http://127.0.0.1:{healthy_a}\n      - http://127.0.0.1:{healthy_b}\n  - path_prefix: /large\n    backends:\n      - http://127.0.0.1:{large_a}\n  - path_prefix: /retry\n    backends:\n      - http://127.0.0.1:{retry_bad}\n      - http://127.0.0.1:{retry_good}\n  - path_prefix: /upload\n    backends:\n      - http://127.0.0.1:{upload_a}\n\nhealth_check:\n  interval_sec: 3600\n  endpoint: /health\n  check_timeout_ms: 1000\n  failure_threshold: 100000\n  recovery_threshold: 1\n  ejection_duration_ms: 1000\n  active_success_status_min: 200\n  active_success_status_max: 399\n  passive_failure_status_min: 500\n  passive_failure_status_max: 599\n\nupstream:\n  connect_timeout_ms: 3000\n  read_timeout_ms: 15000\n  max_request_body_bytes: 33554432\n  max_response_body_bytes: 268435456\n  max_buffered_bodies: 256\n\nretry:\n  max_attempts: 2\n  total_timeout_ms: 3000\n  backoff_ms: 0\n  retry_on_statuses: [503]\n  retry_idempotent_methods: false\n\ndebug:\n  expose_backend_health: true\n  expose_metrics: true\n",
        healthy_a = backend_ports["healthy_a"],
        healthy_b = backend_ports["healthy_b"],
        large_a = backend_ports["large_a"],
        retry_bad = backend_ports["retry_bad"],
        retry_good = backend_ports["retry_good"],
        upload_a = backend_ports["upload_a"],
    )
}

fn run_command_to_logs(
    command: &mut Command,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let output = command.output()?;
    fs::write(stdout_path, output.stdout)?;
    fs::write(stderr_path, output.stderr)?;
    if !output.status.success() {
        return Err(format!("command failed: {:?}", command).into());
    }
    Ok(())
}

fn resolve_executable(candidate: &str) -> Option<PathBuf> {
    let path = Path::new(candidate);
    if path.components().count() > 1 && path.exists() {
        return Some(path.to_path_buf());
    }

    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join(candidate))
            .find(|full| full.exists())
    })
}

fn stop_children(children: &mut Vec<Child>) {
    for child in children.iter_mut().rev() {
        stop_child(child);
    }
}

fn stop_child(child: &mut Child) {
    if let Ok(None) = child.try_wait() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn parse_arg_value<T>(value: Option<String>, flag: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let raw = value.ok_or_else(|| format!("missing value for {flag}"))?;
    raw.parse::<T>()
        .map_err(|err| format!("invalid value for {flag}: {err}"))
}

fn usage() -> String {
    "usage: cargo run --release --bin benchmark_runner -- [--duration 15s] [--warmup 5s] [--timeout 5s] [--results-dir benchmark-results] [--scenarios all|healthy_get,retry_get] [--skip-build] [--wrk-bin /path/to/wrk]".to_string()
}
