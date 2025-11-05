// Numan Thabit 2017
use std::{
    fs::{self, OpenOptions},
    io::BufWriter,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use humantime::format_duration;
use serde::Serialize;
use tokio::{
    io::{AsyncReadExt, BufReader},
    process::Command,
    time::{sleep, timeout},
};
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(author, version, about = "Benchmark harness for solana-ultra-rpc")]
struct BenchArgs {
    /// Path to the solana-ultra-rpc binary to launch for the run.
    #[arg(long, default_value = "target/debug/solana-ultra-rpc")]
    server_bin: PathBuf,

    /// Optional configuration file passed to the server's --config flag.
    #[arg(long)]
    server_config: Option<PathBuf>,

    /// Additional arguments appended to the server invocation.
    #[arg(long = "server-arg", value_name = "ARG", action = clap::ArgAction::Append)]
    server_args: Vec<String>,

    /// Environment assignments (KEY=VALUE) applied to the server process.
    #[arg(long = "server-env", value_name = "KEY=VALUE", action = clap::ArgAction::Append)]
    server_env: Vec<String>,

    /// Optional file to capture server stdout/stderr.
    #[arg(long)]
    server_log: Option<PathBuf>,

    /// How long to wait for the server to warm up before issuing load.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "5s")]
    warmup: Duration,

    /// Maximum time to wait for graceful shutdown before forcing a kill.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "3s")]
    shutdown_grace: Duration,

    /// Optional path to the wrk (or wrk-quic) binary used to generate load.
    #[arg(long)]
    wrk_bin: Option<PathBuf>,

    /// Optional wrk Lua script to drive request patterns.
    #[arg(long)]
    wrk_script: Option<PathBuf>,

    /// Number of wrk threads to launch.
    #[arg(long, default_value_t = 12)]
    wrk_threads: u32,

    /// Number of wrk connections to maintain.
    #[arg(long, default_value_t = 600)]
    wrk_connections: u32,

    /// Duration for the wrk run.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "30s")]
    wrk_duration: Duration,

    /// Number of wrk iterations to execute sequentially.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    iterations: u32,

    /// Cooldown duration between wrk iterations.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "0s")]
    wrk_cooldown: Duration,

    /// Maximum time allowed for a single wrk execution before it is terminated.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "90s")]
    wrk_timeout: Duration,

    /// Optional path to persist structured wrk output as JSON.
    #[arg(long)]
    wrk_output_json: Option<PathBuf>,

    /// RPC endpoint to benchmark (passed as the wrk target URI).
    #[arg(long, default_value = "127.0.0.1:8899")]
    rpc_endpoint: String,

    /// Skip launching the server; assumes an endpoint is already available.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    skip_server: bool,

    /// When set, only print the actions that would be taken.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    dry_run: bool,
}

#[derive(Debug, Clone, Serialize)]
struct WrkReport {
    iteration: u32,
    duration_ms: u64,
    stdout: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    stderr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics: Option<WrkMetrics>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct WrkMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    requests_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transfer_per_sec: Option<TransferRate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    latency: Option<WrkLatencyStats>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    latency_distribution: Vec<WrkPercentile>,
}

#[derive(Debug, Clone, Serialize)]
struct WrkLatencyStats {
    avg_ns: u64,
    stdev_ns: u64,
    max_ns: u64,
    stdev_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
struct WrkPercentile {
    percentile: f64,
    latency_ns: u64,
}

#[derive(Debug, Clone, Serialize)]
struct TransferRate {
    value: f64,
    unit: String,
}

impl WrkMetrics {
    fn is_empty(&self) -> bool {
        self.requests_per_sec.is_none()
            && self.transfer_per_sec.is_none()
            && self.latency.is_none()
            && self.latency_distribution.is_empty()
    }

    fn find_percentile(&self, target: f64) -> Option<&WrkPercentile> {
        self.latency_distribution.iter().find(|entry| {
            (entry.percentile - target).abs() < f64::EPSILON
                || (entry.percentile - target).abs() < 0.0001
        })
    }
}

struct ServerHandle {
    child: tokio::process::Child,
    grace: Duration,
}

impl ServerHandle {
    async fn spawn(args: &BenchArgs) -> Result<Self> {
        if !args.server_bin.exists() {
            return Err(anyhow!(
                "server binary not found at {}",
                args.server_bin.display()
            ));
        }

        let mut cmd = Command::new(&args.server_bin);
        cmd.kill_on_drop(true);

        if let Some(config) = &args.server_config {
            cmd.arg("--config").arg(config);
        }

        if !args.server_args.is_empty() {
            cmd.args(&args.server_args);
        }

        apply_server_env(&mut cmd, &args.server_env)?;

        configure_stdio(&mut cmd, args.server_log.as_deref())?;

        let child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn {}", args.server_bin.display()))?;

        Ok(Self {
            child,
            grace: args.shutdown_grace,
        })
    }

    async fn wait_ready(&mut self, warmup: Duration) {
        if warmup.is_zero() {
            return;
        }
        info!(duration = %format_duration(warmup), "waiting for server warmup");
        sleep(warmup).await;
    }

    async fn shutdown(mut self) -> Result<()> {
        if self.child.id().is_none() {
            return Ok(());
        }

        match timeout(self.grace, self.child.wait()).await {
            Ok(wait_result) => {
                let status = wait_result?;
                info!(?status, "server exited");
                Ok(())
            }
            Err(_) => {
                warn!(
                    timeout = %format_duration(self.grace),
                    "server did not exit in time; forcing kill"
                );
                if let Err(err) = self.child.start_kill() {
                    warn!(%err, "start_kill failed; retrying with kill()");
                    self.child
                        .kill()
                        .await
                        .context("failed to kill stalled server process")?;
                }
                let status = self
                    .child
                    .wait()
                    .await
                    .context("failed to await server termination after kill")?;
                info!(?status, "server killed");
                Ok(())
            }
        }
    }
}

fn configure_stdio(cmd: &mut Command, log_path: Option<&Path>) -> Result<()> {
    if let Some(path) = log_path {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)
                .with_context(|| format!("failed to create log directory {}", dir.display()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .with_context(|| format!("failed to open log file {}", path.display()))?;
        let file_err = file
            .try_clone()
            .context("failed to duplicate log file handle for stderr")?;
        cmd.stdout(Stdio::from(file));
        cmd.stderr(Stdio::from(file_err));
    } else {
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::null());
    }
    Ok(())
}

fn apply_server_env(cmd: &mut Command, assignments: &[String]) -> Result<()> {
    if assignments.is_empty() {
        return Ok(());
    }

    for assignment in assignments {
        let (key, value) = parse_env_assignment(assignment)?;
        cmd.env(key, value);
    }

    Ok(())
}

fn parse_env_assignment(assignment: &str) -> Result<(String, String)> {
    let mut parts = assignment.splitn(2, '=');
    let key = parts
        .next()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .ok_or_else(|| anyhow!("invalid --server-env '{}': missing key", assignment))?;
    let value = parts
        .next()
        .ok_or_else(|| anyhow!("invalid --server-env '{}': missing '='", assignment))?;
    Ok((key.to_string(), value.to_string()))
}

async fn run_wrk_iterations(args: &BenchArgs, wrk_bin: &Path) -> Result<Vec<WrkReport>> {
    if !wrk_bin.exists() {
        return Err(anyhow!("wrk binary not found at {}", wrk_bin.display()));
    }

    if let Some(script) = &args.wrk_script {
        if !script.exists() {
            return Err(anyhow!("wrk script not found at {}", script.display()));
        }
    }

    let mut reports = Vec::with_capacity(args.iterations as usize);

    for iteration in 1..=args.iterations {
        info!(
            iteration,
            total = args.iterations,
            bin = %wrk_bin.display(),
            threads = args.wrk_threads,
            connections = args.wrk_connections,
            duration = %format_duration(args.wrk_duration),
            endpoint = %args.rpc_endpoint,
            "starting wrk iteration"
        );

        let report = run_wrk(args, wrk_bin, iteration).await?;
        log_iteration_metrics(&report);
        reports.push(report);

        if iteration < args.iterations && !args.wrk_cooldown.is_zero() {
            info!(
                iteration,
                cooldown = %format_duration(args.wrk_cooldown),
                "cooldown before next iteration"
            );
            sleep(args.wrk_cooldown).await;
        }
    }

    Ok(reports)
}

async fn run_wrk(args: &BenchArgs, wrk_bin: &Path, iteration: u32) -> Result<WrkReport> {
    let start = Instant::now();

    let mut cmd = Command::new(wrk_bin);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.arg("--latency");
    cmd.arg("-t").arg(args.wrk_threads.to_string());
    cmd.arg("-c").arg(args.wrk_connections.to_string());
    cmd.arg("-d")
        .arg(format!("{}", format_duration(args.wrk_duration)));
    if let Some(script) = &args.wrk_script {
        cmd.arg("-s").arg(script);
    }
    cmd.arg(format!("quic://{}", args.rpc_endpoint));

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed running {}", wrk_bin.display()))?;

    let stdout = child
        .stdout
        .take()
        .context("failed to capture wrk stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("failed to capture wrk stderr")?;

    let stdout_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stdout);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        Ok::<String, std::io::Error>(String::from_utf8_lossy(&buf).into_owned())
    });

    let stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        Ok::<String, std::io::Error>(String::from_utf8_lossy(&buf).into_owned())
    });

    let wait_result = timeout(args.wrk_timeout, child.wait()).await;

    let status = match wait_result {
        Ok(status_res) => {
            status_res.with_context(|| format!("failed waiting for {}", wrk_bin.display()))?
        }
        Err(_) => {
            let timeout_str = format!("{}", format_duration(args.wrk_timeout));
            warn!(
                iteration,
                timeout = %timeout_str,
                "wrk exceeded timeout; terminating"
            );
            if let Err(err) = child.start_kill() {
                warn!(%err, "failed to signal wrk for shutdown; retrying with kill()");
                child
                    .kill()
                    .await
                    .context("failed to kill timed out wrk process")?;
            }
            child
                .wait()
                .await
                .context("failed to await wrk termination after timeout")?;
            stdout_task.abort();
            stderr_task.abort();
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            return Err(anyhow!(
                "wrk iteration {} exceeded timeout {}",
                iteration,
                timeout_str
            ));
        }
    };

    let stdout = stdout_task
        .await
        .context("failed to join wrk stdout task")??;
    let stderr = stderr_task
        .await
        .context("failed to join wrk stderr task")??;

    if !status.success() {
        return Err(anyhow!(
            "wrk iteration {} exited with status {}\nstdout:\n{}\nstderr:\n{}",
            iteration,
            status,
            stdout.trim(),
            stderr.trim()
        ));
    }

    let duration_ms = start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    let metrics = parse_wrk_output(&stdout);

    Ok(WrkReport {
        iteration,
        duration_ms,
        stdout,
        stderr,
        metrics,
    })
}

fn log_iteration_metrics(report: &WrkReport) {
    if let Some(metrics) = &report.metrics {
        let avg_latency = metrics
            .latency
            .as_ref()
            .map(|lat| format!("{}", format_duration(Duration::from_nanos(lat.avg_ns))))
            .unwrap_or_else(|| "<n/a>".to_string());
        let p99_latency = metrics
            .find_percentile(99.0)
            .map(|p| format!("{}", format_duration(Duration::from_nanos(p.latency_ns))))
            .unwrap_or_else(|| "<n/a>".to_string());

        info!(
            iteration = report.iteration,
            duration_ms = report.duration_ms,
            requests_per_sec = metrics.requests_per_sec.unwrap_or_default(),
            avg_latency = %avg_latency,
            p99_latency = %p99_latency,
            "wrk iteration complete"
        );
    } else {
        warn!(
            iteration = report.iteration,
            "wrk output parsing yielded no metrics"
        );
    }
}

fn log_aggregate_metrics(reports: &[WrkReport]) {
    if reports.is_empty() {
        return;
    }

    let mut rps_total = 0.0;
    let mut rps_count = 0;
    let mut p99_values = Vec::new();

    for report in reports {
        if let Some(metrics) = &report.metrics {
            if let Some(rps) = metrics.requests_per_sec {
                rps_total += rps;
                rps_count += 1;
            }
            if let Some(p99) = metrics.find_percentile(99.0) {
                p99_values.push(p99.latency_ns);
            }
        }
    }

    if rps_count > 0 {
        info!(
            iterations = rps_count,
            avg_requests_per_sec = rps_total / rps_count as f64,
            "wrk average throughput"
        );
    }

    if !p99_values.is_empty() {
        p99_values.sort_unstable();
        let median = p99_values[p99_values.len() / 2];
        info!(
            iterations = p99_values.len(),
            median_p99_latency = %format_duration(Duration::from_nanos(median)),
            "wrk median p99 latency"
        );
    }
}

fn write_wrk_reports(path: &Path, reports: &[WrkReport]) -> Result<()> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            fs::create_dir_all(dir).with_context(|| {
                format!("failed to create wrk output directory {}", dir.display())
            })?;
        }
    }

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("failed to open wrk output path {}", path.display()))?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, reports)
        .with_context(|| format!("failed to write wrk output to {}", path.display()))?;
    Ok(())
}

fn parse_wrk_output(stdout: &str) -> Option<WrkMetrics> {
    let mut metrics = WrkMetrics::default();
    let mut in_distribution = false;

    for line in stdout.lines() {
        let trimmed = line.trim();

        if trimmed.is_empty() {
            if in_distribution {
                in_distribution = false;
            }
            continue;
        }

        if let Some(value) = trimmed.strip_prefix("Requests/sec:") {
            if let Some(token) = value.split_whitespace().next() {
                if let Ok(parsed) = token.parse::<f64>() {
                    metrics.requests_per_sec = Some(parsed);
                }
            }
            continue;
        }

        if let Some(value) = trimmed.strip_prefix("Transfer/sec:") {
            if let Some(rate) = parse_transfer_rate(value.trim()) {
                metrics.transfer_per_sec = Some(rate);
            }
            continue;
        }

        if trimmed.starts_with("Latency Distribution") {
            in_distribution = true;
            continue;
        }

        if in_distribution {
            if let Some(percentile) = parse_distribution_line(trimmed) {
                metrics.latency_distribution.push(percentile);
                continue;
            } else {
                in_distribution = false;
            }
        }

        if trimmed.starts_with("Latency") {
            if let Some(latency) = parse_latency_line(trimmed) {
                metrics.latency = Some(latency);
            }
        }
    }

    if metrics.is_empty() {
        None
    } else {
        Some(metrics)
    }
}

fn parse_transfer_rate(value: &str) -> Option<TransferRate> {
    let token = value.split_whitespace().next()?;
    let (amount, unit) = split_numeric_unit(token)?;
    Some(TransferRate {
        value: amount,
        unit: unit.to_string(),
    })
}

fn parse_distribution_line(line: &str) -> Option<WrkPercentile> {
    let mut parts = line.split_whitespace();
    let percentile_token = parts.next()?;
    if !percentile_token.ends_with('%') {
        return None;
    }
    let percentile = percentile_token.trim_end_matches('%').parse::<f64>().ok()?;
    let latency_token = parts.next()?;
    let latency_ns = parse_latency_token(latency_token)?;
    Some(WrkPercentile {
        percentile,
        latency_ns,
    })
}

fn parse_latency_line(line: &str) -> Option<WrkLatencyStats> {
    let mut parts = line.split_whitespace();
    if parts.next()? != "Latency" {
        return None;
    }

    let avg_ns = parse_latency_token(parts.next()?)?;
    let stdev_ns = parse_latency_token(parts.next()?)?;
    let max_ns = parse_latency_token(parts.next()?)?;
    let stdev_pct = parts
        .next()
        .and_then(|value| value.trim_end_matches('%').parse::<f64>().ok())?;

    Some(WrkLatencyStats {
        avg_ns,
        stdev_ns,
        max_ns,
        stdev_pct,
    })
}

fn parse_latency_token(token: &str) -> Option<u64> {
    let (value, unit_raw) = split_numeric_unit(token)?;
    let unit = unit_raw.trim();
    let unit_lower = unit.to_ascii_lowercase();
    let nanos = match unit_lower.as_str() {
        "s" | "sec" | "secs" => value * 1_000_000_000.0,
        "ms" => value * 1_000_000.0,
        "us" => value * 1_000.0,
        "ns" => value,
        _ => match unit {
            "µs" | "μs" => value * 1_000.0,
            _ => return None,
        },
    };
    Some(nanos.round() as u64)
}

fn split_numeric_unit(token: &str) -> Option<(f64, &str)> {
    let token = token.trim();
    if token.is_empty() {
        return None;
    }

    let mut split_idx = None;
    for (idx, ch) in token.char_indices() {
        if !(ch.is_ascii_digit() || ch == '.') {
            split_idx = Some(idx);
            break;
        }
    }

    let idx = split_idx?;
    let (value_part, unit_part) = token.split_at(idx);
    if unit_part.is_empty() {
        return None;
    }
    let value = value_part.parse::<f64>().ok()?;
    Some((value, unit_part))
}

fn log_dry_run(args: &BenchArgs) {
    if args.skip_server {
        info!("dry run: server launch skipped (--skip-server)");
        if !args.warmup.is_zero() {
            info!(
                warmup = %format_duration(args.warmup),
                "dry run: warmup suppressed due to --skip-server"
            );
        }
    } else {
        info!(
            bin = %args.server_bin.display(),
            config = args
                .server_config
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<none>".to_string()),
            warmup = %format_duration(args.warmup),
            shutdown_grace = %format_duration(args.shutdown_grace),
            "dry run: would spawn server"
        );

        if let Some(log_path) = &args.server_log {
            info!(path = %log_path.display(), "dry run: would capture server logs");
        }

        if !args.server_args.is_empty() {
            info!(args = ?args.server_args, "dry run: extra server args");
        }

        if !args.server_env.is_empty() {
            info!(env = ?args.server_env, "dry run: server env assignments");
        }
    }

    if let Some(wrk) = &args.wrk_bin {
        let script = args
            .wrk_script
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<none>".to_string());
        info!(
            bin = %wrk.display(),
            threads = args.wrk_threads,
            connections = args.wrk_connections,
            duration = %format_duration(args.wrk_duration),
            iterations = args.iterations,
            cooldown = %format_duration(args.wrk_cooldown),
            timeout = %format_duration(args.wrk_timeout),
            endpoint = %args.rpc_endpoint,
            script = %script,
            "dry run: would execute wrk"
        );
        if let Some(output_path) = &args.wrk_output_json {
            info!(
                path = %output_path.display(),
                "dry run: would persist wrk output"
            );
        }
    } else {
        info!("dry run: no wrk binary configured");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = BenchArgs::parse();

    if args.dry_run {
        log_dry_run(&args);
        return Ok(());
    }

    let mut server = if args.skip_server {
        info!("server launch skipped (--skip-server)");
        None
    } else {
        Some(ServerHandle::spawn(&args).await?)
    };

    if let Some(handle) = server.as_mut() {
        handle.wait_ready(args.warmup).await;
    } else if !args.warmup.is_zero() {
        info!(
            duration = %format_duration(args.warmup),
            "warmup skipped because server launch was omitted"
        );
    }

    let wrk_result = if let Some(wrk_bin) = args.wrk_bin.as_deref() {
        run_wrk_iterations(&args, wrk_bin).await
    } else {
        warn!("wrk binary not provided; skipping load generation");
        Ok(Vec::new())
    };

    let shutdown_result = if let Some(handle) = server {
        handle.shutdown().await
    } else {
        Ok(())
    };

    let wrk_reports = wrk_result?;
    shutdown_result?;

    if !wrk_reports.is_empty() {
        log_aggregate_metrics(&wrk_reports);
        if let Some(path) = &args.wrk_output_json {
            write_wrk_reports(path, &wrk_reports)?;
            info!(
                path = %path.display(),
                entries = wrk_reports.len(),
                "persisted wrk results"
            );
        }
    } else if args.wrk_output_json.is_some() {
        warn!("wrk output path configured but no runs were executed; nothing written");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_env_assignment_split() {
        let (key, value) = parse_env_assignment("FOO=bar").expect("valid assignment");
        assert_eq!(key, "FOO");
        assert_eq!(value, "bar");
    }

    #[test]
    fn parse_env_assignment_rejects_missing_value() {
        let err = parse_env_assignment("BROKEN").expect_err("missing '=' should fail");
        assert!(err.to_string().contains("missing '='"));
    }

    #[test]
    fn split_numeric_unit_handles_decimals() {
        let (value, unit) = split_numeric_unit("123.45ms").expect("valid pair");
        assert!((value - 123.45).abs() < f64::EPSILON);
        assert_eq!(unit, "ms");
    }

    #[test]
    fn parse_latency_token_supports_micro() {
        let micros = parse_latency_token("1.5ms").expect("parse token");
        assert_eq!(micros, 1_500_000);
        let micro_symbol = parse_latency_token("2µs").expect("micro symbol");
        assert_eq!(micro_symbol, 2_000);
    }

    #[test]
    fn parse_distribution_line_extracts_percentile() {
        let pct = parse_distribution_line("  99.00%   123.4ms").expect("parse line");
        assert_eq!(pct.percentile, 99.0);
        assert_eq!(pct.latency_ns, 123_400_000);
    }

    #[test]
    fn parse_wrk_output_collects_metrics() {
        let output = r#"
Running 1m test @ http://localhost
  1 threads and 10 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.50ms    1.00ms  10.00ms   75.00%
    Req/Sec     4.00k   500.00     5.50k    70.00%
  Latency Distribution
     50%    2.00ms
     75%    3.00ms
     99%    8.00ms

  240000 requests in 60.00s, 18.00MB read
Requests/sec:   4000.00
Transfer/sec:     300.00KB
"#;
        let metrics = parse_wrk_output(output).expect("metrics parsed");
        assert_eq!(metrics.requests_per_sec, Some(4000.0));
        let latency = metrics.latency.as_ref().expect("latency stats");
        assert_eq!(latency.avg_ns, 2_500_000);
        let p99 = metrics.find_percentile(99.0).expect("p99 present");
        assert_eq!(p99.latency_ns, 8_000_000);
        let transfer = metrics.transfer_per_sec.expect("transfer");
        assert_eq!(transfer.unit, "KB");
    }

    #[test]
    fn wrk_metrics_is_empty_checks() {
        let metrics = WrkMetrics::default();
        assert!(metrics.is_empty());
    }
}
