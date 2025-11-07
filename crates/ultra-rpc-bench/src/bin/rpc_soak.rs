use clap::Parser;
use std::time::Duration;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "HTTP→QUIC RPC soak generator against solana-quic-proxy")]
struct Args {
    /// Proxy HTTP endpoint (e.g., http://127.0.0.1:8898/rpc)
    #[arg(long, default_value = "http://127.0.0.1:8898/rpc")]
    endpoint: String,

    /// Steady RPS target
    #[arg(long, default_value_t = 20000u64)]
    rps: u64,

    /// Test duration seconds
    #[arg(long, default_value_t = 60u64)]
    duration_secs: u64,

    /// Max in-flight requests
    #[arg(long, default_value_t = 1024usize)]
    inflight_max: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let client = reqwest::Client::builder()
        .tcp_keepalive(Some(Duration::from_secs(15)))
        .pool_max_idle_per_host(1024)
        .build()?;

    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": []
    });

    let period = if args.rps > 0 {
        Duration::from_nanos(1_000_000_000u64 / args.rps)
    } else {
        Duration::from_millis(1)
    };
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let end = tokio::time::Instant::now() + Duration::from_secs(args.duration_secs);
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(args.inflight_max));
    let mut lat_ms: Vec<f64> = Vec::with_capacity((args.rps * args.duration_secs) as usize);

    while tokio::time::Instant::now() < end {
        ticker.tick().await;
        let permit = match sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => continue,
        };
        let client_ref = client.clone();
        let url = args.endpoint.clone();
        let body = payload.clone();
        let fut = async move {
            let start = tokio::time::Instant::now();
            let ok = match client_ref.post(&url).json(&body).send().await {
                Ok(rsp) => match rsp.error_for_status() {
                    Ok(r2) => r2.bytes().await.is_ok(),
                    Err(_) => false,
                },
                Err(_) => false,
            };
            let elapsed = start.elapsed().as_secs_f64() * 1_000.0;
            drop(permit);
            (ok, elapsed)
        };
        let res = fut.await;
        lat_ms.push(res.1);
    }

    if !lat_ms.is_empty() {
        lat_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p = |q: f64| percentile(&lat_ms, q);
        println!(
            "p50={:.2}ms p95={:.2}ms p99={:.2}ms p99.9={:.2}ms N={} rps={} inflight={}",
            p(50.0),
            p(95.0),
            p(99.0),
            p(99.9),
            lat_ms.len(),
            args.rps,
            args.inflight_max
        );
    }
    Ok(())
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len() as f64;
    let rank = (p / 100.0) * (n - 1.0);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        return sorted[lo];
    }
    let frac = rank - (lo as f64);
    sorted[lo] + (sorted[hi] - sorted[lo]) * frac
}


