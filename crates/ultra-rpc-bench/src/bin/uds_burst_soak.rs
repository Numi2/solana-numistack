use clap::Parser;
use faststreams::{
    encode_record_with, write_all_vectored_slices, AccountUpdate, EncodeOptions, Record,
};
use std::io::IoSlice;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "UDS burst/soak load generator for ultra aggregator"
)]
struct Args {
    /// UDS path to aggregator
    #[arg(long, default_value = "/var/run/ultra-geyser.sock")]
    uds_path: String,

    /// Steady RPS for soak mode
    #[arg(long, default_value_t = 20000)]
    rps: u64,

    /// Soak duration (seconds)
    #[arg(long, default_value_t = 30)]
    duration_secs: u64,

    /// Burst multiplier (10 => 10x for 1s)
    #[arg(long, default_value_t = 10.0)]
    burst_multiplier: f64,

    /// Interval between 1s bursts (seconds)
    #[arg(long, default_value_t = 30)]
    burst_interval_secs: u64,

    /// Target batch payload size in bytes per syscall
    #[arg(long, default_value_t = 64 * 1024)]
    batch_bytes_max: usize,

    /// Account data size to encode (bytes)
    #[arg(long, default_value_t = 512)]
    account_data_bytes: usize,
}

fn gen_record(size: usize, slot: u64) -> Record {
    let mut data = vec![0u8; size];
    for (i, b) in data.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(31).wrapping_add(7);
    }
    Record::Account(AccountUpdate {
        slot,
        is_startup: false,
        pubkey: [1u8; 32],
        lamports: 42,
        owner: [2u8; 32],
        executable: false,
        rent_epoch: 0,
        data,
    })
}

fn percentile(sorted: &mut [f64], p: f64) -> f64 {
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

fn main() {
    let args = Args::parse();
    let mut stream = UnixStream::connect(&args.uds_path)
        .unwrap_or_else(|e| panic!("connect {} failed: {e}", &args.uds_path));
    stream
        .set_write_timeout(Some(Duration::from_millis(200)))
        .ok();

    let opts = EncodeOptions::latency_uds();
    let mut slot = 1u64;
    let mut batch: Vec<Vec<u8>> = Vec::with_capacity(1024);
    let mut lat_ms: Vec<f64> = Vec::with_capacity(100_000);

    let start = Instant::now();
    let mut next_burst = start + Duration::from_secs(args.burst_interval_secs);
    let mut now;
    loop {
        now = Instant::now();
        if now.duration_since(start).as_secs() >= args.duration_secs {
            break;
        }

        let in_burst = now >= next_burst && now <= next_burst + Duration::from_secs(1);
        if now > next_burst + Duration::from_secs(1) {
            next_burst += Duration::from_secs(args.burst_interval_secs);
        }

        let rps = if in_burst {
            (args.rps as f64 * args.burst_multiplier) as u64
        } else {
            args.rps
        };
        let interval = if rps > 0 {
            Duration::from_nanos(1_000_000_000u64 / rps)
        } else {
            Duration::from_millis(1)
        };

        // Assemble a batch up to batch_bytes_max
        batch.clear();
        let t0 = Instant::now();
        let mut bytes = 0usize;
        while bytes < args.batch_bytes_max {
            let rec = gen_record(args.account_data_bytes, slot);
            let frame = encode_record_with(&rec, opts).expect("encode");
            bytes += frame.len();
            batch.push(frame);
            slot = slot.wrapping_add(1);
            if bytes >= args.batch_bytes_max {
                break;
            }
        }

        // Write the batch
        let mut ios_mut: Vec<IoSlice<'_>> = Vec::with_capacity(batch.len());
        for f in &batch {
            ios_mut.push(IoSlice::new(f.as_slice()));
        }
        let wstart = Instant::now();
        let _ = write_all_vectored_slices(&mut stream, ios_mut.as_mut_slice());
        let took = wstart.elapsed().as_secs_f64() * 1_000.0;
        lat_ms.push(took);

        // pace
        let elapsed = t0.elapsed();
        if elapsed < interval {
            std::thread::sleep(interval - elapsed);
        }
    }

    if !lat_ms.is_empty() {
        let mut sorted = lat_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = percentile(&mut sorted, 50.0);
        let p99 = percentile(&mut sorted, 99.0);
        let p999 = percentile(&mut sorted, 99.9);
        println!(
            "batches={}, p50_ms={:.3}, p99_ms={:.3}, p99.9_ms={:.3}",
            sorted.len(),
            p50,
            p99,
            p999
        );
    }
}
