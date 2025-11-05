use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use faststreams::{decode_record_from_slice, encode_into_with, encode_record_with, EncodeOptions, Record};

fn gen_account(len: usize) -> Record {
    let mut data = vec![0u8; len];
    for (i, b) in data.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(31).wrapping_add(7);
    }
    faststreams::Record::Account(faststreams::AccountUpdate {
        slot: 1,
        is_startup: false,
        pubkey: [1u8; 32],
        lamports: 42,
        owner: [2u8; 32],
        executable: false,
        rent_epoch: 0,
        data,
    })
}

fn bench_encode_decode(c: &mut Criterion) {
    let sizes = [128usize, 1024, 4096, 16384];
    let mut group = c.benchmark_group("faststreams_encode_decode");
    for &size in &sizes {
        let rec = gen_account(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("encode_vec", size), &rec, |b, r| {
            b.iter(|| {
                let _ = encode_record_with(r, EncodeOptions::latency_uds()).unwrap();
            })
        });

        group.bench_with_input(BenchmarkId::new("encode_into", size), &rec, |b, r| {
            b.iter(|| {
                let mut buf = Vec::with_capacity(12 + size);
                encode_into_with(r, &mut buf, EncodeOptions::latency_uds()).unwrap();
            })
        });

        // Pre-encode for decode benches
        let frame = encode_record_with(&rec, EncodeOptions::latency_uds()).unwrap();
        group.bench_with_input(BenchmarkId::new("decode", size), &frame, |b, f| {
            b.iter(|| {
                let mut scratch = Vec::with_capacity(4096);
                let _ = decode_record_from_slice(f.as_slice(), &mut scratch).unwrap();
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode_decode);
criterion_main!(benches);


