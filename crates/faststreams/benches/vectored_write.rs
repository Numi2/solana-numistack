use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use faststreams::write_all_vectored_slices;
use std::io::{IoSlice, Write};

struct CountingWriter {
    bytes: usize,
}

impl CountingWriter {
    fn new() -> Self {
        Self { bytes: 0 }
    }
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        let mut total = 0usize;
        for b in bufs {
            total += b.len();
        }
        self.bytes += total;
        Ok(total)
    }
}

fn bench_vectored(c: &mut Criterion) {
    let counts = [16usize, 64, 128, 256, 512];
    let slice_len = 1024usize;
    let mut group = c.benchmark_group("faststreams_vectored_write");
    for &n in &counts {
        let total = n * slice_len;
        group.throughput(Throughput::Bytes(total as u64));
        group.bench_with_input(BenchmarkId::new("vectored", n), &n, |b, &m| {
            // Prepare input buffers
            let payload: Vec<u8> = vec![7u8; slice_len];
            let mut slices: Vec<IoSlice<'_>> = Vec::with_capacity(m);
            let mut owned: Vec<Vec<u8>> = Vec::with_capacity(m);
            for _ in 0..m {
                owned.push(payload.clone());
            }
            for o in &owned {
                slices.push(IoSlice::new(o.as_slice()));
            }
            b.iter(|| {
                let mut w = CountingWriter::new();
                let mut ios = slices.clone();
                let _ = write_all_vectored_slices(&mut w, ios.as_mut_slice()).unwrap();
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_vectored);
criterion_main!(benches);


