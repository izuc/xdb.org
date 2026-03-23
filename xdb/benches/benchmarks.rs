//! Performance benchmarks for xdb.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use tempfile::TempDir;
use xdb::{Db, Options, WriteBatch, WriteOptions};

fn open_bench_db(dir: &TempDir) -> std::sync::Arc<Db> {
    let opts = Options::default()
        .create_if_missing(true)
        .write_buffer_size(64 * 1024 * 1024) // 64 MB - large to avoid flushes during bench
        .bloom_bits_per_key(10);
    Db::open(opts, dir.path()).unwrap()
}

fn bench_sequential_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_write");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let dir = TempDir::new().unwrap();
                let db = open_bench_db(&dir);
                for i in 0..size {
                    let key = format!("key{:08}", i);
                    let val = format!("value{:08}", i);
                    db.put(key.as_bytes(), val.as_bytes()).unwrap();
                }
            });
        });
    }
    group.finish();
}

fn bench_random_read(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let db = open_bench_db(&dir);

    // Pre-populate
    let n = 10000;
    for i in 0..n {
        let key = format!("key{:08}", i);
        let val = format!("value{:08}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    c.bench_function("random_read_10k", |b| {
        let mut i = 0u32;
        b.iter(|| {
            let key = format!("key{:08}", i % n);
            let _ = db.get(key.as_bytes()).unwrap();
            i = i.wrapping_add(7919); // pseudo-random step (prime)
        });
    });
}

fn bench_batch_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_write");

    for batch_size in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                let dir = TempDir::new().unwrap();
                let db = open_bench_db(&dir);
                let mut counter = 0u64;
                b.iter(|| {
                    let mut batch = WriteBatch::new();
                    for _ in 0..batch_size {
                        let key = format!("key{:08}", counter);
                        let val = format!("value{:08}", counter);
                        batch.put(key.as_bytes(), val.as_bytes());
                        counter += 1;
                    }
                    db.write(WriteOptions::default(), batch).unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_iterator_scan(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let db = open_bench_db(&dir);

    let n = 10000;
    for i in 0..n {
        let key = format!("key{:08}", i);
        let val = format!("value{:08}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    c.bench_function("iterator_scan_10k", |b| {
        b.iter(|| {
            let mut iter = db.iter();
            iter.seek_to_first();
            let mut count = 0u64;
            while iter.valid() {
                criterion::black_box(iter.key());
                criterion::black_box(iter.value());
                count += 1;
                iter.next();
            }
            assert_eq!(count, n);
        });
    });
}

fn bench_mixed_workload(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let db = open_bench_db(&dir);

    // Pre-populate
    for i in 0..5000u32 {
        db.put(format!("key{:08}", i).as_bytes(), b"initial").unwrap();
    }

    c.bench_function("mixed_read_write", |b| {
        let mut counter = 5000u32;
        let mut read_key = 0u32;
        b.iter(|| {
            // 1 write per 4 reads
            db.put(
                format!("key{:08}", counter).as_bytes(),
                b"new_value",
            ).unwrap();
            counter += 1;

            for _ in 0..4 {
                let key = format!("key{:08}", read_key % counter);
                let _ = db.get(key.as_bytes());
                read_key = read_key.wrapping_add(997);
            }
        });
    });
}

criterion_group!(
    benches,
    bench_sequential_write,
    bench_random_read,
    bench_batch_write,
    bench_iterator_scan,
    bench_mixed_workload,
);
criterion_main!(benches);
