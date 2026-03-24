//! Head-to-head benchmarks: xdb vs RocksDB.
//!
//! Runs identical workloads on both engines so the numbers are directly
//! comparable. All benchmarks use the same key/value sizes, write buffer,
//! and bloom filter settings.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const WRITE_BUFFER: usize = 64 * 1024 * 1024; // 64 MiB

fn make_key(i: u64) -> String {
    format!("key{:08}", i)
}

fn make_val(i: u64) -> String {
    format!("value{:08}", i)
}

// ---------------------------------------------------------------------------
// xdb helpers
// ---------------------------------------------------------------------------

fn open_xdb(dir: &TempDir) -> std::sync::Arc<xdb::Db> {
    let opts = xdb::Options::default()
        .create_if_missing(true)
        .write_buffer_size(WRITE_BUFFER)
        .bloom_bits_per_key(10);
    xdb::Db::open(opts, dir.path()).unwrap()
}

// ---------------------------------------------------------------------------
// RocksDB helpers
// ---------------------------------------------------------------------------

fn open_rocks(dir: &TempDir) -> rocksdb::DB {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(WRITE_BUFFER);
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, false);
    opts.set_block_based_table_factory(&block_opts);
    rocksdb::DB::open(&opts, dir.path()).unwrap()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_sequential_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("seq_write");

    for &n in &[1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("xdb", n), &n, |b, &n| {
            b.iter(|| {
                let dir = TempDir::new().unwrap();
                let db = open_xdb(&dir);
                for i in 0..n {
                    db.put(make_key(i).as_bytes(), make_val(i).as_bytes()).unwrap();
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("rocksdb", n), &n, |b, &n| {
            b.iter(|| {
                let dir = TempDir::new().unwrap();
                let db = open_rocks(&dir);
                for i in 0..n {
                    db.put(make_key(i).as_bytes(), make_val(i).as_bytes()).unwrap();
                }
            });
        });
    }
    group.finish();
}

fn bench_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_read");
    let n: u64 = 50_000;

    // --- xdb ---
    let xdb_dir = TempDir::new().unwrap();
    let xdb_db = open_xdb(&xdb_dir);
    for i in 0..n {
        xdb_db.put(make_key(i).as_bytes(), make_val(i).as_bytes()).unwrap();
    }
    xdb_db.flush().unwrap();

    group.throughput(Throughput::Elements(1));
    group.bench_function("xdb", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = make_key(i % n);
            criterion::black_box(xdb_db.get(key.as_bytes()).unwrap());
            i = i.wrapping_add(7919);
        });
    });

    // --- RocksDB ---
    let rocks_dir = TempDir::new().unwrap();
    let rocks_db = open_rocks(&rocks_dir);
    for i in 0..n {
        rocks_db.put(make_key(i).as_bytes(), make_val(i).as_bytes()).unwrap();
    }
    rocks_db.flush().unwrap();

    group.bench_function("rocksdb", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = make_key(i % n);
            criterion::black_box(rocks_db.get(key.as_bytes()).unwrap());
            i = i.wrapping_add(7919);
        });
    });
    group.finish();
}

fn bench_batch_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_write");

    for &batch_size in &[100u64, 1_000] {
        group.throughput(Throughput::Elements(batch_size));

        group.bench_with_input(BenchmarkId::new("xdb", batch_size), &batch_size, |b, &bs| {
            let dir = TempDir::new().unwrap();
            let db = open_xdb(&dir);
            let mut ctr = 0u64;
            b.iter(|| {
                let mut batch = xdb::WriteBatch::new();
                for _ in 0..bs {
                    batch.put(make_key(ctr).as_bytes(), make_val(ctr).as_bytes());
                    ctr += 1;
                }
                db.write(xdb::WriteOptions::default(), batch).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("rocksdb", batch_size), &batch_size, |b, &bs| {
            let dir = TempDir::new().unwrap();
            let db = open_rocks(&dir);
            let mut ctr = 0u64;
            b.iter(|| {
                let mut batch = rocksdb::WriteBatch::default();
                for _ in 0..bs {
                    batch.put(make_key(ctr).as_bytes(), make_val(ctr).as_bytes());
                    ctr += 1;
                }
                db.write(batch).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_iterator_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterator_scan");
    let n: u64 = 50_000;

    // --- xdb ---
    let xdb_dir = TempDir::new().unwrap();
    let xdb_db = open_xdb(&xdb_dir);
    for i in 0..n {
        xdb_db.put(make_key(i).as_bytes(), make_val(i).as_bytes()).unwrap();
    }
    xdb_db.flush().unwrap();

    group.throughput(Throughput::Elements(n));

    group.bench_function("xdb", |b| {
        b.iter(|| {
            let mut iter = xdb_db.iter();
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

    // --- RocksDB ---
    let rocks_dir = TempDir::new().unwrap();
    let rocks_db = open_rocks(&rocks_dir);
    for i in 0..n {
        rocks_db.put(make_key(i).as_bytes(), make_val(i).as_bytes()).unwrap();
    }
    rocks_db.flush().unwrap();

    group.bench_function("rocksdb", |b| {
        b.iter(|| {
            let iter = rocks_db.iterator(rocksdb::IteratorMode::Start);
            let mut count = 0u64;
            for item in iter {
                let (k, v) = item.unwrap();
                criterion::black_box(&k);
                criterion::black_box(&v);
                count += 1;
            }
            assert_eq!(count, n);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_write,
    bench_random_read,
    bench_batch_write,
    bench_iterator_scan,
);
criterion_main!(benches);
