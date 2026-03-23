//! WriteBatch and Snapshot usage.

use xdb::{Db, Options, WriteBatch, WriteOptions};

fn main() -> xdb::Result<()> {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::open(Options::default().create_if_missing(true), dir.path())?;

    // Batch write: atomic insert of multiple keys.
    let mut batch = WriteBatch::new();
    batch.put(b"user:1:name", b"Alice");
    batch.put(b"user:1:email", b"alice@example.com");
    batch.put(b"user:2:name", b"Bob");
    batch.put(b"user:2:email", b"bob@example.com");
    db.write(WriteOptions::default(), batch)?;

    println!("After batch write:");
    let mut iter = db.iter();
    iter.seek_to_first();
    while iter.valid() {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(iter.key()),
            String::from_utf8_lossy(iter.value()),
        );
        iter.next();
    }

    // Take a snapshot.
    let snap = db.snapshot();

    // Modify data after the snapshot.
    db.put(b"user:1:name", b"Alice Updated")?;
    db.delete(b"user:2:name")?;
    db.put(b"user:3:name", b"Charlie")?;

    // Current state.
    println!("\nCurrent state:");
    let mut iter = db.iter();
    iter.seek_to_first();
    while iter.valid() {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(iter.key()),
            String::from_utf8_lossy(iter.value()),
        );
        iter.next();
    }

    // Snapshot state (should show old values).
    println!("\nSnapshot state (before modifications):");
    let mut iter = db.iter_with_snapshot(&snap);
    iter.seek_to_first();
    while iter.valid() {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(iter.key()),
            String::from_utf8_lossy(iter.value()),
        );
        iter.next();
    }

    db.release_snapshot(&snap);

    Ok(())
}
