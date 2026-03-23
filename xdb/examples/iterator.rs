//! Iterator usage: forward scan, reverse scan, seek.

use xdb::{Db, Options};

fn main() -> xdb::Result<()> {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::open(Options::default().create_if_missing(true), dir.path())?;

    // Insert sorted data.
    for i in 0..20u32 {
        let key = format!("key_{:03}", i);
        let val = format!("value_{}", i);
        db.put(key.as_bytes(), val.as_bytes())?;
    }
    db.flush()?;

    // Forward scan.
    println!("=== Forward scan ===");
    let mut iter = db.iter();
    iter.seek_to_first();
    while iter.valid() {
        println!(
            "  {} => {}",
            String::from_utf8_lossy(iter.key()),
            String::from_utf8_lossy(iter.value()),
        );
        iter.next();
    }

    // Reverse scan.
    println!("\n=== Reverse scan (last 5) ===");
    let mut iter = db.iter();
    iter.seek_to_last();
    for _ in 0..5 {
        if !iter.valid() {
            break;
        }
        println!(
            "  {} => {}",
            String::from_utf8_lossy(iter.key()),
            String::from_utf8_lossy(iter.value()),
        );
        iter.prev();
    }

    // Seek.
    println!("\n=== Seek to key_010 ===");
    let mut iter = db.iter();
    iter.seek(b"key_010");
    for _ in 0..5 {
        if !iter.valid() {
            break;
        }
        println!(
            "  {} => {}",
            String::from_utf8_lossy(iter.key()),
            String::from_utf8_lossy(iter.value()),
        );
        iter.next();
    }

    Ok(())
}
