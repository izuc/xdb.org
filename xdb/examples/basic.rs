//! Basic xdb usage: put, get, delete.

use xdb::{Db, Options};

fn main() -> xdb::Result<()> {
    // Create a temporary directory for the database.
    let dir = tempfile::TempDir::new().unwrap();

    // Open the database.
    let opts = Options::default()
        .create_if_missing(true)
        .write_buffer_size(4 * 1024 * 1024)
        .bloom_bits_per_key(10);
    let db = Db::open(opts, dir.path())?;

    // Insert some data.
    db.put(b"name", b"xdb")?;
    db.put(b"language", b"Rust")?;
    db.put(b"version", b"0.1.0")?;

    // Read it back.
    println!(
        "name = {:?}",
        db.get(b"name")?
            .map(|v| String::from_utf8_lossy(&v).to_string())
    );
    println!(
        "language = {:?}",
        db.get(b"language")?
            .map(|v| String::from_utf8_lossy(&v).to_string())
    );
    println!(
        "version = {:?}",
        db.get(b"version")?
            .map(|v| String::from_utf8_lossy(&v).to_string())
    );

    // Delete a key.
    db.delete(b"version")?;
    println!("version after delete = {:?}", db.get(b"version")?);

    // Flush to disk.
    db.flush()?;

    // Print statistics.
    println!("\n{}", db.stats());

    Ok(())
}
