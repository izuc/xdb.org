#![no_main]
use libfuzzer_sys::fuzz_target;
use std::io::Write;

fuzz_target!(|data: &[u8]| {
    // Feed arbitrary bytes to the WAL reader and ensure it doesn't panic.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("fuzz.log");

    // Write the fuzz data as a raw file.
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(data).unwrap();
    }

    // Try to read it as a WAL -- should return errors, never panic.
    let f = std::fs::File::open(&path).unwrap();
    let mut reader = xdb::wal::WalReader::new(f);
    loop {
        match reader.read_record() {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => break,
        }
    }
});
