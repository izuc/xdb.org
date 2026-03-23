#![no_main]
use libfuzzer_sys::fuzz_target;
use std::io::Write;

fuzz_target!(|data: &[u8]| {
    // Feed arbitrary bytes to the SST reader and ensure it doesn't panic.
    if data.len() < 48 {
        return; // Too small for even a footer.
    }

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("fuzz.sst");

    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(data).unwrap();
    }

    let f = std::fs::File::open(&path).unwrap();
    let file_size = data.len() as u64;
    let opts = xdb::Options::default();

    // Try to open -- should return error on malformed data, never panic.
    match xdb::sst::TableReader::open(f, file_size, opts) {
        Ok(reader) => {
            // If it somehow opened, try iterating and a point lookup.
            let _ = reader.iter_entries();
            let _ = reader.get(b"test_key");
        }
        Err(_) => {}
    }
});
