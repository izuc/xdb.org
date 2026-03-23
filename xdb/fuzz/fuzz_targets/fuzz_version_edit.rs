#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Feed arbitrary bytes to VersionEdit::decode and ensure it doesn't panic.
    let _ = xdb::version::edit::VersionEdit::decode(data);
});
