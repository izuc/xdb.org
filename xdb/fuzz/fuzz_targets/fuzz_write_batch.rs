#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Feed arbitrary bytes as a WriteBatch and try to iterate.
    // The batch format is: [seq:8][count:4][records...]
    if data.len() < 12 {
        return;
    }

    // Construct a WriteBatch from the raw data.
    let batch = xdb::WriteBatch::from_data(data.to_vec());

    // Try to iterate -- should handle malformed data gracefully.
    struct NoopHandler;
    impl xdb::batch::WriteBatchHandler for NoopHandler {
        fn put(&mut self, _key: &[u8], _value: &[u8]) {}
        fn delete(&mut self, _key: &[u8]) {}
    }

    let mut handler = NoopHandler;
    let _ = batch.iterate(&mut handler);
});
