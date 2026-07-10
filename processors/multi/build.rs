fn main() {
    if let Err(e) = rdfc_processor_buildtools::generate(&[
        "processor.ttl",
        "send_processor.ttl",
        "log_processor.ttl",
    ]) {
        panic!("generating processor.ttl's artifact manifest: {e:#}");
    }
}
