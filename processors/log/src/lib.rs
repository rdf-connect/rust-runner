//! Log processor — prints every incoming message to stderr.

use rdfc_processor_sdk::*;

fn log_proc(ctx: ProcessorContext) -> Result<(), String> {
    let reader = ctx.reader("reader")?;
    let label = ctx
        .args
        .get("label")
        .and_then(|v| v.as_str())
        .unwrap_or("log")
        .to_string();

    for bytes in reader.iter_bytes() {
        match std::str::from_utf8(&bytes) {
            Ok(s) => eprintln!("[{label}] {s}"),
            Err(_) => eprintln!("[{label}] <{} bytes>", bytes.len()),
        }
    }
    Ok(())
}

rdfc_export!(log_proc);

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::testing::*;
    use rdfc_processor_sdk::{ProcessorContext, RdfcArgs};

    #[test]
    fn log_consumes_all_messages() {
        let (_, reader_ch) = make_mock_reader(vec![
            b"hello".to_vec(),
            b"world".to_vec(),
        ]);

        let reader_nc = make_named("reader", reader_ch);

        let args_json = br#"{"label":"test"}"#;
        let empty: Vec<rdfc_processor_sdk::RdfcNamedChannel> = vec![];
        let ffi_args = RdfcArgs {
            args_json: args_json.as_ptr(),
            args_json_len: args_json.len(),
            readers: &reader_nc,
            n_readers: 1,
            writers: empty.as_ptr(),
            n_writers: 0,
        };

        let ctx = unsafe { ProcessorContext::from_ffi(&ffi_args) };
        // should complete without error
        super::log_proc(ctx).unwrap();
    }
}
