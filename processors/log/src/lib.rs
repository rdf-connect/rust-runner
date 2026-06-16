//! Log processor — prints every incoming message to stderr.

use rdfc_processor_sdk::{futures::StreamExt, *};

#[derive(Deserialize)]
struct Config {
    reader: Reader,
    label: Option<String>,
}

async fn log_proc(Config { reader, label }: Config) -> Result<(), String> {
    let label = label.unwrap_or_else(|| "log".to_string());
    let mut stream = reader.into_stream();
    while let Some(bytes) = stream.next().await {
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
    use rdfc_processor_sdk::RdfcArgs;

    #[test]
    fn log_consumes_all_messages() {
        let (_, reader_ch) = make_mock_reader("mock://reader", vec![
            b"hello".to_vec(),
            b"world".to_vec(),
        ]);
        let reader_nc = make_named("reader", reader_ch);

        let args_json = br#"{
            "reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"},
            "label": "test"
        }"#;

        let empty: Vec<rdfc_processor_sdk::RdfcNamedChannel> = vec![];
        let ffi_args = RdfcArgs {
            args_json: args_json.as_ptr(),
            args_json_len: args_json.len(),
            readers: &reader_nc,
            n_readers: 1,
            writers: empty.as_ptr(),
            n_writers: 0,
        };

        assert_eq!(unsafe { super::rdfc_run(&ffi_args) }, 0);
    }
}

