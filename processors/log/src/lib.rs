//! Log processor — prints every incoming message to stderr.

use rdfc_processor_sdk::{futures::StreamExt, ProcessorArgs, Reader};
use serde::Deserialize;

#[derive(Deserialize)]
struct Config {
    reader: Reader,
    label: Option<String>,
}

pub async fn log_proc(mut args: ProcessorArgs) -> Result<(), String> {
    let Config { reader, label } = args.deserialize()?;
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

rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#LogProcessor", log_proc);
rdfc_processor_sdk::rdfc_export_processors!();

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::testing::*;
    use serde_json::json;

    #[tokio::test]
    async fn log_consumes_all_messages() {
        let reader = make_test_reader("mock://reader", vec![b"hello".to_vec(), b"world".to_vec()]);
        let args = make_test_args(
            json!({
                "reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"},
                "label": "test"
            }),
            [("reader", reader)],
            [],
        );
        super::log_proc(args).await.unwrap();
    }
}
