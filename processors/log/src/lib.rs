//! Log processor — prints every incoming message to stderr.

use rdfc_processor_sdk::{futures::StreamExt, Processor, Reader, Slot};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Args {
    reader: Reader,
    label: Option<String>,
}

pub struct Log {
    reader: Slot<Reader>,
    label: String,
}

impl Processor for Log {
    type Args = Args;

    fn new(args: Args) -> Self {
        Log {
            reader: Slot::new(args.reader),
            label: args.label.unwrap_or_else(|| "log".to_string()),
        }
    }

    async fn transform(&self) -> Result<(), String> {
        let mut stream = self.reader.take().into_stream();
        while let Some(bytes) = stream.next().await {
            match std::str::from_utf8(&bytes) {
                Ok(s) => eprintln!("[{}] {s}", self.label),
                Err(_) => eprintln!("[{}] <{} bytes>", self.label, bytes.len()),
            }
        }
        Ok(())
    }
}

rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#LogProcessor", Log);
rdfc_processor_sdk::rdfc_export_processors!();

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::testing::*;
    use rdfc_processor_sdk::Processor;
    use serde_json::json;

    #[tokio::test]
    async fn log_consumes_all_messages() {
        let reader = make_test_reader("mock://reader", vec![b"hello".to_vec(), b"world".to_vec()]);
        let proc: super::Log = make_test_processor(
            json!({
                "reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"},
                "label": "test"
            }),
            [("reader", reader)],
            [],
        );
        proc.transform().await.unwrap();
    }
}
