//! Multi-processor crate — exercises the case where a single cdylib exports
//! more than one processor class, with each class declared in its own .ttl
//! source file (all pointing at the same `generated.ttl` via `owl:imports`).

use rdfc_processor_sdk::{futures::StreamExt, Processor, Reader, Slot, Writer};
use serde::Deserialize;

// ── Echo ──────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct EchoArgs {
    reader: Reader,
    writer: Writer,
}

pub struct Echo {
    reader: Slot<Reader>,
    writer: Slot<Writer>,
}

impl Processor for Echo {
    type Args = EchoArgs;

    fn new(args: EchoArgs) -> Self {
        Echo {
            reader: Slot::new(args.reader),
            writer: Slot::new(args.writer),
        }
    }

    async fn transform(&self) -> Result<(), String> {
        let writer = self.writer.take();
        let mut stream = self.reader.take().into_stream();
        while let Some(bytes) = stream.next().await {
            writer.write(&bytes).await?;
        }
        writer.close();
        Ok(())
    }
}

// ── Send ──────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct SendArgs {
    writer: Writer,
    #[serde(default)]
    msgs: Vec<String>,
}

pub struct Send {
    writer: Slot<Writer>,
    msgs: Vec<String>,
}

impl Processor for Send {
    type Args = SendArgs;

    fn new(args: SendArgs) -> Self {
        Send {
            writer: Slot::new(args.writer),
            msgs: args.msgs,
        }
    }

    async fn produce(&self) -> Result<(), String> {
        let writer = self.writer.take();
        for msg in &self.msgs {
            writer.write_str(msg).await?;
        }
        writer.close();
        Ok(())
    }
}

// ── Log ───────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct LogArgs {
    reader: Reader,
    label: Option<String>,
}

pub struct Log {
    reader: Slot<Reader>,
    label: String,
}

impl Processor for Log {
    type Args = LogArgs;

    fn new(args: LogArgs) -> Self {
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

// ── Registration ──────────────────────────────────────────────────────────────

rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#EchoProcessor", Echo);
rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#SendProcessor", Send);
rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#LogProcessor", Log);

rdfc_processor_sdk::rdfc_export_processors!();

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::{make_writer, testing::*, Processor};
    use serde_json::json;

    #[tokio::test]
    async fn echo_forwards_all_messages() {
        let reader = make_test_reader("mock://reader", vec![b"hello".to_vec(), b"world".to_vec()]);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let proc: super::Echo = make_test_processor(
            json!({
                "reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"},
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"}
            }),
            [("reader", reader)],
            [("writer", writer)],
        );
        let (result, (received, closed)) = tokio::join!(proc.transform(), collect_writer(bridge));
        result.unwrap();
        assert_eq!(received, vec![b"hello".to_vec(), b"world".to_vec()]);
        assert!(closed);
    }

    #[tokio::test]
    async fn send_emits_configured_messages() {
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let proc: super::Send = make_test_processor(
            json!({
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "msgs": ["alpha", "beta"]
            }),
            [],
            [("writer", writer)],
        );
        let (result, (received, closed)) = tokio::join!(proc.produce(), collect_writer(bridge));
        result.unwrap();
        assert_eq!(received, vec![b"alpha".to_vec(), b"beta".to_vec()]);
        assert!(closed);
    }

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
