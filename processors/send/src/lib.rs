//! Send processor — writes a predefined list of messages from args then closes.

use rdfc_processor_sdk::{Processor, Slot, Writer};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Args {
    writer: Writer,
    #[serde(default)]
    msgs: Vec<String>,
}

pub struct Send {
    writer: Slot<Writer>,
    msgs: Vec<String>,
}

impl Processor for Send {
    type Args = Args;

    fn new(args: Args) -> Self {
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

rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#SendProcessor", Send);
rdfc_processor_sdk::rdfc_export_processors!();

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::testing::*;
    use rdfc_processor_sdk::{make_writer, Processor};
    use serde_json::json;

    async fn run_send(msgs: serde_json::Value) -> (Vec<Vec<u8>>, bool) {
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let proc: super::Send = make_test_processor(
            json!({
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "msgs": msgs
            }),
            [],
            [("writer", writer)],
        );
        let (result, collected) = tokio::join!(proc.produce(), collect_writer(bridge));
        result.unwrap();
        collected
    }

    #[tokio::test]
    async fn send_emits_array_messages() {
        let (received, closed) = run_send(json!(["alpha", "beta", "gamma"])).await;
        assert_eq!(
            received,
            vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]
        );
        assert!(closed);
    }

    #[tokio::test]
    async fn send_no_messages_closes_writer() {
        let (received, closed) = run_send(json!([])).await;
        assert!(received.is_empty());
        assert!(closed);
    }
}
