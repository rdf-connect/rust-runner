//! Echo processor — forwards every message from `reader` to `writer`.

use rdfc_processor_sdk::{futures::StreamExt, ProcessorArgs, Reader, Writer};
use serde::Deserialize;

#[derive(Deserialize)]
struct Config {
    reader: Reader,
    writer: Writer,
}

pub async fn echo(mut args: ProcessorArgs) -> Result<(), String> {
    let Config { reader, writer } = args.deserialize()?;
    let mut stream = reader.into_stream();
    while let Some(bytes) = stream.next().await {
        writer.write(&bytes).await?;
    }
    writer.close();
    Ok(())
}

rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#EchoProcessor", echo);

rdfc_processor_sdk::rdfc_export_processors!();

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::make_writer;
    use rdfc_processor_sdk::testing::*;
    use serde_json::json;

    fn args_json() -> serde_json::Value {
        json!({
            "reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"},
            "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"}
        })
    }

    #[tokio::test]
    async fn echo_forwards_all_messages() {
        let reader = make_test_reader("mock://reader", vec![b"hello".to_vec(), b"world".to_vec()]);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let args = make_test_args(args_json(), [("reader", reader)], [("writer", writer)]);

        let (result, (received, closed)) = tokio::join!(super::echo(args), collect_writer(bridge));

        result.unwrap();
        assert_eq!(received, vec![b"hello".to_vec(), b"world".to_vec()]);
        assert!(closed);
    }

    #[tokio::test]
    async fn echo_empty_stream_closes_writer() {
        let reader = make_test_reader("mock://reader", vec![]);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let args = make_test_args(args_json(), [("reader", reader)], [("writer", writer)]);

        let (result, (received, closed)) = tokio::join!(super::echo(args), collect_writer(bridge));

        result.unwrap();
        assert!(received.is_empty());
        assert!(closed);
    }
}
