//! Multi-processor crate — exercises the case where a single cdylib exports
//! more than one processor class, with each class declared in its own .ttl
//! source file (all pointing at the same `generated.ttl` via `owl:imports`).

use rdfc_processor_sdk::{futures::StreamExt, ProcessorArgs, Reader, Writer};
use serde::Deserialize;

// ── Echo ──────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct EchoConfig {
    reader: Reader,
    writer: Writer,
}

pub async fn echo(mut args: ProcessorArgs) -> Result<(), String> {
    let EchoConfig { reader, writer } = args.deserialize()?;
    let mut stream = reader.into_stream();
    while let Some(bytes) = stream.next().await {
        writer.write(&bytes).await?;
    }
    writer.close();
    Ok(())
}

// ── Send ──────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct SendConfig {
    writer: Writer,
    #[serde(default)]
    msgs: Vec<String>,
}

pub async fn send_proc(mut args: ProcessorArgs) -> Result<(), String> {
    let SendConfig { writer, msgs } = args.deserialize()?;
    for msg in &msgs {
        writer.write_str(msg).await?;
    }
    writer.close();
    Ok(())
}

// ── Log ───────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct LogConfig {
    reader: Reader,
    label: Option<String>,
}

pub async fn log_proc(mut args: ProcessorArgs) -> Result<(), String> {
    let LogConfig { reader, label } = args.deserialize()?;
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

// ── Registration ──────────────────────────────────────────────────────────────

rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#EchoProcessor", echo);
rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#SendProcessor", send_proc);
rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#LogProcessor", log_proc);

rdfc_processor_sdk::rdfc_export_processors!();

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::{make_writer, testing::*};
    use serde_json::json;

    #[tokio::test]
    async fn echo_forwards_all_messages() {
        let reader = make_test_reader("mock://reader", vec![b"hello".to_vec(), b"world".to_vec()]);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let args = make_test_args(
            json!({
                "reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"},
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"}
            }),
            [("reader", reader)],
            [("writer", writer)],
        );
        let (result, (received, closed)) = tokio::join!(super::echo(args), collect_writer(bridge));
        result.unwrap();
        assert_eq!(received, vec![b"hello".to_vec(), b"world".to_vec()]);
        assert!(closed);
    }

    #[tokio::test]
    async fn send_emits_configured_messages() {
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let args = make_test_args(
            json!({
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "msgs": ["alpha", "beta"]
            }),
            [],
            [("writer", writer)],
        );
        let (result, (received, closed)) =
            tokio::join!(super::send_proc(args), collect_writer(bridge));
        result.unwrap();
        assert_eq!(received, vec![b"alpha".to_vec(), b"beta".to_vec()]);
        assert!(closed);
    }

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
