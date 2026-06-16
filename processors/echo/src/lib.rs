//! Echo processor — forwards every message from `reader` to `writer`.

use rdfc_processor_sdk::{futures::StreamExt, *};

#[derive(Deserialize)]
struct Config {
    reader: Reader,
    writer: Writer,
}

async fn echo(Config { reader, writer }: Config) -> Result<(), String> {
    let mut stream = reader.into_stream();
    while let Some(bytes) = stream.next().await {
        writer.write(&bytes).await?;
    }
    writer.close();
    Ok(())
}

rdfc_export!(echo);

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::testing::*;
    use rdfc_processor_sdk::RdfcArgs;

    const ARGS: &[u8] = br#"{
        "reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"},
        "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"}
    }"#;

    #[test]
    fn echo_forwards_all_messages() {
        let (_, reader_ch) = make_mock_reader("mock://reader", vec![b"hello".to_vec(), b"world".to_vec()]);
        let (writer_state, writer_ch) = make_mock_writer("mock://writer");
        let reader_nc = make_named("reader", reader_ch);
        let writer_nc = make_named("writer", writer_ch);
        let ffi_args = RdfcArgs {
            args_json: ARGS.as_ptr(), args_json_len: ARGS.len(),
            readers: &reader_nc, n_readers: 1,
            writers: &writer_nc, n_writers: 1,
        };
        assert_eq!(unsafe { super::rdfc_run(&ffi_args) }, 0);
        assert_eq!(*writer_state.received.lock().unwrap(), vec![b"hello".to_vec(), b"world".to_vec()]);
        assert!(*writer_state.closed.lock().unwrap());
    }

    #[test]
    fn echo_empty_stream_closes_writer() {
        let (_, reader_ch) = make_mock_reader("mock://reader", vec![]);
        let (writer_state, writer_ch) = make_mock_writer("mock://writer");
        let reader_nc = make_named("reader", reader_ch);
        let writer_nc = make_named("writer", writer_ch);
        let ffi_args = RdfcArgs {
            args_json: ARGS.as_ptr(), args_json_len: ARGS.len(),
            readers: &reader_nc, n_readers: 1,
            writers: &writer_nc, n_writers: 1,
        };
        assert_eq!(unsafe { super::rdfc_run(&ffi_args) }, 0);
        assert!(writer_state.received.lock().unwrap().is_empty());
        assert!(*writer_state.closed.lock().unwrap());
    }
}
