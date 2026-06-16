//! Send processor — writes a predefined list of messages from args then closes.

use rdfc_processor_sdk::*;

#[derive(Deserialize)]
struct Config {
    writer: Writer,
    #[serde(default)]
    msgs: Vec<String>,
}

async fn send_proc(Config { writer, msgs }: Config) -> Result<(), String> {
    for msg in &msgs {
        writer.write_str(msg).await?;
    }
    writer.close();
    Ok(())
}

rdfc_export!(send_proc);

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::testing::*;
    use rdfc_processor_sdk::RdfcArgs;

    fn run_send(extra_json: &str) -> (Vec<Vec<u8>>, bool) {
        let (writer_state, writer_ch) = make_mock_writer("mock://writer");
        let writer_nc = make_named("writer", writer_ch);

        let args_json = format!(
            r#"{{"writer":{{"@type":"https://w3id.org/rdf-connect#Writer","@id":"mock://writer"}},{}}}"#,
            extra_json
        );
        let args_bytes = args_json.as_bytes().to_vec();

        let ffi_args = RdfcArgs {
            args_json: args_bytes.as_ptr(),
            args_json_len: args_bytes.len(),
            readers: std::ptr::NonNull::dangling().as_ptr(),
            n_readers: 0,
            writers: &writer_nc,
            n_writers: 1,
        };

        assert_eq!(unsafe { super::rdfc_run(&ffi_args) }, 0);

        let received = writer_state.received.lock().unwrap().clone();
        let closed = *writer_state.closed.lock().unwrap();
        (received, closed)
    }

    #[test]
    fn send_emits_array_messages() {
        let (received, closed) = run_send(r#""msgs":["alpha","beta","gamma"]"#);
        assert_eq!(received, vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]);
        assert!(closed);
    }

    #[test]
    fn send_no_messages_closes_writer() {
        let (received, closed) = run_send(r#""msgs":[]"#);
        assert!(received.is_empty());
        assert!(closed);
    }
}

