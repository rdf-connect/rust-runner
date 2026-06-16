//! Send processor — writes a predefined list of messages from args then closes.

use rdfc_processor_sdk::*;

fn send_proc(ctx: ProcessorContext) -> Result<(), String> {
    let writer = ctx.writer("writer")?;

    let messages: Vec<String> = match ctx.args.get("msgs") {
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        Some(serde_json::Value::String(s)) => vec![s.clone()],
        _ => vec![],
    };

    for msg in &messages {
        writer.write_string(msg)?;
    }
    writer.close();
    Ok(())
}

rdfc_export!(send_proc);

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::testing::*;
    use rdfc_processor_sdk::{ProcessorContext, RdfcArgs};

    fn run_send(msgs_json: &[u8]) -> (Vec<Vec<u8>>, bool) {
        let (writer_state, writer_ch) = make_mock_writer();
        let writer_nc = make_named("writer", writer_ch);

        let ffi_args = RdfcArgs {
            args_json: msgs_json.as_ptr(),
            args_json_len: msgs_json.len(),
            readers: std::ptr::NonNull::dangling().as_ptr(),
            n_readers: 0,
            writers: &writer_nc,
            n_writers: 1,
        };

        let ctx = unsafe { ProcessorContext::from_ffi(&ffi_args) };
        super::send_proc(ctx).unwrap();

        let received = writer_state.received.lock().unwrap().clone();
        let closed = *writer_state.closed.lock().unwrap();
        (received, closed)
    }

    #[test]
    fn send_emits_array_messages() {
        let (received, closed) = run_send(br#"{"msgs":["alpha","beta","gamma"]}"#);
        assert_eq!(received, vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]);
        assert!(closed);
    }

    #[test]
    fn send_emits_single_string() {
        let (received, closed) = run_send(br#"{"msgs":"hello"}"#);
        assert_eq!(received, vec![b"hello".to_vec()]);
        assert!(closed);
    }

    #[test]
    fn send_no_messages_closes_writer() {
        let (received, closed) = run_send(br#"{}"#);
        assert!(received.is_empty());
        assert!(closed);
    }
}
