//! Echo processor — forwards every message from `reader` to `writer`.

use rdfc_processor_sdk::*;

fn echo(ctx: ProcessorContext) -> Result<(), String> {
    let reader = ctx.reader("reader")?;
    let writer = ctx.writer("writer")?;
    for bytes in reader.iter_bytes() {
        writer.write_bytes(&bytes)?;
    }
    writer.close();
    Ok(())
}

rdfc_export!(echo);

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::testing::*;
    use rdfc_processor_sdk::{ProcessorContext, RdfcArgs};

    #[test]
    fn echo_forwards_all_messages() {
        let (_, reader_ch) = make_mock_reader(vec![
            b"hello".to_vec(),
            b"world".to_vec(),
        ]);
        let (writer_state, writer_ch) = make_mock_writer();

        let reader_nc = make_named("reader", reader_ch);
        let writer_nc = make_named("writer", writer_ch);

        let args_json = b"{}";
        let ffi_args = RdfcArgs {
            args_json: args_json.as_ptr(),
            args_json_len: args_json.len(),
            readers: &reader_nc,
            n_readers: 1,
            writers: &writer_nc,
            n_writers: 1,
        };

        let ctx = unsafe { ProcessorContext::from_ffi(&ffi_args) };
        super::echo(ctx).unwrap();

        let received = writer_state.received.lock().unwrap();
        assert_eq!(received.len(), 2);
        assert_eq!(received[0], b"hello");
        assert_eq!(received[1], b"world");
        assert!(*writer_state.closed.lock().unwrap());
    }

    #[test]
    fn echo_empty_stream_closes_writer() {
        let (_, reader_ch) = make_mock_reader(vec![]);
        let (writer_state, writer_ch) = make_mock_writer();

        let reader_nc = make_named("reader", reader_ch);
        let writer_nc = make_named("writer", writer_ch);

        let args_json = b"{}";
        let ffi_args = RdfcArgs {
            args_json: args_json.as_ptr(),
            args_json_len: args_json.len(),
            readers: &reader_nc,
            n_readers: 1,
            writers: &writer_nc,
            n_writers: 1,
        };

        let ctx = unsafe { ProcessorContext::from_ffi(&ffi_args) };
        super::echo(ctx).unwrap();

        assert!(writer_state.received.lock().unwrap().is_empty());
        assert!(*writer_state.closed.lock().unwrap());
    }
}
