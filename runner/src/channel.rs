//! Async `Reader` and `Writer` channel types for built-in processors,
//! plus FFI-safe adapters that dynamic processors access through the SDK.

use std::collections::HashMap;
use std::sync::{atomic::AtomicU32, atomic::Ordering, Arc, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::proto::rdfc::{
    from_runner::Inner as FromRunnerInner, Close, FromRunner, GlobalAck, SendingMessage,
};

// ── Internal message type ─────────────────────────────────────────────────────

pub(crate) enum ReaderMsg {
    Data(Bytes, u32), // (payload, global_sequence_number)
    Close,
}

// ── Shared registries (owned by RunnerState, shared with channel instances) ──

/// channel_uri → list of senders used by the runner to push incoming messages.
/// A Vec supports fan-out: multiple processors may read from the same channel.
pub(crate) type ReaderRegistry =
    Arc<Mutex<HashMap<String, Vec<mpsc::UnboundedSender<ReaderMsg>>>>>;

/// (channel_uri, local_seq) → oneshot ack for each pending write.
pub(crate) type WriterAckRegistry =
    Arc<Mutex<HashMap<(String, u32), std::sync::mpsc::SyncSender<Option<String>>>>>;

// ── Reader ────────────────────────────────────────────────────────────────────

/// Input channel for a processor.  
/// Receives messages forwarded by the orchestrator through the runner.
pub struct Reader {
    channel_uri: String,
    rx: mpsc::UnboundedReceiver<ReaderMsg>,
    orch_tx: mpsc::UnboundedSender<FromRunner>,
}

impl Reader {
    pub(crate) fn new(
        channel_uri: String,
        rx: mpsc::UnboundedReceiver<ReaderMsg>,
        orch_tx: mpsc::UnboundedSender<FromRunner>,
    ) -> Self {
        Reader {
            channel_uri,
            rx,
            orch_tx,
        }
    }

    pub fn channel_uri(&self) -> &str {
        &self.channel_uri
    }

    /// Receive the next message.  
    /// Returns `None` when the channel is closed.  
    /// Sends a `GlobalAck` to the orchestrator after each message is consumed.
    pub async fn recv(&mut self) -> Option<Bytes> {
        match self.rx.recv().await? {
            ReaderMsg::Data(data, seq) => {
                self.send_global_ack(seq, None);
                Some(data)
            }
            ReaderMsg::Close => None,
        }
    }

    /// Blocking variant — call from a plain OS thread (not inside a tokio task).
    pub fn blocking_recv(&mut self) -> Option<Bytes> {
        match self.rx.blocking_recv()? {
            ReaderMsg::Data(data, seq) => {
                self.send_global_ack(seq, None);
                Some(data)
            }
            ReaderMsg::Close => None,
        }
    }

    pub(crate) fn close(&mut self) {
        // Drain pending item if any and stop delivering
        while self.rx.try_recv().is_ok() {}
    }

    fn send_global_ack(&self, global_seq: u32, error: Option<String>) {
        let msg = FromRunner {
            inner: Some(FromRunnerInner::Processed(GlobalAck {
                global_sequence_number: global_seq,
                channel: self.channel_uri.clone(),
                error,
            })),
        };
        if let Err(e) = self.orch_tx.send(msg) {
            error!(
                channel = %self.channel_uri,
                "failed to send GlobalAck: {e}"
            );
        }
    }
}

// ── Writer ────────────────────────────────────────────────────────────────────

/// Output channel for a processor.  
/// Sends messages to the orchestrator, which routes them to subscribed readers.
pub struct Writer {
    channel_uri: String,
    local_seq: AtomicU32,
    orch_tx: mpsc::UnboundedSender<FromRunner>,
    pending_acks: WriterAckRegistry,
}

impl Writer {
    pub(crate) fn new(
        channel_uri: String,
        orch_tx: mpsc::UnboundedSender<FromRunner>,
        pending_acks: WriterAckRegistry,
    ) -> Self {
        Writer {
            channel_uri,
            local_seq: AtomicU32::new(1),
            orch_tx,
            pending_acks,
        }
    }

    pub fn channel_uri(&self) -> &str {
        &self.channel_uri
    }

    /// Send bytes and await the `LocalAck` from the orchestrator (backpressure).
    pub async fn write(&self, data: Bytes) -> anyhow::Result<()> {
        let seq = self.next_seq();
        let rx = self.register_ack(seq);
        self.send_message(seq, data)?;
        debug!(channel = %self.channel_uri, seq, "awaiting LocalAck");
        // Block a thread-pool thread rather than holding the executor
        let result = tokio::task::spawn_blocking(move || rx.recv().ok().flatten()).await?;
        if let Some(err) = result {
            anyhow::bail!("writer ack error on '{}': {}", self.channel_uri, err);
        }
        Ok(())
    }

    pub async fn write_str(&self, s: &str) -> anyhow::Result<()> {
        self.write(Bytes::copy_from_slice(s.as_bytes())).await
    }

    /// Blocking variant — call from a plain OS thread.
    pub fn write_blocking(&self, data: &[u8]) -> Result<(), String> {
        let seq = self.next_seq();
        let rx = self.register_ack(seq);
        self.send_message(seq, Bytes::copy_from_slice(data))
            .map_err(|e| e.to_string())?;
        match rx.recv() {
            Ok(None) => Ok(()),
            Ok(Some(err)) => Err(err),
            Err(_) => Err("ack channel closed".into()),
        }
    }

    /// Signal that no more messages will be sent on this channel.
    pub fn close(&self) {
        let msg = FromRunner {
            inner: Some(FromRunnerInner::Close(Close {
                channel: self.channel_uri.clone(),
            })),
        };
        if let Err(e) = self.orch_tx.send(msg) {
            error!(channel = %self.channel_uri, "failed to send Close: {e}");
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    fn next_seq(&self) -> u32 {
        self.local_seq.fetch_add(1, Ordering::SeqCst)
    }

    fn register_ack(&self, seq: u32) -> std::sync::mpsc::Receiver<Option<String>> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.pending_acks
            .lock()
            .unwrap()
            .insert((self.channel_uri.clone(), seq), tx);
        rx
    }

    fn send_message(&self, seq: u32, data: Bytes) -> anyhow::Result<()> {
        let msg = FromRunner {
            inner: Some(FromRunnerInner::Msg(SendingMessage {
                local_sequence_number: seq,
                channel: self.channel_uri.clone(),
                data: data.to_vec(),
            })),
        };
        self.orch_tx.send(msg)?;
        Ok(())
    }

    /// Called by the runner when a `LocalAck` arrives for this channel.
    #[allow(dead_code)]
    pub(crate) fn deliver_ack(&self, local_seq: u32, error: Option<String>) {
        if let Some(tx) = self
            .pending_acks
            .lock()
            .unwrap()
            .remove(&(self.channel_uri.clone(), local_seq))
        {
            let _ = tx.send(error);
        }
    }
}

// ── FFI adapter (bridges Runner channels to the SDK's C ABI) ─────────────────

use rdfc_processor_sdk::{RdfcChannel, RdfcData};
use std::ffi::c_void;

/// Heap-allocated state for a reader channel exposed over FFI.
struct FfiReaderState {
    // We move the Reader into the heap so we can keep a raw pointer to it.
    reader: Reader,
}

unsafe extern "C" fn ffi_read(ctx: *mut c_void, out: *mut RdfcData) -> i32 {
    let state = &mut *(ctx as *mut FfiReaderState);
    match state.reader.blocking_recv() {
        Some(bytes) => {
            let boxed = bytes.to_vec().into_boxed_slice();
            let ptr = boxed.as_ptr() as *mut u8;
            let len = boxed.len();
            std::mem::forget(boxed);
            (*out).ptr = ptr;
            (*out).len = len;
            0
        }
        None => 1, // closed
    }
}

unsafe extern "C" fn ffi_read_write(_ctx: *mut c_void, _ptr: *const u8, _len: usize) -> i32 {
    // Readers don't support writes — should never be called.
    -1
}

unsafe extern "C" fn ffi_reader_close(ctx: *mut c_void) {
    let state = &mut *(ctx as *mut FfiReaderState);
    state.reader.close();
}

unsafe extern "C" fn ffi_free_data(ptr: *mut u8, len: usize) {
    // Re-construct the Box so it gets properly deallocated.
    let _ = Vec::from_raw_parts(ptr, len, len);
}

/// Heap-allocated state for a writer channel exposed over FFI.
struct FfiWriterState {
    writer: Writer,
}

unsafe extern "C" fn ffi_write_noop(_ctx: *mut c_void, _out: *mut RdfcData) -> i32 {
    -1 // Writers don't support reads.
}

unsafe extern "C" fn ffi_write(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32 {
    let state = &mut *(ctx as *mut FfiWriterState);
    let data = std::slice::from_raw_parts(ptr, len);
    match state.writer.write_blocking(data) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

unsafe extern "C" fn ffi_writer_close(ctx: *mut c_void) {
    let state = &mut *(ctx as *mut FfiWriterState);
    state.writer.close();
}

unsafe extern "C" fn ffi_writer_free(_ptr: *mut u8, _len: usize) {
    // Writers never allocate read buffers.
}

/// Wrap a `Reader` as an [`RdfcChannel`] for the SDK.
///
/// The returned channel's `ctx` owns a `Box<FfiReaderState>` that must be freed
/// by calling `drop_ffi_reader_channel` when done.
pub(crate) fn make_ffi_reader_channel(uri: String, reader: Reader) -> RdfcChannel {
    let uri_bytes = uri.into_bytes().into_boxed_slice();
    let uri_ptr = uri_bytes.as_ptr();
    let uri_len = uri_bytes.len();
    std::mem::forget(uri_bytes);

    let state = Box::new(FfiReaderState { reader });
    let ctx = Box::into_raw(state) as *mut c_void;

    RdfcChannel {
        uri: uri_ptr,
        uri_len,
        ctx,
        read: ffi_read,
        write: ffi_read_write,
        close: ffi_reader_close,
        free_data: ffi_free_data,
    }
}

/// Free all resources owned by an FFI reader channel created by [`make_ffi_reader_channel`].
///
/// # Safety
/// `ch` must have been produced by `make_ffi_reader_channel` and not yet freed.
pub(crate) unsafe fn drop_ffi_reader_channel(ch: RdfcChannel) {
    let _ = Box::from_raw(ch.ctx as *mut FfiReaderState);
    let uri_bytes = std::slice::from_raw_parts_mut(ch.uri as *mut u8, ch.uri_len);
    let _ = Box::from_raw(uri_bytes as *mut [u8]);
}

/// Wrap a `Writer` as an [`RdfcChannel`] for the SDK.
pub(crate) fn make_ffi_writer_channel(uri: String, writer: Writer) -> RdfcChannel {
    let uri_bytes = uri.into_bytes().into_boxed_slice();
    let uri_ptr = uri_bytes.as_ptr();
    let uri_len = uri_bytes.len();
    std::mem::forget(uri_bytes);

    let state = Box::new(FfiWriterState { writer });
    let ctx = Box::into_raw(state) as *mut c_void;

    RdfcChannel {
        uri: uri_ptr,
        uri_len,
        ctx,
        read: ffi_write_noop,
        write: ffi_write,
        close: ffi_writer_close,
        free_data: ffi_writer_free,
    }
}

/// Free all resources owned by an FFI writer channel created by [`make_ffi_writer_channel`].
///
/// # Safety
/// `ch` must have been produced by `make_ffi_writer_channel` and not yet freed.
pub(crate) unsafe fn drop_ffi_writer_channel(ch: RdfcChannel) {
    let _ = Box::from_raw(ch.ctx as *mut FfiWriterState);
    let uri_bytes = std::slice::from_raw_parts_mut(ch.uri as *mut u8, ch.uri_len);
    let _ = Box::from_raw(uri_bytes as *mut [u8]);
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_orch_channel() -> (
        mpsc::UnboundedSender<FromRunner>,
        mpsc::UnboundedReceiver<FromRunner>,
    ) {
        mpsc::unbounded_channel()
    }

    #[allow(dead_code)]
    fn make_reader_registry() -> ReaderRegistry {
        Arc::new(Mutex::new(HashMap::new()))
    }

    fn make_writer_ack_registry() -> WriterAckRegistry {
        Arc::new(Mutex::new(HashMap::new()))
    }

    // Helper: create a (Reader, sender) pair
    fn create_reader(
        uri: &str,
        orch_tx: mpsc::UnboundedSender<FromRunner>,
    ) -> (Reader, mpsc::UnboundedSender<ReaderMsg>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let reader = Reader::new(uri.to_string(), rx, orch_tx);
        (reader, tx)
    }

    // Helper: create a Writer
    fn create_writer(
        uri: &str,
        orch_tx: mpsc::UnboundedSender<FromRunner>,
        acks: WriterAckRegistry,
    ) -> Writer {
        Writer::new(uri.to_string(), orch_tx, acks)
    }

    #[tokio::test]
    async fn reader_delivers_message_and_acks() {
        let (orch_tx, mut orch_rx) = make_orch_channel();
        let (mut reader, reader_tx) = create_reader("http://example.org/ch1", orch_tx);

        // Push a message to the reader
        reader_tx
            .send(ReaderMsg::Data(Bytes::from_static(b"hello"), 42))
            .unwrap();

        let msg = reader.recv().await.expect("expected a message");
        assert_eq!(msg, Bytes::from_static(b"hello"));

        // A GlobalAck should have been emitted
        let ack = orch_rx.recv().await.expect("expected ack");
        match ack.inner {
            Some(FromRunnerInner::Processed(g)) => {
                assert_eq!(g.global_sequence_number, 42);
                assert_eq!(g.channel, "http://example.org/ch1");
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn reader_returns_none_on_close() {
        let (orch_tx, _orch_rx) = make_orch_channel();
        let (mut reader, reader_tx) = create_reader("http://example.org/ch2", orch_tx);

        reader_tx.send(ReaderMsg::Close).unwrap();
        assert!(reader.recv().await.is_none());
    }

    #[tokio::test]
    async fn writer_sends_message_and_resolves_on_ack() {
        let (orch_tx, mut orch_rx) = make_orch_channel();
        let acks = make_writer_ack_registry();
        let writer = create_writer("http://example.org/ch3", orch_tx, Arc::clone(&acks));

        let write_task = tokio::spawn(async move {
            writer.write(Bytes::from_static(b"world")).await.unwrap();
        });

        // The runner should have received a SendingMessage
        let msg = orch_rx.recv().await.expect("expected SendingMessage");
        let (channel, local_seq) = match &msg.inner {
            Some(FromRunnerInner::Msg(m)) => (m.channel.clone(), m.local_sequence_number),
            other => panic!("unexpected: {other:?}"),
        };
        assert_eq!(channel, "http://example.org/ch3");
        assert_eq!(&msg.inner.as_ref().map(|i| match i {
            FromRunnerInner::Msg(m) => m.data.clone(),
            _ => vec![],
        }).unwrap(), b"world");

        // Deliver the LocalAck so the writer future resolves
        acks.lock()
            .unwrap()
            .remove(&(channel, local_seq))
            .unwrap()
            .send(None)
            .unwrap();

        write_task.await.unwrap();
    }

    #[tokio::test]
    async fn writer_close_sends_close_message() {
        let (orch_tx, mut orch_rx) = make_orch_channel();
        let acks = make_writer_ack_registry();
        let writer = create_writer("http://example.org/ch4", orch_tx, acks);

        writer.close();

        let msg = orch_rx.recv().await.expect("expected Close message");
        match msg.inner {
            Some(FromRunnerInner::Close(c)) => {
                assert_eq!(c.channel, "http://example.org/ch4");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
