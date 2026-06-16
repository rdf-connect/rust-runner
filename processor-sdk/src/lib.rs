//! # RDF-Connect Processor SDK
//!
//! Shared types for building RDF-Connect Rust processor plugins.
//!
//! A processor crate should be compiled as a `cdylib` and use the
//! [`rdfc_export!`] macro to expose the required FFI entry point.
//!
//! ## Example
//!
//! ```rust,no_run
//! use rdfc_processor_sdk::*;
//!
//! #[derive(Deserialize)]
//! struct Config {
//!     reader: Reader,
//!     writer: Writer,
//! }
//!
//! fn my_processor(Config { reader, writer }: Config) -> Result<(), String> {
//!     for bytes in reader.iter_bytes() {
//!         writer.write_bytes(&bytes)?;
//!     }
//!     writer.close();
//!     Ok(())
//! }
//!
//! rdfc_export!(my_processor);
//! ```

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::c_void;

pub use serde::Deserialize;

// ── FFI primitives ────────────────────────────────────────────────────────────

/// Owned byte buffer returned by a `ReadFn` call.  
/// The runner allocates this memory; the processor must call `free_data` to release it.
#[repr(C)]
pub struct RdfcData {
    pub ptr: *mut u8,
    pub len: usize,
}

/// Called by the processor to read the next message from an input channel.
///
/// Blocking: returns when a message is available or the channel is closed.
///
/// Returns:
/// - `0`  — data available; `*out` is populated, caller must call `free_data`.
/// - `1`  — channel closed, no more messages.
/// - `-1` — error.
pub type ReadFn = unsafe extern "C" fn(ctx: *mut c_void, out: *mut RdfcData) -> i32;

/// Called by the processor to write bytes to an output channel.
///
/// Blocking: returns when the downstream runner has acknowledged the message
/// (backpressure).
///
/// Returns `0` on success, `-1` on error.
pub type WriteFn = unsafe extern "C" fn(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32;

/// Called to signal that no more data will be sent on this channel.
pub type CloseFn = unsafe extern "C" fn(ctx: *mut c_void);

/// Called to release memory allocated by `ReadFn`.
pub type FreeDataFn = unsafe extern "C" fn(ptr: *mut u8, len: usize);

// ── Channel descriptors ───────────────────────────────────────────────────────

/// A single bidirectional channel descriptor passed over the FFI boundary.
/// Each field is valid for the lifetime of the `rdfc_run` call.
#[repr(C)]
pub struct RdfcChannel {
    /// Channel URI string (not null-terminated).
    pub uri: *const u8,
    pub uri_len: usize,

    /// Opaque per-channel state owned by the runner.
    pub ctx: *mut c_void,

    pub read: ReadFn,
    pub write: WriteFn,
    pub close: CloseFn,
    pub free_data: FreeDataFn,
}

/// Named channel entry inside [`RdfcArgs`].
#[repr(C)]
pub struct RdfcNamedChannel {
    /// Field name from the SHACL shape (e.g. `"reader"`, `"input"`).
    pub name: *const u8,
    pub name_len: usize,
    pub channel: RdfcChannel,
}

/// All arguments passed to [`rdfc_run`].
#[repr(C)]
pub struct RdfcArgs {
    /// Raw JSON bytes of the processor arguments (non-null-terminated).
    pub args_json: *const u8,
    pub args_json_len: usize,

    pub readers: *const RdfcNamedChannel,
    pub n_readers: usize,

    pub writers: *const RdfcNamedChannel,
    pub n_writers: usize,
}

// ── Channel wrappers ──────────────────────────────────────────────────────────

/// Ergonomic reader backed by the FFI [`RdfcChannel`].
///
/// Implements [`serde::Deserialize`]: when a field is annotated with type
/// `Reader` in a `#[derive(Deserialize)]` config struct, the deserializer reads
/// the `@id` URI from the JSON-LD channel reference and looks up the actual
/// channel from the thread-local pool installed by [`ProcessorContext::deserialize`].
pub struct Reader {
    channel: *const RdfcChannel,
}

// SAFETY: the runner guarantees `RdfcChannel` is valid for the duration of `rdfc_run`.
unsafe impl Send for Reader {}
unsafe impl Sync for Reader {}

impl Reader {
    /// The channel URI (as registered by the orchestrator).
    pub fn channel_uri(&self) -> &str {
        unsafe {
            let bytes = std::slice::from_raw_parts((*self.channel).uri, (*self.channel).uri_len);
            std::str::from_utf8_unchecked(bytes)
        }
    }

    /// Read the next message.  Returns `None` when the channel is closed.
    pub fn next_bytes(&self) -> Option<Vec<u8>> {
        let mut data = RdfcData { ptr: std::ptr::null_mut(), len: 0 };
        let rc = unsafe { ((*self.channel).read)((*self.channel).ctx, &mut data) };
        if rc == 0 {
            let bytes = unsafe { std::slice::from_raw_parts(data.ptr, data.len) }.to_vec();
            unsafe { ((*self.channel).free_data)(data.ptr, data.len) };
            Some(bytes)
        } else {
            None
        }
    }

    /// Read the next message as a UTF-8 string.
    pub fn next_string(&self) -> Option<String> {
        self.next_bytes().and_then(|b| String::from_utf8(b).ok())
    }

    /// Iterate over all incoming bytes.
    pub fn iter_bytes(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
        std::iter::from_fn(move || self.next_bytes())
    }

    /// Iterate over all incoming strings.
    pub fn iter_strings(&self) -> impl Iterator<Item = String> + '_ {
        std::iter::from_fn(move || self.next_string())
    }
}

/// Ergonomic writer backed by the FFI [`RdfcChannel`].
///
/// Implements [`serde::Deserialize`] the same way as [`Reader`].
pub struct Writer {
    channel: *const RdfcChannel,
}

unsafe impl Send for Writer {}
unsafe impl Sync for Writer {}

impl Writer {
    /// The channel URI (as registered by the orchestrator).
    pub fn channel_uri(&self) -> &str {
        unsafe {
            let bytes = std::slice::from_raw_parts((*self.channel).uri, (*self.channel).uri_len);
            std::str::from_utf8_unchecked(bytes)
        }
    }

    /// Write bytes to the channel (blocks until acknowledged).
    pub fn write_bytes(&self, data: &[u8]) -> Result<(), String> {
        let rc =
            unsafe { ((*self.channel).write)((*self.channel).ctx, data.as_ptr(), data.len()) };
        if rc == 0 { Ok(()) } else { Err(format!("write failed with code {rc}")) }
    }

    /// Write a UTF-8 string to the channel.
    pub fn write_string(&self, s: &str) -> Result<(), String> {
        self.write_bytes(s.as_bytes())
    }

    /// Signal that no more data will be written.
    pub fn close(&self) {
        unsafe { ((*self.channel).close)((*self.channel).ctx) }
    }
}

// ── Thread-local channel pools (used during deserialization) ──────────────────

// When `ProcessorContext::deserialize::<C>()` is called, the reader and writer
// channels are temporarily moved into these thread-local pools keyed by channel
// URI.  The custom `Deserialize` impls for `Reader` and `Writer` pop entries
// from these pools by matching the `@id` found in the JSON-LD argument object.
thread_local! {
    static READER_POOL: RefCell<HashMap<String, Reader>> = RefCell::new(HashMap::new());
    static WRITER_POOL: RefCell<HashMap<String, Writer>> = RefCell::new(HashMap::new());
}

// ── serde::Deserialize for Reader ─────────────────────────────────────────────

impl<'de> serde::Deserialize<'de> for Reader {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        // The orchestrator sends channel references as JSON-LD objects:
        // { "@type": "https://w3id.org/rdf-connect#Reader", "@id": "<channel-uri>" }
        #[derive(serde::Deserialize)]
        struct ChannelRef {
            #[serde(rename = "@id")]
            id: String,
        }

        let chan = ChannelRef::deserialize(deserializer)?;

        READER_POOL.with(|pool| {
            pool.borrow_mut().remove(&chan.id).ok_or_else(|| {
                D::Error::custom(format!(
                    "no reader registered for channel '{}' — \
                     make sure to call ProcessorContext::deserialize()",
                    chan.id
                ))
            })
        })
    }
}

// ── serde::Deserialize for Writer ─────────────────────────────────────────────

impl<'de> serde::Deserialize<'de> for Writer {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        #[derive(serde::Deserialize)]
        struct ChannelRef {
            #[serde(rename = "@id")]
            id: String,
        }

        let chan = ChannelRef::deserialize(deserializer)?;

        WRITER_POOL.with(|pool| {
            pool.borrow_mut().remove(&chan.id).ok_or_else(|| {
                D::Error::custom(format!(
                    "no writer registered for channel '{}' — \
                     make sure to call ProcessorContext::deserialize()",
                    chan.id
                ))
            })
        })
    }
}

// ── ProcessorContext ──────────────────────────────────────────────────────────

/// High-level view of the arguments passed to a processor.
pub struct ProcessorContext {
    /// Raw JSON args (full JSON-LD including Reader/Writer channel objects).
    pub args: serde_json::Value,
    readers: HashMap<String, Reader>,
    writers: HashMap<String, Writer>,
}

impl ProcessorContext {
    /// Build a `ProcessorContext` from the raw FFI args.
    ///
    /// # Safety
    /// `ffi_args` must be a valid pointer produced by the runner.
    pub unsafe fn from_ffi(ffi_args: *const RdfcArgs) -> Self {
        let a = &*ffi_args;

        let json_bytes = std::slice::from_raw_parts(a.args_json, a.args_json_len);
        let args: serde_json::Value =
            serde_json::from_slice(json_bytes).unwrap_or(serde_json::Value::Null);

        let mut readers = HashMap::new();
        let reader_slice = std::slice::from_raw_parts(a.readers, a.n_readers);
        for nc in reader_slice {
            let name_bytes = std::slice::from_raw_parts(nc.name, nc.name_len);
            let name = String::from_utf8_lossy(name_bytes).into_owned();
            readers.insert(name, Reader { channel: &nc.channel });
        }

        let mut writers = HashMap::new();
        let writer_slice = std::slice::from_raw_parts(a.writers, a.n_writers);
        for nc in writer_slice {
            let name_bytes = std::slice::from_raw_parts(nc.name, nc.name_len);
            let name = String::from_utf8_lossy(name_bytes).into_owned();
            writers.insert(name, Writer { channel: &nc.channel });
        }

        ProcessorContext { args, readers, writers }
    }

    /// Deserialize the processor's JSON-LD config into a typed struct `C`.
    ///
    /// Any `Reader` or `Writer` fields in `C` are automatically populated by
    /// matching the `@id` URI in the JSON-LD against the channels passed by the
    /// runner.  All other fields deserialize normally from the JSON.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use rdfc_processor_sdk::*;
    /// # use serde::Deserialize;
    /// #[derive(Deserialize)]
    /// struct Config {
    ///     reader: Reader,
    ///     writer: Writer,
    ///     label: Option<String>,
    /// }
    ///
    /// fn run(mut ctx: ProcessorContext) -> Result<(), String> {
    ///     let Config { reader, writer, label } = ctx.deserialize()?;
    ///     // ...
    /// #   Ok(())
    /// }
    /// ```
    pub fn deserialize<C: serde::de::DeserializeOwned>(&mut self) -> Result<C, String> {
        // Rekey channels by URI so the Deserialize impls can look them up by @id.
        let reader_pool: HashMap<String, Reader> = self
            .readers
            .drain()
            .map(|(_, r)| (r.channel_uri().to_string(), r))
            .collect();
        let writer_pool: HashMap<String, Writer> = self
            .writers
            .drain()
            .map(|(_, w)| (w.channel_uri().to_string(), w))
            .collect();

        READER_POOL.with(|p| *p.borrow_mut() = reader_pool);
        WRITER_POOL.with(|p| *p.borrow_mut() = writer_pool);

        let result = serde_json::from_value::<C>(self.args.clone()).map_err(|e| e.to_string());

        // Clean up any channels that weren't consumed (e.g. optional ones).
        READER_POOL.with(|p| p.borrow_mut().clear());
        WRITER_POOL.with(|p| p.borrow_mut().clear());

        result
    }

    /// Get a reader by SHACL field name (lower-level alternative to `deserialize`).
    pub fn reader(&self, name: &str) -> Result<&Reader, String> {
        self.readers.get(name).ok_or_else(|| format!("no reader named '{name}'"))
    }

    /// Get a writer by SHACL field name (lower-level alternative to `deserialize`).
    pub fn writer(&self, name: &str) -> Result<&Writer, String> {
        self.writers.get(name).ok_or_else(|| format!("no writer named '{name}'"))
    }
}

// ── Test helpers ─────────────────────────────────────────────────────────────

/// Utilities for writing unit-tests against processor functions.
pub mod testing {
    use std::collections::VecDeque;
    use std::ffi::c_void;
    use std::sync::{Arc, Mutex};

    use crate::{RdfcChannel, RdfcData, RdfcNamedChannel};

    pub struct MockReaderState {
        pub messages: Mutex<VecDeque<Vec<u8>>>,
    }

    pub struct MockWriterState {
        pub received: Mutex<Vec<Vec<u8>>>,
        pub closed: Mutex<bool>,
    }

    unsafe extern "C" fn mock_read(ctx: *mut c_void, out: *mut RdfcData) -> i32 {
        let state = &*(ctx as *const MockReaderState);
        match state.messages.lock().unwrap().pop_front() {
            Some(msg) => {
                let mut boxed = msg.into_boxed_slice();
                let ptr = boxed.as_mut_ptr();
                let len = boxed.len();
                std::mem::forget(boxed);
                (*out) = RdfcData { ptr, len };
                0
            }
            None => 1,
        }
    }

    unsafe extern "C" fn mock_write(ctx: *mut c_void, ptr: *const u8, len: usize) -> i32 {
        let state = &*(ctx as *const MockWriterState);
        let bytes = std::slice::from_raw_parts(ptr, len).to_vec();
        state.received.lock().unwrap().push(bytes);
        0
    }

    unsafe extern "C" fn mock_close_writer(ctx: *mut c_void) {
        let state = &*(ctx as *const MockWriterState);
        *state.closed.lock().unwrap() = true;
    }

    unsafe extern "C" fn noop_read(_: *mut c_void, _: *mut RdfcData) -> i32 { 1 }
    unsafe extern "C" fn noop_write(_: *mut c_void, _: *const u8, _: usize) -> i32 { 0 }
    unsafe extern "C" fn noop_close(_: *mut c_void) {}
    unsafe extern "C" fn mock_free_data(ptr: *mut u8, len: usize) {
        let _ = unsafe { Vec::from_raw_parts(ptr, len, len) };
    }

    /// A mock reader channel backed by an in-memory queue.
    ///
    /// `uri` must match the `@id` used in the test args JSON when calling
    /// [`ProcessorContext::deserialize`].
    pub fn make_mock_reader(uri: &'static str, messages: Vec<Vec<u8>>) -> (Arc<MockReaderState>, RdfcChannel) {
        let state = Arc::new(MockReaderState { messages: Mutex::new(messages.into()) });
        let raw = Arc::into_raw(Arc::clone(&state)) as *mut c_void;
        let channel = RdfcChannel {
            uri: uri.as_ptr(),
            uri_len: uri.len(),
            ctx: raw,
            read: mock_read,
            write: noop_write,
            close: noop_close,
            free_data: mock_free_data,
        };
        (state, channel)
    }

    /// A mock writer channel that collects written bytes.
    ///
    /// `uri` must match the `@id` used in the test args JSON when calling
    /// [`ProcessorContext::deserialize`].
    pub fn make_mock_writer(uri: &'static str) -> (Arc<MockWriterState>, RdfcChannel) {
        let state = Arc::new(MockWriterState {
            received: Mutex::new(Vec::new()),
            closed: Mutex::new(false),
        });
        let raw = Arc::into_raw(Arc::clone(&state)) as *mut c_void;
        let channel = RdfcChannel {
            uri: uri.as_ptr(),
            uri_len: uri.len(),
            ctx: raw,
            read: noop_read,
            write: mock_write,
            close: mock_close_writer,
            free_data: mock_free_data,
        };
        (state, channel)
    }

    /// Build a [`RdfcNamedChannel`] for testing (wraps a channel with a field name).
    pub fn make_named(name: &'static str, channel: RdfcChannel) -> RdfcNamedChannel {
        RdfcNamedChannel { name: name.as_ptr(), name_len: name.len(), channel }
    }
}

// ── Export macro ──────────────────────────────────────────────────────────────

/// Export a processor function as the standard `rdfc_run` FFI entry point.
///
/// The config type is deserialized automatically from the JSON-LD arguments
/// supplied by the orchestrator. `Reader` and `Writer` fields are resolved
/// from the channel map by their `@id`. Rust infers the config type from the
/// function signature, so no explicit type annotation is needed.
///
/// ```ignore
/// #[derive(serde::Deserialize)]
/// struct Config { reader: Reader, writer: Writer }
///
/// fn my_processor(cfg: Config) -> Result<(), String> { … }
///
/// rdfc_export!(my_processor);
/// ```
#[macro_export]
macro_rules! rdfc_export {
    ($run_fn:path) => {
        /// Entry point called by the RDF-Connect Rust runner.
        ///
        /// # Safety
        /// `args` must be a valid `RdfcArgs` pointer produced by the runner.
        #[no_mangle]
        pub unsafe extern "C" fn rdfc_run(
            args: *const rdfc_processor_sdk::RdfcArgs,
        ) -> i32 {
            let result = std::panic::catch_unwind(|| {
                let mut ctx =
                    unsafe { rdfc_processor_sdk::ProcessorContext::from_ffi(args) };
                // T is inferred from $run_fn's parameter type.
                let config = ctx.deserialize()?;
                $run_fn(config)
            });
            match result {
                Ok(Ok(())) => 0,
                Ok(Err(e)) => {
                    eprintln!("[rdfc] processor error: {e}");
                    -1
                }
                Err(_) => {
                    eprintln!("[rdfc] processor panicked");
                    -1
                }
            }
        }
    };
}
