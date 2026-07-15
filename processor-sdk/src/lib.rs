//! # RDF-Connect Processor SDK
//!
//! Shared types for building RDF-Connect Rust processors that are compiled as
//! `cdylib` shared libraries and loaded by the runner at pipeline-startup time.
//!
//! The FFI boundary is built on [`abi_stable`] (stable struct/vtable layout)
//! plus [`async_ffi`] (stable `Future`s), so processor authors still write
//! ordinary `async fn`s — the ABI machinery is entirely hidden behind
//! [`rdfc_register!`] / [`rdfc_export_processors!`].
//!
//! ## Quick start
//!
//! Implement [`Processor`] — a three-phase `init` / `transform` / `produce`
//! lifecycle mirroring RDF-Connect's JS/TS runner — for a type whose
//! `Args` config is deserialized automatically (`Reader`/`Writer` fields
//! included) before construction.
//!
//! A crate can export any number of processors — call [`rdfc_register!`]
//! once per processor, then [`rdfc_export_processors!`] exactly once (a
//! `cdylib` can only have one root-module export; see its docs for why).
//!
//! ```rust,ignore
//! use rdfc_processor_sdk::{futures::StreamExt, Processor, Reader, Writer};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Args { reader: Reader, writer: Writer }
//!
//! struct MyProcessor { reader: Option<Reader>, writer: Option<Writer> }
//!
//! impl Processor for MyProcessor {
//!     type Args = Args;
//!
//!     fn new(args: Args) -> Self {
//!         MyProcessor { reader: Some(args.reader), writer: Some(args.writer) }
//!     }
//!
//!     async fn transform(&mut self) -> Result<(), String> {
//!         let writer = self.writer.take().unwrap();
//!         let mut stream = self.reader.take().unwrap().into_stream();
//!         while let Some(bytes) = stream.next().await {
//!             writer.write(&bytes).await?;
//!         }
//!         writer.close();
//!         Ok(())
//!     }
//! }
//!
//! rdfc_register!(
//!     "https://w3id.org/rdf-connect#MyProcessor",
//!     MyProcessor
//! );
//! rdfc_export_processors!();
//! ```
//!
//! ## A note on async runtimes
//!
//! Each `cdylib` links its own copy of every crate it depends on, `tokio`
//! included. That copy does **not** share the reactor/timer driver installed
//! by the runner's `#[tokio::main]` — only the [`Reader`]/[`Writer`] calls
//! cross back into runner-compiled code (where a live reactor exists).
//! Processor code must not rely on an ambient Tokio runtime (e.g.
//! `tokio::time::sleep`, `tokio::spawn`) unless it starts its own; doing pure
//! computation and `.await`ing [`Reader`]/[`Writer`] methods is always safe.

// `#[sabi_trait]`'s expansion (abi_stable_derive 0.11.3) trips this lint on
// current rustc; the generated `impl`s are sound, just not written the way
// the lint expects a manually-written `impl` to be scoped.
#![allow(non_local_definitions)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

pub use abi_stable;
pub use async_ffi;
pub use bytes;
pub use futures;
pub use serde::Deserialize;

use abi_stable::{
    library::RootModule,
    sabi_trait,
    sabi_trait::prelude::TD_Opaque,
    std_types::{RBox, RResult, RStr, RString, RVec, ROption},
    StableAbi,
};
use async_ffi::{FfiFuture, FutureExt};
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};

// ── ABI compatibility token ────────────────────────────────────────────────────

/// Opaque token identifying the exact SDK / `abi_stable` / `async-ffi` versions
/// this crate was built against.
///
/// The runner compares this against the `:abiStable` literal declared in a
/// processor's `processor.ttl` *before* downloading or loading anything, so a
/// mismatch produces a clear diagnostic instead of a layout-check failure deep
/// inside `abi_stable::library::RootModule::load_from_file`.
///
/// This is a convenience pre-flight check on top of (not a replacement for)
/// `abi_stable`'s own type-layout verification, which still runs at load time.
pub const ABI_TOKEN: &str = concat!(
    "rdfc-processor-sdk-",
    env!("CARGO_PKG_VERSION"),
    "+abi_stable-0.11.3+async-ffi-0.5.0"
);

// ── Raw (FFI-safe) channel trait objects ──────────────────────────────────────

/// FFI-safe interface for reading messages from an input channel.
///
/// Implemented by the runner and passed across the ABI boundary as a trait
/// object; processor authors interact with [`Reader`] instead.
#[sabi_trait]
pub trait RawReader: Send + Sync {
    /// Read the next message, or `RNone` when the channel is closed.
    #[sabi(last_prefix_field)]
    fn read(&self) -> FfiFuture<ROption<RVec<u8>>>;
}

/// FFI-safe interface for writing messages to an output channel.
///
/// Implemented by the runner and passed across the ABI boundary as a trait
/// object; processor authors interact with [`Writer`] instead.
#[sabi_trait]
pub trait RawWriter: Send + Sync {
    /// Write a message and await backpressure acknowledgement from the runner.
    fn write(&self, data: RVec<u8>) -> FfiFuture<RResult<(), RString>>;
    /// Signal that no more messages will be written.
    #[sabi(last_prefix_field)]
    fn close(&self);
}

/// A type-erased [`RawReader`] trait object.
pub type BoxedRawReader = RawReader_TO<'static, RBox<()>>;
/// A type-erased [`RawWriter`] trait object.
pub type BoxedRawWriter = RawWriter_TO<'static, RBox<()>>;

// ── Raw processor trait object (phased lifecycle, crosses the FFI boundary) ───

/// FFI-safe interface for a live processor instance, already constructed
/// from its deserialized arguments.
///
/// Implemented (generically, via [`RawProcessorWrapper`]) for every type
/// that implements [`Processor`]; the runner drives the three phases
/// through this trait object instead of calling processor code directly.
#[sabi_trait]
pub trait RawProcessor: Send + Sync {
    /// Phase 1 — allocate external resources. Awaited once, before the
    /// pipeline starts.
    fn init(&self) -> FfiFuture<RResult<(), RString>>;
    /// Phase 2 — register/start consuming input channels. Called for every
    /// processor before any processor's `produce` runs.
    fn transform(&self) -> FfiFuture<RResult<(), RString>>;
    /// Phase 3 — emit data on output channels, starting the pipeline. The
    /// processor is only finished once this returns.
    #[sabi(last_prefix_field)]
    fn produce(&self) -> FfiFuture<RResult<(), RString>>;
}

/// A type-erased [`RawProcessor`] trait object.
pub type BoxedRawProcessor = RawProcessor_TO<'static, RBox<()>>;

// ── Raw processor arguments (crosses the FFI boundary) ────────────────────────

/// One reader entry, as passed across the FFI boundary.
///
/// `uri` is the channel's true URI (the `@id` used by the JSON-LD argument
/// object) — not the SHACL field name — since that's what
/// [`ProcessorArgs::deserialize`] matches on.
#[repr(C)]
#[derive(StableAbi)]
pub struct RawNamedReader {
    pub uri: RString,
    pub reader: BoxedRawReader,
}

/// One writer entry, as passed across the FFI boundary. See [`RawNamedReader`].
#[repr(C)]
#[derive(StableAbi)]
pub struct RawNamedWriter {
    pub uri: RString,
    pub writer: BoxedRawWriter,
}

/// All arguments passed to a processor factory across the FFI boundary.
#[repr(C)]
#[derive(StableAbi)]
pub struct RawProcessorArgs {
    /// Raw JSON-LD argument object, serialized to a string by the runner.
    pub args_json: RString,
    pub readers: RVec<RawNamedReader>,
    pub writers: RVec<RawNamedWriter>,
}

// ── Registration / root module (exported by every processor cdylib) ──────────

/// One processor type exported by a `cdylib`.
#[repr(C)]
#[derive(StableAbi)]
pub struct Registration {
    /// The RDF class URI of this processor (e.g. `"https://w3id.org/rdf-connect#EchoProcessor"`).
    pub type_uri: RStr<'static>,
    /// Factory function that deserializes arguments and constructs a live
    /// processor instance, ready to be driven through its `init` /
    /// `transform` / `produce` phases via [`RawProcessor`].
    pub factory: extern "C" fn(RawProcessorArgs) -> RResult<BoxedRawProcessor, RString>,
}

/// Root module exported by every processor `cdylib` (one per shared library).
///
/// A single well-known `#[no_mangle]` symbol (generated by
/// `#[export_root_module]`) carries this struct's vtable — that symbol name
/// is fixed by `abi_stable`'s own convention (not derived from the
/// annotated function's name), so it can only be emitted **once** per
/// compiled `cdylib`; a second `#[export_root_module]` in the same crate is
/// a duplicate-symbol link error, not just a Rust-level name collision. See
/// [`rdfc_export_processors!`] for how multiple [`rdfc_register!`] calls
/// still end up behind that one export.
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = ProcessorModuleRef)))]
#[sabi(missing_field(panic))]
pub struct ProcessorModule {
    /// Must equal [`ABI_TOKEN`] on the loading side.
    pub abi_token: RStr<'static>,
    #[sabi(last_prefix_field)]
    pub registrations: extern "C" fn() -> RVec<Registration>,
}

impl RootModule for ProcessorModuleRef {
    abi_stable::declare_root_module_statics! {ProcessorModuleRef}
    const BASE_NAME: &'static str = "rdfc_processor";
    const NAME: &'static str = "rdfc_processor";
    const VERSION_STRINGS: abi_stable::sabi_types::VersionStrings =
        abi_stable::package_version_strings!();
}

// ── rdfc_register! / rdfc_export_processors! macros ───────────────────────────

/// Plain (non-FFI-safe) registration entry, collected within a single
/// compilation unit via [`linkme`] — safe here because, unlike
/// [`Registration`]/[`ProcessorModule`], this collection never crosses a
/// `cdylib` boundary: every [`rdfc_register!`] call and the one
/// [`rdfc_export_processors!`] call that reads this slice are compiled into
/// the *same* `cdylib`.
#[doc(hidden)]
pub struct RegistrationEntry {
    pub type_uri: &'static str,
    pub factory: extern "C" fn(RawProcessorArgs) -> RResult<BoxedRawProcessor, RString>,
}

#[doc(hidden)]
#[linkme::distributed_slice]
pub static REGISTRATIONS: [RegistrationEntry] = [..];

pub use linkme;

/// Register one processor. Call this any number of times in a processor
/// crate — once per processor type the resulting `cdylib` should export.
///
/// Each call only submits an entry to a crate-local collection; it does not
/// by itself export anything. Follow every `rdfc_register!` call in the
/// crate with exactly one [`rdfc_export_processors!`] call (typically at the
/// end of `lib.rs`) to actually emit the `cdylib`'s root module export.
///
/// # Arguments
/// - `$type_uri` — the RDF class URI string (must match the class the
///   processor's `processor.ttl` shape targets).
/// - `$ty` — a type implementing [`Processor`]. Its `Args` config type is
///   deserialized from the JSON-LD arguments automatically (any `Reader` /
///   `Writer` fields are populated exactly like [`ProcessorArgs::deserialize`]
///   does) before [`Processor::new`] is called.
#[macro_export]
macro_rules! rdfc_register {
    ($type_uri:expr, $ty:ty) => {
        const _: () = {
            extern "C" fn __rdfc_factory(
                raw: $crate::RawProcessorArgs,
            ) -> $crate::abi_stable::std_types::RResult<
                $crate::BoxedRawProcessor,
                $crate::abi_stable::std_types::RString,
            > {
                use $crate::abi_stable::std_types::{RResult, RString};

                let mut args = $crate::ProcessorArgs::from_raw(raw);
                match args.deserialize::<<$ty as $crate::Processor>::Args>() {
                    Ok(config) => {
                        let processor = <$ty as $crate::Processor>::new(config);
                        let wrapper = $crate::RawProcessorWrapper::new(processor);
                        let boxed = $crate::RawProcessor_TO::from_value(
                            wrapper,
                            $crate::abi_stable::sabi_trait::prelude::TD_Opaque,
                        );
                        RResult::ROk(boxed)
                    }
                    Err(e) => RResult::RErr(RString::from(e)),
                }
            }

            #[$crate::linkme::distributed_slice($crate::REGISTRATIONS)]
            #[linkme(crate = $crate::linkme)]
            static REGISTRATION: $crate::RegistrationEntry = $crate::RegistrationEntry {
                type_uri: $type_uri,
                factory: __rdfc_factory,
            };
        };
    };
}

/// Export every processor registered in this crate via [`rdfc_register!`] as
/// the crate's single `cdylib` root module. Call this exactly once, at crate
/// scope (not inside a function), after all `rdfc_register!` calls.
#[macro_export]
macro_rules! rdfc_export_processors {
    () => {
        extern "C" fn __rdfc_registrations(
        ) -> $crate::abi_stable::std_types::RVec<$crate::Registration> {
            $crate::abi_stable::std_types::RVec::from(
                $crate::REGISTRATIONS
                    .iter()
                    .map(|r| $crate::Registration {
                        type_uri: $crate::abi_stable::std_types::RStr::from_str(r.type_uri),
                        factory: r.factory,
                    })
                    .collect::<::std::vec::Vec<_>>(),
            )
        }

        #[$crate::abi_stable::export_root_module]
        pub fn __rdfc_root_module() -> $crate::ProcessorModuleRef {
            use $crate::abi_stable::prefix_type::PrefixTypeTrait;
            $crate::ProcessorModule {
                abi_token: $crate::abi_stable::std_types::RStr::from_str($crate::ABI_TOKEN),
                registrations: __rdfc_registrations,
            }
            .leak_into_prefix()
        }
    };
}

// ── Reader ────────────────────────────────────────────────────────────────────

/// Input channel for a processor.
///
/// Implements [`serde::Deserialize`]: when a config struct has a `Reader`
/// field, the deserializer matches the `@id` URI from the JSON-LD argument
/// and retrieves the matching channel from the thread-local pool installed by
/// [`ProcessorArgs::deserialize`].
pub struct Reader {
    uri: String,
    inner: BoxedRawReader,
}

impl Reader {
    #[doc(hidden)]
    pub fn from_parts(uri: String, inner: BoxedRawReader) -> Self {
        Reader { uri, inner }
    }

    /// The channel URI as provided by the orchestrator.
    pub fn channel_uri(&self) -> &str {
        &self.uri
    }

    /// Receive the next message from the channel.
    /// Returns `None` when the channel is closed (no more messages).
    pub async fn read(&self) -> Option<Bytes> {
        self.inner
            .read()
            .await
            .into_option()
            .map(|v| Bytes::from(v.into_vec()))
    }

    /// Consume the reader as an async [`Stream`](futures::Stream) of byte messages.
    pub fn into_stream(self) -> std::pin::Pin<Box<dyn futures::Stream<Item = Bytes> + Send>> {
        Box::pin(futures::stream::unfold(self, |r| async {
            r.read().await.map(|b| (b, r))
        }))
    }

    /// Extract the raw FFI trait object, discarding the URI.
    ///
    /// Used by the runner to build [`RawNamedReader`] entries when crossing
    /// into a processor's `cdylib` — not normally called by processor authors.
    #[doc(hidden)]
    pub fn into_raw(self) -> BoxedRawReader {
        self.inner
    }
}

// ── Writer ────────────────────────────────────────────────────────────────────

/// Output channel for a processor.
///
/// Implements [`serde::Deserialize`] the same way as [`Reader`].
pub struct Writer {
    uri: String,
    inner: BoxedRawWriter,
}

impl Writer {
    #[doc(hidden)]
    pub fn from_parts(uri: String, inner: BoxedRawWriter) -> Self {
        Writer { uri, inner }
    }

    /// The channel URI as provided by the orchestrator.
    pub fn channel_uri(&self) -> &str {
        &self.uri
    }

    /// Send bytes and await confirmation from the underlying channel.
    pub async fn write(&self, data: &[u8]) -> Result<(), String> {
        self.inner
            .write(RVec::from(data.to_vec()))
            .await
            .into_result()
            .map_err(|e| e.into_string())
    }

    /// Send a UTF-8 string and await confirmation.
    pub async fn write_str(&self, s: &str) -> Result<(), String> {
        self.write(s.as_bytes()).await
    }

    /// Signal that no more data will be written to this channel.
    pub fn close(&self) {
        self.inner.close()
    }

    /// Extract the raw FFI trait object, discarding the URI.
    ///
    /// Used by the runner to build [`RawNamedWriter`] entries when crossing
    /// into a processor's `cdylib` — not normally called by processor authors.
    #[doc(hidden)]
    pub fn into_raw(self) -> BoxedRawWriter {
        self.inner
    }
}

// ── Channel bridge (mpsc-backed RawReader / RawWriter impls) ──────────────────
//
// Used both by the runner (bridging its gRPC-backed channels to the SDK's
// FFI-safe trait objects) and by the `testing` module (in-process mocks) —
// the same code path is exercised in both cases.

struct ChannelReader {
    rx: Arc<AsyncMutex<mpsc::UnboundedReceiver<Option<Bytes>>>>,
}

impl RawReader for ChannelReader {
    fn read(&self) -> FfiFuture<ROption<RVec<u8>>> {
        let rx = self.rx.clone();
        async move {
            let mut guard = rx.lock().await;
            match guard.recv().await.flatten() {
                Some(data) => ROption::RSome(RVec::from(data.to_vec())),
                None => ROption::RNone,
            }
        }
        .into_ffi()
    }
}

/// A single message flowing through the writer bridge.
///
/// `Data` carries the payload plus a one-shot ack that the bridge must send
/// once the underlying channel has accepted the data (preserving backpressure).
pub enum WriterMsg {
    Data(Bytes, oneshot::Sender<Result<(), String>>),
    Close,
}

struct ChannelWriter {
    tx: mpsc::UnboundedSender<WriterMsg>,
}

impl RawWriter for ChannelWriter {
    fn write(&self, data: RVec<u8>) -> FfiFuture<RResult<(), RString>> {
        let tx = self.tx.clone();
        async move {
            let (ack_tx, ack_rx) = oneshot::channel();
            if tx
                .send(WriterMsg::Data(Bytes::from(data.into_vec()), ack_tx))
                .is_err()
            {
                return RResult::RErr(RString::from("writer channel closed"));
            }
            match ack_rx.await {
                Ok(Ok(())) => RResult::ROk(()),
                Ok(Err(e)) => RResult::RErr(RString::from(e)),
                Err(_) => RResult::RErr(RString::from("writer bridge closed")),
            }
        }
        .into_ffi()
    }

    fn close(&self) {
        let _ = self.tx.send(WriterMsg::Close);
    }
}

/// Create an SDK [`Reader`] together with a [`ReaderBridge`] for the runner to push data in.
pub fn make_reader(uri: String) -> (Reader, ReaderBridge) {
    let (tx, rx) = mpsc::unbounded_channel();
    let inner = RawReader_TO::from_value(
        ChannelReader {
            rx: Arc::new(AsyncMutex::new(rx)),
        },
        TD_Opaque,
    );
    (Reader::from_parts(uri, inner), ReaderBridge { tx })
}

/// Create an SDK [`Writer`] together with a [`WriterBridge`] for the runner to drain data from.
pub fn make_writer(uri: String) -> (Writer, WriterBridge) {
    let (tx, rx) = mpsc::unbounded_channel();
    let inner = RawWriter_TO::from_value(ChannelWriter { tx }, TD_Opaque);
    (Writer::from_parts(uri, inner), WriterBridge { rx })
}

/// Runner-side handle for feeding data into an SDK [`Reader`].
pub struct ReaderBridge {
    tx: mpsc::UnboundedSender<Option<Bytes>>,
}

impl ReaderBridge {
    /// Push a data message.
    pub fn send(&self, data: Bytes) -> Result<(), String> {
        self.tx
            .send(Some(data))
            .map_err(|_| "reader bridge closed".to_string())
    }
    /// Signal EOF — no more data will follow.
    pub fn close(&self) {
        let _ = self.tx.send(None);
    }
}

/// Runner-side handle for draining data that a processor writes.
pub struct WriterBridge {
    rx: mpsc::UnboundedReceiver<WriterMsg>,
}

impl WriterBridge {
    /// Receive the next writer message (data or close).
    pub async fn recv(&mut self) -> Option<WriterMsg> {
        self.rx.recv().await
    }
}

// ── Thread-local channel pools (used during deserialization) ──────────────────

thread_local! {
    static READER_POOL: RefCell<HashMap<String, Reader>> = RefCell::new(HashMap::new());
    static WRITER_POOL: RefCell<HashMap<String, Writer>> = RefCell::new(HashMap::new());
}

impl<'de> serde::Deserialize<'de> for Reader {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
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
                     call ProcessorArgs::deserialize() to populate the pool",
                    chan.id
                ))
            })
        })
    }
}

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
                     call ProcessorArgs::deserialize() to populate the pool",
                    chan.id
                ))
            })
        })
    }
}

// ── ProcessorArgs ─────────────────────────────────────────────────────────────

/// All arguments handed to a processor at runtime.
///
/// The `readers` and `writers` maps are keyed by SHACL field name
/// (e.g. `"reader"`, `"input"`).  Call [`ProcessorArgs::deserialize`] to
/// unpack them into a typed config struct.
pub struct ProcessorArgs {
    /// Full JSON-LD argument object (Reader/Writer fields included).
    pub args: serde_json::Value,
    readers: HashMap<String, Reader>,
    writers: HashMap<String, Writer>,
}

impl ProcessorArgs {
    /// Construct a `ProcessorArgs`.  Normally called by the runner, not by processors.
    pub fn new(
        args: serde_json::Value,
        readers: HashMap<String, Reader>,
        writers: HashMap<String, Writer>,
    ) -> Self {
        ProcessorArgs {
            args,
            readers,
            writers,
        }
    }

    /// Build a `ProcessorArgs` from the raw FFI arguments handed across the
    /// ABI boundary. Called by [`rdfc_register!`]'s generated factory — not
    /// normally called directly by processor authors.
    #[doc(hidden)]
    pub fn from_raw(raw: RawProcessorArgs) -> Self {
        let args: serde_json::Value =
            serde_json::from_str(raw.args_json.as_str()).unwrap_or(serde_json::Value::Null);
        let readers = raw
            .readers
            .into_iter()
            .map(|nr| {
                let uri = nr.uri.into_string();
                (uri.clone(), Reader::from_parts(uri, nr.reader))
            })
            .collect();
        let writers = raw
            .writers
            .into_iter()
            .map(|nw| {
                let uri = nw.uri.into_string();
                (uri.clone(), Writer::from_parts(uri, nw.writer))
            })
            .collect();
        ProcessorArgs::new(args, readers, writers)
    }

    /// Deserialize the processor's JSON-LD config into a typed struct `C`.
    ///
    /// Any `Reader` or `Writer` fields in `C` are populated by matching the
    /// `@id` URI in the JSON-LD against the channels passed by the runner.
    pub fn deserialize<C: serde::de::DeserializeOwned>(&mut self) -> Result<C, String> {
        // Rekey by the channel's true URI (not whatever key `self.readers`/
        // `self.writers` happened to use) since that's what `@id` matches.
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

        READER_POOL.with(|p| p.borrow_mut().clear());
        WRITER_POOL.with(|p| p.borrow_mut().clear());

        result
    }
}

// ── Processor trait (ergonomic, phased lifecycle) ──────────────────────────────

/// Implemented by processor authors instead of hand-rolling an
/// `async fn(ProcessorArgs) -> Result<(), String>` and calling
/// [`ProcessorArgs::deserialize`] themselves.
///
/// `Args` is the processor's config type: [`rdfc_register!`] deserializes it
/// automatically from the JSON-LD arguments (any `Reader`/`Writer` fields
/// are populated the same way [`ProcessorArgs::deserialize`] does) and hands
/// the result to [`Processor::new`] — processor code never touches
/// [`ProcessorArgs`] directly.
///
/// Mirrors the three-phase lifecycle of RDF-Connect's JS/TS runner
/// (`Processor<T>` there): a processor only implements whichever phases it
/// actually needs — all three default to a no-op.
///
/// - `init` — allocate external resources (e.g. open a DB connection);
///   awaited once per processor, before the pipeline starts.
/// - `transform` — register / start consuming input channels; only needs to
///   have *started* (not finished) before any processor's `produce` starts.
/// - `produce` — emit data on output channels, starting the pipeline.
///
/// **`transform` and `produce` run concurrently** — the runner starts both
/// at (roughly) the same time and awaits them together, it does not wait
/// for `transform` to finish first. That's why both take `&self` rather
/// than `&mut self`: a processor that needs state in both phases (or that
/// needs to hand a `Reader`/`Writer` to whichever phase uses it) must use
/// its own interior synchronization. [`Slot`] covers the common case of a
/// resource consumed by exactly one phase.
///
/// ```rust,ignore
/// use rdfc_processor_sdk::{futures::StreamExt, Processor, Reader, Slot, Writer};
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Args { reader: Reader, writer: Writer }
///
/// struct Echo { reader: Slot<Reader>, writer: Slot<Writer> }
///
/// impl Processor for Echo {
///     type Args = Args;
///
///     fn new(args: Args) -> Self {
///         Echo { reader: Slot::new(args.reader), writer: Slot::new(args.writer) }
///     }
///
///     async fn transform(&self) -> Result<(), String> {
///         let writer = self.writer.take();
///         let mut stream = self.reader.take().into_stream();
///         while let Some(bytes) = stream.next().await {
///             writer.write(&bytes).await?;
///         }
///         writer.close();
///         Ok(())
///     }
/// }
///
/// rdfc_register!("https://w3id.org/rdf-connect#EchoProcessor", Echo);
/// ```
pub trait Processor: Send + Sync + 'static {
    /// The processor's config type, deserialized from JSON-LD arguments.
    type Args: serde::de::DeserializeOwned + Send;

    /// Construct the processor from its already-deserialized arguments.
    /// Purely synchronous setup — use `init` for anything that needs to run
    /// (and be awaited) asynchronously.
    fn new(args: Self::Args) -> Self;

    /// Phase 1 — allocate external resources. Defaults to a no-op.
    fn init(&self) -> impl std::future::Future<Output = Result<(), String>> + Send {
        async { Ok(()) }
    }

    /// Phase 2 — register/start consuming input channels. Defaults to a
    /// no-op. Runs concurrently with `produce` — see the trait-level docs.
    fn transform(&self) -> impl std::future::Future<Output = Result<(), String>> + Send {
        async { Ok(()) }
    }

    /// Phase 3 — emit data on output channels, starting the pipeline.
    /// Defaults to a no-op. Runs concurrently with `transform` — see the
    /// trait-level docs.
    fn produce(&self) -> impl std::future::Future<Output = Result<(), String>> + Send {
        async { Ok(()) }
    }
}

/// A `Sync` cell that can be handed off exactly once.
///
/// [`Processor::transform`]/[`Processor::produce`] take `&self` (so the
/// runner can run them concurrently), but a resource like a `Reader` or
/// `Writer` is still normally consumed by exactly one of them. `Slot` lets a
/// processor field hold such a resource and `take()` it out from whichever
/// phase actually uses it, panicking if that phase is (incorrectly) entered
/// twice or if the resource was never meant for it.
pub struct Slot<T> {
    inner: std::sync::Mutex<Option<T>>,
}

impl<T> Slot<T> {
    pub fn new(value: T) -> Self {
        Slot {
            inner: std::sync::Mutex::new(Some(value)),
        }
    }

    /// Take the value out. Panics if it was already taken.
    pub fn take(&self) -> T {
        self.inner
            .lock()
            .unwrap()
            .take()
            .expect("Slot::take() called twice")
    }
}

/// Adapts any [`Processor`] into the FFI-safe [`RawProcessor`] trait object
/// [`rdfc_register!`] exports. Not normally constructed directly by
/// processor authors.
///
/// Holds the processor behind an `Arc` (rather than borrowing `&mut self`)
/// so each phase method can hand `async_ffi` a `'static` future — and, since
/// [`Processor`]'s phase methods take `&self`, so `transform` and `produce`
/// can be polled concurrently without one blocking the other.
#[doc(hidden)]
pub struct RawProcessorWrapper<P: Processor> {
    inner: Arc<P>,
}

impl<P: Processor> RawProcessorWrapper<P> {
    pub fn new(processor: P) -> Self {
        RawProcessorWrapper {
            inner: Arc::new(processor),
        }
    }
}

impl<P: Processor> RawProcessor for RawProcessorWrapper<P> {
    fn init(&self) -> FfiFuture<RResult<(), RString>> {
        let inner = self.inner.clone();
        async move {
            match inner.init().await {
                Ok(()) => RResult::ROk(()),
                Err(e) => RResult::RErr(RString::from(e)),
            }
        }
        .into_ffi()
    }

    fn transform(&self) -> FfiFuture<RResult<(), RString>> {
        let inner = self.inner.clone();
        async move {
            match inner.transform().await {
                Ok(()) => RResult::ROk(()),
                Err(e) => RResult::RErr(RString::from(e)),
            }
        }
        .into_ffi()
    }

    fn produce(&self) -> FfiFuture<RResult<(), RString>> {
        let inner = self.inner.clone();
        async move {
            match inner.produce().await {
                Ok(()) => RResult::ROk(()),
                Err(e) => RResult::RErr(RString::from(e)),
            }
        }
        .into_ffi()
    }
}


// ── Test helpers ──────────────────────────────────────────────────────────────

/// Utilities for writing unit tests against processor functions.
pub mod testing {
    use super::{make_reader, ProcessorArgs, Reader, Writer, WriterBridge, WriterMsg};
    use bytes::Bytes;
    use std::collections::HashMap;

    /// Create a [`Reader`] pre-populated with `messages`.
    /// An EOF signal is automatically appended so the processor loop terminates.
    pub fn make_test_reader(uri: &str, messages: Vec<Vec<u8>>) -> Reader {
        let (reader, bridge) = make_reader(uri.to_string());
        for msg in messages {
            bridge.send(Bytes::from(msg)).expect("bridge open");
        }
        bridge.close();
        reader
    }

    /// Drain all messages from a [`WriterBridge`], auto-acking each write.
    ///
    /// Returns `(messages, was_closed)`.
    pub async fn collect_writer(mut bridge: WriterBridge) -> (Vec<Vec<u8>>, bool) {
        let mut received = Vec::new();
        let mut closed = false;
        while let Some(msg) = bridge.recv().await {
            match msg {
                WriterMsg::Data(data, ack) => {
                    received.push(data.to_vec());
                    let _ = ack.send(Ok(()));
                }
                WriterMsg::Close => {
                    closed = true;
                    break;
                }
            }
        }
        (received, closed)
    }

    /// Build a [`ProcessorArgs`] for testing from raw JSON and pre-built channels.
    pub fn make_test_args(
        json: serde_json::Value,
        readers: impl IntoIterator<Item = (&'static str, Reader)>,
        writers: impl IntoIterator<Item = (&'static str, Writer)>,
    ) -> ProcessorArgs {
        ProcessorArgs::new(
            json,
            readers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<HashMap<_, _>>(),
            writers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<HashMap<_, _>>(),
        )
    }

    /// Build a [`Processor`] instance for testing directly from raw JSON and
    /// pre-built channels — the same deserialize-then-`new` path
    /// [`rdfc_register!`] generates, without going through the FFI boundary.
    pub fn make_test_processor<P: super::Processor>(
        json: serde_json::Value,
        readers: impl IntoIterator<Item = (&'static str, Reader)>,
        writers: impl IntoIterator<Item = (&'static str, Writer)>,
    ) -> P {
        let mut args = make_test_args(json, readers, writers);
        let config: P::Args = args.deserialize().expect("valid processor args");
        P::new(config)
    }
}
