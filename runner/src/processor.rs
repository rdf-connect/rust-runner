//! Processor trait and the context object handed to each processor at startup.

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;

use crate::channel::{bridge_reader, bridge_writer, Reader, Writer};

// ── Trait ─────────────────────────────────────────────────────────────────────

/// A processor handles a discrete data-processing step in an RDF-Connect pipeline.
///
/// Three-phase lifecycle:
/// 1. `init`      – allocate external resources; awaited before `ProcessorInitialized`.
/// 2. `transform` – register listeners / start consuming input channels.
/// 3. `produce`   – emit data on output channels; called after the `start` signal.
#[async_trait]
pub trait Processor: Send {
    async fn init(&mut self) -> Result<()>;
    async fn transform(&mut self) -> Result<()>;
    async fn produce(&mut self) -> Result<()>;
}

// ── Context ───────────────────────────────────────────────────────────────────

/// Holds everything a processor needs at construction time.
pub struct ProcessorContext {
    pub uri: String,
    /// Parsed JSON-LD arguments (Reader/Writer fields already extracted).
    pub args: serde_json::Value,
    readers: HashMap<String, Reader>,
    writers: HashMap<String, Writer>,
}

impl ProcessorContext {
    pub fn new(
        uri: String,
        args: serde_json::Value,
        readers: HashMap<String, Reader>,
        writers: HashMap<String, Writer>,
    ) -> Self {
        ProcessorContext { uri, args, readers, writers }
    }

    /// Convert this context into a [`rdfc_processor_sdk::RawProcessorArgs`],
    /// ready to cross the FFI boundary into a processor `cdylib`.
    ///
    /// Bridge tasks (gRPC-backed runner channel ↔ raw FFI channel) are
    /// spawned on the current tokio executor.
    pub fn into_raw_args(self) -> rdfc_processor_sdk::RawProcessorArgs {
        use rdfc_processor_sdk::abi_stable::std_types::{RString, RVec};
        use rdfc_processor_sdk::{RawNamedReader, RawNamedWriter};

        let readers = self
            .readers
            .into_iter()
            .map(|(_, r)| {
                let uri = r.channel_uri().to_string();
                RawNamedReader {
                    uri: RString::from(uri),
                    reader: bridge_reader(r),
                }
            })
            .collect::<Vec<_>>();

        let writers = self
            .writers
            .into_iter()
            .map(|(_, w)| {
                let uri = w.channel_uri().to_string();
                RawNamedWriter {
                    uri: RString::from(uri),
                    writer: bridge_writer(w),
                }
            })
            .collect::<Vec<_>>();

        rdfc_processor_sdk::RawProcessorArgs {
            args_json: RString::from(serde_json::to_string(&self.args).unwrap_or_default()),
            readers: RVec::from(readers),
            writers: RVec::from(writers),
        }
    }
}

// ── Factory type ──────────────────────────────────────────────────────────────

/// A function that constructs a `Processor` from a `ProcessorContext`.
#[allow(dead_code)]
pub type ProcessorFactory =
    Box<dyn Fn(ProcessorContext) -> Result<Box<dyn Processor>> + Send + Sync>;
