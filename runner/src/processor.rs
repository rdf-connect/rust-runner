//! Processor trait and the context object handed to each processor at startup.

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;

use crate::channel::{make_ffi_reader_channel, make_ffi_writer_channel, Reader, Writer};
use rdfc_processor_sdk::RdfcChannel;

// ── Trait ─────────────────────────────────────────────────────────────────────

/// A processor handles a discrete data-processing step in an RDF-Connect pipeline.
///
/// The three-phase lifecycle mirrors the JS and JVM runner implementations:
///
/// 1. `init`      – allocate external resources (DB connections, file handles, …).
///                  Awaited before sending `ProcessorInitialized` to the orchestrator.
/// 2. `transform` – register listeners / start consuming input channels.  
///                  Started (but not awaited) right after `init`.
/// 3. `produce`   – emit data on output channels.  
///                  Called in parallel for all processors after the `start` signal.
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
        ProcessorContext {
            uri,
            args,
            readers,
            writers,
        }
    }

    /// Move a reader out of the context by field name.
    pub fn take_reader(&mut self, name: &str) -> Result<Reader> {
        self.readers
            .remove(name)
            .ok_or_else(|| anyhow::anyhow!("processor '{}': no reader named '{name}'", self.uri))
    }

    /// Move a writer out of the context by field name.
    pub fn take_writer(&mut self, name: &str) -> Result<Writer> {
        self.writers
            .remove(name)
            .ok_or_else(|| anyhow::anyhow!("processor '{}': no writer named '{name}'", self.uri))
    }
    /// Consume the context and convert readers/writers to FFI channel descriptors
    /// for use with dynamically loaded processors.
    pub fn into_ffi_channels(
        self,
    ) -> (
        Vec<(String, RdfcChannel)>,
        Vec<(String, RdfcChannel)>,
    ) {
        let reader_channels = self
            .readers
            .into_iter()
            .map(|(name, reader)| {
                let uri = reader.channel_uri().to_string();
                (name, make_ffi_reader_channel(uri, reader))
            })
            .collect();

        let writer_channels = self
            .writers
            .into_iter()
            .map(|(name, writer)| {
                let uri = writer.channel_uri().to_string();
                (name, make_ffi_writer_channel(uri, writer))
            })
            .collect();

        (reader_channels, writer_channels)
    }
}

// ── Factory type ──────────────────────────────────────────────────────────────

/// A function that constructs a `Processor` from a `ProcessorContext`.
#[allow(dead_code)]
pub type ProcessorFactory =
    Box<dyn Fn(ProcessorContext) -> Result<Box<dyn Processor>> + Send + Sync>;
