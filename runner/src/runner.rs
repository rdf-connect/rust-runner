//! Core runner implementation.
//!
//! The runner:
//! 1. Connects to the orchestrator via gRPC bidirectional streaming.
//! 2. Identifies itself.
//! 3. Receives and instantiates processors.
//! 4. Dispatches incoming channel messages to the correct reader.
//! 5. Routes `LocalAck` back to the correct writer.
//! 6. Starts all processors on the `start` signal.
//! 7. Closes the stream when all processors finish.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, info, warn};

use crate::channel::{Reader, ReaderMsg, ReaderRegistry, Writer, WriterAckRegistry};
use crate::processor::{Processor, ProcessorContext};
use crate::proto::rdfc::{
    from_runner::Inner as FromRunnerInner, to_runner::Inner as ToRunnerInner, FromRunner,
    ProcessorInitialized, RunnerIdentify,
};
use crate::registry::{ProcessorConfig, ProcessorRegistry};

// ── JSON-LD channel extraction ────────────────────────────────────────────────

const RDFC_READER: &str = "https://w3id.org/rdf-connect#Reader";
const RDFC_WRITER: &str = "https://w3id.org/rdf-connect#Writer";

/// Walk a JSON-LD value and collect all Reader / Writer channel references.
///
/// Returns `(readers, writers)` where each entry is `(field_name, channel_uri)`.
fn extract_channels(
    value: &serde_json::Value,
    readers: &mut Vec<(String, String)>,
    writers: &mut Vec<(String, String)>,
) {
    match value {
        serde_json::Value::Object(map) => {
            let type_str = map
                .get("@type")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let id_str = map.get("@id").and_then(|v| v.as_str()).unwrap_or_default();

            // This object itself is a channel reference
            if type_str == RDFC_READER && !id_str.is_empty() {
                // The field name is provided by the caller (see recursive arm)
                return;
            }
            if type_str == RDFC_WRITER && !id_str.is_empty() {
                return;
            }

            for (key, val) in map {
                if key.starts_with('@') {
                    continue;
                }
                match val {
                    serde_json::Value::Object(inner) => {
                        let t = inner
                            .get("@type")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        let id = inner
                            .get("@id")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        if t == RDFC_READER && !id.is_empty() {
                            readers.push((key.clone(), id.to_string()));
                        } else if t == RDFC_WRITER && !id.is_empty() {
                            writers.push((key.clone(), id.to_string()));
                        } else {
                            extract_channels(val, readers, writers);
                        }
                    }
                    serde_json::Value::Array(arr) => {
                        // A multi-valued Reader/Writer property (e.g. an
                        // `sh:property` with no `sh:maxCount`) shows up here
                        // as an array of `{"@type": Reader|Writer, "@id": ...}`
                        // objects. Recursing straight into `extract_channels`
                        // would lose `key` (its "field name is provided by
                        // the caller" branch only fires for the *singular*
                        // case, reached via the Object arm above), silently
                        // dropping every channel in the array. Check each
                        // item against the same Reader/Writer shape first,
                        // still tagged with this property's `key`, and only
                        // fall back to a plain recursive walk for items that
                        // aren't themselves channel references.
                        for item in arr {
                            if let serde_json::Value::Object(inner) = item {
                                let t = inner
                                    .get("@type")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or_default();
                                let id =
                                    inner.get("@id").and_then(|v| v.as_str()).unwrap_or_default();
                                if t == RDFC_READER && !id.is_empty() {
                                    readers.push((key.clone(), id.to_string()));
                                    continue;
                                } else if t == RDFC_WRITER && !id.is_empty() {
                                    writers.push((key.clone(), id.to_string()));
                                    continue;
                                }
                            }
                            extract_channels(item, readers, writers);
                        }
                    }
                    _ => {}
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                extract_channels(item, readers, writers);
            }
        }
        _ => {}
    }
}

// ── RunnerState ───────────────────────────────────────────────────────────────

pub struct RunnerState {
    #[allow(dead_code)]
    runner_uri: String,
    /// Send `FromRunner` messages to the orchestrator.
    orch_tx: mpsc::UnboundedSender<FromRunner>,
    /// Dispatch incoming channel messages to readers.
    reader_registry: ReaderRegistry,
    /// Route `LocalAck` events back to waiting writers.
    writer_ack_registry: WriterAckRegistry,
    /// Processor factory registry.
    proc_registry: ProcessorRegistry,
    /// Initialised processors waiting for the `start` signal.
    processors: Vec<Box<dyn Processor>>,
    /// Background tasks for all running processors.
    tasks: JoinSet<()>,
}

impl RunnerState {
    /// Create a new runner state. Processors are resolved on demand as `Proc`
    /// messages arrive, by loading (and caching) the `cdylib` their
    /// `config.file` points at — see [`crate::registry`].
    pub fn new(runner_uri: String, orch_tx: mpsc::UnboundedSender<FromRunner>) -> Self {
        RunnerState {
            runner_uri,
            orch_tx,
            reader_registry: Arc::new(Mutex::new(HashMap::new())),
            writer_ack_registry: Arc::new(Mutex::new(HashMap::new())),
            proc_registry: ProcessorRegistry::new(),
            processors: Vec::new(),
            tasks: JoinSet::new(),
        }
    }

    // ── Message dispatch ──────────────────────────────────────────────────────

    pub async fn handle(&mut self, msg: crate::proto::rdfc::ToRunner) -> Result<()> {
        match msg.inner {
            Some(ToRunnerInner::Pipeline(ttl)) => {
                debug!("received pipeline ({} bytes)", ttl.len());
                // The pipeline Turtle is available for processors that want it,
                // but the runner relies on JSON-LD arguments instead.
            }

            Some(ToRunnerInner::Proc(proc_msg)) => {
                self.handle_proc(proc_msg).await?;
            }

            Some(ToRunnerInner::Start(_)) => {
                self.start_processors().await;
            }

            Some(ToRunnerInner::Msg(m)) => {
                self.dispatch_msg(
                    m.channel.clone(),
                    Bytes::from(m.data),
                    m.global_sequence_number,
                );
            }

            Some(ToRunnerInner::StreamMsg(sm)) => {
                warn!(
                    "streaming messages not yet implemented (channel '{}'), sending GlobalAck",
                    sm.channel
                );
                // Send GlobalAck so the orchestrator's flow-control doesn't stall.
                use crate::proto::rdfc::{from_runner::Inner as FRI, GlobalAck};
                let _ = self.orch_tx.send(crate::proto::rdfc::FromRunner {
                    inner: Some(FRI::Processed(GlobalAck {
                        global_sequence_number: sm.global_sequence_number,
                        channel: sm.channel,
                        error: Some("streaming not implemented".to_string()),
                    })),
                });
            }

            Some(ToRunnerInner::Close(c)) => {
                self.close_reader(&c.channel);
            }

            Some(ToRunnerInner::Processed(ack)) => {
                self.deliver_writer_ack(&ack.channel, ack.local_sequence_number, ack.error);
            }

            None => {
                warn!("received empty ToRunner message");
            }
        }
        Ok(())
    }

    // ── Processor setup ───────────────────────────────────────────────────────

    async fn handle_proc(&mut self, proc_msg: crate::proto::rdfc::Processor) -> Result<()> {
        let uri = proc_msg.uri.clone();
        let result = self.try_setup_proc(proc_msg).await;
        if let Err(ref e) = result {
            self.send_initialized(&uri, Some(format!("{e:#}")));
        }
        result
    }

    async fn try_setup_proc(&mut self, proc_msg: crate::proto::rdfc::Processor) -> Result<()> {
        let uri = proc_msg.uri.clone();
        info!("setting up processor '{uri}'");

        let config: ProcessorConfig = serde_json::from_str(&proc_msg.config)
            .with_context(|| format!("parsing config for processor '{uri}'"))?;

        let args: serde_json::Value = serde_json::from_str(&proc_msg.arguments)
            .with_context(|| format!("parsing arguments for processor '{uri}'"))?;

        // The processor's class URI (`@type` in the JSON-LD arguments) selects
        // which exported `Registration` to run from the loaded `cdylib`.
        let type_uri = args
            .get("@type")
            .and_then(|v| v.as_str())
            .with_context(|| format!("processor '{uri}' arguments have no '@type'"))?
            .to_string();

        // Extract channel URIs from JSON-LD arguments
        let mut reader_refs: Vec<(String, String)> = Vec::new(); // (field_name, channel_uri)
        let mut writer_refs: Vec<(String, String)> = Vec::new();
        extract_channels(&args, &mut reader_refs, &mut writer_refs);

        // Create Reader instances. A `Vec`, not a `HashMap`: multiple entries
        // can (and for a multi-valued Reader property, will) share the same
        // `field_name` — see `ProcessorContext`'s own doc comment.
        let mut readers = Vec::with_capacity(reader_refs.len());
        for (field_name, channel_uri) in reader_refs {
            let reader = self.get_or_create_reader(channel_uri.clone());
            readers.push((field_name, reader));
        }

        // Create Writer instances
        let mut writers = Vec::with_capacity(writer_refs.len());
        for (field_name, channel_uri) in writer_refs {
            let writer = self.get_or_create_writer(channel_uri.clone());
            writers.push((field_name, writer));
        }

        let ctx = ProcessorContext::new(uri.clone(), args, readers, writers);

        // Instantiate (may compile/download the processor's cdylib on first run)
        let processor = self
            .proc_registry
            .create(&config, &type_uri, ctx)
            .await
            .with_context(|| format!("creating processor '{uri}'"))?;

        // Phase 1: init (awaited; signals readiness to orchestrator)
        if let Err(e) = processor.init().await {
            self.send_initialized(&uri, Some(format!("{e:#}")));
            return Ok(());
        }

        self.processors.push(processor);
        self.send_initialized(&uri, None);

        Ok(())
    }

    // ── Start signal ──────────────────────────────────────────────────────────

    async fn start_processors(&mut self) {
        info!(
            "start signal received — launching {} processors",
            self.processors.len()
        );

        // `transform` and `produce` run *concurrently* within one processor
        // (an `Arc` — not a `Box` — is what lets both hold a reference at
        // once): `transform` only needs to have *started*, not finished,
        // before any processor's `produce` starts. A processor that reads
        // forever in `transform` (e.g. Echo) must not block a sibling
        // processor's `produce` from ever starting, nor its own `produce`
        // (for processors that both consume and independently emit).
        //
        // Tasks also run concurrently across processors, so a SendProcessor's
        // produce() can write while an EchoProcessor's transform() is blocked
        // reading — exactly the data-flow topology the pipeline describes.
        for proc in self.processors.drain(..) {
            let proc: std::sync::Arc<dyn Processor> = std::sync::Arc::from(proc);
            let transform_proc = proc.clone();
            let produce_proc = proc.clone();
            self.tasks.spawn(async move {
                let (transform_result, produce_result) =
                    tokio::join!(transform_proc.transform(), produce_proc.produce());
                if let Err(e) = transform_result {
                    error!("transform error: {e:#}");
                }
                if let Err(e) = produce_result {
                    error!("produce error: {e:#}");
                }
            });
        }
    }

    /// Wait for all processor tasks to finish, then return.
    pub async fn wait_done(&mut self) {
        while self.tasks.join_next().await.is_some() {}
    }

    /// Drain channel registries so blocking processor threads can unblock and exit.
    ///
    /// Dropping the reader senders causes `blocking_recv()` to return `None`.
    /// Dropping the writer ack senders causes `write_blocking()` to return an error.
    /// Both signals tell the processor to stop, allowing `spawn_blocking` threads to finish.
    pub fn drain_channels(&self) {
        self.reader_registry.lock().unwrap().clear();
        self.writer_ack_registry.lock().unwrap().clear();
    }

    // ── Channel helpers ───────────────────────────────────────────────────────

    fn get_or_create_reader(&mut self, channel_uri: String) -> Reader {
        let (tx, rx) = mpsc::unbounded_channel();
        self.reader_registry
            .lock()
            .unwrap()
            .entry(channel_uri.clone())
            .or_insert_with(Vec::new)
            .push(tx);
        Reader::new(channel_uri, rx, self.orch_tx.clone())
    }

    fn get_or_create_writer(&mut self, channel_uri: String) -> Writer {
        Writer::new(
            channel_uri,
            self.orch_tx.clone(),
            Arc::clone(&self.writer_ack_registry),
        )
    }

    fn dispatch_msg(&self, channel: String, data: Bytes, global_seq: u32) {
        debug!(channel = %channel, seq = global_seq, "dispatching message");
        if let Some(senders) = self.reader_registry.lock().unwrap().get(&channel) {
            for tx in senders {
                let _ = tx.send(ReaderMsg::Data(data.clone(), global_seq));
            }
        } else {
            warn!("no reader for channel '{channel}'");
        }
    }

    fn close_reader(&self, channel: &str) {
        if let Some(senders) = self.reader_registry.lock().unwrap().get(channel) {
            for tx in senders {
                let _ = tx.send(ReaderMsg::Close);
            }
        }
    }

    fn deliver_writer_ack(&self, channel: &str, local_seq: u32, error: Option<String>) {
        debug!(channel = %channel, seq = local_seq, "delivering LocalAck");
        // The ack is stored directly in writer_ack_registry keyed by (channel, seq)
        if let Some(tx) = self
            .writer_ack_registry
            .lock()
            .unwrap()
            .remove(&(channel.to_string(), local_seq))
        {
            let _ = tx.send(error);
        } else {
            warn!("no pending ack for channel '{channel}' seq {local_seq}");
        }
    }

    // ── Outbound helpers ──────────────────────────────────────────────────────

    fn send_initialized(&self, uri: &str, error: Option<String>) {
        use crate::proto::rdfc::Error;
        let msg = FromRunner {
            inner: Some(FromRunnerInner::Initialized(ProcessorInitialized {
                uri: uri.to_string(),
                error: error.map(|cause| Error { cause }),
            })),
        };
        let _ = self.orch_tx.send(msg);
    }
}

// ── Top-level run() functions ─────────────────────────────────────────────────

/// Connect to the orchestrator.
///
/// Processors are discovered automatically via [`inventory`]: any processor
/// crate linked into the binary via `use rdfc_processor_foo as _;` is
/// registered at startup and will be found here.
///
/// # Example
/// ```ignore
/// // In your binary's main.rs:
/// rdfc_rust_runner::run(addr, uri).await?;
/// ```
pub async fn run(addr: String, runner_uri: String) -> Result<()> {
    // Use tonic::client::Grpc directly so we can call `rpc connect` by its exact
    // URL path — the generated RunnerClient would produce E0592 because tonic also
    // generates a same-named transport-level `connect<D>(dst)` in the same impl block.
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .context("invalid orchestrator address")?
        .connect_lazy();

    let mut grpc = tonic::client::Grpc::new(channel);

    // poll_ready() must be called before any RPC — mirroring what the generated client does.
    grpc.ready()
        .await
        .map_err(|e| anyhow::anyhow!("orchestrator not ready: {e}"))?;

    let (orch_tx, orch_rx) = mpsc::unbounded_channel::<FromRunner>();

    // grpc-js (used by the orchestrator) sends response headers lazily — only on first write().
    // To break the deadlock (server waits for Identify before writing; client waits for response
    // headers before sending Identify), queue the Identify *before* calling streaming() so the
    // h2 transport sends it as the first DATA frame during the handshake.
    orch_tx.send(FromRunner {
        inner: Some(FromRunnerInner::Identify(RunnerIdentify {
            uri: runner_uri.clone(),
        })),
    })?;

    let outbound = UnboundedReceiverStream::new(orch_rx);

    let path = http::uri::PathAndQuery::from_static("/rdfc.Runner/connect");
    let codec = tonic::codec::ProstCodec::<FromRunner, crate::proto::rdfc::ToRunner>::default();

    let mut inbound = grpc
        .streaming(tonic::Request::new(outbound), path, codec)
        .await
        .context("connecting to orchestrator")?
        .into_inner();

    info!("connected to orchestrator at {addr}");
    info!("identified as '{runner_uri}'");

    let mut state = RunnerState::new(runner_uri, orch_tx);

    // Main message loop — exits when:
    //   a) the orchestrator closes the stream, OR
    //   b) all processor tasks have completed (pipeline done)
    let mut processors_started = false;
    loop {
        tokio::select! {
            msg = inbound.message() => {
                match msg {
                    Ok(Some(to_runner_msg)) => {
                        if let Err(e) = state.handle(to_runner_msg).await {
                            error!("error handling orchestrator message: {e}");
                        }
                        // Track whether Start has been processed (tasks now live)
                        if !processors_started && !state.tasks.is_empty() {
                            processors_started = true;
                        }
                    }
                    Ok(None) => {
                        info!("orchestrator stream closed");
                        break;
                    }
                    Err(e) => {
                        error!("gRPC stream error: {e}");
                        break;
                    }
                }
            }
            // Once started, monitor tasks so we can exit when all processors finish.
            Some(_) = state.tasks.join_next(), if processors_started => {
                if state.tasks.is_empty() {
                    info!("all processor tasks completed — closing connection");
                    break;
                }
            }
        }
    }

    // Drain channel registries so any blocking processor threads can unblock and stop.
    state.drain_channels();

    // Wait for all processors to finish
    state.wait_done().await;
    info!("all processors finished");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_singular_reader_and_writer() {
        let args = json!({
            "reader": {"@type": RDFC_READER, "@id": "urn:in"},
            "writer": {"@type": RDFC_WRITER, "@id": "urn:out"}
        });
        let mut readers = Vec::new();
        let mut writers = Vec::new();
        extract_channels(&args, &mut readers, &mut writers);
        assert_eq!(readers, vec![("reader".to_string(), "urn:in".to_string())]);
        assert_eq!(writers, vec![("writer".to_string(), "urn:out".to_string())]);
    }

    #[test]
    fn extracts_every_reader_in_a_multi_valued_array_property() {
        // A multi-valued SHACL property (no sh:maxCount) — e.g. kolibrie-processor's
        // `readers: Vec<Reader>` — serialises as a JSON array of Reader objects
        // under one field name.
        let args = json!({
            "readers": [
                {"@type": RDFC_READER, "@id": "urn:a"},
                {"@type": RDFC_READER, "@id": "urn:b"},
                {"@type": RDFC_READER, "@id": "urn:c"}
            ]
        });
        let mut readers = Vec::new();
        let mut writers = Vec::new();
        extract_channels(&args, &mut readers, &mut writers);
        assert_eq!(
            readers,
            vec![
                ("readers".to_string(), "urn:a".to_string()),
                ("readers".to_string(), "urn:b".to_string()),
                ("readers".to_string(), "urn:c".to_string()),
            ]
        );
        assert!(writers.is_empty());
    }

    #[test]
    fn array_property_mixing_channels_with_other_values_only_extracts_channels() {
        let args = json!({
            "readers": [
                {"@type": RDFC_READER, "@id": "urn:a"},
                {"not": "a channel"}
            ]
        });
        let mut readers = Vec::new();
        let mut writers = Vec::new();
        extract_channels(&args, &mut readers, &mut writers);
        assert_eq!(readers, vec![("readers".to_string(), "urn:a".to_string())]);
    }
}
