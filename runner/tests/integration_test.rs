//! Integration test: a mock orchestrator drives the Rust runner through a
//! full pipeline lifecycle (connect → identify → pipeline → proc × N → start → data flow).
//!
//! Pipeline under test:
//!   <sender>  (SendProcessor)  writes ["hello", "world"] to <ch1>
//!   <echo>    (EchoProcessor)  reads <ch1>, writes to <ch2>
//!   <log>     (LogProcessor)   reads <ch2>
//!
//! The mock orchestrator routes messages between processors by reflecting
//! `FromRunner::Msg` back as `ToRunner::Msg` + `ToRunner::Processed(LocalAck)`,
//! and forwards channel closes so each downstream reader sees EOF.
//!
//! Processor discovery: there is no `rdfc:file` / `builtin://` special-casing
//! — `proc.config` carries an `artifacts` list (as the orchestrator's SHACL
//! shape in `runner/index.ttl` would extract from a processor's own
//! `:artifact` declarations) naming a local `cdylib` path per target triple.
//! This test builds `processors/{echo,log,send}` itself (via `cargo build`)
//! and points each artifact entry straight at the resulting `.so`. The
//! processor's class URI comes from `@type` in the JSON-LD arguments, not
//! from `config`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

use rdfc_rust_runner::proto::rdfc::{
    from_runner::Inner as FromRunnerInner,
    runner_server::{Runner, RunnerServer},
    to_runner::Inner as ToRunnerInner,
    Close, DataChunk, FromRunner, LocalAck, LogMessage, Processor, ReceivingMessage,
    ReceivingStreamControl, RunnerIdentify, SendingStreamControl, StreamChunk, ToRunner,
};

// ── Mock orchestrator server ──────────────────────────────────────────────────

/// Minimal orchestrator that:
/// 1. Sends the scripted `Proc` + `Start` messages after receiving `Identify`.
/// 2. Routes `FromRunner::Msg` ↔ `ToRunner::Msg` + `LocalAck`.
/// 3. Forwards `FromRunner::Close` as `ToRunner::Close`.
/// 4. Records all `FromRunner` messages for assertions.
struct MockOrchestrator {
    from_runner_tx: mpsc::UnboundedSender<FromRunner>,
    script: Arc<Mutex<Vec<ToRunner>>>,
}

#[tonic::async_trait]
impl Runner for MockOrchestrator {
    #[allow(non_camel_case_types)]
    type connectStream = ReceiverStream<Result<ToRunner, Status>>;
    #[allow(non_camel_case_types)]
    type sendStreamMessageStream = ReceiverStream<Result<ReceivingStreamControl, Status>>;
    #[allow(non_camel_case_types)]
    type receiveStreamMessageStream = ReceiverStream<Result<DataChunk, Status>>;

    async fn connect(
        &self,
        request: Request<Streaming<FromRunner>>,
    ) -> Result<Response<Self::connectStream>, Status> {
        let (to_tx, to_rx) = mpsc::channel(256);
        let from_tx = self.from_runner_tx.clone();
        let script = Arc::clone(&self.script);

        let mut inbound = request.into_inner();
        let tx = to_tx.clone();

        tokio::spawn(async move {
            let mut global_seq = 0u32;
            let mut script_sent = false;

            while let Ok(Some(msg)) = inbound.message().await {
                let _ = from_tx.send(msg.clone());

                match &msg.inner {
                    // After the runner identifies, send Proc × N + Start.
                    Some(FromRunnerInner::Identify(_)) if !script_sent => {
                        script_sent = true;
                        let msgs = script.lock().unwrap().clone();
                        for m in msgs {
                            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                            if tx.send(Ok(m)).await.is_err() { break; }
                        }
                    }

                    // Route data: unblock the writer then forward to downstream readers.
                    Some(FromRunnerInner::Msg(sending)) => {
                        global_seq += 1;
                        // LocalAck first so write_blocking() unblocks ASAP.
                        let _ = tx.send(Ok(ToRunner {
                            inner: Some(ToRunnerInner::Processed(LocalAck {
                                channel: sending.channel.clone(),
                                local_sequence_number: sending.local_sequence_number,
                                error: None,
                            })),
                        })).await;
                        // Forward payload to the channel's reader.
                        let _ = tx.send(Ok(ToRunner {
                            inner: Some(ToRunnerInner::Msg(ReceivingMessage {
                                channel: sending.channel.clone(),
                                global_sequence_number: global_seq,
                                data: sending.data.clone(),
                            })),
                        })).await;
                    }

                    // Forward channel closes so downstream readers see EOF.
                    Some(FromRunnerInner::Close(c)) => {
                        let _ = tx.send(Ok(ToRunner {
                            inner: Some(ToRunnerInner::Close(Close {
                                channel: c.channel.clone(),
                            })),
                        })).await;
                    }

                    _ => {}
                }
            }
            // Inbound stream ended — dropping `tx` closes the outbound stream.
        });

        Ok(Response::new(ReceiverStream::new(to_rx)))
    }

    async fn send_stream_message(
        &self,
        _request: Request<Streaming<StreamChunk>>,
    ) -> Result<Response<Self::sendStreamMessageStream>, Status> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn receive_stream_message(
        &self,
        _request: Request<Streaming<SendingStreamControl>>,
    ) -> Result<Response<Self::receiveStreamMessageStream>, Status> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn log_stream(
        &self,
        _request: Request<Streaming<LogMessage>>,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build `package` (a workspace member) and return the path to its `cdylib`
/// — standing in for what a real pipeline author would do once, ahead of
/// time, rather than relying on any runtime auto-compile.
fn build_processor_so(package: &str, crate_name: &str) -> String {
    let status = std::process::Command::new("cargo")
        .args(["build", "-p", package])
        .status()
        .expect("failed to run cargo build — is cargo installed?");
    assert!(status.success(), "cargo build failed for '{package}'");

    let so_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../target/debug")
        .join(format!("lib{crate_name}.so"));
    assert!(
        so_path.is_file(),
        "expected '{}' to exist after building '{package}'",
        so_path.display()
    );
    so_path.to_string_lossy().into_owned()
}

/// Build a `proc.config` JSON string carrying a single-target `artifacts`
/// entry — what the orchestrator's SHACL shape (see `runner/index.ttl`)
/// would extract from a processor's `:artifact` declaration. No `:sha256`,
/// matching the "optional, for easier local testing" allowance.
fn make_config(so_path: &str) -> String {
    serde_json::json!({
        "artifacts": [
            { "target_triple": rdfc_rust_runner::registry::TARGET_TRIPLE, "path": so_path }
        ]
    })
    .to_string()
}

fn make_proc(uri: &str, config_json: &str, args_json: &str) -> ToRunner {
    ToRunner {
        inner: Some(ToRunnerInner::Proc(Processor {
            uri: uri.to_string(),
            config: config_json.to_string(),
            arguments: args_json.to_string(),
        })),
    }
}

fn make_start() -> ToRunner {
    ToRunner {
        inner: Some(ToRunnerInner::Start(())),
    }
}

// ── Test ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn runner_full_lifecycle() {
    let ch1 = "http://example.org/ch1";
    let ch2 = "http://example.org/ch2";

    let sender_args = serde_json::json!({
        "@type": "https://w3id.org/rdf-connect#SendProcessor",
        "msgs": ["hello", "world"],
        "writer": { "@type": "https://w3id.org/rdf-connect#Writer", "@id": ch1 }
    }).to_string();

    let echo_args = serde_json::json!({
        "@type": "https://w3id.org/rdf-connect#EchoProcessor",
        "reader": { "@type": "https://w3id.org/rdf-connect#Reader", "@id": ch1 },
        "writer": { "@type": "https://w3id.org/rdf-connect#Writer", "@id": ch2 }
    }).to_string();

    let log_args = serde_json::json!({
        "@type": "https://w3id.org/rdf-connect#LogProcessor",
        "reader": { "@type": "https://w3id.org/rdf-connect#Reader", "@id": ch2 }
    }).to_string();

    let send_so = build_processor_so("rdfc-processor-send", "rdfc_processor_send");
    let echo_so = build_processor_so("rdfc-processor-echo", "rdfc_processor_echo");
    let log_so  = build_processor_so("rdfc-processor-log",  "rdfc_processor_log");

    let script = vec![
        make_proc("http://example.org/sender", &make_config(&send_so), &sender_args),
        make_proc("http://example.org/echo",   &make_config(&echo_so), &echo_args),
        make_proc("http://example.org/log",    &make_config(&log_so),  &log_args),
        make_start(),
    ];

    let (from_tx, mut from_rx) = mpsc::unbounded_channel::<FromRunner>();

    let orchestrator = MockOrchestrator {
        from_runner_tx: from_tx,
        script: Arc::new(Mutex::new(script)),
    };

    // ── Start mock gRPC server ────────────────────────────────────────────────
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(RunnerServer::new(orchestrator))
            .serve_with_incoming(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
            )
            .await
            .unwrap();
    });

    // ── Start the runner ──────────────────────────────────────────────────────
    let runner_uri = "http://example.org/runner1";
    let runner_task = tokio::spawn(async move {
        rdfc_rust_runner::run(
            addr.to_string(),
            runner_uri.to_string(),
        )
        .await
        .unwrap();
    });

    // ── 1. Verify Identify ────────────────────────────────────────────────────
    let identify_msg = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        async { loop {
            if let Ok(msg) = from_rx.try_recv() { return msg; }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }},
    ).await.expect("runner should identify within 5 s");

    assert!(
        matches!(identify_msg.inner, Some(FromRunnerInner::Identify(RunnerIdentify { .. }))),
        "first message should be Identify, got {:?}", identify_msg.inner
    );

    // ── 2. Wait for 3 ProcessorInitialized messages ───────────────────────────
    // Processors need to compile on first run; allow up to 30 s.
    let mut initialized = std::collections::HashSet::new();
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    while initialized.len() < 3 && tokio::time::Instant::now() < deadline {
        if let Ok(msg) = from_rx.try_recv() {
            if let Some(FromRunnerInner::Initialized(p)) = msg.inner {
                assert!(p.error.is_none(), "processor init error: {:?}", p.error);
                initialized.insert(p.uri);
            }
        } else {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }
    assert_eq!(initialized.len(), 3, "expected 3 ProcessorInitialized, got {initialized:?}");

    // ── 3. Wait for the runner to complete the full pipeline ──────────────────
    // The mock routes messages back, so the pipeline runs to completion.
    // Allow up to 15 s after initialization for data flow.
    tokio::time::timeout(
        tokio::time::Duration::from_secs(15),
        runner_task,
    ).await
    .expect("runner should complete within 15 s after initialization")
    .expect("runner task should not panic");
}
