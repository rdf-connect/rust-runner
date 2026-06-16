# rdfc-rust-runner

A [RDF-Connect](https://rdf-connect.github.io/specification/) runner for Rust processors.

Processors are standalone `cdylib` crates compiled to shared libraries (`.so` / `.dylib`). The runner loads them at runtime — **no recompilation of the runner itself is needed** when adding or changing processors.

---

## Architecture

```
orchestrator (grpc)
      │
      ▼
 rust-runner  ──────────────────────────────────────────────┐
      │                                                      │
  channel plumbing                                  processor-sdk
  (gRPC ↔ mpsc)                          (FFI ABI + async Reader/Writer)
      │                                                      │
  ┌───┴───────┐                                      ┌───────┴───────┐
  │  builtin  │  dlopen()                            │  your crate   │
  │ processors│◄─────────────────────────────────────│  (cdylib)     │
  └───────────┘                                      └───────────────┘
```

| Crate | Purpose |
|-------|---------|
| `rdfc-rust-runner` | gRPC runner binary — connects to the orchestrator, loads processors |
| `rdfc-processor-sdk` | Shared types, `Reader`/`Writer` wrappers, `rdfc_export!` macro |
| `processors/echo` | Built-in: forwards every message from reader → writer |
| `processors/log` | Built-in: prints every message to stderr |
| `processors/send` | Built-in: sends a predefined list of messages then closes |

---

## Prerequisites

- Rust (stable, 1.70+)
- `protoc` (Protocol Buffers compiler)
- Node.js + the [RDF-Connect orchestrator](https://github.com/rdf-connect/orchestrator-js)

```bash
npm install -g @rdfc/orchestrator   # or use npx rdfc
```

---

## Building

```bash
cd rust-runner
cargo build --release -p rdfc-rust-runner
# binary at target/release/rust-runner
```

Put `rust-runner` somewhere on `$PATH` (the orchestrator looks it up by name):

```bash
cp target/release/rust-runner /usr/local/bin/
```

---

## Running a pipeline

```bash
npx rdfc runner/index.ttl
```

The included `runner/index.ttl` runs a three-processor pipeline: **send → echo → log**.

---

## Writing a processor

Create a new `cdylib` crate and add `rdfc-processor-sdk` as a dependency:

```toml
# Cargo.toml
[lib]
crate-type = ["cdylib"]

[dependencies]
rdfc-processor-sdk = { path = "../processor-sdk" }
```

Implement your processor as an `async fn` that takes a `#[derive(Deserialize)]` config struct. `Reader` and `Writer` fields are injected automatically from the pipeline arguments:

```rust
use rdfc_processor_sdk::{futures::StreamExt, *};

#[derive(Deserialize)]
struct Config {
    input:  Reader,
    output: Writer,
    label:  Option<String>,
}

async fn my_processor(Config { input, output, label }: Config) -> Result<(), String> {
    let label = label.unwrap_or_else(|| "proc".into());
    let mut stream = input.into_stream();
    while let Some(bytes) = stream.next().await {
        eprintln!("[{label}] {} bytes", bytes.len());
        output.write(&bytes).await?;
    }
    // Writer sends Close automatically on Drop
    Ok(())
}

rdfc_export!(my_processor);
```

### Field names

Field names in the config struct must match the `sh:name` values declared in the processor's SHACL shape in your pipeline `.ttl`.

### Reader / Writer

| Method | Description |
|--------|-------------|
| `reader.read().await` | Next message as `Option<Vec<u8>>` — `None` when closed |
| `reader.into_stream()` | Consume as a `Stream<Item = Vec<u8>>` |
| `writer.write(&bytes).await` | Send bytes (awaits `LocalAck` from orchestrator) |
| `writer.write_str(s).await` | Send a UTF-8 string |
| `writer.close()` | Signal no more messages (also called automatically on `Drop`) |

---

## Registering a processor in a pipeline

Processors are referenced via `rdfc:file`. Three forms are supported:

| Value | Meaning |
|-------|---------|
| `builtin://echo` | Built-in processor compiled into the runner |
| `/abs/path/to/libmy_proc.so` | Pre-compiled shared library |
| `/abs/path/to/my_proc/` | Cargo project directory — compiled on first run, cached |

Example pipeline TTL:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix :     <https://w3id.org/rdf-connect/rust#>.

rdfc:RustRunner a rdfc:Runner;
  rdfc:handlesSubjectsOf :rustImplementationOf;
  rdfc:command "rust-runner".

rdfc:MyProcessor :rustImplementationOf rdfc:Processor;
  rdfc:file </path/to/my_proc/>.   # Cargo project, compiled on first use

[ ] a sh:NodeShape;
  sh:targetClass rdfc:MyProcessor;
  sh:property
    [ sh:path rdfc:input;  sh:name "input";  sh:class rdfc:Reader ],
    [ sh:path rdfc:output; sh:name "output"; sh:class rdfc:Writer ].

<> a rdfc:Pipeline;
  rdfc:consistsOf [
    rdfc:instantiates rdfc:RustRunner;
    rdfc:processor <my-proc>;
  ].

<ch> a rdfc:Writer, rdfc:Reader.
<my-proc> a rdfc:MyProcessor; rdfc:input <ch>; rdfc:output <ch>.
```

---

## Testing a processor

The SDK ships a `testing` module with mock channels:

```rust
#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::testing::*;
    use rdfc_processor_sdk::RdfcArgs;

    #[test]
    fn it_echoes() {
        let (_, reader_ch)      = make_mock_reader("mock://in",  vec![b"hello".to_vec()]);
        let (writer_state, writer_ch) = make_mock_writer("mock://out");
        let reader_nc = make_named("input",  reader_ch);
        let writer_nc = make_named("output", writer_ch);

        let args_json = br#"{
            "input":  {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://in"},
            "output": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://out"}
        }"#;

        let ffi_args = RdfcArgs {
            args_json: args_json.as_ptr(), args_json_len: args_json.len(),
            readers: &reader_nc, n_readers: 1,
            writers: &writer_nc, n_writers: 1,
        };

        assert_eq!(unsafe { rdfc_run(&ffi_args) }, 0);
        assert_eq!(*writer_state.received.lock().unwrap(), vec![b"hello".to_vec()]);
    }
}
```

Run tests with:

```bash
cargo test --workspace
```

---

## Known limitations (MVP)

- **Streaming messages** (`StreamMsg`) are acknowledged but not delivered; large-payload streaming is not yet implemented.
- **GlobalAck backpressure** is sent when a message is dequeued by the runner, not after the processor has finished processing it. This means the orchestrator may pipeline more messages than strictly necessary.
