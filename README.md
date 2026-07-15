# rdfc-rust-runner

A [RDF-Connect](https://rdf-connect.github.io/specification/) runner for Rust processors.

Processors are `cdylib` shared libraries (`.so` / `.dylib` / `.dll`) loaded at
runtime via [`abi_stable`](https://github.com/rodrimati1992/abi_stable_crates)
— **no recompilation of the runner itself is needed** when adding or changing
processors, and there's no `rdfc:file` / "builtin" special-casing either.
Using a processor is just `owl:import`ing its `processor.ttl` (locally, or
straight from a GitHub Release — see `kolibrie-processor/` for a full example
with a multi-target-triple release, including the GitHub Actions workflow
that builds and publishes one). That file declares the processor's class,
its argument shape, and an `:artifact` table (one entry per target triple,
each with a path and a `:sha256`); a shared SHACL shape (`runner/index.ttl`)
extracts that table straight into the orchestrator's process config for the
runner — no Turtle parsing happens in the runner itself. The runner matches
its own compiled triple against the table, downloads and checksum-verifies
the right artifact if it's remote, caches it, and loads it.

---

## Architecture

```
                     owl:imports processor.ttl (class + shape + :artifact)
pipeline.ttl ───────────────────────────────────────────────────────┐
      │                                                              │
      ▼                                                              ▼
orchestrator (grpc)                                    generic SHACL shape (index.ttl)
      │                                          extracts :artifact/:abiStable → JSON
      ▼                                                              │
 rust-runner ◄────────────────────── proc.config = {abi_token, artifacts[]} 
      │                                                           │
  channel plumbing                                       processor-sdk
  (gRPC ↔ mpsc)                             (abi_stable ABI + async_ffi Futures)
      │                                                           │
  ┌───┴─────────────────────────┐                        ┌───────┴───────┐
  │ registry (runner/src/registry.rs)                     │  your crate   │
  │  • pick :artifact for this runner's own target triple │  (cdylib,     │
  │  • http(s):// path → download + verify sha256         │  abi_stable-  │
  │  • local path → open directly, verify if sha256 given │   exported)   │
  │  • load via abi_stable (RawLibrary/LibHeader)         └───────┬───────┘
  └─────────────────────────────┬───────────────────────────────────┘
                                 ▼
                         loaded cdylib, dlopen'd
```

| Crate | Purpose |
|-------|---------|
| `rdfc-rust-runner` | gRPC runner binary — connects to the orchestrator, resolves + loads processor `cdylib`s |
| `rdfc-processor-sdk` | Shared `abi_stable` FFI types, `Reader`/`Writer` wrappers, `rdfc_register!` macro, `ABI_TOKEN` |
| `processors/echo` | Example: forwards every message from reader → writer |
| `processors/log` | Example: prints every message to stderr |
| `processors/send` | Example: sends a predefined list of messages then closes |
| `kolibrie-processor` | Out-of-tree example: a "real" processor published as a multi-target `cdylib` release |
| `examples/echo-pipeline` | A runnable pipeline `owl:import`ing the three example processors above |

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
cd examples/echo-pipeline
npx rdfc pipeline.ttl
```

`examples/echo-pipeline/pipeline.ttl` runs a three-processor pipeline
(**send → echo → log**) built entirely from `owl:imports`: one for
`runner/index.ttl` (the `rdfc:RustRunner` definition and the shared
SHACL shape), and one each for `processors/{send,echo,log}/processor.ttl`.

---

## Writing a processor

Create a new `cdylib` crate and add `rdfc-processor-sdk` (plus `abi_stable`
itself — required directly, see below) as dependencies:

```toml
# Cargo.toml
[lib]
crate-type = ["cdylib", "rlib"]  # rlib is only so `cargo test` works directly

[dependencies]
rdfc-processor-sdk = { path = "../processor-sdk" }
# `abi_stable`'s `#[export_root_module]` macro expands to code with an
# absolute `::abi_stable::...` path, so it must be a direct dependency here
# too, pinned to the exact version rdfc-processor-sdk uses.
abi_stable = "=0.11.3"
```

Implement your processor as a type implementing [`Processor`], with an
associated `Args` config type deserialized automatically — `Reader` and
`Writer` fields are populated from the pipeline arguments the same way
before your `new` is called. The three-phase lifecycle (`init` /
`transform` / `produce`) mirrors RDF-Connect's JS/TS runner; implement only
whichever phases your processor needs — all three default to a no-op.
`transform` and `produce` take `&self` (not `&mut self`) because **they run
concurrently**: the runner starts both together and awaits them together,
it does not wait for `transform` to finish before starting `produce`. Use
[`Slot`] for a resource (like a `Reader`/`Writer`) that's consumed by
exactly one of the two phases:

```rust
use rdfc_processor_sdk::{futures::StreamExt, Processor, Reader, Slot, Writer};
use serde::Deserialize;

#[derive(Deserialize)]
struct Args {
    input:  Reader,
    output: Writer,
    label:  Option<String>,
}

struct MyProcessor {
    input: Slot<Reader>,
    output: Slot<Writer>,
    label: String,
}

impl Processor for MyProcessor {
    type Args = Args;

    fn new(args: Args) -> Self {
        MyProcessor {
            input: Slot::new(args.input),
            output: Slot::new(args.output),
            label: args.label.unwrap_or_else(|| "proc".into()),
        }
    }

    // Runs concurrently with `produce` (see above) — only its *start* is
    // ordered relative to every other processor's `produce`.
    async fn transform(&self) -> Result<(), String> {
        let output = self.output.take();
        let mut stream = self.input.take().into_stream();
        while let Some(bytes) = stream.next().await {
            eprintln!("[{}] {} bytes", self.label, bytes.len());
            output.write(&bytes).await?;
        }
        output.close();
        Ok(())
    }
}

rdfc_processor_sdk::rdfc_register!("https://example.org/my-vocab#MyProcessor", MyProcessor);
```

`rdfc_register!` is the crate's single exported entry point (one processor
type per `cdylib`) — it generates the `#[export_root_module]` the runner
loads, so processor registration survives `cdylib` symbol stripping without
needing any link-time collection tricks.

### Field names

Field names in the config struct must match the `sh:name` values declared in the processor's SHACL shape in your pipeline `.ttl`.

### Lifecycle phases

| Phase | When | Typical use |
|-------|------|-------------|
| `init` | Awaited once per processor, before the pipeline starts | Allocate external resources (DB connections, etc.) |
| `transform` | Started for every processor before any processor's `produce` starts, then runs *concurrently* with `produce` | Register/start consuming input channels |
| `produce` | Started once every processor has started `transform`, running concurrently with it | Emit data on output channels, starting the pipeline |

A processor is only considered finished once both `transform` and `produce`
have returned — since they run concurrently, a processor that reads forever
in `transform` (like `echo`, above) doesn't block its own `produce` (or any
other processor's) from running.

### Reader / Writer

| Method | Description |
|--------|-------------|
| `reader.read().await` | Next message as `Option<Bytes>` — `None` when closed |
| `reader.into_stream()` | Consume as a `Stream<Item = Bytes>` |
| `writer.write(&bytes).await` | Send bytes (awaits `LocalAck` from orchestrator) |
| `writer.write_str(s).await` | Send a UTF-8 string |
| `writer.close()` | Signal no more messages |

> **Async runtimes:** each `cdylib` links its own copy of `tokio`, which does
> **not** share the reactor/timer driver the runner installs via
> `#[tokio::main]`. Only `Reader`/`Writer` calls cross back into
> runner-compiled code (where a live reactor exists). Don't rely on an
> ambient Tokio runtime inside processor code (`tokio::time::sleep`,
> `tokio::spawn`) unless you start your own.

---

## Registering a processor in a pipeline

There's no registry to update and no `rdfc:file` property to set — using a
processor in a pipeline is just `owl:import`ing its `processor.ttl` (from a
local path or an `http(s)://` URL). That file declares three things on the
processor's class, together:

1. Its argument SHACL shape (as usual — `sh:targetClass` + `sh:property`).
2. `:abiStable "rdfc-processor-sdk-<version>+abi_stable-<version>+async-ffi-<version>"`
   — checked against the runner's own build before loading anything.
3. `:artifact [ :targetTriple "..."; :path <...>; :sha256 "..." ], [ ... ]`
   — one entry per published target triple. `:sha256` can be omitted (e.g.
   for local development builds you rebuild often) to skip verification.

The **runner never reads any of this Turtle itself.** A shared SHACL shape,
declared once in `runner/index.ttl` and targeting every subject of
`:rustImplementationOf`, tells the orchestrator to extract `:abiStable` and
`:artifact` straight into the process config it hands the runner (as
`abi_token` / `artifacts` JSON fields) — the same mechanism that already
turns `sh:property` into `arguments` for `rdfc:reader`/`rdfc:writer`/etc.
`:path` is declared as an IRI (so the orchestrator resolves relative
references against wherever the file was fetched from) but typed
`xsd:string` in the shape, so the runner receives an already-absolute path
or URL as a plain string.

### You don't hand-write `:abiStable` / `:artifact`

Hand-maintaining a real path and checksum in `processor.ttl` would go stale
on every rebuild, so don't — `rdfc-processor-buildtools` generates them:

```rust
// build.rs
fn main() {
    rdfc_processor_buildtools::generate(&["processor.ttl"]).unwrap();
}
```

Your checked-in `processor.ttl` just does `<> owl:imports <generated.ttl>.`
once (see `processors/echo/processor.ttl`); `generated.ttl` itself is
gitignored and rewritten by every `cargo build`, always pointing at *this*
build's own compiled `cdylib` (no `:sha256` — the artifact doesn't exist yet
when `build.rs` runs, so local dev just trusts it). A real, checksummed,
multi-target-triple release is a separate, genuinely post-build concern —
see `kolibrie-processor/.github/workflows/build-processor.yml` and
`rdfc-processor-buildtools`'s own CLI (`checksum`, `snippet`, `merge`,
`flatten`) for how CI builds one.

### One `cdylib` can export several processors

Call [`rdfc_register!`] as many times as you like in one crate — once per
processor — followed by exactly one `rdfc_export_processors!()` (a `cdylib`
can only have one root-module export; see its docs for why). If several of
those classes are declared in the same source file with one
`owl:imports`, `generate()` writes all their data into that one target.

Example pipeline TTL:

```turtle
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.

<> owl:imports <../../runner/index.ttl>.        # rdfc:RustRunner + the shared shape
<> owl:imports </path/to/my_proc/processor.ttl>. # or an http(s):// URL to one

rdfc:RustRunner2 a rdfc:Runner;
  rdfc:handlesSubjectsOf <https://w3id.org/rdf-connect/rust#rustImplementationOf>;
  rdfc:command "rust-runner".

<> a rdfc:Pipeline;
  rdfc:consistsOf [
    rdfc:instantiates rdfc:RustRunner2;
    rdfc:processor <my-proc>;
  ].

<ch> a rdfc:Writer, rdfc:Reader.
<my-proc> a rdfc:MyProcessor; rdfc:input <ch>; rdfc:output <ch>.
```

See `processors/echo/processor.ttl` for the simplest possible worked example
(single local target, real checksum), and `kolibrie-processor/processor.ttl`
for a multi-target-triple release manifest, plus
`kolibrie-processor/.github/workflows/build-processor.yml` for how such a
release is actually built and published.

---

## Testing a processor

The SDK ships a `testing` module with mock channels that call your processor
function directly — no `cdylib`/FFI boundary involved, so plain `cargo test`
works without cross-compiling anything:

```rust
#[cfg(test)]
mod tests {
    use rdfc_processor_sdk::testing::*;
    use rdfc_processor_sdk::make_writer;
    use serde_json::json;

    #[tokio::test]
    async fn it_echoes() {
        let reader = make_test_reader("mock://in", vec![b"hello".to_vec()]);
        let (writer, bridge) = make_writer("mock://out".to_string());
        let args = make_test_args(
            json!({
                "input":  {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://in"},
                "output": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://out"}
            }),
            [("input", reader)],
            [("output", writer)],
        );

        let (result, (received, _closed)) = tokio::join!(super::my_processor(args), collect_writer(bridge));
        result.unwrap();
        assert_eq!(received, vec![b"hello".to_vec()]);
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
- **`abi_stable` / `async-ffi` versions are pinned exactly** (`Cargo.toml`'s `[workspace.dependencies]`) across the runner and every processor. A processor built against a different pin either fails `rdfc_processor_sdk::ABI_TOKEN`'s pre-flight check (clear error, nothing downloaded/loaded) or `abi_stable`'s own type-layout check at load time (also a clear error, not a crash) — never a silent ABI mismatch.
- **No unloading**: once a processor `cdylib` is loaded, it stays mapped and cached for the life of the process (this is inherent to `abi_stable`'s loading model, which assumes libraries are never unloaded).
