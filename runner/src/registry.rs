//! Processor registry — maps type URIs to factories.
//!
//! Built-in processors are registered with `builtin://` file URIs.
//! External processors are loaded from compiled shared libraries (`.so` / `.dylib`).
//! If `config.file` points to a directory containing a `Cargo.toml`, the crate is
//! compiled automatically before loading.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use tracing::info;

use crate::processor::{Processor, ProcessorContext};

// Compile-time paths to the builtin processor crates, relative to this crate's manifest.
const BUILTIN_PROCESSOR_DIRS: &[(&str, &str)] = &[
    ("echo", concat!(env!("CARGO_MANIFEST_DIR"), "/../processors/echo")),
    ("log",  concat!(env!("CARGO_MANIFEST_DIR"), "/../processors/log")),
    ("send", concat!(env!("CARGO_MANIFEST_DIR"), "/../processors/send")),
];

// ── Config extracted from proc.config ─────────────────────────────────────────

/// Runner-specific configuration for one processor definition.
/// Extracted from `proc.config` (JSON-LD → plain JSON).
#[derive(Debug, Deserialize)]
pub struct ProcessorConfig {
    /// `"builtin://<name>"` for built-in processors,  
    /// or a filesystem path to a `.so` / Cargo project directory.
    pub file: String,
    /// Optional type identifier (only meaningful for multi-processor libraries).
    #[serde(default)]
    pub r#type: String,
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Loads external `.so` processors on demand and auto-compiles Cargo projects.
pub struct ProcessorRegistry {
    /// Cache of already-loaded shared libraries (path → library handle).
    loaded_libs: HashMap<PathBuf, libloading::Library>,
}

impl ProcessorRegistry {
    pub fn new() -> Self {
        ProcessorRegistry {
            loaded_libs: HashMap::new(),
        }
    }

    /// Create a processor instance from the runner config JSON and a prepared context.
    pub async fn create(
        &mut self,
        config: &ProcessorConfig,
        ctx: ProcessorContext,
    ) -> Result<Box<dyn Processor>> {
        if let Some(name) = config.file.strip_prefix("builtin://") {
            let dir = BUILTIN_PROCESSOR_DIRS
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, dir)| *dir)
                .ok_or_else(|| anyhow::anyhow!("unknown built-in processor '{name}'"))?;
            let so_path = self.resolve_lib_async(dir).await?;
            return self.load_dynamic(so_path, ctx);
        }

        // External shared library or Cargo project directory
        let so_path = self.resolve_lib_async(&config.file).await?;
        self.load_dynamic(so_path, ctx)
    }

    // ── private helpers ───────────────────────────────────────────────────────

    /// Resolve the `.so` path, compiling a Cargo project if necessary.
    /// Compilation is offloaded to a blocking thread so the async executor is not stalled.
    async fn resolve_lib_async(&self, file: &str) -> Result<PathBuf> {
        let path = PathBuf::from(file);

        // Case 1: already a compiled shared library
        if path.is_file() {
            return Ok(path);
        }

        // Case 2: a directory (or Cargo.toml) → auto-compile
        let manifest = if path.is_dir() {
            path.join("Cargo.toml")
        } else if path.file_name().map_or(false, |n| n == "Cargo.toml") {
            path.clone()
        } else {
            bail!("processor file '{}' not found", file);
        };

        if !manifest.exists() {
            bail!("no Cargo.toml found at '{}'", manifest.display());
        }

        // Return cached .so immediately if it already exists from a previous run.
        let crate_dir = manifest.parent().unwrap_or(Path::new(".")).to_path_buf();
        if let Some(so) = find_shared_lib(&crate_dir.join("target").join("release")) {
            info!("using cached shared library: {}", so.display());
            return Ok(so);
        }

        // Compile in a blocking thread — cargo can take seconds and must not
        // block the async executor.
        tokio::task::spawn_blocking(move || compile_crate(&manifest))
            .await
            .context("compile thread panicked")?
    }

    /// Load the `rdfc_run` symbol from a shared library and wrap it as a `Processor`.
    fn load_dynamic(
        &mut self,
        so_path: PathBuf,
        ctx: ProcessorContext,
    ) -> Result<Box<dyn Processor>> {
        // Load the shared library, keeping the handle alive in `loaded_libs`
        // so the code stays mapped while the processor runs.
        let lib = unsafe { libloading::Library::new(&so_path) }
            .with_context(|| format!("loading '{}'", so_path.display()))?;

        // Locate the `rdfc_run` entry point
        type RunFn = unsafe extern "C" fn(*const rdfc_processor_sdk::RdfcArgs) -> i32;
        let run_fn: libloading::Symbol<RunFn> = unsafe { lib.get(b"rdfc_run\0") }
            .with_context(|| {
                format!("symbol 'rdfc_run' not found in '{}'", so_path.display())
            })?;
        // Extend lifetime — the Library is kept alive in `loaded_libs`.
        let run_fn: RunFn = unsafe { std::mem::transmute(run_fn.into_raw()) };

        self.loaded_libs.entry(so_path).or_insert(lib);

        Ok(Box::new(DynamicProcessor::new(run_fn, ctx)))
    }
}

// ── Auto-compile helper ───────────────────────────────────────────────────────

/// Find the first `.so` / `.dylib` / `.dll` in `dir`.
fn find_shared_lib(dir: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if (name.starts_with("lib") && name.ends_with(".so"))
            || name.ends_with(".dylib")
            || (name.ends_with(".dll") && !name.ends_with(".dll.exp"))
        {
            return Some(entry.path());
        }
    }
    None
}

/// Run `cargo build --release` on the crate at `manifest` and return the path
/// to the resulting shared library.
fn compile_crate(manifest: &Path) -> Result<PathBuf> {
    info!("compiling processor crate at '{}'", manifest.display());

    let crate_dir = manifest.parent().unwrap_or(Path::new("."));
    // Use the crate's own target dir so the .so is always in a known place,
    // even when the crate lives inside a Cargo workspace that redirects output.
    let target_dir = crate_dir.join("target");

    let status = std::process::Command::new("cargo")
        .args(["build", "--release", "--manifest-path"])
        .arg(manifest)
        .arg("--target-dir")
        .arg(&target_dir)
        .status()
        .context("cargo build failed to start — is cargo installed?")?;

    if !status.success() {
        bail!("cargo build failed for '{}'", manifest.display());
    }

    let release_dir = target_dir.join("release");

    find_shared_lib(&release_dir).ok_or_else(|| {
        anyhow::anyhow!("no shared library found in '{}' after build", release_dir.display())
    })
}

// ── DynamicProcessor ─────────────────────────────────────────────────────────

use rdfc_processor_sdk::{RdfcArgs, RdfcNamedChannel};

type RunFn = unsafe extern "C" fn(*const RdfcArgs) -> i32;

/// Wraps a dynamically-loaded `rdfc_run` function as an async `Processor`.
struct DynamicProcessor {
    run_fn: RunFn,
    ctx: Option<ProcessorContext>, // taken in produce()
}

impl DynamicProcessor {
    fn new(run_fn: RunFn, ctx: ProcessorContext) -> Self {
        DynamicProcessor {
            run_fn,
            ctx: Some(ctx),
        }
    }
}

#[async_trait::async_trait]
impl Processor for DynamicProcessor {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn transform(&mut self) -> Result<()> {
        Ok(())
    }

    /// Run the processor's `rdfc_run` in a dedicated OS thread.
    /// The function calls blocking SDK read/write operations; running it on a
    /// tokio thread pool thread would exhaust the pool, so we use `spawn_blocking`.
    async fn produce(&mut self) -> Result<()> {
        let ctx = self
            .ctx
            .take()
            .ok_or_else(|| anyhow::anyhow!("DynamicProcessor.produce() called twice"))?;

        let run_fn = self.run_fn;

        tokio::task::spawn_blocking(move || run_dynamic(run_fn, ctx))
            .await
            .context("dynamic processor thread panicked")??;

        Ok(())
    }
}

/// Build the `RdfcArgs` struct, call `rdfc_run`, and clean up.
fn run_dynamic(run_fn: RunFn, ctx: ProcessorContext) -> Result<()> {
    use crate::channel::{drop_ffi_reader_channel, drop_ffi_writer_channel};

    let args_json = serde_json::to_vec(&ctx.args).unwrap_or_default();

    let (reader_channels, writer_channels) = ctx.into_ffi_channels();

    // Build RdfcNamedChannel slices, leaking name strings for the FFI boundary.
    let ffi_readers: Vec<RdfcNamedChannel> = reader_channels
        .into_iter()
        .map(|(name, ch)| {
            let name_bytes = name.into_bytes().into_boxed_slice();
            let name_ptr = name_bytes.as_ptr();
            let name_len = name_bytes.len();
            std::mem::forget(name_bytes);
            RdfcNamedChannel { name: name_ptr, name_len, channel: ch }
        })
        .collect();

    let ffi_writers: Vec<RdfcNamedChannel> = writer_channels
        .into_iter()
        .map(|(name, ch)| {
            let name_bytes = name.into_bytes().into_boxed_slice();
            let name_ptr = name_bytes.as_ptr();
            let name_len = name_bytes.len();
            std::mem::forget(name_bytes);
            RdfcNamedChannel { name: name_ptr, name_len, channel: ch }
        })
        .collect();

    let ffi_args = RdfcArgs {
        args_json: args_json.as_ptr(),
        args_json_len: args_json.len(),
        readers: ffi_readers.as_ptr(),
        n_readers: ffi_readers.len(),
        writers: ffi_writers.as_ptr(),
        n_writers: ffi_writers.len(),
    };

    let rc = unsafe { run_fn(&ffi_args) };

    // Free all leaked FFI state so `orch_tx` clones are dropped and the
    // gRPC outbound stream can see EOF once all processors finish.
    for nc in ffi_readers {
        let name = unsafe { Vec::from_raw_parts(nc.name as *mut u8, nc.name_len, nc.name_len) };
        drop(name);
        unsafe { drop_ffi_reader_channel(nc.channel) };
    }
    for nc in ffi_writers {
        let name = unsafe { Vec::from_raw_parts(nc.name as *mut u8, nc.name_len, nc.name_len) };
        drop(name);
        unsafe { drop_ffi_writer_channel(nc.channel) };
    }

    if rc == 0 { Ok(()) } else { bail!("rdfc_run returned error code {rc}") }
}
